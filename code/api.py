import json
import uuid
from fastapi import FastAPI, HTTPException, Depends, BackgroundTasks
from fastapi.responses import StreamingResponse
from sqlalchemy.orm import Session
from datetime import datetime
import pika
import asyncio
import logging
from typing import List, Dict, Optional

from database import Job, DistortedPhrase, Guess, get_db, init_db
from config import settings, RabbitMQConfig, JobStatus, MessageFormats, Limits, ResponseMessages
from noise import add_noise

# Configure logging
logging.basicConfig(level=settings.log_level)
logger = logging.getLogger(__name__)

# Initialize FastAPI app
app = FastAPI(title="Telephone API", version="1.0.0")

# Store SSE subscriptions: {job_id: [queue1, queue2, ...]}
sse_subscriptions: Dict[str, List[asyncio.Queue]] = {}


# ============ Database Initialization ============
@app.on_event("startup")
async def startup():
    """Initialize database and RabbitMQ on startup."""
    init_db()
    logger.info("Database initialized")
    
    try:
        setup_rabbitmq()
        logger.info("RabbitMQ setup completed")
    except Exception as e:
        logger.warning(f"RabbitMQ setup failed during startup (will retry on first publish): {e}")
    
    logger.info("API startup complete")


# ============ Helper Functions ============
def get_rabbitmq_connection():
    """Create RabbitMQ connection using environment credentials."""
    try:
        # Parse RABBITMQ_URL: amqp://user:pass@host:port//
        url = settings.rabbitmq_url
        from urllib.parse import urlparse
        parsed = urlparse(url)
        
        credentials = pika.PlainCredentials(parsed.username or 'guest', parsed.password or 'guest')
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host=parsed.hostname or 'rabbitmq',
                port=parsed.port or 5672,
                credentials=credentials,
                connection_attempts=5,
                retry_delay=2
            )
        )
        return connection
    except Exception as e:
        logger.error(f"Failed to connect to RabbitMQ: {e}")
        raise


def setup_rabbitmq():
    """Setup RabbitMQ exchange and classic durable queues."""
    try:
        connection = get_rabbitmq_connection()
        channel = connection.channel()
        
        # Declare exchange
        channel.exchange_declare(
            exchange=RabbitMQConfig.EXCHANGE_NAME,
            exchange_type=RabbitMQConfig.EXCHANGE_TYPE,
            durable=True
        )
        
        # Declare jobs queue
        channel.queue_declare(
            queue=RabbitMQConfig.JOBS_QUEUE,
            durable=True,
        )
        channel.queue_bind(
            exchange=RabbitMQConfig.EXCHANGE_NAME,
            queue=RabbitMQConfig.JOBS_QUEUE,
            routing_key=RabbitMQConfig.ROUTING_KEY_JOBS
        )
        
        # Declare results queue
        channel.queue_declare(
            queue=RabbitMQConfig.RESULTS_QUEUE,
            durable=True,
        )
        channel.queue_bind(
            exchange=RabbitMQConfig.EXCHANGE_NAME,
            queue=RabbitMQConfig.RESULTS_QUEUE,
            routing_key=RabbitMQConfig.ROUTING_KEY_RESULTS
        )
        
        logger.info("RabbitMQ setup completed")
        connection.close()
    except Exception as e:
        logger.error(f"RabbitMQ setup failed: {e}")


def publish_job_to_workers(job_id: str, phrase: str, num_workers: int):
    """
    Publica N mensajes individuales a la cola de jobs.
    Cualquier worker libre irá tomando las tareas secuencialmente.
    """
    try:
        connection = get_rabbitmq_connection()
        channel = connection.channel()
        
        # Enviamos N copias individuales a la cola
        for replica_id in range(1, num_workers + 1):
            message = {
                "job_id": job_id,
                "phrase": phrase,
                "num_workers": num_workers,
                "replica_id": replica_id
            }
            
            channel.basic_publish(
                exchange=RabbitMQConfig.EXCHANGE_NAME,
                routing_key=RabbitMQConfig.ROUTING_KEY_JOBS,
                body=json.dumps(message),
                properties=pika.BasicProperties(delivery_mode=2),
            )
            
        logger.info(f"Published {num_workers} tasks for job {job_id} to queue")
        connection.close()
    except Exception as e:
        logger.error(f"Failed to publish job to workers: {e}")
        raise


def broadcast_sse_event(job_id: str, event_type: str, data: dict):
    """
    Broadcast event to all SSE subscribers of a job.
    """
    if job_id not in sse_subscriptions:
        return
    
    event_message = json.dumps({
        "type": event_type,
        "timestamp": datetime.utcnow().isoformat(),
        "data": data
    })
    
    # Send to all subscribed clients
    for queue in sse_subscriptions.get(job_id, []):
        try:
            queue.put_nowait(event_message)
        except asyncio.QueueFull:
            logger.warning(f"SSE queue full for job {job_id}")


def get_effective_job_progress(db: Session, job_id: str) -> tuple[Job | None, int, int, int]:
    """Return the job plus normalized and raw distortion counts for display."""
    job = db.query(Job).filter(Job.id == job_id).first()
    if not job:
        return None, 0, 0, 0

    raw_completed = db.query(DistortedPhrase).filter(
        DistortedPhrase.job_id == job_id
    ).count()

    observed_worker_fanout = db.query(DistortedPhrase.worker_id).filter(
        DistortedPhrase.job_id == job_id
    ).distinct().count()
    observed_worker_fanout = max(observed_worker_fanout, 1)

    completed_copies = raw_completed // observed_worker_fanout
    return job, raw_completed, completed_copies, observed_worker_fanout


# ============ API Endpoints ============

@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "ok", "service": "telephone-api"}


@app.post("/send")
async def send_phrase(
    phrase: str,
    num_workers: int,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    """
    POST /send
    Send a phrase to be distorted by N workers.
    
    Args:
        phrase: The phrase to distort
        num_workers: Number of workers to process it
    
    Returns:
        Job ID and status details
    """
    # Validate inputs
    if not phrase or len(phrase.strip()) == 0:
        raise HTTPException(status_code=400, detail=ResponseMessages.INVALID_PHRASE)
    
    if len(phrase) > Limits.MAX_PHRASE_LENGTH:
        raise HTTPException(status_code=400, detail=f"Phrase too long (max {Limits.MAX_PHRASE_LENGTH} chars)")
    
    if not (Limits.MIN_WORKERS <= num_workers <= Limits.MAX_WORKERS):
        raise HTTPException(
            status_code=400,
            detail=f"Workers must be between {Limits.MIN_WORKERS} and {Limits.MAX_WORKERS}"
        )
    
    # Create job
    job_id = str(uuid.uuid4())
    job = Job(
        id=job_id,
        phrase=phrase,
        num_workers=num_workers,
        status=JobStatus.PROCESSING,
        created_at=datetime.utcnow()
    )
    db.add(job)
    db.commit()
    
    # Publish to RabbitMQ
    try:
        publish_job_to_workers(job_id, phrase, num_workers)
    except Exception as e:
        logger.error(f"Failed to publish job {job_id}: {e}")
        job.status = JobStatus.FAILED
        db.commit()
        raise HTTPException(status_code=500, detail=ResponseMessages.INTERNAL_ERROR)
    
    return {
        "job_id": job_id,
        "phrase": phrase,
        "num_workers": num_workers,
        "status": JobStatus.PROCESSING
    }


@app.get("/job/{job_id}")
async def get_job(job_id: str, db: Session = Depends(get_db)):
    job, raw_completed, completed_copies, observed_worker_fanout = get_effective_job_progress(db, job_id)
    if not job:
        raise HTTPException(status_code=404, detail=ResponseMessages.JOB_NOT_FOUND)

    total_esperado = job.num_workers

    progress_percentage = (completed_copies / total_esperado * 100) if total_esperado > 0 else 0

    # Cierre automático basado en la realidad física de la DB
    if total_esperado > 0 and completed_copies >= total_esperado:
        progress_percentage = 100.0
        if job.status != JobStatus.COMPLETED:
            job.status = JobStatus.COMPLETED
            db.commit()

    return {
        "job_id": job_id,
        "phrase": job.phrase,
        "num_workers": total_esperado,
        "completed_workers": completed_copies,
        "raw_completed_workers": raw_completed,
        "observed_worker_fanout": observed_worker_fanout,
        "progress_percentage": min(round(progress_percentage, 2), 100.0),
        "status": job.status,
        "created_at": job.created_at.isoformat()
    }

@app.get("/job/{job_id}/stream")
async def stream_job_events(job_id: str, db: Session = Depends(get_db)):
    """
    GET /job/{id}/stream
    Stream job events via Server-Sent Events (SSE).
    Emits events when distortions are received and guesses are made.
    
    Args:
        job_id: The job ID
    
    Returns:
        SSE stream of events
    """
    # Verify job exists
    job = db.query(Job).filter(Job.id == job_id).first()
    if not job:
        raise HTTPException(status_code=404, detail=ResponseMessages.JOB_NOT_FOUND)
    
    # Create queue for this client
    queue = asyncio.Queue()
    if job_id not in sse_subscriptions:
        sse_subscriptions[job_id] = []
    sse_subscriptions[job_id].append(queue)
    
    async def event_generator():
        try:
            # Send initial event
            yield f"data: {json.dumps({'type': 'connected', 'job_id': job_id})}\n\n"
            
            # Stream events
            while True:
                try:
                    event = await asyncio.wait_for(queue.get(), timeout=Limits.SSE_TIMEOUT)
                    yield f"data: {event}\n\n"
                except asyncio.TimeoutError:
                    # Send ping to keep connection alive
                    yield f": ping\n\n"
        except Exception as e:
            logger.error(f"SSE error for job {job_id}: {e}")
        finally:
            # Clean up
            if job_id in sse_subscriptions and queue in sse_subscriptions[job_id]:
                sse_subscriptions[job_id].remove(queue)
    
    return StreamingResponse(event_generator(), media_type="text/event-stream")


@app.get("/job/{job_id}/distortions")
async def get_job_distortions(job_id: str, db: Session = Depends(get_db)):
    job = db.query(Job).filter(Job.id == job_id).first()
    if not job:
        raise HTTPException(status_code=404, detail=ResponseMessages.JOB_NOT_FOUND)
    
    distortions = db.query(DistortedPhrase).filter(
        DistortedPhrase.job_id == job_id
    ).order_by(DistortedPhrase.worker_id.asc()).all()
    
    return {
        "job_id": job_id,
        "phrase": job.phrase,
        "total_distortions": len(distortions),
        "distortions": [
            {
                "worker_id": d.worker_id,
                "distorted_phrase": d.distorted_phrase
            }
            for d in distortions
        ]
    }


@app.get("/job/{job_id}/stream-guesses")
async def stream_guesses(job_id: str, db: Session = Depends(get_db)):
    """
    GET /job/{id}/stream-guesses
    Stream guesses via Server-Sent Events (SSE).
    Polls database for new guesses every 2 seconds.
    
    Args:
        job_id: The job ID
    
    Returns:
        SSE stream of guess events
    """
    # Verify job exists
    job = db.query(Job).filter(Job.id == job_id).first()
    if not job:
        raise HTTPException(status_code=404, detail=ResponseMessages.JOB_NOT_FOUND)
    
    last_guess_id = 0  # Track last emitted guess
    
    async def event_generator():
        nonlocal last_guess_id
        try:
            # Send initial event
            yield f"data: {json.dumps({'type': 'connected', 'job_id': job_id})}\n\n"
            
            # Poll for new guesses
            timeout_count = 0
            while timeout_count < Limits.SSE_TIMEOUT / 2:  # 2-second polling, 5-min total timeout
                try:
                    # Query new guesses since last emission
                    new_guesses = db.query(Guess).filter(
                        Guess.job_id == job_id,
                        Guess.id > last_guess_id
                    ).order_by(Guess.id).all()
                    
                    if new_guesses:
                        timeout_count = 0  # Reset timeout counter on activity
                        for guess in new_guesses:
                            event_data = json.dumps({
                                "type": "guess_received",
                                "batch_num": guess.batch_num,
                                "guess": guess.guess,
                                "ollama_response": guess.ollama_response
                            })
                            yield f"data: {event_data}\n\n"
                            last_guess_id = guess.id
                    else:
                        # Send ping to keep connection alive
                        yield f": ping\n\n"
                        timeout_count += 1
                    
                    # Poll every 2 seconds
                    await asyncio.sleep(2)
                except Exception as e:
                    logger.error(f"Error in stream-guesses: {e}")
                    yield f"data: {json.dumps({'type': 'error', 'message': str(e)})}\n\n"
                    timeout_count += 1
        except Exception as e:
            logger.error(f"SSE error for job {job_id}: {e}")
    
    return StreamingResponse(event_generator(), media_type="text/event-stream")


@app.get("/job/{job_id}/guesses")
async def get_guesses(job_id: str, db: Session = Depends(get_db)):
    """
    GET /job/{id}/guesses
    Get all guesses made by the LLM worker for this job.
    
    Args:
        job_id: The job ID
    
    Returns:
        List of guesses ordered by batch number
    """
    job = db.query(Job).filter(Job.id == job_id).first()
    if not job:
        raise HTTPException(status_code=404, detail=ResponseMessages.JOB_NOT_FOUND)
    
    guesses = db.query(Guess).filter(Guess.job_id == job_id).order_by(Guess.batch_num).all()
    
    return {
        "job_id": job_id,
        "phrase": job.phrase,
        "guesses": [
            {
                "batch_num": g.batch_num,
                "guess": g.guess,
                "ollama_response": g.ollama_response
            }
            for g in guesses
        ],
        "total_guesses": len(guesses)
    }


@app.get("/jobs")
async def list_jobs(db: Session = Depends(get_db)):
    jobs = db.query(Job).order_by(Job.created_at.desc()).all()
    
    result = []
    for job in jobs:
        _, raw_completed, completed_copies, observed_worker_fanout = get_effective_job_progress(db, job.id)
        
        total_esperado = job.num_workers
        
        effective_status = (
            JobStatus.COMPLETED if completed_copies >= total_esperado and total_esperado > 0 else job.status
        )
        
        progress_percentage = (completed_copies / total_esperado * 100) if total_esperado > 0 else 0
        
        result.append({
            "job_id": job.id,
            "phrase": job.phrase[:100],
            "num_workers": total_esperado,
            "completed_workers": completed_copies,
            "raw_completed_workers": raw_completed,
            "observed_worker_fanout": observed_worker_fanout,
            "progress_percentage": min(round(progress_percentage, 2), 100.0),
            "status": effective_status,
            "created_at": job.created_at.isoformat()
        })
    
    return {
        "total_jobs": len(result),
        "jobs": result
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        app,
        host=settings.api_host,
        port=settings.api_port,
        log_level=settings.log_level.lower()
    )