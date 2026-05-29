"""
Worker service for the Telephone game.

Consumes distortion jobs from RabbitMQ, applies noise, saves to DB,
and publishes results back to RabbitMQ for the LLM worker.
"""

import json
import logging
import time
import sys
import os
import pika
from datetime import datetime

from database import SessionLocal, DistortedPhrase, init_db
from config import settings, RabbitMQConfig, MessageFormats
from noise import add_noise

# Configure logging
logging.basicConfig(
    level=settings.log_level,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Worker ID - from environment variable or command line argument
WORKER_ID = int(os.getenv('WORKER_ID', sys.argv[1] if len(sys.argv) > 1 else '1'))


def get_rabbitmq_connection():
    """Create RabbitMQ connection with retries."""
    max_retries = 5
    retry_delay = 2
    
    for attempt in range(max_retries):
        try:
            # Parse RABBITMQ_URL: amqp://user:pass@host:port//
            from urllib.parse import urlparse
            parsed = urlparse(settings.rabbitmq_url)
            
            credentials = pika.PlainCredentials(
                parsed.username or 'guest', 
                parsed.password or 'guest'
            )
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(
                    host=parsed.hostname or 'rabbitmq',
                    port=parsed.port or 5672,
                    credentials=credentials,
                    connection_attempts=5,
                    retry_delay=2
                )
            )
            logger.info("Connected to RabbitMQ")
            return connection
        except Exception as e:
            if attempt < max_retries - 1:
                logger.warning(f"RabbitMQ connection attempt {attempt + 1} failed: {e}. Retrying in {retry_delay}s...")
                time.sleep(retry_delay)
            else:
                logger.error(f"Failed to connect to RabbitMQ after {max_retries} attempts")
                raise


def setup_worker_queue():
    """Setup shared job queue and results queue."""
    try:
        connection = get_rabbitmq_connection()
        channel = connection.channel()
        
        # Declare exchange
        channel.exchange_declare(
            exchange=RabbitMQConfig.EXCHANGE_NAME,
            exchange_type=RabbitMQConfig.EXCHANGE_TYPE,
            durable=True
        )
        
        # Declare SHARED jobs queue (all workers consume from here)
        channel.queue_declare(
            queue=RabbitMQConfig.JOBS_QUEUE,
            durable=True
        )
        channel.queue_bind(
            exchange=RabbitMQConfig.EXCHANGE_NAME,
            queue=RabbitMQConfig.JOBS_QUEUE,
            routing_key=RabbitMQConfig.ROUTING_KEY_JOBS
        )
        
        # Declare results queue (where workers publish)
        channel.queue_declare(
            queue=RabbitMQConfig.RESULTS_QUEUE,
            durable=True
        )
        channel.queue_bind(
            exchange=RabbitMQConfig.EXCHANGE_NAME,
            queue=RabbitMQConfig.RESULTS_QUEUE,
            routing_key=RabbitMQConfig.ROUTING_KEY_RESULTS
        )
        
        logger.info(f"Worker {WORKER_ID} queue setup completed")
        connection.close()
    except Exception as e:
        logger.error(f"Failed to setup worker queue: {e}")
        raise


def process_message(message: dict) -> dict:
    """
    Process a job message: distort phrase and create result message.
    
    Args:
        message: {job_id, phrase, worker_id}
    
    Returns:
        result: {job_id, worker_id, distorted_phrase}
    """
    job_id = message['job_id']
    phrase = message['phrase']
    worker_id = message['worker_id']
    
    try:
        # Apply distortion
        distorted_phrase = add_noise(phrase, prob=settings.distortion_probability)
        
        logger.info(f"Job {job_id}: Worker {worker_id} distorted phrase")
        logger.debug(f"  Original: {phrase}")
        logger.debug(f"  Distorted: {distorted_phrase}")
        
        # Save to database
        db = SessionLocal()
        try:
            distorted = DistortedPhrase(
                job_id=job_id,
                worker_id=worker_id,
                distorted_phrase=distorted_phrase
            )
            db.add(distorted)
            db.commit()
            logger.info(f"Job {job_id}: Saved distorted phrase to DB from worker {worker_id}")
        except Exception as e:
            logger.error(f"Failed to save to DB: {e}")
            db.rollback()
            raise
        finally:
            db.close()
        
        # Create result message
        result = MessageFormats.result_message(job_id, worker_id, distorted_phrase)
        return result
    
    except Exception as e:
        logger.error(f"Error processing message for job {job_id}: {e}")
        raise


def publish_result(result: dict):
    """
    Publish result to RabbitMQ results queue.
    
    Args:
        result: {job_id, worker_id, distorted_phrase}
    """
    try:
        connection = get_rabbitmq_connection()
        channel = connection.channel()
        
        # Publish result
        channel.basic_publish(
            exchange=RabbitMQConfig.EXCHANGE_NAME,
            routing_key=RabbitMQConfig.ROUTING_KEY_RESULTS,
            body=json.dumps(result),
            properties=pika.BasicProperties(delivery_mode=2)  # Persistent
        )
        logger.info(f"Published result for job {result['job_id']} from worker {result['worker_id']}")
        connection.close()
    except Exception as e:
        logger.error(f"Failed to publish result: {e}")
        raise


def consume_jobs():
    """
    Main worker loop: consume job messages, process, and publish results.
    """
    logger.info(f"Worker {WORKER_ID} starting...")
    
    try:
        # Initialize database
        init_db()
        logger.info("Database initialized")
        
        # Setup queue
        setup_worker_queue()
        
        # Connect and consume
        connection = get_rabbitmq_connection()
        channel = connection.channel()
        
        # Consume from SHARED jobs queue
        jobs_queue = RabbitMQConfig.JOBS_QUEUE
        
        # Set prefetch to 1 (process one message at a time)
        channel.basic_qos(prefetch_count=1)
        
        def callback(ch, method, properties, body):
            """Process a single message."""
            try:
                message = json.loads(body)
                logger.info(f"Received message for job {message['job_id']}")
                
                # Process the message
                result = process_message(message)
                
                # Publish result
                publish_result(result)
                
                # Acknowledge message
                ch.basic_ack(delivery_tag=method.delivery_tag)
                logger.info(f"Successfully processed job {message['job_id']}")
                
            except Exception as e:
                logger.error(f"Error in callback: {e}")
                # Reject and requeue
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
        
        # Start consuming from shared queue
        channel.basic_consume(
            queue=jobs_queue,
            on_message_callback=callback
        )
        
        logger.info(f"Worker {WORKER_ID} listening on shared queue {jobs_queue}")
        channel.start_consuming()
    
    except KeyboardInterrupt:
        logger.info(f"Worker {WORKER_ID} shutting down...")
    except Exception as e:
        logger.error(f"Worker {WORKER_ID} error: {e}")
        raise


if __name__ == "__main__":
    try:
        consume_jobs()
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)
