"""
llm_worker.py
-------------
Servicio que consume frases distorsionadas de la cola RabbitMQ (`telephone.results`),
las agrupa en batches por job, y consulta a Ollama para reconstruir la frase original.

Persiste los resultados usando SQLAlchemy con el mismo modelo definido en models.py.

Flujo:
  RabbitMQ cola `telephone.results`
    └─> acumular por job_id
        └─> cuando batch_size >= BATCH_SIZE  ó  timeout >= BATCH_TIMEOUT_SEC
            └─> llamar Ollama /api/generate
                └─> guardar Guess en PostgreSQL
                    └─> marcar Job como 'completed' si ya llegaron todos los workers
"""

import os
import json
import time
import logging
import threading
from collections import defaultdict
from datetime import datetime

import pika
import requests
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

# ---------------------------------------------------------------------------
# Config desde env
# ---------------------------------------------------------------------------
RABBITMQ_URL        = os.getenv("RABBITMQ_URL",        "amqp://guest:guest@rabbitmq:5672/%2F")
DATABASE_URL        = os.getenv("DATABASE_URL",        "postgresql://user:devpassword123@postgres:5432/telephone_db")
OLLAMA_URL          = os.getenv("OLLAMA_URL",          "http://ollama:11434")
OLLAMA_MODEL        = os.getenv("OLLAMA_MODEL",        "qwen2.5:0.5b")
LOG_LEVEL           = os.getenv("LOG_LEVEL",           "INFO")
BATCH_SIZE          = int(os.getenv("LLM_BATCH_SIZE",    "3"))
BATCH_TIMEOUT_SEC   = float(os.getenv("LLM_BATCH_TIMEOUT", "10"))

QUEUE_NAME    = "telephone.results"
EXCHANGE_NAME = "telephone"

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL.upper(), logging.INFO),
    format="%(asctime)s [llm-worker] %(levelname)s %(message)s",
)
log = logging.getLogger("llm-worker")

# ---------------------------------------------------------------------------
# DB — importamos los modelos del proyecto
# ---------------------------------------------------------------------------
from database import Job, Guess, init_db

engine = create_engine(DATABASE_URL, pool_pre_ping=True)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# ---------------------------------------------------------------------------
# Buffer en memoria: acumula frases por job hasta armar un batch
# ---------------------------------------------------------------------------
# job_id -> {"phrases": [...], "total": int, "received": int, "last_ts": float, "batch_num": int}
job_buffers: dict = defaultdict(lambda: {
    "phrases":   [],
    "total":     0,
    "received":  0,
    "last_ts":   time.monotonic(),
    "batch_num": 0,
})
buffers_lock = threading.Lock()

# ---------------------------------------------------------------------------
# Ollama
# ---------------------------------------------------------------------------
def call_ollama(phrases: list[str], job_id: str, batch_num: int) -> tuple[str, str]:
    """
    Llama a Ollama con las frases distorsionadas del batch.
    Devuelve (guess_text, raw_response).
    """
    numbered = "\n".join(f"  {i+1}. {p}" for i, p in enumerate(phrases))
    prompt = (
        "Sos un experto en reconstrucción de texto. "
        "Las siguientes frases son versiones distorsionadas (con ruido, errores tipográficos "
        "y cambios de palabras) de UNA MISMA frase original en español.\n\n"
        f"Versiones distorsionadas:\n{numbered}\n\n"
        "¿Cuál crees que era la frase original? "
        "Responde ÚNICAMENTE con la frase reconstruida, sin explicaciones, "
        "sin comillas, sin prefijos."
    )

    try:
        resp = requests.post(
            f"{OLLAMA_URL}/api/generate",
            json={
                "model":  OLLAMA_MODEL,
                "prompt": prompt,
                "stream": False,
                "options": {"temperature": 0.2, "num_predict": 128},
            },
            timeout=60,
        )
        resp.raise_for_status()
        data        = resp.json()
        raw         = json.dumps(data, ensure_ascii=False)
        guess       = data.get("response", "").strip()
        log.info(f"[job={job_id[:8]}] Ollama batch={batch_num} → '{guess}'")
        return guess, raw

    except requests.exceptions.Timeout:
        log.error(f"[job={job_id[:8]}] Timeout llamando a Ollama")
        return "[timeout]", ""
    except Exception as e:
        log.error(f"[job={job_id[:8]}] Error Ollama: {e}")
        return "[error]", ""


# ---------------------------------------------------------------------------
# Procesamiento de un batch
# ---------------------------------------------------------------------------
def process_batch(job_id: str, phrases: list[str], batch_num: int, received: int, total: int):
    """Llama a Ollama, guarda el Guess y actualiza el Job si corresponde."""
    log.info(
        f"[job={job_id[:8]}] Procesando batch={batch_num} "
        f"| frases_en_batch={len(phrases)} | recibidas={received}/{total}"
    )

    guess_text, ollama_raw = call_ollama(phrases, job_id, batch_num)

    db = SessionLocal()
    try:
        # Guardar el guess
        guess = Guess(
            job_id=job_id,
            batch_num=batch_num,
            guess=guess_text,
            ollama_response=ollama_raw,
        )
        db.add(guess)

        # Si ya llegaron todos los workers, marcar job como completado
        if total > 0 and received >= total:
            job = db.query(Job).filter(Job.id == job_id).first()
            if job and job.status != "completed":
                job.status = "completed"
                log.info(f"[job={job_id[:8]}] Marcado como 'completed'.")

        db.commit()
        log.debug(f"[job={job_id[:8]}] Guess batch={batch_num} guardado en DB.")

    except Exception as e:
        db.rollback()
        log.error(f"[job={job_id[:8]}] Error guardando en DB: {e}")
    finally:
        db.close()


# ---------------------------------------------------------------------------
# RabbitMQ: callback por mensaje
# ---------------------------------------------------------------------------
def on_message(channel, method, properties, body):
    """
    Formato esperado del mensaje JSON:
    {
        "job_id":           "string-id-del-job",
        "worker_id":        1,
        "distorted_phrase": "frase con ruido",
        "num_workers":      5
    }
    """
    try:
        data = json.loads(body.decode())
    except Exception as e:
        log.warning(f"Mensaje no parseable: {e} | body={body[:200]}")
        channel.basic_ack(delivery_tag=method.delivery_tag)
        return

    job_id      = data.get("job_id", "").strip()
    distorted   = data.get("distorted_phrase", "").strip()
    num_workers = int(data.get("num_workers", 0))

    if not job_id or not distorted:
        log.warning(f"Mensaje incompleto ignorado: {data}")
        channel.basic_ack(delivery_tag=method.delivery_tag)
        return

    log.debug(f"[job={job_id[:8]}] Recibido worker_id={data.get('worker_id')} | '{distorted}'")

    # Variables para procesar fuera del lock
    flush_phrases    = None
    flush_batch_num  = None
    flush_received   = None
    flush_total      = None

    with buffers_lock:
        buf = job_buffers[job_id]

        if num_workers > 0:
            buf["total"] = num_workers

        buf["phrases"].append(distorted)
        buf["received"] += 1
        buf["last_ts"]   = time.monotonic()

        received_now = buf["received"]
        total_now    = buf["total"]

        # Hacer flush cuando:
        #   a) se llenó el batch configurado
        #   b) llegaron TODOS los workers del job (flush final obligatorio)
        should_flush = (
            len(buf["phrases"]) >= BATCH_SIZE
            or (total_now > 0 and received_now >= total_now)
        )

        if should_flush:
            flush_phrases    = buf["phrases"][:]
            buf["batch_num"] += 1
            flush_batch_num  = buf["batch_num"]
            flush_received   = received_now
            flush_total      = total_now
            buf["phrases"]   = []   # resetear sólo las frases; received/total siguen acumulando

    # ACK siempre antes del procesamiento pesado
    channel.basic_ack(delivery_tag=method.delivery_tag)

    if flush_phrases:
        t = threading.Thread(
            target=process_batch,
            args=(job_id, flush_phrases, flush_batch_num, flush_received, flush_total),
            daemon=True,
        )
        t.start()


# ---------------------------------------------------------------------------
# Hilo de timeout: flush forzado si un batch lleva demasiado tiempo abierto
# ---------------------------------------------------------------------------
def timeout_flusher():
    """
    Revisa periódicamente todos los buffers.
    Si el último mensaje de un job llegó hace más de BATCH_TIMEOUT_SEC
    y quedan frases sin procesar, hace flush igual.
    """
    while True:
        time.sleep(max(1.0, BATCH_TIMEOUT_SEC / 2))
        now = time.monotonic()

        to_flush = []
        with buffers_lock:
            for job_id, buf in list(job_buffers.items()):
                if not buf["phrases"]:
                    continue
                if (now - buf["last_ts"]) >= BATCH_TIMEOUT_SEC:
                    buf["batch_num"] += 1
                    to_flush.append((
                        job_id,
                        buf["phrases"][:],
                        buf["batch_num"],
                        buf["received"],
                        buf["total"],
                    ))
                    buf["phrases"] = []

        for args in to_flush:
            job_id = args[0]
            log.info(f"[job={job_id[:8]}] Timeout flush: {len(args[1])} frases")
            threading.Thread(target=process_batch, args=args, daemon=True).start()


# ---------------------------------------------------------------------------
# Esperas con retry
# ---------------------------------------------------------------------------
def wait_for_ollama(max_retries=20, delay=5):
    log.info(f"Esperando Ollama en {OLLAMA_URL} ...")
    for i in range(max_retries):
        try:
            r = requests.get(f"{OLLAMA_URL}/api/tags", timeout=5)
            if r.status_code == 200:
                log.info("Ollama disponible.")
                return
        except Exception:
            pass
        log.info(f"Ollama no disponible — reintento {i+1}/{max_retries} en {delay}s ...")
        time.sleep(delay)
    log.warning("Ollama no respondió al arrancar, continuando de todos modos ...")


def wait_for_postgres(max_retries=20, delay=5):
    log.info("Esperando PostgreSQL ...")
    for i in range(max_retries):
        db = None
        try:
            db = SessionLocal()
            db.execute(text("SELECT 1"))
            log.info("PostgreSQL disponible.")
            return
        except Exception as e:
            log.info(f"PostgreSQL no disponible ({e}) — reintento {i+1}/{max_retries} en {delay}s ...")
            time.sleep(delay)
        finally:
            if db is not None:
                db.close()
    raise RuntimeError("No se pudo conectar a PostgreSQL")


def wait_for_rabbitmq(max_retries=20, delay=5) -> pika.BlockingConnection:
    log.info(f"Conectando a RabbitMQ en {RABBITMQ_URL} ...")
    for i in range(max_retries):
        try:
            from urllib.parse import urlparse, unquote

            parsed = urlparse(RABBITMQ_URL)
            credentials = pika.PlainCredentials(
                parsed.username or "guest",
                parsed.password or "guest",
            )
            virtual_host = unquote(parsed.path.lstrip("/") or "/")
            conn = pika.BlockingConnection(
                pika.ConnectionParameters(
                    host=parsed.hostname or "rabbitmq",
                    port=parsed.port or 5672,
                    virtual_host=virtual_host,
                    credentials=credentials,
                    heartbeat=600,
                    blocked_connection_timeout=300,
                )
            )
            log.info("RabbitMQ conectado.")
            return conn
        except Exception as e:
            log.info(f"RabbitMQ no disponible ({e}) — reintento {i+1}/{max_retries} en {delay}s ...")
            time.sleep(delay)
    raise RuntimeError("No se pudo conectar a RabbitMQ")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    log.info("=== llm-worker iniciando ===")
    log.info(f"BATCH_SIZE={BATCH_SIZE} | BATCH_TIMEOUT={BATCH_TIMEOUT_SEC}s | MODEL={OLLAMA_MODEL}")

    wait_for_postgres()
    # init_db() crea las tablas si no existen (idempotente)
    init_db()
    wait_for_ollama()

    # Hilo de timeout en background
    threading.Thread(target=timeout_flusher, daemon=True).start()

    # Loop principal con reconexión automática a RabbitMQ
    while True:
        try:
            rmq_conn = wait_for_rabbitmq()
            channel  = rmq_conn.channel()

            # Declaraciones idempotentes
            channel.exchange_declare(
                exchange=EXCHANGE_NAME,
                exchange_type="direct",
                durable=True,
            )
            channel.queue_declare(queue=QUEUE_NAME, durable=True)
            channel.queue_bind(
                queue=QUEUE_NAME,
                exchange=EXCHANGE_NAME,
                routing_key=QUEUE_NAME,
            )

            # prefetch=1: no tomar otro mensaje hasta terminar el actual
            channel.basic_qos(prefetch_count=1)
            channel.basic_consume(
                queue=QUEUE_NAME,
                on_message_callback=on_message,
                auto_ack=False,
            )

            log.info(f"Consumiendo cola '{QUEUE_NAME}' ...")
            channel.start_consuming()

        except KeyboardInterrupt:
            log.info("Interrumpido, saliendo.")
            break
        except Exception as e:
            log.error(f"Error en consumer RabbitMQ: {e}. Reconectando en 5s ...")
            time.sleep(5)


if __name__ == "__main__":
    main()
