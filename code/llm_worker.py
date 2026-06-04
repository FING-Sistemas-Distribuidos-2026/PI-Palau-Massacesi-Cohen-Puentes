"""
llm_worker.py
-------------
Servicio que consume frases distorsionadas de la cola RabbitMQ (`telephone.results`),
las agrupa en batches por job, y consulta a Ollama para reconstruir la frase original.
"""

import os
import json
import time
import logging
import threading
import re
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
OLLAMA_MODEL        = os.getenv("OLLAMA_MODEL",        "llama3.2:1b")
LOG_LEVEL           = os.getenv("LOG_LEVEL",           "INFO")
BATCH_SIZE          = int(os.getenv("LLM_BATCH_SIZE",    "3"))
BATCH_TIMEOUT_SEC   = float(os.getenv("LLM_BATCH_TIMEOUT", "20"))
STREAM_OFFSET       = os.getenv("LLM_STREAM_OFFSET", "next")

QUEUE_NAME    = "telephone.results"
EXCHANGE_NAME = "telephone"
ROUTING_KEY_RESULTS = "results"
RESULTS_STREAM_MAX_AGE = os.getenv("RESULTS_STREAM_MAX_AGE", "2h")

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
# Buffer en memoria y Locks de control de concurrencia
# ---------------------------------------------------------------------------
# Estructura del buffer protegida con estados de procesamiento
job_buffers: dict = defaultdict(lambda: {
    "phrases":   [],
    "total":     0,
    "received":  0,
    "last_ts":   time.monotonic(),
    "batch_num": 0,
    "is_processing": False, # Nuevo: Evita que el flusher duplique tareas activas
})
buffers_lock = threading.Lock()
ollama_global_lock = threading.Lock() # Nuevo: Fuerza a Ollama a procesar de a UN lote a la vez

# ---------------------------------------------------------------------------
# Ollama
# ---------------------------------------------------------------------------
def call_ollama(phrases: list[str], job_id: str, batch_num: int) -> tuple[str, str]:
    # Unimos las frases del lote separadas por un salto de línea limpio
    lineas_entrada = "\n".join(phrases)
    
    url = f"{OLLAMA_URL}/api/chat"
    
    payload = {
        "model": OLLAMA_MODEL,
        "messages": [
            {
                "role": "system",
                "content": (
                    "Eres un detective de mensajes. "
                    "El usuario te dará varias versiones distorsionadas de un mismo mensaje original. "
                    "Tu tarea es deducir cuál era el mensaje original. "
                    "REGLAS ESTRICTAS:\n"
                    "- Responde SOLO con el mensaje original reconstruido.\n"
                    "- Una única línea, sin explicaciones, sin comillas, sin guiones, sin prefijos."
                )
            },
            {
                "role": "user",
                "content": (
                    f"Estas {len(phrases)} frases son versiones distorsionadas del mismo mensaje original:\n"
                    f"{lineas_entrada}\n\n"
                    "¿Cuál era el mensaje original?"
                )
            }
        ],
        "stream": False,
        "options": {
            "temperature": 0.0,
            "top_p": 0.1,
            "num_predict": 150
        }
    }

    with ollama_global_lock:
        try:
            resp = requests.post(url, json=payload, timeout=60)
            resp.raise_for_status()
            data = resp.json()
            raw = json.dumps(data, ensure_ascii=False)
            
            log.info(f"[DEBUG CHAT CONTENT]: {raw}")

            guess = ""
            message_obj = data.get("message", {})

            if "content" in message_obj and message_obj["content"].strip():
                # Obtenemos la respuesta limpia y podamos espacios sobrantes
                lines = [l.strip() for l in message_obj["content"].strip().split("\n") if l.strip()]
                # Removemos guiones o viñetas molestas que Llama a veces mete por reflejo
                lines_cleaned = [re.sub(r"^[-*•]\s*", "", l) for l in lines]
                guess = "\n".join(lines_cleaned)

            if not guess:
                guess = "[No se obtuvo respuesta limpia del modelo]"

            log.info(f"[job={job_id[:8]}] Ollama batch={batch_num} → '{guess}'")
            return guess, raw

        except Exception as e:
            log.error(f"[job={job_id[:8]}] Error Ollama: {e}")
            return "[error]", ""
# ---------------------------------------------------------------------------
# Procesamiento de un batch
# ---------------------------------------------------------------------------
def process_batch(job_id: str, phrases: list[str], batch_num: int, received_at_moment: int, total: int):
    log.info(f"[job={job_id[:8]}] Procesando batch={batch_num} | frases_en_batch={len(phrases)}")

    guess_text, ollama_raw = call_ollama(phrases, job_id, batch_num)

    db = SessionLocal()
    try:
        guess = Guess(
            job_id=job_id,
            batch_num=batch_num,
            guess=guess_text,
            ollama_response=ollama_raw,
        )
        db.add(guess)
        db.commit()

        # Evaluamos el cierre del JOB consultando los totales reales y el número de batch
        with buffers_lock:
            buf = job_buffers.get(job_id)
            
            # Condición segura: Si ya recibimos todo lo esperado y estamos procesando el lote final (batch 3)
            if buf and buf["total"] > 0 and received_at_moment >= buf["total"]:
                job = db.query(Job).filter(Job.id == job_id).first()
                if job and job.status != "completed":
                    job.status = "completed"
                    db.commit()
                    log.info(f"[job={job_id[:8]}] ¡Todas las réplicas procesadas! Marcado como 'completed'.")
                    
                    # Ahora sí borramos el buffer de memoria de forma segura
                    del job_buffers[job_id]

    except Exception as e:
        db.rollback()
        log.error(f"[job={job_id[:8]}] Error guardando en DB: {e}")
    finally:
        db.close()
# ---------------------------------------------------------------------------
# RabbitMQ: callback por mensaje
# ---------------------------------------------------------------------------
def on_message(channel, method, properties, body):
    try:
        data = json.loads(body.decode())
    except Exception as e:
        channel.basic_ack(delivery_tag=method.delivery_tag)
        return

    job_id      = data.get("job_id", "").strip()
    distorted   = data.get("distorted_phrase", "").strip()
    num_workers = int(data.get("num_workers", 0))

    if not job_id or not distorted:
        channel.basic_ack(delivery_tag=method.delivery_tag)
        return

    flush_phrases    = None
    flush_batch_num  = None
    flush_received   = None
    flush_total      = None

    with buffers_lock:
        buf = job_buffers[job_id]
        if num_workers > 0:
            buf["total"] = num_workers

        # 1. Acumulamos SIEMPRE en la lista histórica del buffer
        # 1. Acumulamos la frase en el buffer histórico
        buf["phrases"].append(distorted)
        buf["received"] += 1
        
        # Guardamos cuántas frases tenemos en este instante exacto de la ejecución
        current_count = buf["received"] 

        
        # 2. Control estricto de disparadores usando porciones disjuntas (Slicing limpio)
        disparar_batch = False
        
        if current_count == 3:
            buf["batch_num"] = 1
            flush_phrases = buf["phrases"][0:3]  # Frases 1, 2 y 3
            disparar_batch = True
        elif current_count == 6:
            buf["batch_num"] = 2
            flush_phrases = buf["phrases"][3:6]  # Frases 4, 5 y 6
            disparar_batch = True
        elif current_count == buf["total"] or current_count == 9:
            buf["batch_num"] = 3
            flush_phrases = buf["phrases"][6:9]  # Frases 7, 8 y 9 (o hasta el total)
            disparar_batch = True

        if disparar_batch:
            # Ya extrajimos la porción exacta arriba, solo congelamos los metadatos
            flush_batch_num = buf["batch_num"]
            flush_received = current_count
            flush_total = buf["total"]

    channel.basic_ack(delivery_tag=method.delivery_tag)

    # Disparamos el hilo asíncrono para Ollama solo si cumplió la condición de los 3 pasos
    if flush_phrases:
        threading.Thread(
            target=process_batch,
            args=(job_id, flush_phrases, flush_batch_num, flush_received, flush_total),
            daemon=True,
        ).start()

# ---------------------------------------------------------------------------
# Hilo de timeout: flush forzado
# ---------------------------------------------------------------------------
def timeout_flusher():
    while True:
        time.sleep(5.0)
        now = time.monotonic()
        to_flush = []

        with buffers_lock:
            for job_id, buf in list(job_buffers.items()):
                if not buf["phrases"]:
                    continue
                
                # Si pasaron los segundos de timeout sin recibir nada nuevo, forzamos lo que haya
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
            log.info(f"[job={args[0][:8]}] Timeout alcanzado en buffer. Forzando batch {args[2]}.")
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
    init_db()
    wait_for_ollama()

    threading.Thread(target=timeout_flusher, daemon=True).start()

    while True:
        try:
            rmq_conn = wait_for_rabbitmq()
            channel  = rmq_conn.channel()

            channel.exchange_declare(
                exchange=EXCHANGE_NAME,
                exchange_type="direct",
                durable=True,
            )
            channel.queue_declare(
                queue=QUEUE_NAME,
                durable=True,
                arguments={
                    "x-queue-type": "stream",
                    "x-max-age": RESULTS_STREAM_MAX_AGE,
                },
            )
            channel.queue_bind(
                queue=QUEUE_NAME,
                exchange=EXCHANGE_NAME,
                routing_key=ROUTING_KEY_RESULTS,
            )

            channel.basic_qos(prefetch_count=1)
            channel.basic_consume(
                queue=QUEUE_NAME,
                on_message_callback=on_message,
                auto_ack=False,
                arguments={"x-stream-offset": STREAM_OFFSET},
            )

            log.info(f"Consumiendo stream '{QUEUE_NAME}' (offset={STREAM_OFFSET}) ...")
            channel.start_consuming()

        except KeyboardInterrupt:
            log.info("Interrumpido, saliendo.")
            break
        except Exception as e:
            log.error(f"Error en consumer RabbitMQ: {e}. Reconectando en 5s ...")
            time.sleep(5)


if __name__ == "__main__":
    main()