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

QUEUE_NAME          = "telephone.results"
EXCHANGE_NAME       = "telephone"
ROUTING_KEY_RESULTS = "results"

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
    "is_processing": False, # Evita que el flusher duplique tareas activas
})
buffers_lock = threading.Lock()
ollama_global_lock = threading.Lock() # Fuerza a Ollama a procesar de a UN lote a la vez

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
                    "- Responde SOLO con el mensaje original reconstruido sin introducciones"
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
            "temperature": 0.3,
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
# Procesamiento de lotes del LLM (Basado 100% en la Realidad de la DB)
# ---------------------------------------------------------------------------
def process_batch(job_id: str, phrases: list[str], batch_num: int):
    log.info(f"[job={job_id[:8]}] Procesando batch={batch_num} | frases_en_batch={len(phrases)}")

    # 1. Llamamos a Ollama con nuestro lote fraccionado (sea del tamaño que sea)
    guess_text, ollama_raw = call_ollama(phrases, job_id, batch_num)

    db = SessionLocal()
    try:
        # 2. Insertamos el veredicto actual de este lote en la tabla Guess
        guess = Guess(
            job_id=job_id,
            batch_num=batch_num,
            guess=guess_text,
            ollama_response=ollama_raw,
        )
        db.add(guess)
        db.commit()

        # 3. DETERMINACIÓN RELACIONAL DEL CIERRE (Sin supuestos de memoria)
        # Consultamos el registro del Job para saber cuántas copias reales publicó la API.
        job = db.query(Job).filter(Job.id == job_id).first()
        if job and job.status != "completed":
            from database import DistortedPhrase
            total_frases_distorsionadas = db.query(DistortedPhrase).filter(DistortedPhrase.job_id == job_id).count()
            
            # El flujo se considera completo cuando ingresan todas las copias enviadas al Rabbit.
            if total_frases_distorsionadas > 0 and total_frases_distorsionadas >= job.num_workers:
                job.status = "completed"
                db.commit()
                log.info(f"[job={job_id[:8]}] 🏁 Cierre relacional certificado: {total_frases_distorsionadas} frases en DB.")

        # 4. Limpieza del buffer de ráfaga
        with buffers_lock:
            if job_id in job_buffers and not job_buffers[job_id]["phrases"]:
                del job_buffers[job_id]
                log.info(f"[job={job_id[:8]}] Memoria volátil purgada por inactividad del flujo.")

    except Exception as e:
        db.rollback()
        log.error(f"[job={job_id[:8]}] Error en la transacción de la DB: {e}")
    finally:
        db.close()

# Tope seguro para Ollama. Más de 10 frases rotas confunden al modelo de 1B.
MAX_BATCH_SIZE_OLLAMA = 10 

# ---------------------------------------------------------------------------
# RabbitMQ: callback por mensaje
# ---------------------------------------------------------------------------
def on_message(channel, method, properties, body):
    try:
        data = json.loads(body.decode())
    except Exception as e:
        channel.basic_ack(delivery_tag=method.delivery_tag)
        return

    job_id    = data.get("job_id", "").strip()
    distorted = data.get("distorted_phrase", "").strip()

    if not job_id or not distorted:
        channel.basic_ack(delivery_tag=method.delivery_tag)
        return

    flush_phrases   = None
    flush_batch_num = None

    with buffers_lock:
        buf = job_buffers[job_id]
        buf["phrases"].append(distorted)
        buf["received"] += 1
        buf["last_ts"] = time.monotonic()

        if len(buf["phrases"]) >= BATCH_SIZE or len(buf["phrases"]) >= MAX_BATCH_SIZE_OLLAMA:
            buf["batch_num"] += 1
            flush_phrases = buf["phrases"][:]
            buf["phrases"] = []
            flush_batch_num = buf["batch_num"]

    channel.basic_ack(delivery_tag=method.delivery_tag)

    # Si se llenó un lote óptimo en caliente, se procesa de inmediato
    if flush_phrases:
        threading.Thread(
            target=process_batch,
            args=(job_id, flush_phrases, flush_batch_num),
            daemon=True,
        ).start()


# ---------------------------------------------------------------------------
# Hilo de timeout: El encargado de segmentar el remanente por silencio
# ---------------------------------------------------------------------------
def timeout_flusher():
    while True:
        time.sleep(1.0)
        now = time.monotonic()
        
        with buffers_lock:
            for job_id, buf in list(job_buffers.items()):
                if not buf["phrases"]:
                    continue
                
                # Si la manguera se quedó en silencio por el tiempo estipulado
                if (now - buf["last_ts"]) >= BATCH_TIMEOUT_SEC:
                    log.info(f"[job={job_id[:8]}] ⏱️ Silencio detectado. Procesando remanente de {len(buf['phrases'])} frases.")
                    
                    frases_remanentes = buf["phrases"][:]
                    buf["phrases"] = []
                    
                    sub_lotes = [frases_remanentes[i:i + MAX_BATCH_SIZE_OLLAMA] for i in range(0, len(frases_remanentes), MAX_BATCH_SIZE_OLLAMA)]
                    
                    for lote in sub_lotes:
                        buf["batch_num"] += 1
                        threading.Thread(
                            target=process_batch,
                            args=(job_id, lote, buf["batch_num"]),
                            daemon=True,
                        ).start()

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