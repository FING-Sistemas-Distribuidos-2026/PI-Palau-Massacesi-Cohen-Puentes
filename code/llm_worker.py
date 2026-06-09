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
OLLAMA_MODEL        = os.getenv("OLLAMA_MODEL",        "qwen2.5:0.5b")
LOG_LEVEL           = os.getenv("LOG_LEVEL",           "INFO")
BATCH_TIMEOUT_SEC   = float(120)
ADAPTIVE_SPLIT_THRESHOLD = 40

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
# Cada job conserva todas las frases recibidas para poder reconstruir batches incrementales.
job_buffers: dict = defaultdict(lambda: {
    "phrases": [],
    "received": 0,
    "last_ts": time.monotonic(),
    "batch_num": 0,
    "processed_up_to": 0,
    "expected_total": None,
    "planned_total": 0,
    "targets": [],
    "target_index": 0,
    "force_flush": False,
    "is_processing": False,  # Evita que se lancen dos procesadores para el mismo job.
})
buffers_lock = threading.Lock()
ollama_global_lock = threading.Lock() # Fuerza a Ollama a procesar de a UN lote a la vez


def get_job_expected_total(job_id: str):
    db = SessionLocal()
    try:
        job = db.query(Job).filter(Job.id == job_id).first()
        if job:
            return job.num_workers
        return None
    finally:
        db.close()


def start_job_processor(job_id: str):
    with buffers_lock:
        buf = job_buffers.get(job_id)
        if not buf or buf["is_processing"]:
            return

        buf["is_processing"] = True

    threading.Thread(target=process_job_batches, args=(job_id,), daemon=True).start()


def choose_batch_count(total_messages: int) -> int:
    if total_messages <= 0:
        return 0
    desired = 5 if total_messages >= ADAPTIVE_SPLIT_THRESHOLD else 3
    return min(desired, total_messages)


def build_cumulative_targets(total_messages: int, batch_count: int) -> list[int]:
    if total_messages <= 0 or batch_count <= 0:
        return []

    targets: list[int] = []
    for i in range(1, batch_count + 1):
        # Ceil(i * total / batch_count) sin usar math para mantener dependencias mínimas.
        target = (i * total_messages + batch_count - 1) // batch_count
        if not targets or target > targets[-1]:
            targets.append(target)

    if targets and targets[-1] != total_messages:
        targets[-1] = total_messages

    return targets


def ensure_plan(buf: dict, total_messages: int):
    batch_count = choose_batch_count(total_messages)
    targets = build_cumulative_targets(total_messages, batch_count)
    buf["planned_total"] = total_messages
    buf["targets"] = targets
    buf["target_index"] = 0
    while buf["target_index"] < len(buf["targets"]) and buf["targets"][buf["target_index"]] <= buf["processed_up_to"]:
        buf["target_index"] += 1


def process_job_batches(job_id: str):
    try:
        while True:
            with buffers_lock:
                buf = job_buffers.get(job_id)
                if not buf:
                    return

                available = len(buf["phrases"])
                processed_up_to = buf["processed_up_to"]
                expected_total = buf["expected_total"]

                if expected_total is not None and available >= expected_total:
                    if buf["planned_total"] != expected_total:
                        ensure_plan(buf, expected_total)
                elif buf["force_flush"] and available > processed_up_to:
                    if buf["planned_total"] != available:
                        ensure_plan(buf, available)
                else:
                    return

                if buf["target_index"] >= len(buf["targets"]):
                    return

                target_count = buf["targets"][buf["target_index"]]
                if available < target_count:
                    return

                buf["batch_num"] += 1
                batch_num = buf["batch_num"]
                batch_phrases = buf["phrases"][:target_count]
                buf["target_index"] += 1

            process_batch(job_id, batch_phrases, batch_num)

            with buffers_lock:
                buf = job_buffers.get(job_id)
                if not buf:
                    return

                buf["processed_up_to"] = target_count
                if buf["target_index"] >= len(buf["targets"]):
                    buf["force_flush"] = False

                db = SessionLocal()
                try:
                    job = db.query(Job).filter(Job.id == job_id).first()
                finally:
                    db.close()

                if (
                    job
                    and job.status == "completed"
                    and buf["processed_up_to"] >= job.num_workers
                    and len(buf["phrases"]) >= job.num_workers
                ):
                    del job_buffers[job_id]
                    log.info(f"[job={job_id[:8]}] Memoria volátil purgada tras completar todos los batches.")
                    return
    finally:
        with buffers_lock:
            buf = job_buffers.get(job_id)
            if buf:
                buf["is_processing"] = False

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
                    "- Responde SOLO con el mensaje original reconstruido sin introducciones, viñetas ni contenido extra"
                )
            },
            {
                "role": "user",
                "content": (
                    f"Estas {len(phrases)} frases son versiones distorsionadas del mismo mensaje original:\n"
                    f"{lineas_entrada}\n\n"
                    "¿Cuál era el mensaje original? Contesta únicamente con el mensaje"
                )
            }
        ],
        "stream": False,
        "options": {
            "temperature": 0.3,
            "top_p": 0.1,
            "num_predict": 150,
            "num_ctx": 3072
        }
    }

    with ollama_global_lock:
        try:
            resp = requests.post(url, json=payload, timeout=300)
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

    except Exception as e:
        db.rollback()
        log.error(f"[job={job_id[:8]}] Error en la transacción de la DB: {e}")
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

    job_id    = data.get("job_id", "").strip()
    distorted = data.get("distorted_phrase", "").strip()

    if not job_id or not distorted:
        channel.basic_ack(delivery_tag=method.delivery_tag)
        return

    should_start_processor = False
    expected_total = None

    # Evitamos consultas de DB bajo lock.
    with buffers_lock:
        buf = job_buffers[job_id]
        expected_total = buf["expected_total"]

    if expected_total is None:
        expected_total = get_job_expected_total(job_id)

    with buffers_lock:
        buf = job_buffers[job_id]
        if buf["expected_total"] is None:
            buf["expected_total"] = expected_total

        buf["phrases"].append(distorted)
        buf["received"] += 1
        buf["last_ts"] = time.monotonic()
        buf["force_flush"] = False

        if buf["expected_total"] is not None and len(buf["phrases"]) >= buf["expected_total"]:
            buf["force_flush"] = True
            if buf["planned_total"] != buf["expected_total"]:
                buf["targets"] = []
                buf["target_index"] = 0

        if buf["force_flush"]:
            should_start_processor = True

    channel.basic_ack(delivery_tag=method.delivery_tag)

    # Si hay suficientes mensajes o ya llegó el total esperado, se procesa en serie.
    if should_start_processor:
        start_job_processor(job_id)


# ---------------------------------------------------------------------------
# Hilo de timeout: El encargado de segmentar el remanente por silencio
# ---------------------------------------------------------------------------
def timeout_flusher():
    while True:
        time.sleep(1.0)
        now = time.monotonic()
        jobs_to_start = []
        
        with buffers_lock:
            for job_id, buf in list(job_buffers.items()):
                if not buf["phrases"]:
                    continue
                
                # Si la manguera se quedó en silencio por el tiempo estipulado
                if (now - buf["last_ts"]) >= BATCH_TIMEOUT_SEC:
                    # log.info(f"[job={job_id[:8]}] ⏱️ Silencio detectado. Reanudando procesamiento incremental con {len(buf['phrases'])} frases acumuladas.")
                    buf["force_flush"] = True
                if buf["phrases"] and buf["force_flush"] and not buf["is_processing"]:
                    jobs_to_start.append(job_id)

        for job_id in jobs_to_start:
            start_job_processor(job_id)

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
    log.info(
        f"BATCH_MODE=adaptive(3|5) | SPLIT_THRESHOLD={ADAPTIVE_SPLIT_THRESHOLD} | "
        f"BATCH_TIMEOUT={BATCH_TIMEOUT_SEC}s | MODEL={OLLAMA_MODEL}"
    )

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