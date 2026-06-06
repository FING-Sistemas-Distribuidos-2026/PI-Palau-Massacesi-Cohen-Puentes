import os
from pydantic_settings import BaseSettings
from typing import Optional

batch_timeout = float(os.getenv("BATCH_TIMEOUT_SEC", "20"))

class Settings(BaseSettings):
    """
    Application configuration loaded from environment variables (.env file).
    """
    
    # Database Configuration
    database_url: str = "postgresql://user:devpassword123@localhost:5432/telephone_db"
    
    # RabbitMQ Configuration
    rabbitmq_url: str = "amqp://guest:guest@localhost:5672/%2F"
    
    # Ollama Configuration
    ollama_url: str = "http://localhost:11434"
    ollama_model: str = "llama3.2:1b"
    
    # API Configuration
    api_host: str = "0.0.0.0"
    api_port: int = 8000
    
    # Application Configuration
    distortion_probability: float = 0.3
    batch_size: int = 5
    batch_timeout_seconds: float = batch_timeout
    
    # Logging
    log_level: str = "INFO"
    
    class Config:
        env_file = ".env"
        case_sensitive = False


# ============ RabbitMQ Constants ============
class RabbitMQConfig:
    """RabbitMQ exchange and queue configuration."""
    
    # Exchange
    EXCHANGE_NAME = "telephone"
    EXCHANGE_TYPE = "direct"
    
    # Stream queues
    JOBS_QUEUE = "telephone.jobs"
    RESULTS_QUEUE = "telephone.results"
    
    # Routing keys
    ROUTING_KEY_JOBS = "jobs"
    ROUTING_KEY_RESULTS = "results"

    # Stream settings
    QUEUE_TYPE_STREAM = "stream"
    JOBS_STREAM_MAX_AGE = "1h"
    RESULTS_STREAM_MAX_AGE = "2h"
    STREAM_CONSUMER_OFFSET = "next"
    LLM_STREAM_OFFSET = "first"


# ============ Job Status Constants ============
class JobStatus:
    """Job status values."""
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"


# ============ Message Formats ============
class MessageFormats:
    """
    Standard message formats for inter-service communication.
    """
    
    @staticmethod
    def job_message(job_id: str, phrase: str, num_workers: int) -> dict:
        """Format for job message to worker stream consumers."""
        return {
            "job_id": job_id,
            "phrase": phrase,
            "num_workers": num_workers
        }
    
    @staticmethod
    def result_message(job_id: str, worker_id: int, distorted_phrase: str, num_workers: int) -> dict:
        """Format for result message from worker."""
        return {
            "job_id": job_id,
            "worker_id": worker_id,
            "distorted_phrase": distorted_phrase,
            "num_workers": num_workers
        }
    
    @staticmethod
    def guess_message(job_id: str, batch_num: int, guess: str, ollama_response: str) -> dict:
        """Format for guess message from LLM worker."""
        return {
            "job_id": job_id,
            "batch_num": batch_num,
            "guess": guess,
            "ollama_response": ollama_response
        }


# ============ API Response Models ============
class ResponseMessages:
    """Standard response messages."""
    
    JOB_CREATED = "Job created successfully"
    JOB_NOT_FOUND = "Job not found"
    INVALID_PHRASE = "Phrase cannot be empty"
    INVALID_WORKERS = "Number of workers must be between 1 and 2000"
    INTERNAL_ERROR = "Internal server error"


# ============ Timeouts & Limits ============
class Limits:
    """Application limits and timeouts."""
    
    MAX_WORKERS = 2000
    MIN_WORKERS = 1
    MAX_PHRASE_LENGTH = 1000
    MIN_PHRASE_LENGTH = 1
    
    # Timeouts (seconds)
    JOB_TIMEOUT = 300  # 5 minutes
    WORKER_TIMEOUT = 60  # 1 minute
    OLLAMA_TIMEOUT = 30  # 30 seconds
    
    # SSE Settings
    SSE_PING_INTERVAL = 15  # seconds
    SSE_TIMEOUT = 300  # 5 minutes


# Initialize settings
settings = Settings()
