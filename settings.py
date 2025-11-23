import os
from typing import Optional


class Settings:
    # DB y endpoints externos
    DATABASE_URL: str = os.getenv("DATABASE_URL", "postgresql://app:app@postgres:5432/deportes")
    OVERPASS_URL: str = os.getenv("OVERPASS_URL", "https://overpass-api.de/api/interpreter")
    GEOCODER_URL: str = os.getenv("GEOCODER_URL", "https://nominatim.openstreetmap.org/reverse")
    GEOCODER_EMAIL: Optional[str] = os.getenv("GEOCODER_EMAIL")
    GEOCODER_LANGUAGE: str = os.getenv("GEOCODER_LANGUAGE", "es")
    GEOCODER_TIMEOUT_S: int = int(os.getenv("GEOCODER_TIMEOUT_S", "15"))
    GEOCODER_USER_AGENT: str = os.getenv("GEOCODER_USER_AGENT", "sports-ingest/1.0")
    GEOCODER_CACHE_PRECISION: int = int(os.getenv("GEOCODER_CACHE_PRECISION", "5"))
    GEOCODER_CACHE_TTL_HOURS: int = int(os.getenv("GEOCODER_CACHE_TTL_HOURS", "720"))
    JAEGER_HOST: str = os.getenv("JAEGER_HOST", "localhost")
    JAEGER_PORT: int = int(os.getenv("JAEGER_PORT", "6831"))

    # Parámetros del worker
    WORKER_BATCH: int = int(os.getenv("BATCH", "25"))
    WORKER_MAX: int = int(os.getenv("MAX_TILES_TOTAL", "100"))
    BACKOFF_OK_MIN: int = int(os.getenv("BACKOFF_OK_MIN", "1440"))
    BACKOFF_ERR_MIN: int = int(os.getenv("BACKOFF_ERR_MIN", "30"))
    MAX_RETRIES: int = int(os.getenv("MAX_RETRIES", "5"))
    WORKER_RELEASE_BATCH: int = int(os.getenv("WORKER_RELEASE_BATCH", "100"))
    MIN_REQ_INTERVAL_MS: int = int(os.getenv("MIN_REQ_INTERVAL_MS", "900"))
    JITTER_MS: int = int(os.getenv("JITTER_MS", "250"))
    HTTP_TIMEOUT_S: int = int(os.getenv("HTTP_TIMEOUT_S", "60"))
    REAPER_STALE_SECS: int = int(os.getenv("REAPER_STALE_SECS", "600"))
    WORKER_USER_AGENT: str = os.getenv("WORKER_USER_AGENT", "sports-app-tiles/1.0")
    REGION_ISO: Optional[str] = os.getenv("REGION_ISO")


settings = Settings()
