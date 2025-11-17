"""Configuration management for all modules."""

import os
from typing import Optional


class Config:
    """Centralized configuration from environment variables."""
    
    # Kafka configuration
    KAFKA_BOOTSTRAP_SERVERS: str = os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"
    )
    KAFKA_TOPIC_HASHES: str = os.getenv(
        "KAFKA_TOPIC_HASHES", "torrent-hashes"
    )
    KAFKA_TOPIC_METADATA: str = os.getenv(
        "KAFKA_TOPIC_METADATA", "torrent-metadata"
    )
    
    # Redis configuration
    REDIS_HOST: str = os.getenv("REDIS_HOST", "redis")
    REDIS_PORT: int = int(os.getenv("REDIS_PORT", "6379"))
    REDIS_DB: int = int(os.getenv("REDIS_DB", "0"))
    REDIS_PASSWORD: Optional[str] = os.getenv("REDIS_PASSWORD")
    
    # Logging configuration
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")
    
    # Module-specific configurations
    HASH_COLLECTOR_DHT_PORT: int = int(os.getenv("DHT_PORT", "6881"))
    METADATA_COLLECTOR_PORT: int = int(os.getenv("METADATA_PORT", "6882"))
    METADATA_COLLECTOR_TIMEOUT: int = int(os.getenv("METADATA_TIMEOUT", "60"))
    
    # Meilisearch configuration
    MEILISEARCH_URL: str = os.getenv("MEILISEARCH_URL", "http://meilisearch:7700")
    MEILISEARCH_API_KEY: Optional[str] = os.getenv("MEILISEARCH_API_KEY")
    MEILISEARCH_INDEX: str = os.getenv("MEILISEARCH_INDEX", "torrents")
    
    @classmethod
    def get_redis_url(cls) -> str:
        """Get Redis connection URL."""
        if cls.REDIS_PASSWORD:
            return f"redis://:{cls.REDIS_PASSWORD}@{cls.REDIS_HOST}:{cls.REDIS_PORT}/{cls.REDIS_DB}"
        return f"redis://{cls.REDIS_HOST}:{cls.REDIS_PORT}/{cls.REDIS_DB}"
