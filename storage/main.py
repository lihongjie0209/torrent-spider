"""Storage module - Persists torrent metadata to Meilisearch."""

import sys
import os
import logging
import redis

# Add shared module to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from shared.config import Config
from shared.kafka_client import KafkaConsumerClient
from shared.schemas import TorrentMetadataMessage
from database import TorrentDatabase

# Configure logging
logging.basicConfig(
    level=getattr(logging, Config.LOG_LEVEL),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class StorageService:
    """Stores torrent metadata in Meilisearch with Redis deduplication."""
    
    def __init__(self):
        self.database = None
        self.redis_client = None
        self.kafka_consumer = None
        self.processed_count = 0
        self.duplicate_count = 0
    
    def setup_database(self):
        """Initialize Meilisearch database."""
        logger.info(f"Initializing Meilisearch at {Config.MEILISEARCH_URL}")
        
        self.database = TorrentDatabase(
            url=Config.MEILISEARCH_URL,
            api_key=Config.MEILISEARCH_API_KEY,
            index_name=Config.MEILISEARCH_INDEX
        )
    
    def setup_redis(self):
        """Initialize Redis connection for deduplication."""
        logger.info(f"Connecting to Redis at {Config.REDIS_HOST}:{Config.REDIS_PORT}")
        
        try:
            self.redis_client = redis.Redis(
                host=Config.REDIS_HOST,
                port=Config.REDIS_PORT,
                db=Config.REDIS_DB,
                password=Config.REDIS_PASSWORD,
                decode_responses=True,
                socket_connect_timeout=5,
                socket_timeout=5
            )
            # Test connection
            self.redis_client.ping()
            logger.info("Redis connection established")
        except Exception as e:
            logger.error(f"Failed to connect to Redis: {e}", exc_info=True)
            raise
    
    def setup_kafka(self):
        """Initialize Kafka consumer."""
        logger.info("Connecting to Kafka...")
        self.kafka_consumer = KafkaConsumerClient(
            topic=Config.KAFKA_TOPIC_METADATA,
            group_id="storage-service-group"
        )
    
    def is_duplicate(self, info_hash: str) -> bool:
        """
        Check if info_hash has been processed using Redis.
        
        Args:
            info_hash: Torrent info hash
            
        Returns:
            True if duplicate, False otherwise
        """
        try:
            redis_key = f"torrent:hash:{info_hash}"
            
            # Check if exists in Redis
            if self.redis_client.exists(redis_key):
                return True
            
            # Also check database as fallback
            if self.database.torrent_exists(info_hash):
                # Add to Redis for future checks
                self.redis_client.set(redis_key, "1")
                return True
            
            return False
            
        except Exception as e:
            logger.error(f"Error checking duplicate for {info_hash}: {e}", exc_info=True)
            # If Redis fails, check database only
            return self.database.torrent_exists(info_hash)
    
    def mark_processed(self, info_hash: str):
        """
        Mark info_hash as processed in Redis.
        
        Args:
            info_hash: Torrent info hash
        """
        try:
            redis_key = f"torrent:hash:{info_hash}"
            # Store permanently (no expiration)
            self.redis_client.set(redis_key, "1")
        except Exception as e:
            logger.error(f"Error marking processed {info_hash}: {e}", exc_info=True)
    
    def handle_message(self, message_json: str):
        """
        Handle incoming metadata message from Kafka.
        
        Args:
            message_json: JSON string of TorrentMetadataMessage
        """
        try:
            # Parse message
            metadata = TorrentMetadataMessage.from_json(message_json)
            info_hash = metadata.info_hash
            
            logger.info(f"Processing metadata for {info_hash}: {metadata.name}")
            
            # Check for duplicates
            if self.is_duplicate(info_hash):
                self.duplicate_count += 1
                logger.info(f"Skipping duplicate torrent {info_hash}")
                return
            
            # Insert into database
            success = self.database.insert_torrent(
                info_hash=info_hash,
                name=metadata.name,
                total_size=metadata.total_size,
                files=metadata.files,
                piece_length=metadata.piece_length,
                num_pieces=metadata.num_pieces,
                comment=metadata.comment,
                created_date=metadata.created_date,
                collected_at=metadata.collected_at
            )
            
            if success:
                # Mark as processed in Redis
                self.mark_processed(info_hash)
                self.processed_count += 1
                logger.info(
                    f"Stored torrent {info_hash} "
                    f"(Total: {self.processed_count}, Duplicates: {self.duplicate_count})"
                )
            else:
                logger.error(f"Failed to store torrent {info_hash}")
            
        except Exception as e:
            logger.error(f"Error handling message: {e}", exc_info=True)
    
    def run(self):
        """Main storage loop."""
        logger.info("Starting storage service...")
        
        try:
            self.setup_database()
            self.setup_redis()
            self.setup_kafka()
            
            # Log initial stats
            total_torrents = self.database.get_torrent_count()
            logger.info(f"Database contains {total_torrents} torrents")
            
            # Start consuming messages
            self.kafka_consumer.consume(self.handle_message)
            
        except KeyboardInterrupt:
            logger.info("Storage service stopped by user")
        except Exception as e:
            logger.error(f"Fatal error in storage service: {e}", exc_info=True)
            raise
        finally:
            self.cleanup()
    
    def cleanup(self):
        """Clean up resources."""
        logger.info("Cleaning up resources...")
        
        logger.info(f"Final stats - Processed: {self.processed_count}, Duplicates: {self.duplicate_count}")
        
        if self.database:
            self.database.close()
        if self.redis_client:
            self.redis_client.close()
        if self.kafka_consumer:
            self.kafka_consumer.close()


def main():
    """Entry point."""
    logger.info("=" * 60)
    logger.info("Storage Service Starting")
    logger.info(f"Kafka: {Config.KAFKA_BOOTSTRAP_SERVERS}")
    logger.info(f"Source Topic: {Config.KAFKA_TOPIC_METADATA}")
    logger.info(f"Meilisearch: {Config.MEILISEARCH_URL}")
    logger.info(f"Meilisearch Index: {Config.MEILISEARCH_INDEX}")
    logger.info(f"Redis: {Config.REDIS_HOST}:{Config.REDIS_PORT}")
    logger.info("=" * 60)
    
    service = StorageService()
    service.run()


if __name__ == "__main__":
    main()
