"""Kafka producer and consumer utilities."""

import logging
from typing import Optional, Callable, Any
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError

from .config import Config

logger = logging.getLogger(__name__)


class KafkaProducerClient:
    """Wrapper for Kafka producer with error handling."""
    
    def __init__(self, bootstrap_servers: Optional[str] = None):
        self.bootstrap_servers = bootstrap_servers or Config.KAFKA_BOOTSTRAP_SERVERS
        self.producer = None
        self._connect()
    
    def _connect(self):
        """Establish connection to Kafka."""
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers.split(','),
                value_serializer=lambda v: v.encode('utf-8'),
                acks='all',
                retries=3,
                max_in_flight_requests_per_connection=1
            )
            logger.info(f"Connected to Kafka at {self.bootstrap_servers}")
        except Exception as e:
            logger.error(f"Failed to connect to Kafka: {e}")
            raise
    
    def send(self, topic: str, message: str, key: Optional[str] = None) -> bool:
        """
        Send message to Kafka topic.
        
        Args:
            topic: Kafka topic name
            message: JSON message string
            key: Optional message key for partitioning
            
        Returns:
            True if successful, False otherwise
        """
        try:
            key_bytes = key.encode('utf-8') if key else None
            future = self.producer.send(topic, value=message, key=key_bytes)
            future.get(timeout=10)  # Wait for confirmation
            logger.debug(f"Sent message to {topic}: {message[:100]}")
            return True
        except KafkaError as e:
            logger.error(f"Failed to send message to {topic}: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error sending message: {e}")
            return False
    
    def close(self):
        """Close the producer connection."""
        if self.producer:
            self.producer.flush()
            self.producer.close()
            logger.info("Kafka producer closed")


class KafkaConsumerClient:
    """Wrapper for Kafka consumer with error handling."""
    
    def __init__(
        self,
        topic: str,
        group_id: str,
        bootstrap_servers: Optional[str] = None,
        auto_offset_reset: str = 'earliest'
    ):
        self.topic = topic
        self.group_id = group_id
        self.bootstrap_servers = bootstrap_servers or Config.KAFKA_BOOTSTRAP_SERVERS
        self.auto_offset_reset = auto_offset_reset
        self.consumer = None
        self._connect()
    
    def _connect(self):
        """Establish connection to Kafka."""
        try:
            self.consumer = KafkaConsumer(
                self.topic,
                bootstrap_servers=self.bootstrap_servers.split(','),
                group_id=self.group_id,
                value_deserializer=lambda m: m.decode('utf-8'),
                auto_offset_reset=self.auto_offset_reset,
                enable_auto_commit=True,
                auto_commit_interval_ms=5000
            )
            logger.info(f"Subscribed to Kafka topic '{self.topic}' with group '{self.group_id}'")
        except Exception as e:
            logger.error(f"Failed to connect to Kafka consumer: {e}")
            raise
    
    def consume(self, handler: Callable[[str], None]):
        """
        Consume messages from topic and process with handler.
        
        Args:
            handler: Function to process each message (receives JSON string)
        """
        logger.info(f"Starting to consume messages from {self.topic}")
        try:
            for message in self.consumer:
                try:
                    logger.debug(f"Received message from {self.topic}: {message.value[:100]}")
                    handler(message.value)
                except Exception as e:
                    logger.error(f"Error processing message: {e}", exc_info=True)
                    # Continue processing other messages
        except KeyboardInterrupt:
            logger.info("Consumer interrupted by user")
        except Exception as e:
            logger.error(f"Consumer error: {e}", exc_info=True)
        finally:
            self.close()
    
    def close(self):
        """Close the consumer connection."""
        if self.consumer:
            self.consumer.close()
            logger.info("Kafka consumer closed")
