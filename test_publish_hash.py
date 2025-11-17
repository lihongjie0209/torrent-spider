"""Test script to manually publish a torrent hash to Kafka."""

import sys
import os
import time

# Add shared module to path
sys.path.insert(0, os.path.dirname(__file__))

from shared.config import Config
from shared.kafka_client import KafkaProducerClient
from shared.schemas import TorrentHashMessage

def main():
    """Publish a test torrent hash."""
    # Use a known public domain torrent hash (Ubuntu ISO)
    # This is Ubuntu 22.04.3 LTS Desktop amd64 ISO
    test_hash = "cc954e0c31ba6c40f4f0386e79b89d5f9c1e6af8"
    
    print(f"Connecting to Kafka at {Config.KAFKA_BOOTSTRAP_SERVERS}...")
    producer = KafkaProducerClient()
    
    print(f"Publishing test hash: {test_hash}")
    message = TorrentHashMessage(info_hash=test_hash)
    
    success = producer.send(
        Config.KAFKA_TOPIC_HASHES,
        message.to_json(),
        key=test_hash
    )
    
    if success:
        print(f"✓ Successfully published hash to {Config.KAFKA_TOPIC_HASHES}")
        print(f"  Now check metadata-collector logs to see if it picks up this hash")
    else:
        print("✗ Failed to publish hash")
        return 1
    
    producer.close()
    print("\nTest complete!")
    return 0

if __name__ == "__main__":
    sys.exit(main())
