"""Metadata collector module - Fetches torrent metadata via libtorrent."""

import sys
import os
import logging
import time
import libtorrent as lt
from datetime import datetime

# Add shared module to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from shared.config import Config
from shared.kafka_client import KafkaProducerClient, KafkaConsumerClient
from shared.schemas import TorrentHashMessage, TorrentMetadataMessage

# Configure logging
logging.basicConfig(
    level=getattr(logging, Config.LOG_LEVEL),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class MetadataCollector:
    """Fetches torrent metadata from info_hashes."""
    
    def __init__(self):
        self.session = None
        self.kafka_producer = None
        self.kafka_consumer = None
        self.timeout = Config.METADATA_COLLECTOR_TIMEOUT
        self.active_downloads = {}
        
    def setup_session(self):
        """Initialize libtorrent session for metadata downloads."""
        port = Config.METADATA_COLLECTOR_PORT
        logger.info(f"Initializing libtorrent session for metadata collection on port {port}")
        
        self.session = lt.session()
        
        # Configure session settings
        settings = {
            'enable_dht': True,
            'enable_lsd': False,
            'enable_upnp': False,
            'enable_natpmp': False,
            'anonymous_mode': False,
            'listen_interfaces': f'0.0.0.0:{port}',
        }
        
        self.session.apply_settings(settings)
        
        # Add DHT routers
        self.session.add_dht_router('router.bittorrent.com', 6881)
        self.session.add_dht_router('router.utorrent.com', 6881)
        self.session.add_dht_router('dht.transmissionbt.com', 6881)
        
        # Set alert mask for metadata
        self.session.set_alert_mask(
            lt.alert.category_t.status_notification |
            lt.alert.category_t.error_notification
        )
        
        logger.info("Metadata session initialized")
    
    def setup_kafka(self):
        """Initialize Kafka producer and consumer."""
        logger.info("Connecting to Kafka...")
        self.kafka_producer = KafkaProducerClient()
        self.kafka_consumer = KafkaConsumerClient(
            topic=Config.KAFKA_TOPIC_HASHES,
            group_id="metadata-collector-group"
        )
    
    def download_metadata(self, info_hash: str):
        """
        Download metadata for a given info_hash.
        
        Args:
            info_hash: Hex string of the info_hash
        """
        try:
            logger.info(f"Requesting metadata for {info_hash}")
            
            # Create magnet link
            magnet_link = f"magnet:?xt=urn:btih:{info_hash}"
            
            # Add torrent for metadata only
            params = lt.add_torrent_params()
            params.url = magnet_link
            params.save_path = '/tmp'
            params.flags = lt.torrent_flags.upload_mode  # Don't download files
            
            handle = self.session.add_torrent(params)
            self.active_downloads[info_hash] = {
                'handle': handle,
                'start_time': time.time()
            }
            
            logger.debug(f"Added {info_hash} to download queue")
            
        except Exception as e:
            logger.error(f"Failed to add torrent {info_hash}: {e}", exc_info=True)
    
    def process_metadata(self, info_hash: str, handle):
        """
        Process and publish downloaded metadata.
        
        Args:
            info_hash: Hex string of the info_hash
            handle: libtorrent torrent_handle
        """
        try:
            if not handle.has_metadata():
                logger.warning(f"Handle has no metadata for {info_hash}")
                return
            
            torrent_info = handle.get_torrent_info()
            
            # Extract file information
            files = []
            file_storage = torrent_info.files()
            for i in range(file_storage.num_files()):
                file_entry = file_storage.at(i)
                files.append({
                    'path': file_entry.path,
                    'size': file_entry.size
                })
            
            # Create metadata message
            metadata = TorrentMetadataMessage(
                info_hash=info_hash,
                name=torrent_info.name(),
                files=files,
                total_size=torrent_info.total_size(),
                piece_length=torrent_info.piece_length(),
                num_pieces=torrent_info.num_pieces(),
                comment=torrent_info.comment() if torrent_info.comment() else None,
                created_date=datetime.fromtimestamp(
                    torrent_info.creation_date()
                ).isoformat() if torrent_info.creation_date() > 0 else None
            )
            
            # Publish to Kafka
            success = self.kafka_producer.send(
                Config.KAFKA_TOPIC_METADATA,
                metadata.to_json(),
                key=info_hash
            )
            
            if success:
                logger.info(f"Published metadata for {info_hash}: {torrent_info.name()}")
            else:
                logger.error(f"Failed to publish metadata for {info_hash}")
            
            # Remove torrent from session
            self.session.remove_torrent(handle)
            
        except Exception as e:
            logger.error(f"Error processing metadata for {info_hash}: {e}", exc_info=True)
    
    def check_timeouts(self):
        """Check and clean up timed out downloads."""
        current_time = time.time()
        to_remove = []
        
        for info_hash, data in self.active_downloads.items():
            elapsed = current_time - data['start_time']
            
            if elapsed > self.timeout:
                logger.warning(f"Metadata download timeout for {info_hash} ({elapsed:.1f}s)")
                try:
                    self.session.remove_torrent(data['handle'])
                except:
                    pass
                to_remove.append(info_hash)
        
        for info_hash in to_remove:
            del self.active_downloads[info_hash]
    
    def handle_message(self, message_json: str):
        """
        Handle incoming hash message from Kafka.
        
        Args:
            message_json: JSON string of TorrentHashMessage
        """
        try:
            message = TorrentHashMessage.from_json(message_json)
            info_hash = message.info_hash
            
            # Skip if already downloading
            if info_hash in self.active_downloads:
                logger.debug(f"Already downloading metadata for {info_hash}")
                return
            
            self.download_metadata(info_hash)
            
        except Exception as e:
            logger.error(f"Error handling message: {e}", exc_info=True)
    
    def process_alerts(self):
        """Process libtorrent alerts."""
        alerts = self.session.pop_alerts()
        
        for alert in alerts:
            if isinstance(alert, lt.metadata_received_alert):
                info_hash = str(alert.handle.info_hash())
                logger.info(f"Metadata received for {info_hash}")
                
                if info_hash in self.active_downloads:
                    self.process_metadata(info_hash, alert.handle)
                    del self.active_downloads[info_hash]
            
            elif isinstance(alert, lt.metadata_failed_alert):
                info_hash = str(alert.handle.info_hash())
                logger.warning(f"Metadata failed for {info_hash}: {alert.message()}")
                
                if info_hash in self.active_downloads:
                    del self.active_downloads[info_hash]
            
            elif isinstance(alert, lt.torrent_error_alert):
                logger.error(f"Torrent error: {alert.message()}")
    
    def run(self):
        """Main collection loop."""
        logger.info("Starting metadata collection...")
        
        try:
            self.setup_session()
            self.setup_kafka()
            
            # Start consuming in a separate thread-like manner
            import threading
            
            consumer_thread = threading.Thread(
                target=self.kafka_consumer.consume,
                args=(self.handle_message,),
                daemon=True
            )
            consumer_thread.start()
            
            last_cleanup = time.time()
            
            # Main processing loop
            while True:
                # Process libtorrent alerts
                self.process_alerts()
                
                # Check for timeouts
                current_time = time.time()
                if current_time - last_cleanup >= 10:  # Every 10 seconds
                    self.check_timeouts()
                    last_cleanup = current_time
                    
                    # Log status
                    logger.info(f"Active downloads: {len(self.active_downloads)}")
                
                time.sleep(0.1)
                
        except KeyboardInterrupt:
            logger.info("Metadata collector stopped by user")
        except Exception as e:
            logger.error(f"Fatal error in metadata collector: {e}", exc_info=True)
            raise
        finally:
            self.cleanup()
    
    def cleanup(self):
        """Clean up resources."""
        logger.info("Cleaning up resources...")
        if self.session:
            self.session.pause()
        if self.kafka_producer:
            self.kafka_producer.close()
        if self.kafka_consumer:
            self.kafka_consumer.close()


def main():
    """Entry point."""
    logger.info("=" * 60)
    logger.info("Metadata Collector Starting")
    logger.info(f"Kafka: {Config.KAFKA_BOOTSTRAP_SERVERS}")
    logger.info(f"Source Topic: {Config.KAFKA_TOPIC_HASHES}")
    logger.info(f"Target Topic: {Config.KAFKA_TOPIC_METADATA}")
    logger.info(f"Timeout: {Config.METADATA_COLLECTOR_TIMEOUT}s")
    logger.info("=" * 60)
    
    collector = MetadataCollector()
    collector.run()


if __name__ == "__main__":
    main()
