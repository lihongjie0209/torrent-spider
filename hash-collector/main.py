"""Hash collector module - Discovers torrent info_hashes via DHT."""

import sys
import os
import logging
import time
import libtorrent as lt
from datetime import datetime

# Add shared module to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from shared.config import Config
from shared.kafka_client import KafkaProducerClient
from shared.schemas import TorrentHashMessage

# Configure logging
logging.basicConfig(
    level=getattr(logging, Config.LOG_LEVEL),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class HashCollector:
    """Collects torrent info_hashes from DHT network."""
    
    def __init__(self):
        self.session = None
        self.kafka_producer = None
        self.dht_port = Config.HASH_COLLECTOR_DHT_PORT
        
    def setup_session(self):
        """Initialize libtorrent session with DHT enabled."""
        logger.info(f"Initializing libtorrent session on port {self.dht_port}")
        
        # Create session with DHT settings
        self.session = lt.session()
        
        # Configure session settings for aggressive DHT discovery
        settings = {
            'enable_dht': True,
            'enable_lsd': False,
            'enable_upnp': False,
            'enable_natpmp': False,
            'anonymous_mode': False,  # Allow DHT to work better
            'force_proxy': False,
            'listen_interfaces': f'0.0.0.0:{self.dht_port}',
            'dht_announce_interval': 900,  # Announce every 15 minutes
        }
        
        self.session.apply_settings(settings)
        
        # Start DHT with bootstrap nodes
        logger.info("Adding DHT bootstrap routers...")
        self.session.add_dht_router('router.bittorrent.com', 6881)
        self.session.add_dht_router('router.utorrent.com', 6881)
        self.session.add_dht_router('dht.transmissionbt.com', 6881)
        self.session.add_dht_router('dht.libtorrent.org', 25401)
        self.session.add_dht_router('dht.aelitis.com', 6881)
        
        # Enable DHT alerts
        self.session.set_alert_mask(
            lt.alert.category_t.dht_notification |
            lt.alert.category_t.error_notification |
            lt.alert.category_t.status_notification
        )
        
        # Start DHT
        self.session.start_dht()
        logger.info("DHT session initialized and started")
    
    def setup_kafka(self):
        """Initialize Kafka producer."""
        logger.info("Connecting to Kafka...")
        self.kafka_producer = KafkaProducerClient()
    
    def run(self):
        """Main collection loop."""
        logger.info("Starting hash collection...")
        
        try:
            self.setup_session()
            self.setup_kafka()
            
            seen_hashes = set()
            last_status_time = time.time()
            last_bootstrap_check = time.time()
            dht_bootstrapped = False
            
            while True:
                # Check for alerts from libtorrent
                alerts = self.session.pop_alerts()
                
                for alert in alerts:
                    # Process DHT announce alerts to discover torrents
                    if isinstance(alert, lt.dht_announce_alert):
                        info_hash = str(alert.info_hash)
                        
                        if info_hash not in seen_hashes:
                            seen_hashes.add(info_hash)
                            logger.info(f"Discovered new info_hash: {info_hash}")
                            
                            # Create and send message
                            message = TorrentHashMessage(info_hash=info_hash)
                            success = self.kafka_producer.send(
                                Config.KAFKA_TOPIC_HASHES,
                                message.to_json(),
                                key=info_hash
                            )
                            
                            if success:
                                logger.debug(f"Published hash {info_hash} to Kafka")
                            else:
                                logger.error(f"Failed to publish hash {info_hash}")
                    
                    # Process DHT get_peers replies - these contain info_hashes
                    elif isinstance(alert, lt.dht_get_peers_reply_alert):
                        info_hash = str(alert.info_hash)
                        num_peers = alert.num_peers()
                        
                        if info_hash not in seen_hashes and num_peers > 0:
                            seen_hashes.add(info_hash)
                            logger.info(f"Discovered info_hash from DHT reply: {info_hash} ({num_peers} peers)")
                            
                            # Create and send message
                            message = TorrentHashMessage(info_hash=info_hash)
                            success = self.kafka_producer.send(
                                Config.KAFKA_TOPIC_HASHES,
                                message.to_json(),
                                key=info_hash
                            )
                            
                            if success:
                                logger.debug(f"Published hash {info_hash} to Kafka")
                            else:
                                logger.error(f"Failed to publish hash {info_hash}")
                    
                    # Monitor DHT bootstrap status
                    elif isinstance(alert, lt.dht_bootstrap_alert):
                        if not dht_bootstrapped:
                            logger.info("DHT bootstrap complete!")
                            dht_bootstrapped = True
                    
                    # Log any error alerts
                    elif hasattr(alert, 'category') and 'error' in str(alert.category()):
                        logger.error(f"Alert: {alert}")
                
                # Check DHT status periodically
                current_time = time.time()
                if current_time - last_bootstrap_check >= 30:  # Every 30 seconds
                    status = self.session.status()
                    dht_nodes = status.dht_nodes
                    
                    if dht_nodes == 0 and not dht_bootstrapped:
                        logger.warning("DHT not bootstrapped yet, re-adding routers...")
                        self.session.add_dht_router('router.bittorrent.com', 6881)
                        self.session.add_dht_router('router.utorrent.com', 6881)
                        self.session.add_dht_router('dht.transmissionbt.com', 6881)
                    elif dht_nodes > 0 and not dht_bootstrapped:
                        logger.info(f"DHT now has {dht_nodes} nodes!")
                        dht_bootstrapped = True
                    
                    last_bootstrap_check = current_time
                
                # Log status periodically (every 60 seconds for better monitoring)
                if current_time - last_status_time >= 60:
                    status = self.session.status()
                    logger.info(
                        f"Status: DHT nodes={status.dht_nodes}, "
                        f"DHT torrents={status.dht_torrents}, "
                        f"Discovered hashes={len(seen_hashes)}"
                    )
                    last_status_time = current_time
                
                time.sleep(0.1)  # Small delay to avoid busy loop
                
        except KeyboardInterrupt:
            logger.info("Hash collector stopped by user")
        except Exception as e:
            logger.error(f"Fatal error in hash collector: {e}", exc_info=True)
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


def main():
    """Entry point."""
    logger.info("=" * 60)
    logger.info("Hash Collector Starting")
    logger.info(f"DHT Port: {Config.HASH_COLLECTOR_DHT_PORT}")
    logger.info(f"Kafka: {Config.KAFKA_BOOTSTRAP_SERVERS}")
    logger.info(f"Target Topic: {Config.KAFKA_TOPIC_HASHES}")
    logger.info("=" * 60)
    
    collector = HashCollector()
    collector.run()


if __name__ == "__main__":
    main()
