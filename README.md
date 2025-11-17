# Torrent Spider

A distributed torrent spider system built with Python, featuring three independent modules that communicate via Kafka. The system discovers torrent info_hashes from the DHT network, fetches their metadata, and stores them in SQLite.

## Architecture

```
┌─────────────────┐      ┌─────────────────┐      ┌─────────────────┐
│  Hash Collector │      │Metadata Collector│     │     Storage     │
│   (x2 nodes)    │──────▶│   (x3 nodes)    │────▶│   (x1 node)     │
│                 │      │                 │      │                 │
│  libtorrent DHT │      │  libtorrent BT  │      │  SQLite + Redis │
└─────────────────┘      └─────────────────┘      └─────────────────┘
         │                        │                        │
         └────────────────────────┴────────────────────────┘
                                  │
                          ┌───────▼────────┐
                          │  Kafka (KRaft) │
                          │  + Redis       │
                          └────────────────┘
```

### Modules

1. **Hash Collector**: Discovers torrent info_hashes by connecting to the BitTorrent DHT network. Runs multiple instances with unique DHT ports.

2. **Metadata Collector**: Consumes info_hashes from Kafka, fetches full torrent metadata via libtorrent, and publishes to the metadata topic. Supports horizontal scaling with consumer groups.

3. **Storage**: Consumes metadata from Kafka, uses Redis for deduplication, and persists data to SQLite with a normalized schema (torrents + files tables).

### Communication

- **Kafka Topics**:
  - `torrent-hashes`: Info hash messages (JSON)
  - `torrent-metadata`: Full metadata messages (JSON)

- **Redis**: Deduplication cache using `torrent:hash:{info_hash}` keys

- **Message Format**: All inter-module communication uses JSON schemas defined in `shared/schemas.py`

## Prerequisites

- Docker and Docker Compose
- At least 4GB RAM for running all services
- Stable internet connection for DHT bootstrapping

## Quick Start

### 1. Clone and Setup

```bash
git clone <repository-url>
cd torrent-spider
cp .env.example .env
```

### 2. Build and Run

```bash
# Build all images
docker-compose build

# Start all services
docker-compose up -d

# View logs
docker-compose logs -f

# View specific module logs
docker-compose logs -f hash-collector-1
docker-compose logs -f metadata-collector-1
docker-compose logs -f storage
```

### 3. Check Status

```bash
# Check running containers
docker-compose ps

# Check Kafka topics
docker exec -it kafka kafka-topics.sh --bootstrap-server localhost:9092 --list

# Check Redis keys
docker exec -it redis redis-cli KEYS "torrent:hash:*" | wc -l

# Check SQLite database
docker exec -it storage sqlite3 /data/torrents.db "SELECT COUNT(*) FROM torrents;"
```

## Scaling

Scale individual modules as needed:

```bash
# Scale metadata collectors
docker-compose up -d --scale metadata-collector-1=5

# Add more hash collectors (modify docker-compose.yml)
# Add more storage nodes (requires database strategy planning)
```

## Module Configuration

### Hash Collector
- `DHT_PORT`: DHT listening port (must be unique per instance)
- `KAFKA_TOPIC_HASHES`: Output topic for discovered hashes

### Metadata Collector
- `METADATA_TIMEOUT`: Timeout in seconds for metadata fetch
- `KAFKA_TOPIC_HASHES`: Input topic (consumer group enabled)
- `KAFKA_TOPIC_METADATA`: Output topic

### Storage
- `STORAGE_DB_PATH`: SQLite database file path
- `KAFKA_TOPIC_METADATA`: Input topic

## Development

### Project Structure

```
torrent-spider/
├── shared/                  # Shared utilities
│   ├── config.py           # Configuration management
│   ├── kafka_client.py     # Kafka producer/consumer wrappers
│   └── schemas.py          # JSON message schemas
├── hash-collector/         # Hash collector module
│   ├── main.py
│   ├── requirements.txt
│   └── Dockerfile
├── metadata-collector/     # Metadata collector module
│   ├── main.py
│   ├── requirements.txt
│   └── Dockerfile
├── storage/                # Storage module
│   ├── main.py
│   ├── database.py
│   ├── requirements.txt
│   └── Dockerfile
└── docker-compose.yml      # Docker orchestration
```

### Local Development

To run modules locally without Docker:

```bash
# Install dependencies
cd hash-collector
pip install -r requirements.txt

# Run module
python main.py
```

Ensure Kafka and Redis are accessible (update `.env` with local addresses).

## Database Schema

### Torrents Table
```sql
CREATE TABLE torrents (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    info_hash TEXT UNIQUE NOT NULL,
    name TEXT NOT NULL,
    total_size INTEGER NOT NULL,
    piece_length INTEGER,
    num_pieces INTEGER,
    comment TEXT,
    created_date TEXT,
    collected_at TEXT NOT NULL,
    inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### Files Table
```sql
CREATE TABLE files (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    torrent_id INTEGER NOT NULL,
    path TEXT NOT NULL,
    size INTEGER NOT NULL,
    FOREIGN KEY (torrent_id) REFERENCES torrents (id)
);
```

## Monitoring

All modules use structured logging with configurable log levels:

```bash
# Set log level in docker-compose.yml or .env
LOG_LEVEL=DEBUG  # DEBUG, INFO, WARNING, ERROR
```

Logs include:
- Hash discovery events
- Metadata fetch success/failure
- Database operations
- Kafka message processing
- Error traces with stack traces

## Troubleshooting

### Kafka Connection Issues
```bash
# Check Kafka health
docker exec -it kafka kafka-broker-api-versions.sh --bootstrap-server localhost:9092
```

### DHT Not Bootstrapping
- Ensure internet connectivity
- Check firewall rules for DHT ports
- Wait up to 60 seconds for bootstrap

### No Metadata Being Fetched
- Verify hash-collectors are discovering hashes
- Check Kafka topic has messages: `docker exec -it kafka kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic torrent-hashes --from-beginning`
- Increase `METADATA_TIMEOUT` if needed

### Database Lock Errors
- Storage module uses single writer (one instance)
- For multiple storage nodes, implement database sharding or use PostgreSQL

## Performance Tuning

- **Hash Collectors**: Add more instances for faster hash discovery (ensure unique DHT ports)
- **Metadata Collectors**: Scale based on hash ingestion rate (3-5 instances recommended)
- **Storage**: Single instance sufficient; bottleneck is typically metadata fetch, not storage
- **Kafka**: Increase partitions for topics if needed: `--partitions 10`

## License

MIT License

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make changes and test locally
4. Submit a pull request

## Security Notes

- DHT discovery is public and anonymous
- No authentication implemented (add as needed)
- Redis has no password by default (set `REDIS_PASSWORD` for production)
- SQLite database is stored in Docker volume (back up regularly)
