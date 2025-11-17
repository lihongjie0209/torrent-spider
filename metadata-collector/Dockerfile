FROM python:3.11-slim

WORKDIR /app

# Install system dependencies for libtorrent
RUN apt-get update && apt-get install -y \
    build-essential \
    libssl-dev \
    libboost-all-dev \
    python3-libtorrent \
    && rm -rf /var/lib/apt/lists/*

# Copy shared module
COPY shared/ /app/shared/

# Copy metadata-collector module
COPY metadata-collector/requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

COPY metadata-collector/ /app/

# Run the collector
CMD ["python", "main.py"]
