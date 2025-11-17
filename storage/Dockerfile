FROM python:3.11-slim

WORKDIR /app

# Copy shared module
COPY shared/ /app/shared/

# Copy storage module
COPY storage/requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

COPY storage/ /app/

# Create data directory
RUN mkdir -p /data

# Run the service
CMD ["python", "main.py"]
