# fraud-stream · Dockerfile
# Shared base image for api, consumer, and dashboard services.
# Build context is the project root.

FROM python:3.12-slim

WORKDIR /app

# Install deps first — cached unless requirements.txt changes
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source (data/ and .env are excluded via .dockerignore)
COPY . .

# No CMD — each service in docker-compose.yml sets its own command
