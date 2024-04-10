# Use an official Python runtime as a parent image for the build stage
FROM python:3.9-slim as builder

# Set the working directory in the container
WORKDIR /app

# Install gcc, libc6-dev (or build-essential for a more comprehensive toolset), and librdkafka-dev
RUN apt-get update && \
    apt-get install -y --no-install-recommends build-essential librdkafka-dev && \
    rm -rf /var/lib/apt/lists/*

# Copy the requirements file
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Start a new stage from a slim image to keep the final image smaller
FROM python:3.9-slim

WORKDIR /app

# Copy installed Python packages from the builder stage
COPY --from=builder /usr/local/lib/python3.9/site-packages/ /usr/local/lib/python3.9/site-packages/

CMD python /app/producers/$APP

