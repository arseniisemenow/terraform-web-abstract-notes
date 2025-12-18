FROM cr.yandex/mirror/ubuntu:20.04

# Set environment variables to avoid interactive prompts
ENV DEBIAN_FRONTEND=noninteractive
ENV TZ=Etc/UTC
ENV PYTHONUNBUFFERED=1

# Install system dependencies
RUN apt-get update && apt-get install -y \
    --no-install-recommends \
    tzdata \
    python3 \
    python3-pip \
    ffmpeg \
    curl \
    && rm -rf /var/lib/apt/lists/* \
    && echo 'Etc/UTC' > /etc/timezone \
    && ln -fs /usr/share/zoneinfo/Etc/UTC /etc/localtime \
    && dpkg-reconfigure -f noninteractive tzdata

# Install Python dependencies
COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

# Set working directory
WORKDIR /app

# Copy application code
COPY worker.py .

# Create logs directory
RUN mkdir -p /app/logs

# Expose port (for health checks)
EXPOSE 8080

# Add health check script
COPY health_check.py .
RUN chmod +x health_check.py

# Default command
CMD ["python3", "worker.py"]