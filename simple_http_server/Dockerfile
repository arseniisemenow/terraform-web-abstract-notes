FROM cr.yandex/mirror/ubuntu:20.04

# Set environment variables to avoid interactive prompts
ENV DEBIAN_FRONTEND=noninteractive
ENV TZ=Etc/UTC
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Install Python3 and pip, then Flask
RUN apt-get update && apt-get install -y \
    --no-install-recommends \
    tzdata \
    python3 \
    python3-pip \
    curl \
    && rm -rf /var/lib/apt/lists/* \
    && echo 'Etc/UTC' > /etc/timezone \
    && ln -fs /usr/share/zoneinfo/Etc/UTC /etc/localtime \
    && dpkg-reconfigure -f noninteractive tzdata \
    && pip3 install --no-cache-dir Flask==2.3.3 boto3==1.26.0

# Copy Flask application
COPY app_flask.py .

EXPOSE 8080

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8080/health || exit 1

CMD ["python3", "app_flask.py"]