FROM cr.yandex/mirror/ubuntu:20.04

WORKDIR /app

# Install Python3 and pip, then Flask
RUN apt-get update && apt-get install -y python3 python3-pip && \
    pip3 install flask==2.3.3 && \
    rm -rf /var/lib/apt/lists/*

# Copy Flask application
COPY app_flask.py .

EXPOSE 8080

CMD ["python3", "app_flask.py"]