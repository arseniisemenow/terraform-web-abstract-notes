FROM cr.yandex/mirror/ubuntu:20.04

RUN apt-get update && apt-get install -y python3 && \
    mkdir -p /app && \
    echo '<html><body><h1>🎓 Lecture Notes Generator</h1><p>✅ Application successfully deployed on Yandex Cloud!</p></body></html>' > /app/index.html

WORKDIR /app

EXPOSE 8080

CMD ["python3", "-m", "http.server", "8080"]