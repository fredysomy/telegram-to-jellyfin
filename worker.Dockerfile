FROM python:3.13-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates ffmpeg \
    && rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir requests

WORKDIR /app
COPY worker.py torrent_source.py /app/

ENTRYPOINT ["python", "/app/worker.py"]
