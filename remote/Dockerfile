FROM python:3.11-slim-bookworm
WORKDIR /app
RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt/lists,sharing=locked \
    apt-get update && apt-get install -y --no-install-recommends openjdk-17-jre-headless curl \
    && rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir fastapi==0.116.1 uvicorn==0.35.0 requests==2.32.4 pyspark==3.5.6
COPY remote ./remote
CMD ["uvicorn", "remote.app:app", "--host", "0.0.0.0", "--port", "8002"]
