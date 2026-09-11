FROM python:3.11-slim-bookworm
WORKDIR /app
RUN pip install --no-cache-dir fastapi==0.116.1 uvicorn==0.35.0 requests==2.32.4
COPY adapter-core ./adapter-core
CMD ["uvicorn", "app:app", "--app-dir", "/app/adapter-core", "--host", "0.0.0.0", "--port", "8004"]
