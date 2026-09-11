FROM python:3.11-slim-bookworm
WORKDIR /app
RUN pip install --no-cache-dir fastapi==0.116.1 uvicorn==0.35.0
COPY storage ./storage
CMD ["uvicorn", "storage.app:app", "--host", "0.0.0.0", "--port", "8003"]
