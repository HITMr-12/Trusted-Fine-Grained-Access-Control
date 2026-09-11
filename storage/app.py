import os
from pathlib import Path
from fastapi import FastAPI, Header, HTTPException
from fastapi.responses import FileResponse

app = FastAPI(title="Unified demo storage")
DATA_ROOT = Path(os.getenv("PARQUET_ROOT", "/parquet"))
STORAGE_TOKEN = os.getenv("STORAGE_TOKEN", "remote-storage-secret")

@app.get("/health")
def health(): return {"status": "ok"}

@app.get("/v1/objects/{relation_id}")
def object_file(relation_id: str, authorization: str | None = Header(default=None)):
    if authorization != f"Bearer {STORAGE_TOKEN}":
        raise HTTPException(status_code=403, detail="Storage access denied")
    if relation_id != "governed.orders.v1":
        raise HTTPException(status_code=404, detail="Unknown storage object")
    files = sorted((DATA_ROOT / "governed" / "orders").glob("*.parquet"))
    if len(files) != 1: raise HTTPException(status_code=500, detail="Parquet unavailable")
    return FileResponse(files[0], media_type="application/vnd.apache.parquet")
