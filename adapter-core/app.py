"""Engine-neutral transport and execute-once materialization core."""
import hashlib, json, os, threading, time
from dataclasses import dataclass
import requests
from fastapi import FastAPI, Header, HTTPException
from fastapi.responses import JSONResponse

app = FastAPI(title="FGAC adapter core")
REMOTE_URL = os.getenv("REMOTE_URL", "http://remote:8002")
TTL = int(os.getenv("MATERIALIZATION_TTL_SECONDS", "300"))
lock = threading.Lock()
metrics = {"proxy_execute_calls": 0, "proxy_estimate_calls": 0,
           "remote_materialize_calls": 0, "local_rescans": 0}

@dataclass
class Item:
    token_hash: str; response: dict; expires: float
items: dict[str, Item] = {}

def bearer(value):
    if not value or not value.startswith("Bearer "): raise HTTPException(401, "Bearer token required")
    return value
def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
def remote(path, body, authorization):
    r=requests.post(REMOTE_URL+path,json=body,headers={"Authorization":bearer(authorization)},timeout=120)
    try: payload=r.json()
    except ValueError: payload={"detail":r.text}
    if r.status_code//100 != 2: raise HTTPException(r.status_code,payload)
    return payload

@app.get("/health")
def health(): return {"status":"ok"}
@app.post("/v2/subplans")
def execute(plan:dict, authorization:str|None=Header(default=None)):
    with lock: metrics["proxy_execute_calls"]+=1
    return JSONResponse(remote("/v2/subplans",plan,authorization),headers={"X-FGAC-Adapter-Core":"1"})
@app.post("/v2/subplans/estimate")
def estimate(plan:dict, authorization:str|None=Header(default=None)):
    with lock: metrics["proxy_estimate_calls"]+=1
    return remote("/v2/subplans/estimate",plan,authorization)
@app.post("/v1/materializations")
def materialize(request:dict, authorization:str|None=Header(default=None)):
    query_id,plan=request.get("query_id"),request.get("plan")
    if not isinstance(query_id,str) or not query_id or not isinstance(plan,dict): raise HTTPException(400,"query_id and plan required")
    auth=bearer(authorization); token_hash=hashlib.sha256(auth.encode()).hexdigest(); key=digest([query_id,token_hash,plan])
    with lock:
        old=items.get(key)
        if old and old.expires>time.time(): return {"materialization_id":key,"remote_executed":False,"rows":len(old.response.get("inline_rows",[]))}
    response=remote("/v2/subplans",plan,auth)
    with lock:
        items[key]=Item(token_hash,response,time.time()+TTL); metrics["remote_materialize_calls"]+=1
    return {"materialization_id":key,"remote_executed":True,"rows":len(response.get("inline_rows",[]))}
@app.get("/v1/materializations/{key}")
def rescan(key:str, authorization:str|None=Header(default=None)):
    token_hash=hashlib.sha256(bearer(authorization).encode()).hexdigest()
    with lock:
        item=items.get(key)
        if not item or item.expires<=time.time(): raise HTTPException(404,"materialization expired")
        if item.token_hash!=token_hash: raise HTTPException(403,"principal mismatch")
        metrics["local_rescans"]+=1; return item.response
@app.delete("/v1/materializations/{key}")
def release(key:str, authorization:str|None=Header(default=None)):
    token_hash=hashlib.sha256(bearer(authorization).encode()).hexdigest()
    with lock:
        item=items.get(key)
        if item and item.token_hash!=token_hash: raise HTTPException(403,"principal mismatch")
        items.pop(key,None)
    return {"released":True}
@app.get("/metrics")
def get_metrics():
    with lock: return {**metrics,"active_materializations":len(items)}
@app.post("/metrics/reset")
def reset():
    with lock:
        for key in metrics: metrics[key]=0
        items.clear()
    return {"reset":True}
