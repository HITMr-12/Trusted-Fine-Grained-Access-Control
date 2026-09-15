"""Isolated instrumentation; imports deployed service without changing it."""
import os,sys,json,time,threading,signal,hashlib
from pathlib import Path
p=Path(sys.argv[1]);raw=Path('/proc/837124/environ').read_bytes()
os.environ.update(x.decode().split('=',1) for x in raw.split(b'\0') if b'=' in x)
os.environ['PYSPARK_SUBMIT_ARGS']=f'--conf spark.eventLog.enabled=true --conf spark.eventLog.dir=file:{p}/events pyspark-shell'
sys.path.insert(0,'/home/lyb/fgac/repo')
from remote import app as service
from streaming.server import SparkBackend,GovernedFlightServer
lock=threading.Lock()
def emit(row):
 with lock,(p/'stages.jsonl').open('a') as f:f.write(json.dumps(row)+'\n')
def ph(payload):return hashlib.sha256(json.dumps(payload,sort_keys=True).encode()).hexdigest()
class ProbeBackend(SparkBackend):
 def authorize(self,payload,token):
  t=time.perf_counter();v=super().authorize(payload,token)
  self.local.auth_ms=(time.perf_counter()-t)*1000
  return v
 def prepare(self,payload,token):
  self.local=threading.local() if not hasattr(self,'local') else self.local
  t=time.perf_counter();v=super().prepare(payload,token)
  emit(dict(kind='prepare',time=time.time(),plan_hash=ph(payload),principal=v.principal,total_ms=(time.perf_counter()-t)*1000,authorize_ms=self.local.auth_ms))
  return v
 def revalidate(self,prepared,token):
  t=time.perf_counter();v=super().revalidate(prepared,token)
  emit(dict(kind='revalidate',time=time.time(),plan_hash=ph(prepared.payload),total_ms=(time.perf_counter()-t)*1000,ok=v))
  return v
 def batches(self,prepared,job_id):
  it=super().batches(prepared,job_id);start=time.time();t=time.perf_counter();wait=yield_wait=0.;first=None;n=0
  try:
   while True:
    q=time.perf_counter()
    try:b=next(it)
    except StopIteration:
     wait+=time.perf_counter()-q;break
    wait+=time.perf_counter()-q
    if first is None:first=(time.perf_counter()-t)*1000
    n+=1;q=time.perf_counter();yield b;yield_wait+=time.perf_counter()-q
  finally:
   it.close()
   emit(dict(kind='stream',job_id=job_id,plan_hash=ph(prepared.payload),principal=prepared.principal,start=start,end=time.time(),first_ms=first,next_wait_ms=wait*1000,yield_wait_ms=yield_wait*1000,batches=n))
backend=ProbeBackend(service,8192)
server=GovernedFlightServer('grpc://172.168.22.25:8815',backend,audit=p/'audit.jsonl')
def stop(*_):threading.Thread(target=server.shutdown,daemon=True).start()
signal.signal(signal.SIGTERM,stop);signal.signal(signal.SIGINT,stop)
pid={'python':os.getpid(),'java':int(backend.spark._jvm.java.lang.management.ManagementFactory.getRuntimeMXBean().getName().split('@')[0])}
(p/'pid.json').write_text(json.dumps(pid));(p/'ready.json').write_text(json.dumps(dict(**pid,spark=backend.spark.version,port=server.port)))
try:server.serve()
finally:backend.spark.stop()
