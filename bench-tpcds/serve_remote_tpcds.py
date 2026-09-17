"""TPC-DS bench Flight endpoint: env-configurable port/run-dir clone of serve_remote.py.
复用 remote_service / streaming 的既有编译与授权链路；旧 18815 实例不受影响。"""
import os, json, time, threading, signal
from pathlib import Path
import remote_service as service
from streaming.server import SparkBackend, GovernedFlightServer

r = Path('/data1/lyb/fgac-lab')
port = int(os.environ.get('FGAC_FLIGHT_PORT', '18833'))
out = Path(os.environ.get('FGAC_RUN_DIR', str(r / 'runs/tpcds-bench')))
out.mkdir(parents=True, exist_ok=True)
lock = threading.Lock()
hz = os.sysconf('SC_CLK_TCK')
jpid = None

def usage():
    stat = Path('/proc', str(jpid), 'stat').read_text().split()
    return time.process_time() + (int(stat[13]) + int(stat[14])) / hz

def emit(row):
    with lock, (out / 'remote-stages.jsonl').open('a') as f:
        f.write(json.dumps(row) + '\n')

class Backend(SparkBackend):
    def prepare(self, payload, token):
        cpu = usage(); t = time.perf_counter(); x = super().prepare(payload, token)
        emit({'kind': 'prepare', 'time': time.time(), 'ms': (time.perf_counter() - t) * 1000,
              'cpu_ms': (usage() - cpu) * 1000, 'principal': x.principal}); return x
    def revalidate(self, prepared, token):
        cpu = usage(); t = time.perf_counter(); x = super().revalidate(prepared, token)
        emit({'kind': 'revalidate', 'time': time.time(), 'ms': (time.perf_counter() - t) * 1000,
              'cpu_ms': (usage() - cpu) * 1000, 'ok': x}); return x
    def batches(self, prepared, job_id):
        cpu = usage(); t = time.perf_counter(); n = rows = 0
        try:
            for batch in super().batches(prepared, job_id):
                n += 1; rows += batch.num_rows; yield batch
        finally:
            emit({'kind': 'stream', 'time': time.time(), 'ms': (time.perf_counter() - t) * 1000,
                  'cpu_ms': (usage() - cpu) * 1000, 'principal': prepared.principal,
                  'job_id': job_id, 'batches': n, 'rows': rows})

backend = Backend(service, 8192)
server = GovernedFlightServer(f'grpc://172.168.22.23:{port}', backend, audit=out / 'remote-audit.jsonl')
pid = {'python': os.getpid(), 'java': int(backend.spark._jvm.java.lang.management.ManagementFactory
       .getRuntimeMXBean().getName().split('@')[0])}
jpid = pid['java']
(out / 'remote-ready.json').write_text(json.dumps(dict(**pid, spark=backend.spark.version, port=port)))
(r / f'manifests/fgac-tpcds-bench.pid').write_text(str(os.getpid()))

def stop(*_):
    threading.Thread(target=server.shutdown, daemon=True).start()

signal.signal(signal.SIGTERM, stop); signal.signal(signal.SIGINT, stop)
try:
    server.serve()
finally:
    backend.spark.stop()
