"""TPC-DS bench Flight endpoint with codec A/B: one Spark, three servers sharing one scan slot.
Ports: FGAC_CODEC_PORT_NONE (default 18833, no compression), FGAC_CODEC_PORT_LZ4 (18834),
FGAC_CODEC_PORT_ZSTD (18835, level from FGAC_CODEC_ZSTD_LEVEL default 1).
"""
import os, json, time, threading, signal
from pathlib import Path
import remote_service as service
from streaming.server import SparkBackend, GovernedFlightServer

r = Path('/data1/lyb/fgac-lab')
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
profiles = [
    ('NONE', int(os.environ.get('FGAC_CODEC_PORT_NONE', '18833')), None, None),
    ('LZ4', int(os.environ.get('FGAC_CODEC_PORT_LZ4', '18834')), 'lz4', None),
    ('ZSTD', int(os.environ.get('FGAC_CODEC_PORT_ZSTD', '18835')), 'zstd',
     int(os.environ.get('FGAC_CODEC_ZSTD_LEVEL', '1'))),
]
servers = [GovernedFlightServer('grpc://172.168.22.23:%d' % port, backend,
                                audit=out / ('remote-audit-%s.jsonl' % name),
                                compression=codec, compression_level=level)
           for name, port, codec, level in profiles]
slot = threading.Semaphore(1)
for server in servers:
    server.scan_slot = slot
pid = {'python': os.getpid(), 'java': int(backend.spark._jvm.java.lang.management.ManagementFactory
       .getRuntimeMXBean().getName().split('@')[0])}
jpid = pid['java']
(out / 'remote-ready-codec.json').write_text(json.dumps(dict(**pid, spark=backend.spark.version,
                                                             profiles=profiles)))
(r / 'manifests/fgac-tpcds-bench.pid').write_text(str(os.getpid()))

def stop(*_):
    for server in servers:
        threading.Thread(target=server.shutdown, daemon=True).start()

signal.signal(signal.SIGTERM, stop); signal.signal(signal.SIGINT, stop)
try:
    threads = [threading.Thread(target=server.serve) for server in servers]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
finally:
    backend.spark.stop()
