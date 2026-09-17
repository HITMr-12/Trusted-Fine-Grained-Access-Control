"""TPC-DS bench Flight endpoint, Parquet-frame re-encode path.

One Spark on R decodes the source once, applies the compiled row policy in-engine,
and encodes the authorized frame to snappy Parquet itself (write.parquet) -- that is
the single encode. Each output part file is then streamed as one binary Flight frame
(schema: a single 'frame' binary column, one row per part file). Wire bytes are
therefore on-disk Parquet bytes; the E side decodes once inside its own Spark engine
via spark.read.parquet. No Arrow IPC row data ever crosses the wire.

Port: FGAC_FRAMES_PORT (default 18836). Env is inherited from FGAC_ENV_FROM_PID
(same mechanism as streaming/server.py --env-from-pid) when set.
"""
import os, sys, json, time, threading, signal, tempfile, shutil, secrets, hashlib
from pathlib import Path

_env_pid = int(os.environ.get('FGAC_ENV_FROM_PID', '0'))
if _env_pid:
    raw = Path('/proc/%d/environ' % _env_pid).read_bytes()
    os.environ.update(item.decode().split('=', 1) for item in raw.split(b'\0') if b'=' in item)

import pyarrow as pa
import pyarrow.flight as flight
import remote_service as service
from streaming.server import SparkBackend, GovernedFlightServer, IdentityFactory

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

FRAME_SCHEMA = pa.schema([('frame', pa.binary())])
TMP_ROOT = str(r / 'tmp')

class FramesFlightServer(GovernedFlightServer):
    """Same ticket/auth contract as GovernedFlightServer; do_get emits whole
    snappy-Parquet part files as binary frames instead of Arrow row batches."""

    def __init__(self, location, backend, *, audit=None):
        # Server send side is not gRPC-size-limited; the client raises its own
        # receive limit. Frames are additionally capped via maxRecordsPerFile.
        flight.FlightServerBase.__init__(self, location,
                                         middleware={'identity': IdentityFactory()})
        self.backend = backend
        self.ticket_ttl = 300
        self.max_tickets = 64
        self.tickets = {}
        self.lock = threading.Lock()
        self.scan_slot = threading.Semaphore(1)
        self.audit_path = Path(audit) if audit else None
        self.write_options = pa.ipc.IpcWriteOptions(compression=None)

    def get_flight_info(self, context, descriptor):
        info = super().get_flight_info(context, descriptor)
        return flight.FlightInfo(FRAME_SCHEMA, info.descriptor, info.endpoints, -1, -1)

    def do_get(self, context, ticket):
        token = context.get_middleware('identity').token
        owner = hashlib.sha256(token.encode()).digest()
        with self.lock:
            state = self.tickets.get(ticket.ticket)
        if state is None or state[0] <= time.monotonic():
            with self.lock:
                self.tickets.pop(ticket.ticket, None)
            raise flight.FlightUnauthorizedError('Unknown, consumed or expired ticket')
        expires, expected_owner, prepared = state
        if not secrets.compare_digest(owner, expected_owner):
            raise flight.FlightUnauthorizedError('Ticket does not belong to caller')
        if not self.backend.revalidate(prepared, token):
            with self.lock:
                self.tickets.pop(ticket.ticket, None)
            raise flight.FlightUnauthorizedError('Policy changed; request a new scan')
        with self.lock:
            if self.tickets.pop(ticket.ticket, None) is None:
                raise flight.FlightUnauthorizedError('Ticket already consumed')

        def produce():
            if not self.scan_slot.acquire(blocking=False):
                raise flight.FlightUnavailableError('Another scan is active')
            job_id = 'frames-' + secrets.token_hex(12)
            start = time.time()
            tmpdir = Path(tempfile.mkdtemp(prefix='fgac-frames-', dir=TMP_ROOT))
            frames = nbytes = 0
            read_ms = 0.0
            first_frame_ms = None
            status = 'aborted'
            cpu = usage()
            # v2 streaming: Spark writes in a background thread; each task commits
            # its part files the moment it finishes, so we poll the output dir and
            # send every committed file as a frame while the job is still running.
            write_done = threading.Event()
            write_ms = 0.0

            def do_write():
                nonlocal write_ms
                t = time.perf_counter()
                try:
                    prepared.frame.write.mode('overwrite').option('compression', 'snappy') \
                        .option('maxRecordsPerFile', 250000).parquet(str(tmpdir))
                finally:
                    write_ms = (time.perf_counter() - t) * 1000
                    write_done.set()

            threading.Thread(target=do_write, daemon=True).start()
            sent = set()
            try:
                while True:
                    done = write_done.is_set()
                    for f in sorted(tmpdir.rglob('*.parquet')):
                        if f in sent:
                            continue
                        if context.is_cancelled():
                            raise flight.FlightCancelledError('Client cancelled scan')
                        if time.monotonic() >= expires:
                            raise flight.FlightUnauthorizedError('Scan lease expired')
                        t = time.perf_counter()
                        data = f.read_bytes()
                        read_ms += (time.perf_counter() - t) * 1000
                        sent.add(f)
                        if len(data) < 500:
                            continue
                        batch = pa.RecordBatch.from_arrays([pa.array([data])], schema=FRAME_SCHEMA)
                        if first_frame_ms is None:
                            first_frame_ms = (time.time() - start) * 1000
                        frames += 1
                        nbytes += len(data)
                        yield batch
                    if done:
                        break
                    time.sleep(0.15)
                status = 'complete'
            finally:
                shutil.rmtree(tmpdir, ignore_errors=True)
                self.scan_slot.release()
                emit({'kind': 'frames', 'time': time.time(), 'job_id': job_id,
                      'principal': prepared.principal, 'status': status,
                      'spark_write_ms': write_ms, 'read_ms': read_ms,
                      'frames': frames, 'parquet_bytes': nbytes,
                      'first_frame_ms': first_frame_ms,
                      'elapsed_ms': (time.time() - start) * 1000,
                      'cpu_ms': (usage() - cpu) * 1000})
                self.audit({'job_id': job_id, 'principal': prepared.principal,
                            'policy_version': prepared.policy_version, 'status': status,
                            'frames': frames, 'parquet_bytes': nbytes,
                            'first_frame_ms': first_frame_ms,
                            'elapsed_ms': (time.time() - start) * 1000})
        return flight.GeneratorStream(FRAME_SCHEMA, produce(), options=self.write_options)

backend = Backend(service, 8192)
port = int(os.environ.get('FGAC_FRAMES_PORT', '18836'))
server = FramesFlightServer('grpc://172.168.22.23:%d' % port, backend,
                            audit=out / 'remote-audit-FRAMES.jsonl')
pid = {'python': os.getpid(), 'java': int(backend.spark._jvm.java.lang.management.ManagementFactory
       .getRuntimeMXBean().getName().split('@')[0])}
jpid = pid['java']
(out / 'remote-ready-frames.json').write_text(json.dumps(dict(**pid, spark=backend.spark.version,
                                                               port=port)))
(r / 'manifests/fgac-tpcds-frames.pid').write_text(str(os.getpid()))

def stop(*_):
    threading.Thread(target=server.shutdown, daemon=True).start()

signal.signal(signal.SIGTERM, stop)
signal.signal(signal.SIGINT, stop)
try:
    server.serve()
finally:
    backend.spark.stop()
