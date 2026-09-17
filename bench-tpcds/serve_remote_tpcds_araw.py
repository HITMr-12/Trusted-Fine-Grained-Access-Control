"""TPC-DS bench Flight endpoint, Arrow-raw byte-pipe path, 2-stream edition.

R: Spark batches are written into TWO independent Arrow IPC streams (even/odd
batches) on local files while the job runs; produce() polls both and sends
interleaved binary frames tagged with a 1-byte stream id. Each stream is a
complete valid IPC stream (schema + its half of the batches + EOS), so the E
bridge can pipe each into its own ArrowSocketSource socket with zero parsing
and zero RecordBatch materialization. Java side unchanged.
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
CHUNK = 8 * 1024 * 1024

class RawFlightServer(GovernedFlightServer):
    """Same ticket/auth contract; do_get streams tagged chunks of TWO Arrow IPC
    byte streams (stream id in the first byte of each frame payload)."""

    def __init__(self, location, backend, *, audit=None):
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
            job_id = 'araw-' + secrets.token_hex(12)
            start = time.time()
            tmpdir = Path(tempfile.mkdtemp(prefix='fgac-araw-', dir=TMP_ROOT))
            paths = [tmpdir / ('s%d.arrow' % i) for i in range(2)]
            wstate = {'writers': None, 'done': False, 'error': None, 'write_ms': 0.0,
                      'batches': 0, 'rows': 0}
            cpu = usage()
            frames = wire_bytes = arrow_bytes = 0
            first_frame_ms = None
            status = 'aborted'

            def writer():
                t0 = time.perf_counter()
                writers = None
                try:
                    from streaming.spark_batches import iter_arrow_batches
                    for i, batch in enumerate(iter_arrow_batches(prepared.frame)):
                        if writers is None:
                            writers = [pa.ipc.new_stream(open(paths[0], 'wb'), batch.schema),
                                       pa.ipc.new_stream(open(paths[1], 'wb'), batch.schema)]
                            wstate['writers'] = True
                        writers[i % 2].write_batch(batch)
                        wstate['batches'] += 1
                        wstate['rows'] += batch.num_rows
                    for w in writers:
                        w.close()
                    wstate['write_ms'] = (time.perf_counter() - t0) * 1000
                except Exception as e:  # surfaced to the reader loop
                    wstate['error'] = repr(e)
                finally:
                    wstate['done'] = True

            wt = threading.Thread(target=writer, daemon=True)
            wt.start()
            offsets = [0, 0]
            try:
                while True:
                    if context.is_cancelled():
                        raise flight.FlightCancelledError('Client cancelled scan')
                    if time.monotonic() >= expires:
                        raise flight.FlightUnauthorizedError('Scan lease expired')
                    if wstate['error'] is not None:
                        raise flight.FlightInternalError('Scan failed: ' + wstate['error'])
                    progressed = False
                    for s in range(2):
                        if not paths[s].exists():
                            continue
                        size = paths[s].stat().st_size
                        if size <= offsets[s]:
                            continue
                        with open(paths[s], 'rb') as fh:
                            fh.seek(offsets[s])
                            data = fh.read(size - offsets[s])
                        offsets[s] = size
                        arrow_bytes += len(data)
                        view = memoryview(data)
                        for pos in range(0, len(view), CHUNK):
                            chunk = bytes([s]) + view[pos:pos + CHUNK]
                            if first_frame_ms is None:
                                first_frame_ms = (time.time() - start) * 1000
                            frames += 1
                            wire_bytes += len(chunk)
                            yield pa.RecordBatch.from_arrays(
                                [pa.array([chunk])], schema=FRAME_SCHEMA)
                        progressed = True
                    if wstate['done'] and all(
                            paths[s].exists() and offsets[s] >= paths[s].stat().st_size
                            for s in range(2)):
                        break
                    if not progressed:
                        time.sleep(0.005)
                status = 'complete'
            finally:
                shutil.rmtree(tmpdir, ignore_errors=True)
                self.scan_slot.release()
                emit({'kind': 'araw', 'time': time.time(), 'job_id': job_id,
                      'principal': prepared.principal, 'status': status,
                      'ipc_write_ms': wstate['write_ms'], 'batches': wstate['batches'],
                      'rows': wstate['rows'], 'frames': frames,
                      'wire_bytes': wire_bytes, 'arrow_bytes': arrow_bytes,
                      'first_frame_ms': first_frame_ms,
                      'elapsed_ms': (time.time() - start) * 1000,
                      'cpu_ms': (usage() - cpu) * 1000})
                self.audit({'job_id': job_id, 'principal': prepared.principal,
                            'policy_version': prepared.policy_version, 'status': status,
                            'rows': wstate['rows'], 'wire_bytes': wire_bytes})

        return flight.GeneratorStream(FRAME_SCHEMA, produce())

backend = Backend(service, 8192)
port = int(os.environ.get('FGAC_ARAW_PORT', '18839'))
server = RawFlightServer('grpc://172.168.22.23:%d' % port, backend,
                         audit=out / 'remote-audit-ARAW.jsonl')
pid = {'python': os.getpid(), 'java': int(backend.spark._jvm.java.lang.management.ManagementFactory
       .getRuntimeMXBean().getName().split('@')[0])}
jpid = pid['java']
(out / 'remote-ready-araw.json').write_text(json.dumps(dict(**pid, spark=backend.spark.version,
                                                             port=port)))
(r / 'manifests/fgac-tpcds-araw.pid').write_text(str(os.getpid()))

def stop(*_):
    threading.Thread(target=server.shutdown, daemon=True).start()

signal.signal(signal.SIGTERM, stop)
signal.signal(signal.SIGINT, stop)
try:
    server.serve()
finally:
    backend.spark.stop()
