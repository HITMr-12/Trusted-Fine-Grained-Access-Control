"""Benchmark-only Parquet-frame bridge: Flight binary frames -> local part files ->
spark.read.parquet. Zero decode in Python; the single decode happens inside the E
Spark engine's native Parquet reader. v1: frames are fully materialized to a local
directory before the engine scan starts (no transfer/scan overlap).
"""
import os, time, threading, tempfile, shutil
from pathlib import Path
import pyarrow as pa
import pyarrow.flight as flight


class FrameBridge:
    def __init__(self, plan, token):
        url = os.getenv('FGAC_FLIGHT_URL', 'grpc://172.168.22.23:18836')
        self.client = flight.FlightClient(
            url, generic_options=[(b'grpc.max_receive_message_length', -1),
                                  (b'grpc.max_send_message_length', -1)])
        self.options = flight.FlightCallOptions(
            timeout=600, headers=[(b'authorization', token.encode())])
        descriptor = flight.FlightDescriptor.for_command(
            __import__('json').dumps(plan).encode())
        self.info = self.client.get_flight_info(descriptor, self.options)
        if len(self.info.endpoints) != 1:
            raise RuntimeError('This client currently supports one scan endpoint')
        self.schema = self.info.schema  # advertised: single binary 'frame' column
        self.tmpdir = Path(tempfile.mkdtemp(prefix='fgac-frames-e-'))
        self.batches = self.rows = self.nbytes = 0
        self.next_wait_ms = 0.
        self.disk_write_ms = 0.
        self.doget_ms = 0.
        self.first_batch_at = None
        self.error = None
        self.thread = threading.Thread(target=self.pump, daemon=True)
        self.thread.start()

    def pump(self):
        files = []
        try:
            q = time.perf_counter()
            reader = self.client.do_get(self.info.endpoints[0].ticket, self.options)
            self.doget_ms = (time.perf_counter() - q) * 1000
            idx = 0
            for chunk in reader:
                if chunk.data is None:
                    continue
                if self.first_batch_at is None:
                    self.first_batch_at = time.perf_counter()
                for cell in chunk.data.column(0).to_pylist():
                    if cell is None:
                        continue
                    t = time.perf_counter()
                    path = self.tmpdir / ('part-%05d.parquet' % idx)
                    with open(path, 'wb') as handle:
                        handle.write(cell)
                    self.disk_write_ms += (time.perf_counter() - t) * 1000
                    files.append(path)
                    idx += 1
                    self.nbytes += len(cell)
                self.batches += 1
                self.rows += chunk.data.num_rows
            reader.cancel()  # graceful close of the stream
        except BaseException as exc:
            self.error = exc
        self.files = files

    def dataframe(self, spark):
        self.thread.join()
        if self.error:
            raise RuntimeError('Frame bridge failed') from self.error
        if not self.files:
            raise RuntimeError('Frame bridge received no Parquet frames')
        return spark.read.parquet(str(self.tmpdir))

    def finish(self):
        self.thread.join(600)
        if self.thread.is_alive():
            raise RuntimeError('Frame bridge did not finish')
        if self.error:
            raise RuntimeError('Frame bridge failed') from self.error

    def close(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)
        try:
            self.client.close()
        except Exception:
            pass
