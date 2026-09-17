"""Benchmark-only Parquet-frame bridge, v2 streaming: Flight binary frames -> local
part files -> wave-by-wave Spark scans with partial digest aggregation.

Zero decode in Python. The per-wave Spark job decodes Parquet inside the E engine
(the single decode) and computes a partial digest (count + double-salted xxhash64
sums, which are arithmetically decomposable), so E-side consumption overlaps frame
arrival. dataframe(spark) blocks until the stream is consumed and returns the
combined digest dict (same shape as the worker's digest sink output).
"""
import json, os, time, threading, tempfile, shutil
from decimal import Decimal
from pathlib import Path
import pyarrow as pa
import pyarrow.flight as flight

COLS = ["ss_item_sk", "ss_store_sk", "ss_quantity", "ss_sales_price", "ss_net_paid"]
WAVE_FRAMES = 16  # start a Spark job every N frames


class FrameBridge:
    def __init__(self, plan, token):
        url = os.getenv('FGAC_FLIGHT_URL', 'grpc://172.168.22.23:18836')
        self.client = flight.FlightClient(
            url, generic_options=[(b'grpc.max_receive_message_length', -1),
                                  (b'grpc.max_send_message_length', -1)])
        self.options = flight.FlightCallOptions(
            timeout=600, headers=[(b'authorization', token.encode())])
        descriptor = flight.FlightDescriptor.for_command(json.dumps(plan).encode())
        self.info = self.client.get_flight_info(descriptor, self.options)
        if len(self.info.endpoints) != 1:
            raise RuntimeError('This client currently supports one scan endpoint')
        self.tmpdir = Path(tempfile.mkdtemp(prefix='fgac-frames-e-'))
        self.batches = self.rows = self.nbytes = 0
        self.waves = 0
        self.wave_ms = 0.
        self.disk_write_ms = 0.
        self.first_frame_at = None
        self.error = None
        self._lock = threading.Lock()
        self._pending = []      # part files written but not yet wave-scanned
        self._stream_done = False
        self._rows = 0
        self._sum1 = Decimal(0)
        self._sum2 = Decimal(0)
        self.thread = threading.Thread(target=self.pump, daemon=True)
        self.thread.start()

    def pump(self):
        try:
            reader = self.client.do_get(self.info.endpoints[0].ticket, self.options)
            idx = 0
            for chunk in reader:
                if chunk.data is None:
                    continue
                if self.first_frame_at is None:
                    self.first_frame_at = time.perf_counter()
                for cell in chunk.data.column(0).to_pylist():
                    if cell is None:
                        continue
                    t = time.perf_counter()
                    path = self.tmpdir / ('part-%05d.parquet' % idx)
                    with open(path, 'wb') as handle:
                        handle.write(cell)
                    self.disk_write_ms += (time.perf_counter() - t) * 1000
                    idx += 1
                    self.nbytes += len(cell)
                    with self._lock:
                        self._pending.append(path)
                self.batches += 1
                self.rows += chunk.data.num_rows
            try:
                reader.cancel()
            except Exception:
                pass
        except BaseException as exc:
            self.error = exc
        finally:
            with self._lock:
                self._stream_done = True

    def _drain(self, spark, all_remaining):
        with self._lock:
            batch = self._pending
            self._pending = []
        if not batch:
            return
        from pyspark.sql import functions as F
        cols = [F.col(c) for c in COLS]
        t = time.perf_counter()
        df = spark.read.parquet(*[str(p) for p in batch])
        row = df.select(F.xxhash64(*cols).cast('decimal(38,0)').alias('h1'),
                        F.xxhash64(F.lit(9173), *cols).cast('decimal(38,0)').alias('h2')) \
                .agg(F.count('*').alias('rows'), F.sum('h1').alias('sum1'),
                     F.sum('h2').alias('sum2')).first()
        self.wave_ms += (time.perf_counter() - t) * 1000
        self.waves += 1
        self._rows += int(row['rows'])
        if row['sum1'] is not None:
            self._sum1 += row['sum1']
            self._sum2 += row['sum2']

    def dataframe(self, spark):
        """Consume the stream wave-by-wave; returns the combined digest dict."""
        while True:
            if self.error is not None:
                raise RuntimeError('Frame bridge failed') from self.error
            with self._lock:
                n = len(self._pending)
                done = self._stream_done
            if n >= WAVE_FRAMES:
                self._drain(spark, False)
            elif done:
                self._drain(spark, True)
                break
            else:
                time.sleep(0.05)
        return {'rows': str(self._rows), 'sum1': str(self._sum1), 'sum2': str(self._sum2)}

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
