"""Small streaming client SDK; yields RecordBatch instead of collecting a table."""
import json
import threading
import time
from collections import deque
from contextlib import contextmanager

import pyarrow.flight as flight


class GovernedClient:
    def __init__(self, location, token, timeout=120, tls_ca=None):
        if tls_ca is not None:
            with open(tls_ca, 'rb') as handle:
                self.client = flight.FlightClient(location, tls_root_certs=handle.read())
        else:
            self.client = flight.FlightClient(location)
        self.options = flight.FlightCallOptions(
            timeout=timeout, headers=[(b"authorization", token.encode())])

    def describe(self, plan):
        descriptor = flight.FlightDescriptor.for_command(json.dumps(plan).encode())
        return self.client.get_flight_info(descriptor, self.options)

    def prefetch(self, info, *, max_batches=2, max_bytes=16 * 1024 * 1024, with_meta=False):
        """Start a scan now, while the caller prepares its consuming engine.

        This consumes the ticket and performs the normal DoGet authorization.
        Close the returned context manager even if engine preparation fails.
        The original synchronous scan API remains available.
        """
        if len(info.endpoints) != 1:
            raise RuntimeError("This client currently supports one scan endpoint")
        if max_batches < 1 or max_bytes < 1:
            raise ValueError("Prefetch limits must be positive")
        reader = self.client.do_get(info.endpoints[0].ticket, self.options)
        return PrefetchedBatches(reader, max_batches=max_batches, max_bytes=max_bytes,
                                 with_meta=with_meta)

    @contextmanager
    def scan(self, plan):
        info = self.describe(plan)
        if len(info.endpoints) != 1:
            raise RuntimeError("This client currently supports one scan endpoint")
        reader = self.client.do_get(info.endpoints[0].ticket, self.options)
        try:
            yield info.schema, reader
        finally:
            reader.cancel()

    def close(self):
        self.client.close()


def iter_batches(reader):
    for chunk in reader:
        if chunk.data is not None:
            yield chunk.data


def iter_chunks(reader):
    """SEC mode: preserve app_metadata chunk-verification markers."""
    for chunk in reader:
        if chunk.data is not None:
            yield chunk.data, chunk.app_metadata


class PrefetchedBatches:
    """Engine-independent bounded queue of unchanged Arrow RecordBatches.

    Limits apply to queued batches, not Flight/Spark internal buffers. One batch
    may exceed the byte budget when the queue is empty; the producer can also
    hold one incoming batch while waiting for queue capacity. No rows are
    converted, reordered, cached across queries, or silently dropped on error.
    """
    def __init__(self, reader, *, max_batches=2, max_bytes=16 * 1024 * 1024, with_meta=False):
        if max_batches < 1 or max_bytes < 1:
            raise ValueError("Prefetch limits must be positive")
        self.reader = reader
        self.max_batches, self.max_bytes = max_batches, max_bytes
        self.with_meta = with_meta
        self._condition = threading.Condition()
        self._queue = deque()
        self._bytes = 0
        self._closed = self._finished = False
        self._error = None
        self.first_batch_at = None
        self.read_ms = self.backpressure_ms = 0.0
        self.peak_queued_batches = self.peak_queued_bytes = 0
        self._thread = threading.Thread(target=self._produce, name="flight-prefetch", daemon=True)
        self._thread.start()

    def _produce(self):
        try:
            iterator = iter(iter_chunks(self.reader)) if self.with_meta else iter(iter_batches(self.reader))
            while True:
                started = time.perf_counter()
                try:
                    item = next(iterator)
                except StopIteration:
                    self.read_ms += (time.perf_counter() - started) * 1000
                    break
                self.read_ms += (time.perf_counter() - started) * 1000
                batch = item[0] if self.with_meta else item
                if self.first_batch_at is None:
                    self.first_batch_at = time.perf_counter()
                size = batch.nbytes
                started = time.perf_counter()
                with self._condition:
                    while not self._closed and (
                        len(self._queue) >= self.max_batches
                        or (self._queue and self._bytes + size > self.max_bytes)
                    ):
                        self._condition.wait()
                    self.backpressure_ms += (time.perf_counter() - started) * 1000
                    if self._closed:
                        break
                    self._queue.append(item)
                    self._bytes += size
                    self.peak_queued_batches = max(self.peak_queued_batches, len(self._queue))
                    self.peak_queued_bytes = max(self.peak_queued_bytes, self._bytes)
                    self._condition.notify_all()
        except Exception as exc:
            with self._condition:
                if not self._closed:
                    self._error = exc
        finally:
            with self._condition:
                self._finished = True
                self._condition.notify_all()

    def __iter__(self):
        return self

    def __next__(self):
        with self._condition:
            while not self._queue and not self._finished and not self._closed:
                self._condition.wait()
            if self._closed:
                raise StopIteration
            if self._queue:
                item = self._queue.popleft()
                self._bytes -= (item[0] if self.with_meta else item).nbytes
                self._condition.notify_all()
                return item
            if self._error is not None:
                raise self._error
            raise StopIteration

    def close(self):
        with self._condition:
            self._closed = True
            self._queue.clear()
            self._bytes = 0
            self._condition.notify_all()
        try:
            self.reader.cancel()
        finally:
            self._thread.join(timeout=5)
        if self._thread.is_alive():
            raise RuntimeError("Flight prefetch did not stop after cancellation")

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()
