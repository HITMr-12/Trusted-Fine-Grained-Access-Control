"""Transport tests run without Spark, a Catalog or source data."""
import threading
import time
import unittest

import pyarrow as pa
import pyarrow.flight as flight

from streaming.client import GovernedClient, PrefetchedBatches, iter_batches
from streaming.server import GovernedFlightServer, Prepared


class FakeBackend:
    def __init__(self):
        self.version = 1
        self.started = 0
        self.started_event = threading.Event()
        self.schema = pa.schema([("value", pa.int32())])

    def prepare(self, plan, token):
        if token not in ("Bearer alice", "Bearer bob"):
            raise flight.FlightUnauthenticatedError("Invalid identity")
        return Prepared(plan, None, self.schema, str(self.version), token[7:], str(self.version))

    def revalidate(self, prepared, token):
        return prepared.contract_hash == str(self.version)

    def batches(self, prepared, job_id):
        self.started += 1
        self.started_event.set()
        if prepared.payload.get("empty"):
            return
        yield pa.record_batch([[1, 2]], schema=self.schema)
        if prepared.payload.get("fail"):
            raise RuntimeError("simulated producer failure")
        yield pa.record_batch([[3]], schema=self.schema)


class StreamingTests(unittest.TestCase):
    def setUp(self):
        self.backend = FakeBackend()
        self.server = GovernedFlightServer("grpc://127.0.0.1:0", self.backend)
        self.thread = threading.Thread(target=self.server.serve, daemon=True)
        self.thread.start()
        self.location = "grpc://127.0.0.1:" + str(self.server.port)
        self.client = GovernedClient(self.location, "Bearer alice")

    def tearDown(self):
        self.client.close()
        self.server.shutdown()
        self.thread.join(timeout=5)

    def test_metadata_does_not_execute_scan(self):
        info = self.client.describe({})
        self.assertEqual(self.backend.started, 0)
        self.assertEqual(info.schema, self.backend.schema)
        self.assertEqual(len(info.endpoints), 1)

    def test_multiple_batches_and_empty(self):
        with self.client.scan({}) as (_, reader):
            batches = list(iter_batches(reader))
        self.assertEqual([b.num_rows for b in batches], [2, 1])
        with self.client.scan({"empty": True}) as (schema, reader):
            self.assertEqual(list(iter_batches(reader)), [])
            self.assertEqual(schema, self.backend.schema)

    def test_invalid_identity(self):
        other = GovernedClient(self.location, "Bearer invalid")
        try:
            with self.assertRaises(flight.FlightUnauthenticatedError):
                other.describe({})
        finally:
            other.close()

    def test_missing_identity(self):
        with self.assertRaises(flight.FlightUnauthenticatedError):
            self.client.client.get_flight_info(flight.FlightDescriptor.for_command(b"{}"))

    def test_ticket_owner_and_single_use(self):
        info = self.client.describe({})
        ticket = info.endpoints[0].ticket
        other = GovernedClient(self.location, "Bearer bob")
        try:
            with self.assertRaises(flight.FlightUnauthorizedError):
                other.client.do_get(ticket, other.options)
        finally:
            other.close()
        reader = self.client.client.do_get(ticket, self.client.options)
        self.assertEqual(sum(b.num_rows for b in iter_batches(reader)), 3)
        with self.assertRaises(flight.FlightUnauthorizedError):
            self.client.client.do_get(ticket, self.client.options)

    def test_expiry(self):
        self.server.ticket_ttl = 0.01
        info = self.client.describe({})
        time.sleep(0.03)
        with self.assertRaises(flight.FlightUnauthorizedError):
            self.client.client.do_get(info.endpoints[0].ticket, self.client.options)

    def test_policy_change(self):
        info = self.client.describe({})
        self.backend.version = 2
        with self.assertRaises(flight.FlightUnauthorizedError):
            self.client.client.do_get(info.endpoints[0].ticket, self.client.options)

    def test_partial_failure_is_not_success(self):
        with self.assertRaises(flight.FlightError):
            with self.client.scan({"fail": True}) as (_, reader):
                list(iter_batches(reader))

    def test_prefetch_starts_before_consumption_and_preserves_rows(self):
        with self.client.prefetch(self.client.describe({})) as batches:
            self.assertTrue(self.backend.started_event.wait(3))
            self.assertEqual([b.to_pydict() for b in batches],
                             [{"value": [1, 2]}, {"value": [3]}])

    def test_prefetch_empty_and_partial_failure(self):
        with self.client.prefetch(self.client.describe({"empty": True})) as batches:
            self.assertEqual(list(batches), [])
        with self.assertRaises(flight.FlightError):
            with self.client.prefetch(self.client.describe({"fail": True})) as batches:
                list(batches)

    def test_prefetch_keeps_policy_revalidation(self):
        info = self.client.describe({})
        self.backend.version += 1
        with self.assertRaises(flight.FlightUnauthorizedError):
            self.client.prefetch(info)

    def test_prefetch_limits_do_not_consume_ticket(self):
        info = self.client.describe({})
        with self.assertRaises(ValueError):
            self.client.prefetch(info, max_batches=0)
        with self.client.prefetch(info) as batches:
            self.assertEqual(sum(b.num_rows for b in batches), 3)


class PrefetchQueueTests(unittest.TestCase):
    def test_close_cancels_blocked_read(self):
        entered, cancelled = threading.Event(), threading.Event()
        class Reader:
            def __iter__(self):
                entered.set()
                cancelled.wait(5)
                return
                yield
            def cancel(self):
                cancelled.set()
        stream = PrefetchedBatches(Reader())
        self.assertTrue(entered.wait(3))
        stream.close()
        self.assertTrue(cancelled.is_set())
        self.assertFalse(stream._thread.is_alive())

    def test_backpressure_and_close_with_full_queue(self):
        reached_third = threading.Event()
        class Reader:
            cancelled = False
            def __iter__(self):
                for i in range(100):
                    if i == 2:
                        reached_third.set()
                    yield type("Chunk", (), {"data": pa.record_batch([[i]], names=["x"])})()
            def cancel(self):
                self.cancelled = True
        reader = Reader()
        stream = PrefetchedBatches(reader, max_batches=2, max_bytes=16)
        try:
            self.assertTrue(reached_third.wait(3))
            self.assertEqual(stream.peak_queued_batches, 2)
            self.assertLessEqual(stream.peak_queued_bytes, 16)
        finally:
            stream.close()
        self.assertTrue(reader.cancelled)
        self.assertFalse(stream._thread.is_alive())

    def test_oversized_batch_and_string_null_values(self):
        batch = pa.record_batch([["long string", None]], names=["text"])
        class Reader:
            def __iter__(self):
                yield type("Chunk", (), {"data": batch})()
            def cancel(self):
                pass
        with PrefetchedBatches(Reader(), max_batches=2, max_bytes=1) as batches:
            result = list(batches)
            self.assertEqual(result, [batch])
            self.assertEqual(batches.peak_queued_batches, 1)


if __name__ == "__main__":
    unittest.main()
