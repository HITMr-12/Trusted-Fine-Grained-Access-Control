"""Standalone authenticated Flight endpoint over the existing policy compiler."""
import argparse
import hashlib
import importlib
import json
import os
import secrets
import signal
import sys
import threading
import time
from dataclasses import dataclass
from pathlib import Path

import pyarrow as pa
import pyarrow.flight as flight

from streaming.spark_batches import iter_arrow_batches


def fingerprint(contract):
    governed = {key: contract[key] for key in ("principal", "schema_version", "policy")}
    return hashlib.sha256(json.dumps(governed, sort_keys=True).encode()).hexdigest()


class Identity(flight.ServerMiddleware):
    def __init__(self, token):
        self.token = token


class IdentityFactory(flight.ServerMiddlewareFactory):
    def start_call(self, info, headers):
        values = headers.get("authorization", [])
        if len(values) != 1:
            raise flight.FlightUnauthenticatedError("A bearer token is required")
        token = values[0]
        if isinstance(token, bytes):
            token = token.decode("utf-8")
        if not token.startswith("Bearer ") or len(token) <= 7:
            raise flight.FlightUnauthenticatedError("A bearer token is required")
        return Identity(token)


@dataclass
class Prepared:
    payload: dict
    frame: object
    schema: object
    contract_hash: str
    principal: str
    policy_version: str


class SparkBackend:
    def __init__(self, service, batch_rows=8192):
        if not os.environ.get("FGAC_ICEBERG_TABLE"):
            raise RuntimeError("The streaming prototype requires an Iceberg source")
        self.service = service
        self.spark = service.spark()
        self.spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", str(batch_rows))
        self.spark.sparkContext.setLogLevel("ERROR")

    def authorize(self, payload, token):
        try:
            request = self.service.SubplanRequest(**payload)
            if request.version != 2:
                raise ValueError("Unsupported protocol version")
            relation, ops, cols = self.service.inspect_plan(request.root)
            return self.service.authorize(token, relation, ops, cols, request.schema_version)
        except Exception as exc:
            status = getattr(exc, "status_code", None)
            if status == 401:
                raise flight.FlightUnauthenticatedError("Catalog rejected identity") from None
            if status == 403:
                raise flight.FlightUnauthorizedError("Catalog denied request") from None
            if status and status >= 500:
                raise flight.FlightUnavailableError("Catalog unavailable") from None
            raise flight.FlightServerError("Invalid request or authorization failed") from None

    def prepare(self, payload, token):
        contract = self.authorize(payload, token)
        source = self.service.fetch_source(contract)
        if not isinstance(source, str):
            raise flight.FlightServerError("Only table sources are supported")
        compiler = self.service.PlanCompiler(
            self.spark, Path("/unused"),
            policy_loader=lambda _: contract["policy"], source_loader=lambda _: source,
        )
        try:
            frame, _ = compiler.compile(payload["root"])
            # Keep the prototype independent of pandas/numpy installations.
            primitive = {"tinyint": pa.int8(), "smallint": pa.int16(),
                         "int": pa.int32(), "bigint": pa.int64(),
                         "float": pa.float32(), "double": pa.float64(),
                         "boolean": pa.bool_(), "string": pa.string(),
                         "binary": pa.binary(), "date": pa.date32()}
            fields = []
            for field in frame.schema.fields:
                dtype = field.dataType
                if dtype.typeName() == "decimal":
                    arrow_type = pa.decimal128(dtype.precision, dtype.scale)
                else:
                    arrow_type = primitive[dtype.simpleString()]
                fields.append(pa.field(field.name, arrow_type, field.nullable))
            schema = pa.schema(fields)
        except Exception:
            raise flight.FlightServerError("Unsupported scan plan or schema") from None
        return Prepared(payload, frame, schema, fingerprint(contract),
                        contract["principal"], str(contract["policy"]["version"]))

    def revalidate(self, prepared, token):
        return fingerprint(self.authorize(prepared.payload, token)) == prepared.contract_hash

    def batches(self, prepared, job_id):
        sc = self.spark.sparkContext
        sc.setJobGroup(job_id, "Flight governed scan", interruptOnCancel=True)
        try:
            yield from iter_arrow_batches(prepared.frame)
        finally:
            sc.cancelJobGroup(job_id)
            sc.setLocalProperty("spark.jobGroup.id", None)


class GovernedFlightServer(flight.FlightServerBase):
    def __init__(self, location, backend, *, ticket_ttl=300, max_tickets=64, audit=None,
                 compression=None, compression_level=None):
        super().__init__(location, middleware={"identity": IdentityFactory()})
        if compression not in (None, "lz4", "zstd"):
            raise ValueError("Unsupported compression codec")
        if compression_level is not None and compression != "zstd":
            raise ValueError("A compression level is only supported for zstd")
        codec = (pa.Codec(compression, compression_level=compression_level)
                 if compression_level is not None else compression)
        self.write_options = pa.ipc.IpcWriteOptions(compression=codec)
        self.compression = compression or "none"
        self.backend = backend
        self.ticket_ttl = ticket_ttl
        self.max_tickets = max_tickets
        self.tickets = {}
        self.lock = threading.Lock()
        self.scan_slot = threading.Semaphore(1)  # Prototype: one active scan.
        self.audit_path = Path(audit) if audit else None

    def audit(self, event):
        if self.audit_path:
            with self.lock, self.audit_path.open("a") as handle:
                handle.write(json.dumps(event) + "\n")

    def get_flight_info(self, context, descriptor):
        token = context.get_middleware("identity").token
        command = descriptor.command
        if not command or len(command) > 65536:
            raise flight.FlightServerError("Invalid scan descriptor")
        try:
            payload = json.loads(command)
            if not isinstance(payload, dict):
                raise ValueError()
        except (ValueError, TypeError):
            raise flight.FlightServerError("Invalid scan descriptor") from None
        prepared = self.backend.prepare(payload, token)
        ticket = secrets.token_urlsafe(32).encode()
        expires = time.monotonic() + self.ticket_ttl
        owner = hashlib.sha256(token.encode()).digest()
        with self.lock:
            self.tickets = {k: v for k, v in self.tickets.items() if v[0] > time.monotonic()}
            if len(self.tickets) >= self.max_tickets:
                raise flight.FlightUnavailableError("Too many pending scans")
            self.tickets[ticket] = (expires, owner, prepared)
        # Empty locations tells clients to use their existing connection. No
        # source object URL or source credentials are exposed in the envelope.
        return flight.FlightInfo(prepared.schema, descriptor,
                                 [flight.FlightEndpoint(flight.Ticket(ticket), [])], -1, -1)

    def do_get(self, context, ticket):
        token = context.get_middleware("identity").token
        owner = hashlib.sha256(token.encode()).digest()
        with self.lock:
            state = self.tickets.get(ticket.ticket)
        if state is None or state[0] <= time.monotonic():
            with self.lock:
                self.tickets.pop(ticket.ticket, None)
            raise flight.FlightUnauthorizedError("Unknown, consumed or expired ticket")
        expires, expected_owner, prepared = state
        if not secrets.compare_digest(owner, expected_owner):
            raise flight.FlightUnauthorizedError("Ticket does not belong to caller")
        if not self.backend.revalidate(prepared, token):
            with self.lock:
                self.tickets.pop(ticket.ticket, None)
            raise flight.FlightUnauthorizedError("Policy changed; request a new scan")
        with self.lock:
            if self.tickets.pop(ticket.ticket, None) is None:
                raise flight.FlightUnauthorizedError("Ticket already consumed")

        def produce():
            if not self.scan_slot.acquire(blocking=False):
                raise flight.FlightUnavailableError("Another scan is active")
            job_id = "flight-" + secrets.token_hex(12)
            start = time.time()
            rows = batches = nbytes = 0
            first_batch_ms = None
            status = "aborted"
            iterator = self.backend.batches(prepared, job_id)
            # Server-side backpressure: bound in-flight batches so a slow client
            # cannot make this process buffer without limit (small-RAM nodes).
            max_inflight_batches = int(os.environ.get("FGAC_MAX_INFLIGHT_BATCHES", "16"))
            max_inflight_bytes = int(os.environ.get("FGAC_MAX_INFLIGHT_BYTES", str(256 * 1024 * 1024)))
            from collections import deque
            buf = deque()
            cond = threading.Condition()
            state = {"bytes": 0, "done": False, "error": None}

            def feed():
                try:
                    for batch in iterator:
                        with cond:
                            while (not state["done"] and len(buf) >= max_inflight_batches
                                   ) or state["bytes"] >= max_inflight_bytes:
                                cond.wait(timeout=30)
                            if state["done"]:
                                return
                            buf.append(batch)
                            state["bytes"] += batch.nbytes
                            cond.notify()
                except BaseException as exc:  # noqa: BLE001 - relay to consumer
                    with cond:
                        state["error"] = exc
                        cond.notify()
                finally:
                    with cond:
                        state["fed"] = True
                        cond.notify()

            feeder = threading.Thread(target=feed, daemon=True)
            feeder.start()
            try:
                while True:
                    with cond:
                        while not buf and state["error"] is None and not state.get("fed"):
                            cond.wait(timeout=30)
                        if state["error"] is not None:
                            raise state["error"]
                        if not buf and state.get("fed"):
                            break
                        batch = buf.popleft()
                        state["bytes"] -= batch.nbytes
                        cond.notify()
                    if context.is_cancelled():
                        raise flight.FlightCancelledError("Client cancelled scan")
                    if time.monotonic() >= expires:
                        raise flight.FlightUnauthorizedError("Scan lease expired")
                    if not batch.schema.equals(prepared.schema, check_metadata=False):
                        raise flight.FlightServerError("Unexpected output schema")
                    if first_batch_ms is None:
                        first_batch_ms = (time.time() - start) * 1000
                    rows += batch.num_rows
                    batches += 1
                    nbytes += batch.nbytes
                    yield batch
                status = "complete"
            finally:
                with cond:
                    state["done"] = True
                    cond.notify_all()
                feeder.join(timeout=30)
                try:
                    iterator.close()
                finally:
                    self.scan_slot.release()
                    self.audit({"job_id": job_id, "principal": prepared.principal,
                                "policy_version": prepared.policy_version,
                                "status": status, "rows": rows, "batches": batches,
                                "arrow_bytes": nbytes, "first_batch_ms": first_batch_ms,
                                "elapsed_ms": (time.time() - start) * 1000})
        return flight.GeneratorStream(prepared.schema, produce(), options=self.write_options)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--service-root", required=True)
    parser.add_argument("--location", default="grpc://127.0.0.1:8815")
    parser.add_argument("--env-from-pid", type=int)
    parser.add_argument("--batch-rows", type=int, default=8192)
    parser.add_argument("--audit")
    parser.add_argument("--ready-file")
    args = parser.parse_args()
    if args.batch_rows < 1 or args.batch_rows > 65536:
        parser.error("batch-rows must be between 1 and 65536")
    if args.env_from_pid:
        # Read configuration into this process only. Never write or log secrets.
        raw = Path(f"/proc/{args.env_from_pid}/environ").read_bytes()
        os.environ.update(item.decode().split("=", 1) for item in raw.split(b"\0") if b"=" in item)
    sys.path.insert(0, args.service_root)
    service = importlib.import_module("remote.app")
    backend = SparkBackend(service, args.batch_rows)
    server = GovernedFlightServer(args.location, backend, audit=args.audit)
    def stop(*_):
        threading.Thread(target=server.shutdown, daemon=True).start()
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    if args.ready_file:
        Path(args.ready_file).write_text(json.dumps({"pid": os.getpid(), "port": server.port,
            "spark_version": backend.spark.version, "batch_rows": args.batch_rows,
            "java_pid": int(backend.spark._jvm.java.lang.management.ManagementFactory
                            .getRuntimeMXBean().getName().split("@")[0])}))
    try:
        server.serve()
    finally:
        backend.spark.stop()


if __name__ == "__main__":
    main()
