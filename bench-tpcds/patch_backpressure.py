# -*- coding: utf-8 -*-
"""给 streaming/server.py 的 produce() 加服务端背压：限制在途 batch 数量与字节数，
避免慢消费时服务端无缓冲上限导致 R 节点（2GB 内存）RESOURCE_EXHAUSTED。"""
from pathlib import Path

p = Path('/data1/lyb/fgac-lab/deploy/fgac-current/streaming/server.py')
src = p.read_text()

old = """        def produce():
            if not self.scan_slot.acquire(blocking=False):
                raise flight.FlightUnavailableError("Another scan is active")
            job_id = "flight-" + secrets.token_hex(12)
            start = time.time()
            rows = batches = nbytes = 0
            first_batch_ms = None
            status = "aborted"
            iterator = self.backend.batches(prepared, job_id)
            try:
                for batch in iterator:
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
                try:
                    iterator.close()
                finally:
                    self.scan_slot.release()
                    self.audit({"job_id": job_id, "principal": prepared.principal,
                                "policy_version": prepared.policy_version,
                                "status": status, "rows": rows, "batches": batches,
                                "arrow_bytes": nbytes, "first_batch_ms": first_batch_ms,
                                "elapsed_ms": (time.time() - start) * 1000})
        return flight.GeneratorStream(prepared.schema, produce())"""

new = """        def produce():
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
        return flight.GeneratorStream(prepared.schema, produce())"""

assert old in src, "produce() 源码与预期不符，拒绝补丁"
p.write_text(src.replace(old, new))
print("patched", p)
