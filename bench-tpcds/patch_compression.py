# -*- coding: utf-8 -*-
"""给 fgac-current/streaming/server.py 增加 Arrow IPC 压缩支持（移植自 fgac-compression 轨道）。
幂等：已打过则跳过。"""
from pathlib import Path

p = Path('/data1/lyb/fgac-lab/deploy/fgac-current/streaming/server.py')
src = p.read_text()

if 'compression_level' in src:
    print('already patched')
    raise SystemExit(0)

old_init = """    def __init__(self, location, backend, *, ticket_ttl=300, max_tickets=64, audit=None):
        super().__init__(location, middleware={"identity": IdentityFactory()})
        self.backend = backend
        self.ticket_ttl = ticket_ttl
        self.max_tickets = max_tickets
        self.tickets = {}
        self.lock = threading.Lock()
        self.scan_slot = threading.Semaphore(1)  # Prototype: one active scan.
        self.audit_path = Path(audit) if audit else None"""
new_init = """    def __init__(self, location, backend, *, ticket_ttl=300, max_tickets=64, audit=None,
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
        self.audit_path = Path(audit) if audit else None"""
assert old_init in src, "init 源码与预期不符"
src = src.replace(old_init, new_init)

old_stream = "return flight.GeneratorStream(prepared.schema, produce())"
new_stream = "return flight.GeneratorStream(prepared.schema, produce(), options=self.write_options)"
assert old_stream in src, "GeneratorStream 行未找到"
src = src.replace(old_stream, new_stream)

p.write_text(src)
print("compression support patched")
