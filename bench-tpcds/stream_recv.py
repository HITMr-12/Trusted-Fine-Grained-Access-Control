# -*- coding: utf-8 -*-
"""Frame receiver for the streaming masking pipeline.
Reads framed records from stdin: u16 name_len + name + u64 payload_len + payload.
Writes each payload into <outdir>/<name>. Prints per-file receive timing.
Usage: stream_recv.py <outdir>
"""
import json, sys, time, struct
from pathlib import Path

outdir = Path(sys.argv[1])
outdir.mkdir(parents=True, exist_ok=True)
inp = sys.stdin.buffer
t_start = time.perf_counter()
got = 0
n = 0
while True:
    hdr = inp.read(2)
    if not hdr:
        break
    (nlen,) = struct.unpack('<H', hdr)
    name = inp.read(nlen).decode()
    (plen,) = struct.unpack('<Q', inp.read(8))
    data = inp.read(plen)
    assert len(data) == plen, 'short read on %s' % name
    t0 = time.perf_counter()
    tmp = outdir / (name + '.part')
    tmp.write_bytes(data)
    tmp.rename(outdir / name)          # atomic publish
    got += plen
    n += 1
    print(json.dumps({'file': name, 'bytes': plen, 'n': n, 'cum_bytes': got,
                      'cum_wall_s': round(time.perf_counter() - t_start, 2)}),
          flush=True)
(outdir / '.done').write_text('files=%d bytes=%d' % (n, got))
print('RECV_DONE files=%d bytes=%d wall_s=%.2f' %
      (n, got, time.perf_counter() - t_start), flush=True)
