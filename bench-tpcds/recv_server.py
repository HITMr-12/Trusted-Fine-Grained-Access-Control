# -*- coding: utf-8 -*-
"""Resident E-side frame receiver (replaces per-query nc + stream_recv).
Listens on a TCP port; each inbound connection carries one query's stream.
First framed record must be the run marker: name '@run@<label>', empty payload.
Files go to <root>/run_<label>/ with .part -> atomic rename publish; on EOF a
.done marker is written and the server loops for the next connection.
Run: python3 recv_server.py <root> [port]
"""
import socket, struct, sys, time
from pathlib import Path

root = Path(sys.argv[1])
port = int(sys.argv[2]) if len(sys.argv) > 2 else 19042
root.mkdir(parents=True, exist_ok=True)

srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
srv.bind(('0.0.0.0', port))
srv.listen(4)
print('RECV_SERVER_READY root=%s port=%d' % (root, port), flush=True)


def read_exact(f, n):
    buf = b''
    while len(buf) < n:
        chunk = f.read(n - len(buf))
        if not chunk:
            raise EOFError('short read')
        buf += chunk
    return buf


while True:
    conn, addr = srv.accept()
    t0 = time.perf_counter()
    try:
        f = conn.makefile('rb')
        outdir = None
        n = got = 0
        while True:
            hdr = f.read(2)
            if not hdr:
                break
            (nlen,) = struct.unpack('<H', read_exact(f, 2) if not hdr else hdr)
            name = read_exact(f, nlen).decode()
            (plen,) = struct.unpack('<Q', read_exact(f, 8))
            if name.startswith('@run@'):
                read_exact(f, plen)
                label = name[5:]
                outdir = root / ('run_' + label)
                outdir.mkdir(parents=True, exist_ok=True)
                for old in outdir.glob('*'):
                    old.unlink()
                continue
            data = read_exact(f, plen)
            tmp = outdir / (name + '.part')
            tmp.write_bytes(data)
            tmp.rename(outdir / name)
            got += plen
            n += 1
        if outdir is not None:
            (outdir / '.done').write_text('files=%d bytes=%d' % (n, got))
        print('RECV_RUN label=%s files=%d bytes=%d wall_s=%.2f'
              % (label, n, got, time.perf_counter() - t0), flush=True)
    except EOFError:
        # mid-stream failure: mark the run as failed so the consumer aborts
        # instead of waiting for .done forever
        try:
            if outdir is not None:
                (outdir / '.fail').write_text('short read from %s' % (addr,))
        except Exception:
            pass
        print('RECV_ERR short read from %s' % (addr,), flush=True)
    except Exception as e:
        try:
            if outdir is not None:
                (outdir / '.fail').write_text('recv error: %r' % e)
        except Exception:
            pass
        print('RECV_ERR %r' % e, flush=True)
    finally:
        conn.close()
