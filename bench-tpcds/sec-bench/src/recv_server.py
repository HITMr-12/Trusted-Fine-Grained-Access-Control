"""MASK v2 E-side pull client. Old unauthenticated push receiver is retired.

fetch() is also used by the persistent E worker, so complete reception and
Catalog authorization are inside its end-to-end timer. EOF never commits.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import socket
import ssl
import time

from mask_protocol import (VERSION, MAX_FILES, MAX_FILE, MAX_TOTAL, LEASE_SECONDS,
                           identifier, digest, read_json, read_exact, send_json)


def fetch(root, request, *, host, port, ca, on_file=None):
    ident = identifier(request['request_id'])
    root = Path(root)
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    # A directory is a durable reservation. Never delete/reuse someone else's ID.
    out = root / ('run_' + ident)
    out.mkdir(mode=0o700)
    deadline = time.monotonic() + LEASE_SECONDS
    try:
        context = ssl.create_default_context(cafile=ca)
        context.minimum_version = ssl.TLSVersion.TLSv1_2
        with socket.create_connection((host, port), timeout=10) as raw:
            with context.wrap_socket(raw, server_hostname=host) as conn:
                send_json(conn, request, deadline)
                manifest = read_json(conn, deadline)
                count = manifest.get('file_count')
                if (manifest.get('type') != 'manifest' or manifest.get('version') != VERSION or
                        manifest.get('request_id') != ident or manifest.get('columns') != request['columns'] or
                        manifest.get('relation_id') != request['relation_id'] or
                        manifest.get('schema_version') != request['schema_version'] or
                        type(count) is not int or not 0 < count <= MAX_FILES):
                    raise ValueError('invalid manifest binding')
                names = [f'part-{i:06d}.parquet' for i in range(count)]
                if manifest.get('files') != names:
                    raise ValueError('invalid file manifest')
                (out / 'manifest.json').write_text(json.dumps(manifest))
                receipts, total = [], 0
                for name in names:
                    header = read_json(conn, deadline)
                    size = header.get('size')
                    if (header.get('type') != 'file' or header.get('name') != name or
                            type(size) is not int or not 0 < size <= MAX_FILE):
                        raise ValueError('invalid file frame')
                    total += size
                    if total > MAX_TOTAL:
                        raise ValueError('delivery byte limit exceeded')
                    checksum = hashlib.sha256()
                    pending = out / (name + '.part')
                    with pending.open('xb') as file:
                        remaining = size
                        while remaining:
                            data = read_exact(conn, min(remaining, 1024 * 1024), deadline)
                            checksum.update(data)
                            file.write(data)
                            remaining -= len(data)
                        file.flush()
                        os.fsync(file.fileno())
                    if checksum.hexdigest() != header.get('sha256'):
                        raise ValueError('file checksum mismatch')
                    pending.rename(out / name)
                    receipts.append({'name': name, 'size': size, 'sha256': checksum.hexdigest()})
                    if on_file is not None:
                        # Tentative computation is allowed; no caller may publish
                        # its result until this function validates the commit.
                        on_file(out / name)
                commit = read_json(conn, deadline)
                expected = {'type': 'commit', 'request_id': ident, 'files': receipts,
                            'total_bytes': total, 'manifest_sha256': digest(manifest)}
                if commit != expected:
                    raise ValueError('missing or mismatched commit')
                with (out / '.done.part').open('x') as marker:
                    json.dump(commit, marker)
                    marker.flush()
                    os.fsync(marker.fileno())
                (out / '.done.part').rename(out / '.done')
        return out, manifest
    except Exception as exc:
        (out / '.fail').write_text(type(exc).__name__)
        raise


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('root', type=Path)
    parser.add_argument('--request', required=True, type=Path, help='JSON without token; no business predicate')
    parser.add_argument('--host', required=True)
    parser.add_argument('--port', type=int, default=19041)
    parser.add_argument('--ca', required=True)
    args = parser.parse_args()
    request = json.loads(args.request.read_text())
    request['authorization'] = 'Bearer ' + os.environ['MASK_TOKEN']
    out, _ = fetch(args.root, request, host=args.host, port=args.port, ca=args.ca)
    print(json.dumps({'request_id': request['request_id'], 'committed_directory': str(out)}))


if __name__ == '__main__':
    main()
