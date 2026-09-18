"""MASK v2 governed scan server. Pull over TLS; never accept SQL or callbacks."""
import argparse
import hashlib
import json
import os
import re
import socket
import ssl
import time
import multiprocessing
from collections import deque
from concurrent.futures import ProcessPoolExecutor, wait
from pathlib import Path
from urllib.parse import urlparse

import requests

from mask_e2e_producer import source_manifest, validate_contract, produce_file
from mask_protocol import (VERSION, MAX_TOTAL, LEASE_SECONDS, identifier, digest,
                           read_json, send_json, send)


def authorize(url, request, source):
    # Same Catalog contract as FGAC; no business filter is submitted.
    response = requests.post(url.rstrip('/') + '/v2/authorize',
        headers={'Authorization': request['authorization']}, timeout=5,
        allow_redirects=False,
        json={'relation_id': request['relation_id'],
              'schema_version': request['schema_version'],
              'requested_columns': request['columns'],
              'requested_operators': ['governed_scan', 'project']})
    if response.status_code != 200:
        raise PermissionError('Catalog denied request or is unavailable')
    contract = response.json()
    validate_contract(contract, request, source)
    return contract


def validate_request(request):
    expected = {'version', 'request_id', 'authorization', 'relation_id', 'schema_version', 'columns'}
    if set(request) != expected or request['version'] != VERSION:
        raise ValueError('MASK v2 scan only: business predicates/callbacks are not accepted')
    identifier(request['request_id'])
    token = request['authorization']
    if not isinstance(token, str) or not token.startswith('Bearer ') or not token[7:].strip():
        raise PermissionError('Bearer token required')
    cols = request['columns']
    if (not isinstance(cols, list) or not 0 < len(cols) <= 128 or
            not all(isinstance(c, str) and re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*', c) for c in cols)
            or len(set(cols)) != len(cols)):
        raise ValueError('invalid projection')


def parallel_files(pool, args, root, source, columns, policy, deadline):
    """Reserve output bytes before submitting; never queue the entire dataset."""
    pending, index, reserved = deque(), 0, 0
    sizes = [4 + len(bytes.fromhex(e['skeleton']['footer'])) +
             sum(len(bytes.fromhex(p['hdr'])) + p['plen'] for p in e['skeleton']['pages'])
             for e in source['files']]
    if max(sizes) > args.max_inflight_bytes:
        raise ValueError('one file exceeds in-flight output budget')
    try:
        while index < len(sizes) or pending:
            while (index < len(sizes) and len(pending) < args.max_inflight and
                   reserved + sizes[index] <= args.max_inflight_bytes):
                future = pool.submit(produce_file, root, source['files'][index], columns, policy)
                pending.append((index, future, sizes[index]))
                reserved += sizes[index]
                index += 1
            current, future, size = pending[0]
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError('production lease expired')
            payload = future.result(timeout=remaining)
            if len(payload) != size:
                raise ValueError('producer output differs from reserved skeleton size')
            yield current, payload
            # Free the sent future before scheduling its replacement.
            pending.popleft()
            reserved -= size
            del payload, future
    finally:
        futures = [f for _, f, _ in pending]
        for future in futures:
            future.cancel()
        # Quiesce this request before admitting another one into the same pool.
        wait(futures)


def handle(conn, args, root, source, pool):
    deadline = time.monotonic() + LEASE_SECONDS
    request = read_json(conn, deadline)
    validate_request(request)
    contract = authorize(args.catalog_url, request, source)
    # Durable request-ID reservation; repeated IDs never reuse prior delivery.
    reservation = args.state_dir / request['request_id']
    with reservation.open('x') as file:
        file.write('reserved\n')
    contract_hash = digest(contract)
    def revalidate():
        if time.monotonic() >= deadline:
            raise TimeoutError('authorization lease expired')
        if digest(authorize(args.catalog_url, request, source)) != contract_hash:
            raise PermissionError('Catalog contract changed')
    revalidate()
    manifest = {'type': 'manifest', 'version': VERSION, 'request_id': request['request_id'],
                'columns': request['columns'], 'principal': contract['principal'],
                'relation_id': request['relation_id'], 'schema_version': request['schema_version'],
                'policy_version': contract['policy']['version'], 'contract_sha256': contract_hash,
                'source_sha256': digest(source), 'file_count': len(source['files']),
                'files': [f'part-{i:06d}.parquet' for i in range(len(source['files']))]}
    send_json(conn, manifest, deadline)
    receipts, total = [], 0
    produced = parallel_files(pool, args, root, source, request['columns'], contract['policy'], deadline)
    try:
        for index, payload in produced:
            revalidate()  # before releasing this file, not merely before preparing it
            total += len(payload)
            if total > MAX_TOTAL:
                raise ValueError('delivery exceeds byte limit')
            receipt = {'name': manifest['files'][index], 'size': len(payload),
                       'sha256': hashlib.sha256(payload).hexdigest() if args.sec == 'on' else None}
            send_json(conn, {'type': 'file', **receipt}, deadline)
            send(conn, payload, deadline)
            receipts.append(receipt)
            del payload
    finally:
        produced.close()
    revalidate()
    send_json(conn, {'type': 'commit', 'request_id': request['request_id'],
                    'files': receipts, 'total_bytes': total,
                    'manifest_sha256': digest(manifest)}, deadline)
    reservation.write_text('committed\n')
    print(json.dumps({'request_id': request['request_id'], 'status': 'committed',
                      'principal': contract['principal'], 'bytes': total}), flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source-manifest', required=True, type=Path)
    parser.add_argument('--state-dir', required=True, type=Path)
    parser.add_argument('--catalog-url', default=os.environ.get('POLARIS_URL', 'http://127.0.0.1:18184'))
    parser.add_argument('--cert')
    parser.add_argument('--key')
    parser.add_argument('--sec', choices=['on', 'off'], default='on')
    parser.add_argument('--host', default='127.0.0.1')
    parser.add_argument('--port', type=int, default=19041)
    parser.add_argument('--workers', type=int, default=16)
    parser.add_argument('--max-inflight', type=int, default=16)
    parser.add_argument('--max-inflight-bytes', type=int, default=512 * 1024 * 1024)
    args = parser.parse_args()
    if min(args.workers, args.max_inflight, args.max_inflight_bytes) <= 0:
        parser.error('worker and backpressure limits must be positive')
    url = urlparse(args.catalog_url)
    if url.scheme != 'https' and not (url.scheme == 'http' and url.hostname in ('127.0.0.1', 'localhost', '::1')):
        parser.error('Catalog requires HTTPS or loopback HTTP')
    root, source = source_manifest(args.source_manifest)
    args.state_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
    context = None
    if args.sec == 'on':
        if not args.cert or not args.key:
            parser.error('--sec on requires --cert and --key')
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.minimum_version = ssl.TLSVersion.TLSv1_2
        context.load_cert_chain(args.cert, args.key)
    with ProcessPoolExecutor(max_workers=args.workers, mp_context=multiprocessing.get_context('spawn')) as pool, \
            socket.create_server((args.host, args.port)) as server:
        list(pool.map(int, range(args.workers)))
        print('R_SERVER_READY protocol=2 sec=%s workers=%d max_inflight=%d bytes=%d' %
              (args.sec, args.workers, args.max_inflight, args.max_inflight_bytes), flush=True)
        while True:
            raw, _ = server.accept()
            raw.settimeout(30)
            try:
                if context is not None:
                    conn = context.wrap_socket(raw, server_side=True)
                else:
                    conn = raw
                with conn:
                    try:
                        handle(conn, args, root, source, pool)
                    except Exception as exc:
                        # Never log request bodies or tokens, nor expose paths.
                        print('R_REQUEST_FAILED ' + type(exc).__name__, flush=True)
                        try:
                            send_json(conn, {'type': 'error', 'message': 'scan rejected or incomplete'},
                                      time.monotonic() + 5)
                        except Exception:
                            pass
            except (OSError, ssl.SSLError):
                raw.close()


if __name__ == '__main__':
    main()
