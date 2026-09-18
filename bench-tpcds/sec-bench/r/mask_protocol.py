"""MASK v2: bounded JSON headers over TLS, explicit commit, no EOF success."""
import hashlib
import json
import re
import struct
import time

VERSION = 2
VALID_COLUMN = '__mask_authorized__'
MAX_HEADER = 1024 * 1024
MAX_FILE = 512 * 1024 * 1024
MAX_FILES = 4096
MAX_TOTAL = 64 * 1024 * 1024 * 1024
LEASE_SECONDS = 300


def identifier(value):
    if not isinstance(value, str) or not re.fullmatch(r'[0-9a-f]{32}', value):
        raise ValueError('request_id must be a fresh UUID hex string')
    return value


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()


def digest(value):
    return hashlib.sha256(canonical(value)).hexdigest()


def budget(sock, deadline):
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError('request lease expired')
    sock.settimeout(min(30, remaining))


def read_exact(sock, size, deadline):
    data = bytearray()
    while len(data) < size:
        budget(sock, deadline)
        part = sock.recv(min(size - len(data), 1024 * 1024))
        if not part:
            raise EOFError('connection ended before explicit commit')
        data.extend(part)
    return bytes(data)


def send(sock, data, deadline):
    for offset in range(0, len(data), 1024 * 1024):
        budget(sock, deadline)
        sock.sendall(data[offset:offset + 1024 * 1024])


def send_json(sock, value, deadline):
    data = canonical(value)
    if len(data) > MAX_HEADER:
        raise ValueError('header too large')
    send(sock, struct.pack('!I', len(data)) + data, deadline)


def read_json(sock, deadline):
    size, = struct.unpack('!I', read_exact(sock, 4, deadline))
    if not 0 < size <= MAX_HEADER:
        raise ValueError('invalid header size')
    value = json.loads(read_exact(sock, size, deadline))
    if not isinstance(value, dict):
        raise ValueError('expected object')
    if value.get('type') == 'error':
        raise RuntimeError(value.get('message', 'MASK request failed'))
    return value
