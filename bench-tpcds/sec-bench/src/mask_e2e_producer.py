"""Catalog-policy byte masking: no data-page re-encoding in produce_file().

The trusted preparation command makes a query-independent uncompressed skeleton
once, as in the original prototype. Query work is decompress, bitmap, overwrite,
copy existing page headers/footer. Row counts/statistics are permitted leakage.
"""
import hashlib
import json
from decimal import Decimal
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from mask_page_header import parse_page_header
from mask_protocol import VALID_COLUMN, MAX_FILE, MAX_FILES

COLS = ['ss_item_sk', 'ss_store_sk', 'ss_quantity', 'ss_sales_price', 'ss_net_paid']
WIDTHS = dict(zip(COLS, [8, 8, 4, 4, 4]))
VIEWS = dict(zip(COLS, ['<i8', '<i8', '<i4', '>i4', '>i4']))
NULLABLE = COLS[1:]
TYPES = [pa.int64(), pa.int64(), pa.int32(), pa.decimal128(7, 2), pa.decimal128(7, 2)]


def validate_layout(parquet):
    schema = parquet.schema_arrow
    if schema.names != COLS or any(f.nullable or f.type != t for f, t in zip(schema, TYPES)):
        raise ValueError('requires verified fixed five-column non-nullable sentinel layout')
    for i in range(parquet.metadata.num_row_groups):
        rg = parquet.metadata.row_group(i)
        for j, name in enumerate(COLS):
            cc = rg.column(j)
            if (cc.compression != 'ZSTD' or cc.has_dictionary_page or
                    set(cc.encodings) - {'PLAIN', 'RLE'} or cc.num_values != rg.num_rows):
                raise ValueError('only required PLAIN/ZSTD DATA_PAGE_V1 is supported')


def pages(data, cc, width, compressed):
    pos, count = cc.data_page_offset, 0
    while count < cc.num_values:
        kind, nvalues, size, end = parse_page_header(data, pos)
        if kind != 0 or nvalues <= 0 or count + nvalues > cc.num_values or size <= 0 or end + size > len(data):
            raise ValueError('invalid fixed-layout page')
        expected = (nvalues + 7) // 8 if width == 0 else nvalues * width
        raw = data[end:end + size]
        if compressed:
            raw = bytes(pa.decompress(raw, decompressed_size=expected, codec='zstd'))
        if len(raw) != expected:
            raise ValueError('page contains unsupported levels or encoding')
        yield count, nvalues, data[pos:end], raw
        count += nvalues
        pos = end + size


def source_manifest(path):
    path = Path(path).resolve(strict=True)
    manifest = json.loads(path.read_text())
    if (manifest.get('format') != 'mask-plain-zstd-v2' or
            manifest.get('null_sentinel') != -2 or not manifest.get('null_sentinel_verified')):
        raise ValueError('requires value-verified sentinel layout and static skeleton')
    files = manifest['files']
    if not 0 < len(files) <= MAX_FILES or len({f['name'] for f in files}) != len(files):
        raise ValueError('invalid source file list')
    for entry in files:
        name = entry['name']
        if Path(name).name != name or '/' in name or '\\' in name or not name.endswith('.parquet'):
            raise ValueError('source names must be simple Parquet filenames')
        source = path.parent / name
        if source.is_symlink() or not source.is_file():
            raise ValueError('source must be a regular file')
    return path.parent, manifest


def validate_contract(contract, request, source):
    if (contract.get('relation_id') != request['relation_id'] or
            contract.get('schema_version') != request['schema_version'] or
            contract.get('storage_object') != source['storage_object'] or
            request['relation_id'] != source['relation_id'] or
            request['schema_version'] != source['schema_version'] or not contract.get('principal')):
        raise PermissionError('Catalog/source binding mismatch')
    policy = contract['policy']
    if not policy.get('version') or policy.get('masks'):
        raise PermissionError('missing version or unsupported Catalog column mask')
    if set(policy) - {'version', 'row_filter', 'masks', 'releasable_columns'}:
        raise PermissionError('unsupported policy obligation')
    if not {'governed_scan', 'project'} <= set(contract['authorized_operators']):
        raise PermissionError('Catalog did not authorize a projected scan')
    allowed = set(contract['authorized_columns']) & set(policy['releasable_columns']) & set(COLS)
    if not set(request['columns']) <= allowed:
        raise PermissionError('column not releasable')
    row = policy['row_filter']
    if row == {'op': 'all'}:
        return
    if (row.get('op') != 'eq' or set(row) != {'op', 'column', 'value'} or
            row.get('column') not in COLS or row.get('value') is None):
        raise PermissionError('unsupported row policy; no fallback to allow-all')


def produce_file(root, entry, columns, policy):
    """Query path: source page decompression + byte overwrite; no Parquet writer."""
    path = root / entry['name']
    if path.is_symlink() or path.stat().st_size > MAX_FILE:
        raise ValueError('invalid source')
    data = path.read_bytes()
    if len(data) > MAX_FILE or hashlib.sha256(data).hexdigest() != entry['sha256']:
        raise ValueError('source snapshot changed')
    parquet = pq.ParquetFile(pa.BufferReader(data))
    validate_layout(parquet)
    if sum(parquet.metadata.row_group(i).total_byte_size for i in range(parquet.metadata.num_row_groups)) > MAX_FILE:
        raise ValueError('decompressed file exceeds limit')
    parts = {c: [] for c in COLS}
    for i in range(parquet.metadata.num_row_groups):
        rg = parquet.metadata.row_group(i)
        for j, name in enumerate(COLS):
            parts[name].extend(raw for _, _, _, raw in pages(data, rg.column(j), WIDTHS[name], True))
    buffers = {c: bytearray(b''.join(parts[c])) for c in COLS}
    nrows = parquet.metadata.num_rows
    if nrows != entry['skeleton']['nrows']:
        raise ValueError('skeleton/source mismatch')
    valid = np.ones(nrows, dtype=np.bool_)
    row = policy['row_filter']
    if row['op'] == 'eq':
        name = row['column']
        literal = Decimal(str(row['value']))
        if name in ('ss_sales_price', 'ss_net_paid'):
            literal *= 100
        if not literal.is_finite() or literal != literal.to_integral_value():
            raise PermissionError('policy literal is not representable')
        values = np.frombuffer(buffers[name], dtype=VIEWS[name])
        valid = values == int(literal)
        if name in NULLABLE:
            valid &= values != -2  # SQL NULL never satisfies equality.
    for name in COLS:
        view = np.frombuffer(buffers[name], dtype=np.uint8).reshape(nrows, WIDTHS[name])
        if name not in columns:
            view[:] = 0  # Entire unrequested/unauthorized physical column is wiped.
        else:
            view[~valid] = 0  # No original bytes remain in an unauthorized row.
    out = bytearray(b'PAR1')
    for page in entry['skeleton']['pages']:
        name, start, count = page['col'], page['rs'], page['nv']
        out.extend(bytes.fromhex(page['hdr']))
        if name == VALID_COLUMN:
            # Only the new security bitmap is packed; source values stay PLAIN bytes.
            raw = np.packbits(valid[start:start + count], bitorder='little').tobytes()
        else:
            width = WIDTHS[name]
            raw = buffers[name][start * width:(start + count) * width]
        if len(raw) != page['plen']:
            raise ValueError('skeleton payload length mismatch')
        out.extend(raw)
    out.extend(bytes.fromhex(entry['skeleton']['footer']))
    if len(out) > MAX_FILE:
        raise ValueError('delivery exceeds limit')
    return bytes(out)


def prepare_skeleton(data):
    """Offline-only template construction, never called by the query server."""
    parquet = pq.ParquetFile(pa.BufferReader(data))
    validate_layout(parquet)
    table = parquet.read()
    table = table.append_column(pa.field(VALID_COLUMN, pa.bool_(), nullable=False),
                                pa.array(np.ones(table.num_rows, dtype=np.bool_)))
    # Query-independent physical template, just as the original skeleton step.
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink, compression='NONE', use_dictionary=False,
                   write_statistics=True, data_page_version='1.0', store_schema=False)
    raw = sink.getvalue().to_pybytes()
    metadata = pq.ParquetFile(pa.BufferReader(raw)).metadata
    result, row_base, body_end = [], 0, 4
    for i in range(metadata.num_row_groups):
        rg = metadata.row_group(i)
        for j in range(rg.num_columns):
            cc = rg.column(j)
            name = cc.path_in_schema
            width = 0 if name == VALID_COLUMN else WIDTHS[name]
            pos = cc.data_page_offset
            for start, count, header, payload in pages(raw, cc, width, False):
                result.append({'col': name, 'rs': row_base + start, 'nv': count,
                               'hdr': header.hex(), 'plen': len(payload)})
                pos += len(header) + len(payload)
            body_end = max(body_end, pos)
        row_base += rg.num_rows
    return {'nrows': table.num_rows, 'pages': result, 'footer': raw[body_end:].hex()}


def main():
    import argparse
    parser = argparse.ArgumentParser(description='Offline skeleton preparation; not a query or test')
    parser.add_argument('directory', type=Path)
    parser.add_argument('--relation', required=True)
    parser.add_argument('--storage-object', required=True)
    parser.add_argument('--schema-version', required=True)
    parser.add_argument('--confirm-verified-sentinel-layout', required=True, action='store_true')
    args = parser.parse_args()
    files = sorted(args.directory.glob('*.parquet'))
    if not 0 < len(files) <= MAX_FILES:
        raise ValueError('invalid source count')
    manifest = {'format': 'mask-plain-zstd-v2', 'relation_id': args.relation,
                'storage_object': args.storage_object, 'schema_version': args.schema_version,
                'null_sentinel': -2, 'null_sentinel_verified': True, 'files': []}
    for path in files:
        if path.is_symlink() or path.stat().st_size > MAX_FILE:
            raise ValueError('invalid source file')
        data = path.read_bytes()
        manifest['files'].append({'name': path.name, 'sha256': hashlib.sha256(data).hexdigest(),
                                  'skeleton': prepare_skeleton(data)})
    with (args.directory / 'source-manifest.json').open('x') as file:
        json.dump(manifest, file)


if __name__ == '__main__':
    main()
