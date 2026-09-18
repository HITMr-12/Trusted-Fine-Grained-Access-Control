"""E-side committed MASK scan: validity selection precedes business filtering."""
import hashlib
import json
from pathlib import Path

from mask_protocol import VALID_COLUMN, digest, identifier


def read_authorized(spark, directory, columns, business='true'):
    from pyspark.sql import functions as F
    from pyspark.sql.types import BooleanType
    directory = Path(directory)
    if (directory / '.fail').exists():
        raise ValueError('failed delivery')
    manifest = json.loads((directory / 'manifest.json').read_text())
    identifier(manifest['request_id'])
    commit = json.loads((directory / '.done').read_text())
    if (manifest['columns'] != columns or commit.get('type') != 'commit' or
            commit.get('request_id') != manifest['request_id'] or
            commit.get('manifest_sha256') != digest(manifest)):
        raise ValueError('uncommitted or mismatched delivery')
    names = [f'part-{i:06d}.parquet' for i in range(manifest['file_count'])]
    receipts = commit['files']
    if (not names or manifest['files'] != names or
            [r['name'] for r in receipts] != names or
            sorted(p.name for p in directory.glob('*.parquet')) != names):
        raise ValueError('incomplete or duplicate delivery')
    total = 0
    for receipt in receipts:
        path = directory / receipt['name']
        if path.is_symlink() or path.stat().st_size != receipt['size']:
            raise ValueError('delivery file changed')
        checksum = hashlib.sha256()
        with path.open('rb') as file:
            for part in iter(lambda: file.read(1024 * 1024), b''):
                checksum.update(part)
        if checksum.hexdigest() != receipt['sha256']:
            raise ValueError('delivery checksum changed')
        total += receipt['size']
    if commit['total_bytes'] != total:
        raise ValueError('delivery total mismatch')
    return read_parts(spark, [directory / name for name in names], columns, business)


def read_parts(spark, paths, columns, business):
    """Read verified parts; caller must gate publication on the global commit."""
    from pyspark.sql import functions as F
    from pyspark.sql.types import BooleanType
    # Source statistics are allowed leakage, but may no longer describe the
    # overwritten values. Never use them to answer or prune this derived scan.
    spark.conf.set('spark.sql.parquet.filterPushdown', 'false')
    spark.conf.set('spark.sql.parquet.aggregatePushdown', 'false')
    physical = ['ss_item_sk', 'ss_store_sk', 'ss_quantity', 'ss_sales_price', 'ss_net_paid']
    df = spark.read.parquet(*[str(path) for path in paths])
    if (df.columns != physical + [VALID_COLUMN] or
            not isinstance(df.schema[VALID_COLUMN].dataType, BooleanType)):
        raise ValueError('unexpected MASK schema')
    # The validity bit removes wiped rows independently of any business filter.
    # -2 restoration is valid only for the independently verified source layout.
    df = df.filter(F.col(VALID_COLUMN) == F.lit(True))
    for column in physical[1:]:
        if column in columns:
            df = df.withColumn(column, F.when(F.col(column) != -2, F.col(column)))
    return df.select(*columns).filter(business)
