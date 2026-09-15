import json
from pathlib import Path
from pyspark.sql import SparkSession

root = Path('/home/lyb/fgac-lab')
spark = SparkSession.builder.appName('fgac-lab-storage-validation').getOrCreate()
try:
    df = spark.table('fgac.nyc.taxi_trips')
    rows = df.groupBy('VendorID').count().collect()
    result = {str(r['VendorID']): r['count'] for r in rows}
    assert result['1'] == 729732 and result['2'] == 2234632, result
    spark.sql('CREATE NAMESPACE IF NOT EXISTS fgac.baseline')
    fixture = spark.createDataFrame([(1, '12345678', 10.0), (2, '87654321', 20.0), (1, None, 30.0)], 'VendorID int, card string, amount double')
    fixture.writeTo('fgac.baseline.authz_fixture').create()
    (root/'runs/storage-validation.json').write_text(json.dumps({'ok': True, 'vendor_counts': result, 'spark': spark.version, 'fixture_rows': 3}, indent=2))
finally:
    spark.stop()
