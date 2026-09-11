$ErrorActionPreference = 'Stop'
$Root = Split-Path -Parent $PSScriptRoot
$Data = Join-Path $Root 'storage/data/governed/nyc_taxi/yellow_tripdata_2024-01.parquet'
$Reports = Join-Path $PSScriptRoot 'reports'
if (-not (Test-Path $Data)) { throw "Missing NYC TLC Parquet: $Data" }
docker run --rm `
  -v "${Root}:/workspace" `
  -v "${Data}:/data/yellow.parquet:ro" `
  -w /workspace `
  --entrypoint /usr/local/bin/spark-submit `
  fgac-remote:latest `
  --master 'local[4]' `
  --conf spark.sql.catalogImplementation=in-memory `
  --conf spark.sql.defaultCatalog=spark_catalog `
  /workspace/tests/remote_scan_cost_benchmark.py `
  --data /data/yellow.parquet --out /workspace/tests/reports --sample 0.01
