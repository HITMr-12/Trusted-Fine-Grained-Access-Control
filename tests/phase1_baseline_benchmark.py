"""Phase 1 native baseline: Spark+Iceberg cross-machine scan without FGAC.

Reads the governed Iceberg table directly over s3a. Policy filters are written
inline in SQL (equivalent to the FGAC row policy), as the two-node plan
requires for a fair comparison.
"""
import argparse
import json
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from pyspark.sql import SparkSession, functions as F

ICEBERG_JAR = "/home/lyb/tools/iceberg-jars/iceberg-spark-runtime-3.5_2.12-1.7.2.jar"
S3A_JARS = ("/home/lyb/tools/s3a-jars/hadoop-common-3.3.4.jar,"
            "/home/lyb/tools/s3a-jars/hadoop-aws-3.3.4.jar,"
            "/home/lyb/tools/s3a-jars/aws-java-sdk-bundle-1.12.262.jar")

PRINCIPALS = {"alice": "VendorID = 1", "bob": "VendorID = 2"}

QUERIES = {
    "Q1_scan_all": "SELECT VendorID, trip_distance, fare_amount, payment_type FROM fgac.nyc.taxi_trips WHERE {policy}",
    "Q2_high_sel": "SELECT VendorID, trip_distance, fare_amount, payment_type FROM fgac.nyc.taxi_trips WHERE {policy} AND fare_amount >= 50",
    "Q3_low_sel": "SELECT VendorID, trip_distance, fare_amount, payment_type FROM fgac.nyc.taxi_trips WHERE {policy} AND fare_amount >= 5",
    "Q4_projection": "SELECT trip_distance, fare_amount FROM fgac.nyc.taxi_trips WHERE {policy}",
    "Q5_compound": "SELECT VendorID, trip_distance, fare_amount, payment_type FROM fgac.nyc.taxi_trips WHERE {policy} AND payment_type = 1 AND fare_amount >= 20",
}


def build_spark(threads: int) -> SparkSession:
    return (SparkSession.builder.master(f"local[{threads}]").appName("fgac-phase1-baseline")
            .config("spark.jars", f"{ICEBERG_JAR},{S3A_JARS}")
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config("spark.sql.catalog.fgac", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.fgac.type", "hadoop")
            .config("spark.sql.catalog.fgac.warehouse", "s3a://fgac/warehouse")
            .config("spark.hadoop.fs.s3a.endpoint", "http://172.168.22.25:9100")
            .config("spark.hadoop.fs.s3a.access.key", "fgacadmin")
            .config("spark.hadoop.fs.s3a.secret.key", "fgac-minio-secret-2025")
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "8").getOrCreate())


def read_proc_stat():
    fields = Path("/proc/stat").read_text().splitlines()[0].split()[1:]
    idle = int(fields[3]) + int(fields[4])
    total = sum(int(x) for x in fields)
    return idle, total


def cpu_seconds_over(fn, **kwargs):
    i0, t0 = read_proc_stat()
    start = time.perf_counter()
    result = fn(**kwargs)
    wall = time.perf_counter() - start
    i1, t1 = read_proc_stat()
    hz = 100.0
    return result, wall, ((t1 - t0) - (i1 - i0)) / hz


def run_once(spark, sql: str):
    n = spark.sql(sql).count()
    return n


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    ap.add_argument("--warmup", type=int, default=2)
    ap.add_argument("--repeats", type=int, default=5)
    ap.add_argument("--concurrencies", default="1,4")
    args = ap.parse_args()
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    concs = [int(c) for c in args.concurrencies.split(",")]
    rows = []
    for conc in concs:
        spark = build_spark(conc)
        spark.sql("SELECT 1").count()
        for principal, policy in PRINCIPALS.items():
            for qname, template in QUERIES.items():
                sql = template.format(policy=policy)
                for _ in range(args.warmup):
                    run_once(spark, sql)
                for rep in range(args.repeats):
                    n, wall, cpu = cpu_seconds_over(run_once, spark=spark, sql=sql)
                    rows.append({"phase": "p1_baseline", "concurrency": conc, "principal": principal,
                                 "query": qname, "rep": rep, "rows": n,
                                 "wall_ms": round(wall * 1000, 2), "engine_cpu_s": round(cpu, 3),
                                 "result_hash": hash((qname, principal, n))})
                    print(f"p1 conc={conc} {principal} {qname} rep{rep}: {n} rows {wall*1000:.0f}ms")
        spark.stop()
    (out / "phase1_results.json").write_text(json.dumps(rows, indent=2), encoding="utf-8")
    print(f"WROTE {out/'phase1_results.json'} total={len(rows)}")


if __name__ == "__main__":
    main()
