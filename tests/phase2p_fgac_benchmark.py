"""Phase 2P FGAC benchmark: subplan execution on Remote with Parquet result
materialization. E submits the plan, then reads the query-scoped result
Parquet over s3a with the per-query credential envelope returned by Remote.
"""
import argparse
import json
import os
import sys
import time
from pathlib import Path

import requests

spark_home = os.environ.get("SPARK_HOME", os.path.expanduser("~/tools/spark"))
sys.path.insert(0, os.path.join(spark_home, "python"))
sys.path.insert(0, os.path.join(spark_home, "python", "lib", "py4j-0.10.9.7-src.zip"))
sys.path.insert(0, "/home/lyb/infra/Trusted-Fine-Grained-Access-Control/tests")
os.environ.setdefault("SPARK_HOME", spark_home)
from phase2_fgac_benchmark import QUERIES, TOKENS, build_subplan

ICEBERG_JAR = "/home/lyb/tools/iceberg-jars/iceberg-spark-runtime-3.5_2.12-1.7.2.jar"
S3A_JARS = ("/home/lyb/tools/s3a-jars/hadoop-common-3.3.4.jar,"
            "/home/lyb/tools/s3a-jars/hadoop-aws-3.3.4.jar,"
            "/home/lyb/tools/s3a-jars/aws-java-sdk-bundle-1.12.262.jar")

REMOTE = "http://172.168.22.25:8002/v2/subplans"

_spark = None


def get_spark():
    global _spark
    if _spark is None:
        from pyspark.sql import SparkSession
        _spark = (SparkSession.builder.master("local[4]").appName("fgac-phase2p")
                  .config("spark.jars", f"{ICEBERG_JAR},{S3A_JARS}")
                  .config("spark.hadoop.fs.s3a.path.style.access", "true")
                  .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                  .config("spark.ui.enabled", "false").getOrCreate())
    return _spark


def run_once(session: requests.Session, token: str, query: str) -> tuple[int, float]:
    t = time.perf_counter()
    response = session.post(REMOTE, json=build_subplan(query),
                            headers={"Authorization": token}, timeout=600)
    submit_ms = (time.perf_counter() - t) * 1000
    if response.status_code != 200:
        raise RuntimeError(f"{query}: HTTP {response.status_code}: {response.text[:200]}")
    body = response.json()
    if "result_uri" not in body:
        raise RuntimeError(f"{query}: no result_uri in response: {list(body)[:8]}")
    spark = get_spark()
    jdf = (spark.read.option("endpoint", body["storage_endpoint"])
           .option("access.key", body["access_key"])
           .option("secret.key", body["secret_key"]))
    spark.conf.set("fs.s3a.endpoint", body["storage_endpoint"])
    spark.conf.set("fs.s3a.access.key", body["access_key"])
    spark.conf.set("fs.s3a.secret.key", body["secret_key"])
    spark.conf.set("fs.s3a.path.style.access", "true")
    t2 = time.perf_counter()
    df = spark.read.parquet(body["result_uri"])
    n = df.count()
    read_ms = (time.perf_counter() - t2) * 1000
    return n, (time.perf_counter() - t) * 1000, submit_ms, read_ms


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    ap.add_argument("--warmup", type=int, default=2)
    ap.add_argument("--repeats", type=int, default=5)
    args = ap.parse_args()
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    session = requests.Session()
    rows = []
    for principal, token in TOKENS.items():
        for query in QUERIES:
            for _ in range(args.warmup):
                run_once(session, token, query)
            for rep in range(args.repeats):
                n, total_ms, submit_ms, read_ms = run_once(session, token, query)
                rows.append({"phase": "p2p_fgac_parquet", "principal": principal,
                             "query": query, "rep": rep, "rows": n,
                             "wall_ms": round(total_ms, 2),
                             "submit_ms": round(submit_ms, 2),
                             "read_ms": round(read_ms, 2)})
                print(f"p2p {principal} {query} rep{rep}: {n} rows total={total_ms:.0f}ms "
                      f"submit={submit_ms:.0f} read={read_ms:.0f}", flush=True)
    (out / "phase2p_results.json").write_text(json.dumps(rows, indent=2), encoding="utf-8")
    print(f"WROTE {out/'phase2p_results.json'} total={len(rows)}")


if __name__ == "__main__":
    main()
