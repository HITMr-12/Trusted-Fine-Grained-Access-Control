"""Phase 2 FGAC benchmark: same workload as Phase 1 but governed scans are
executed on the Remote node (R) via /v2/subplans. The engine-side Spark session
is intentionally not given any governed storage credential: every query goes
Spark -> (plugin path simulated at driver level) -> Remote -> Polaris -> Iceberg.
"""
import argparse
import json
import time
from pathlib import Path

import requests

REMOTE = "http://172.168.22.25:8002/v2/subplans"
TOKENS = {"alice": "Bearer alice-token", "bob": "Bearer bob-token"}

COLUMNS = ["VendorID", "trip_distance", "fare_amount", "payment_type"]


def col(name):
    return {"op": "column", "name": name}


def lit_long(v):
    return {"op": "literal", "data_type": "long", "value": v}


def lit_double(v):
    return {"op": "literal", "data_type": "double", "value": v}


def bin_op(op, l, r):
    return {"op": op, "left": l, "right": r}


QUERIES = {
    "Q1_scan_all": None,
    "Q2_high_sel": bin_op("gte", col("fare_amount"), lit_double(50)),
    "Q3_low_sel": bin_op("gte", col("fare_amount"), lit_double(5)),
    "Q4_projection": None,
    "Q5_compound": bin_op("and",
                          bin_op("eq", col("payment_type"), lit_long(1)),
                          bin_op("gte", col("fare_amount"), lit_double(20))),
}


def build_subplan(query: str, policy_col: str = "VendorID"):
    """Mirror Phase 1 SQL: scan WHERE <policy> [AND predicate] SELECT columns.

    Q4_projection selects only 2 columns; Q1 has no predicate beyond policy.
    The row policy itself is applied by the Remote (Polaris contract), not in
    the plan, matching Phase 1's WHERE VendorID = N semantics.
    """
    node = {"op": "governed_scan", "relation": "lake.sales.orders"}
    predicate = QUERIES[query]
    if predicate is not None:
        node = {"op": "filter", "condition": predicate, "input": node}
    columns = COLUMNS[:2] if query == "Q4_projection" else COLUMNS
    return {"version": 2, "schema_version": "1",
            "root": {"op": "project", "columns": columns, "input": node}}


def run_once(session: requests.Session, token: str, query: str) -> tuple[int, int]:
    t = time.perf_counter()
    response = session.post(REMOTE, json=build_subplan(query),
                            headers={"Authorization": token}, timeout=600)
    wall = (time.perf_counter() - t) * 1000
    if response.status_code != 200:
        raise RuntimeError(f"{query}: HTTP {response.status_code}: {response.text[:200]}")
    body = response.json()
    return len(body["inline_rows"]), wall


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
                n, wall = run_once(session, token, query)
                rows.append({"phase": "p2_fgac", "concurrency": 1, "principal": principal,
                             "query": query, "rep": rep, "rows": n, "wall_ms": round(wall, 2)})
                print(f"p2 {principal} {query} rep{rep}: {n} rows {wall:.0f}ms", flush=True)
    (out / "phase2_results.json").write_text(json.dumps(rows, indent=2), encoding="utf-8")
    print(f"WROTE {out/'phase2_results.json'} total={len(rows)}")


if __name__ == "__main__":
    main()
