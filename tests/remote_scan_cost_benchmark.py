"""NYC TLC public-data benchmark for black-box Remote Scan estimation."""
import argparse, csv, json, statistics, sys, time
from pathlib import Path

from pyspark.sql import SparkSession, functions as F

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from remote.cost_estimator import estimate, q_error

PREDICATES = [
    ("fare_ge_5", lambda: F.col("fare_amount") >= 5),
    ("fare_ge_20", lambda: F.col("fare_amount") >= 20),
    ("fare_ge_50", lambda: F.col("fare_amount") >= 50),
    ("distance_lt_1", lambda: F.col("trip_distance") < 1),
    ("distance_ge_10", lambda: F.col("trip_distance") >= 10),
    ("payment_card", lambda: F.col("payment_type") == 1),
    ("payment_cash", lambda: F.col("payment_type") == 2),
    ("airport", lambda: F.col("Airport_fee") > 0),
    ("compound", lambda: (F.col("payment_type") == 1) & (F.col("fare_amount") >= 20)),
]

def percentile(values, p):
    values = sorted(values); return values[min(len(values)-1, round((len(values)-1)*p))]

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", required=True); ap.add_argument("--out", required=True)
    ap.add_argument("--sample", type=float, default=0.01)
    args = ap.parse_args(); out = Path(args.out); out.mkdir(parents=True, exist_ok=True)
    spark = SparkSession.builder.master("local[4]").appName("remote-scan-cost-benchmark").config("spark.ui.enabled","false").getOrCreate()
    raw = spark.read.parquet(args.data).cache(); base_rows = raw.count()
    results=[]
    for principal, vendor in [("alice",1),("bob",2)]:
        policy_df = raw.filter(F.col("VendorID") == vendor)
        for query, predicate in PREDICATES:
            governed = policy_df.filter(predicate()).select("VendorID","trip_distance","fare_amount","payment_type")
            t=time.perf_counter(); actual=governed.count(); actual_ms=(time.perf_counter()-t)*1000
            for method in ("fixed","heuristic","policy_sample"):
                env=estimate(governed,method,base_rows,32,args.sample)
                d=env.to_dict(); d.update(principal=principal,policy=f"VendorID={vendor}",query=query,
                    base_rows=base_rows,actual_rows=actual,actual_bytes=actual*32,
                    q_error=round(q_error(env.estimated_rows,actual),4),actual_count_ms=round(actual_ms,3),
                    join_decision="broadcast" if env.estimated_bytes<=10*1024*1024 else "shuffle",
                    oracle_join="broadcast" if actual*32<=10*1024*1024 else "shuffle")
                d["decision_correct"]=d["join_decision"]==d["oracle_join"]; results.append(d)
    with (out/"remote_scan_cost_results.csv").open("w",newline="",encoding="utf-8-sig") as f:
        w=csv.DictWriter(f,fieldnames=results[0].keys()); w.writeheader(); w.writerows(results)
    summaries={}
    for method in ("fixed","heuristic","policy_sample"):
        subset=[r for r in results if r["method"]==method]; q=[r["q_error"] for r in subset]
        summaries[method]={"p50_q_error":percentile(q,.5),"p90_q_error":percentile(q,.9),
          "max_q_error":max(q),"mean_estimation_ms":round(statistics.mean(r["estimation_ms"] for r in subset),3),
          "join_decision_accuracy":round(statistics.mean(r["decision_correct"] for r in subset),4)}
    report={"dataset":"NYC TLC Yellow Taxi 2024-01","source":"https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page",
      "base_rows":base_rows,"sample_fraction":args.sample,"queries":len(PREDICATES)*2,"summaries":summaries,"results":results}
    (out/"remote_scan_cost_results.json").write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding="utf-8")
    print(json.dumps({"base_rows":base_rows,"summaries":summaries},ensure_ascii=False,indent=2)); spark.stop()
if __name__ == "__main__": main()
