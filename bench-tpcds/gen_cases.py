# -*- coding: utf-8 -*-
"""生成 TPC-DS store_sales 公平基准 cases（可被后续 benchmark 直接复用）。
口径与 0915 taxi 基线一致：等值行策略 + 业务阈值梯度 + 四/五列投影 + xxhash64 摘要汇。
行数来自 2026-09-16 在 fgac.tpcds.store_sales 上的实测（tune_thresholds.py）。
"""
import json
from pathlib import Path

COLUMNS = ["ss_item_sk", "ss_store_sk", "ss_quantity", "ss_sales_price", "ss_net_paid"]
RELATION = "lake.sales.store_sales"
THRESHOLDS = {"0.001": 173.50, "0.01": 143.70, "0.1": 88.80, "0.5": 27.60}
# (principal, row_filter, authorized_rows, {target: rows})  —— rows 为实测
PRINCIPALS = [
    ("alice", "ss_store_sk = 1", 536919,
     {"0.001": 552, "0.01": 5221, "0.1": 52416, "0.5": 261804, "1.0": 536919}),
    ("bob", "ss_store_sk = 2", 541263,
     {"0.001": 540, "0.01": 5268, "0.1": 52675, "0.5": 264927, "1.0": 541263}),
    ("bench_full", None, 28800991,
     {"0.001": 27668, "0.01": 275793, "0.1": 2752030, "0.5": 13752062, "1.0": 28800991}),
]

def plan_for(target, principal):
    node = {"op": "governed_scan", "relation": RELATION}
    if target != "1.0":
        node = {"op": "filter",
                "condition": {"op": "gte",
                              "left": {"op": "column", "name": "ss_sales_price"},
                              "right": {"op": "literal", "data_type": "double",
                                        "value": THRESHOLDS[target]}},
                "input": node}
    node = {"op": "project", "columns": COLUMNS, "input": node}
    return {"version": 2, "schema_version": "1", "root": node}

cases = []
for principal, rf, auth_rows, rows in PRINCIPALS:
    for target in ["0.001", "0.01", "0.1", "0.5", "1.0"]:
        business = ("true" if target == "1.0"
                    else f"ss_sales_price >= {THRESHOLDS[target]:.2f}")
        cases.append({
            "name": f"{principal}_{target.replace('0.', '').replace('.', '_')}",
            "principal": principal,
            "target": float(target),
            "threshold": None if target == "1.0" else THRESHOLDS[target],
            "plan": plan_for(target, principal),
            "rows": rows[target],
            "authorized_rows": auth_rows,
            "actual_ratio": round(rows[target] / auth_rows, 6),
            "business_sql": business,
            "row_filter_sql": rf,
        })

out = Path(__file__).parent / "cases.json"
out.write_text(json.dumps(cases, indent=1, ensure_ascii=False))
print(f"{len(cases)} cases -> {out}")
for c in cases:
    print(f"  {c['name']:>16} rows={c['rows']:>9} ratio={c['actual_ratio']}")
