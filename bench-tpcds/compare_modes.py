# -*- coding: utf-8 -*-
"""三模式摘要一致性比对：NATIVE(按主体) / INLINE / FGAC 逐案比对 rows+sum1+sum2+schema。"""
import json
from pathlib import Path

base = Path("/home/lyb/fgac-lab/runs/tpcds-bench")
cases = {c["name"]: c for c in json.load(open("/home/lyb/fgac-lab/deploy/fgac-tpcds-bench/cases.json"))}

def load(p):
    out = {}
    for line in Path(p).read_text().splitlines():
        r = json.loads(line)
        if r.get("ok"):
            out[r["case"]] = r
    return out

native = {}
for u in ("alice", "bob", "bench_full"):
    native.update(load(base / f"native-{u}.jsonl"))
inline = load(base / "inline-lyb.jsonl")
fgac = load(base / "fgac-lyb.jsonl")

fails = 0
for name in sorted(cases):
    case = cases[name]
    n, i, f = native.get(name), inline.get(name), fgac.get(name)
    ok = (n and i and f
          and n["rows"] == case["rows"] == i["rows"] == f["rows"]
          and n["sum1"] == i["sum1"] == f["sum1"]
          and n["sum2"] == i["sum2"] == f["sum2"]
          and n["schema"] == i["schema"] == f["schema"])
    digest = (n or i or f or {}).get("sum1")
    print(("PASS " if ok else "FAIL ") + name + " rows=" + str(case["rows"]) + " digest=" + str(digest))
    fails += 0 if ok else 1
print(f"cross-mode comparison: {len(cases) - fails}/{len(cases)} PASS")
