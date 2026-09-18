import json, statistics, collections

rows = [json.loads(l) for l in open(r"D:\Kimi\WorkSpace\可信细粒度访问控制\bench-tpcds\sec-bench\results.jsonl", encoding="utf-8")]
assert len(rows) == 192, len(rows)

# digest consistency check per case
by_case = collections.defaultdict(set)
for r in rows:
    by_case[r["case"]].add((r["rows"], r["sum1"], r["sum2"]))
for c, s in by_case.items():
    assert len(s) == 1, (c, s)
print("digest check: all 8 cases consistent across 6 combos")

# median of reps 1..3 (exclude warmup rep=-1)
med = {}
detail = collections.defaultdict(list)
for r in rows:
    if r["warmup"]:
        continue
    assert r["ok"]
    detail[(r["case"], r["combo"])].append(r["ms"])
for k, v in detail.items():
    assert len(v) == 3, (k, v)
    med[k] = statistics.median(v)

cases = sorted({r["case"] for r in rows}, key=lambda c: (len(c), c))
combos = ["native-off", "native-on", "fgac-off", "fgac-on", "mask-off", "mask-on"]

print("\n== median ms per case x combo (rep1-3) ==")
print(f"{'case':<16}" + "".join(f"{c:>14}" for c in combos))
for c in cases:
    print(f"{c:<16}" + "".join(f"{med[(c,cb)]:>14.0f}" for cb in combos))

print("\n== SEC tax (on/off - 1) per scheme ==")
print(f"{'case':<16}{'native':>10}{'fgac':>10}{'mask':>10}")
for c in cases:
    n = med[(c, "native-on")] / med[(c, "native-off")] - 1
    f = med[(c, "fgac-on")] / med[(c, "fgac-off")] - 1
    m = med[(c, "mask-on")] / med[(c, "mask-off")] - 1
    print(f"{c:<16}{n:>9.1%}{f:>9.1%}{m:>9.1%}")

print("\n== mask vs fgac gap (mask/fgac - 1), off vs on ==")
print(f"{'case':<16}{'sec-off':>10}{'sec-on':>10}")
for c in cases:
    off = med[(c, "mask-off")] / med[(c, "fgac-off")] - 1
    on = med[(c, "mask-on")] / med[(c, "fgac-on")] - 1
    print(f"{c:<16}{off:>9.1%}{on:>9.1%}")

print("\n== vs native baseline (scheme/native - 1) ==")
print(f"{'case':<16}{'fgac-off':>10}{'fgac-on':>10}{'mask-off':>10}{'mask-on':>10}")
for c in cases:
    print(f"{c:<16}"
          f"{med[(c,'fgac-off')]/med[(c,'native-off')]-1:>9.1%}"
          f"{med[(c,'fgac-on')]/med[(c,'native-on')]-1:>9.1%}"
          f"{med[(c,'mask-off')]/med[(c,'native-off')]-1:>9.1%}"
          f"{med[(c,'mask-on')]/med[(c,'native-on')]-1:>9.1%}")

# e_cpu medians too
ecpu = collections.defaultdict(list)
for r in rows:
    if not r["warmup"]:
        ecpu[(r["case"], r["combo"])].append(r["e_cpu_ms"])
print("\n== median e_cpu_ms ==")
print(f"{'case':<16}" + "".join(f"{c:>14}" for c in combos))
for c in cases:
    print(f"{c:<16}" + "".join(f"{statistics.median(ecpu[(c,cb)]):>14.0f}" for cb in combos))
