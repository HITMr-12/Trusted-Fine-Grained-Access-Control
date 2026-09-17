# -*- coding: utf-8 -*-
"""records.jsonl -> summary.csv / summary.json：每 case×mode 的正式轮次耗时统计与方案比值。
用法: python3 analyze_tpcds.py <run_dir>"""
import json, statistics, sys
from pathlib import Path

run = Path(sys.argv[1])
formal = [json.loads(l) for l in (run / 'records.jsonl').read_text().splitlines()
          if l.strip() and not json.loads(l).get('warmup')]

by = {}
for x in formal:
    by.setdefault((x['case'], x['mode']), []).append(x['total_ms'])

cases = sorted({c for c, _ in by})
modes = sorted({x['mode'] for x in formal})
rows = []
for c in cases:
    stat = {'case': c}
    for m in modes:
        v = sorted(by.get((c, m), []))
        if not v:
            continue
        stat[m] = {'n': len(v), 'min': round(v[0], 1), 'median': round(statistics.median(v), 1),
                   'p90': round(v[int(len(v) * 0.9) - 1] if len(v) > 1 else v[0], 1),
                   'max': round(v[-1], 1)}
    if 'NATIVE' in stat and 'FGAC' in stat:
        stat['FGAC_vs_NATIVE'] = round(stat['FGAC']['median'] / stat['NATIVE']['median'], 3)
    if 'NATIVE' in stat and 'INLINE' in stat:
        stat['INLINE_vs_NATIVE'] = round(stat['INLINE']['median'] / stat['NATIVE']['median'], 3)
    rows.append(stat)

(run / 'summary.json').write_text(json.dumps(rows, indent=1))
with (run / 'summary.csv').open('w') as f:
    f.write('case,mode,n,min_ms,median_ms,p90_ms,max_ms\n')
    for c in cases:
        for m in modes:
            if (c, m) in by:
                s = next(r2[m] for r2 in rows if r2['case'] == c)
                f.write('%s,%s,%d,%.1f,%.1f,%.1f,%.1f\n' % (c, m, s['n'], s['min'], s['median'], s['p90'], s['max']))

for r2 in rows:
    n = r2.get('NATIVE', {}).get('median')
    fv = r2.get('FGAC_vs_NATIVE', '-')
    iv = r2.get('INLINE_vs_NATIVE', '-')
    print('%-18s NATIVE=%-8s FGAC=%-8s INLINE=%-8s FGAC/NATIVE=%-6s INLINE/NATIVE=%s' % (
        r2['case'], n, r2.get('FGAC', {}).get('median'), r2.get('INLINE', {}).get('median'), fv, iv))
print('formal records:', len(formal), '-> summary.csv / summary.json in', run)
