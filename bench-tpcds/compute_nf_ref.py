import json
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.compute as pc
from pathlib import Path

d = Path("/data1/lyb/fgac-lab/runs/mask-e2e/delivery_s90")
COLS = ['ss_item_sk', 'ss_store_sk', 'ss_quantity', 'ss_sales_price', 'ss_net_paid']
DEC = {'ss_sales_price', 'ss_net_paid'}
ref = {c: {'sum': 0, 'min': None, 'max': None} for c in COLS}
rows = 0
for f in sorted(d.glob("*.parquet")):
    t = pq.read_table(f)
    rows += t.num_rows
    for c in COLS:
        col = t.column(c)
        if c in DEC:
            a = pc.cast(pc.cast(pc.multiply(col, 100), pa.decimal128(38, 0), safe=False),
                        pa.int64(), safe=False).to_numpy()
        else:
            a = col.to_numpy()
        ref[c]['sum'] += int(a.sum(dtype=np.int64))
        mn, mx = int(a.min()), int(a.max())
        ref[c]['min'] = mn if ref[c]['min'] is None else min(ref[c]['min'], mn)
        ref[c]['max'] = mx if ref[c]['max'] is None else max(ref[c]['max'], mx)
out = {'label': 's90nf', 'threshold': 'NONE',
       'ref_agg': {'rows': rows,
                   'cols': {c: {'sum': str(ref[c]['sum']), 'min': ref[c]['min'],
                                'max': ref[c]['max']} for c in COLS}}}
Path("/data1/lyb/fgac-lab/runs/mask-e2e/result_s90nf.json").write_text(json.dumps(out, indent=1))
print("NF_REF rows=", rows)
