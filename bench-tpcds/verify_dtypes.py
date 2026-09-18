import numpy as np, pyarrow as pa, pyarrow.parquet as pq, pyarrow.compute as pc, importlib.util
spec = importlib.util.spec_from_file_location("m1", "/data1/lyb/fgac-lab/runs/mask-phase1/mask_phase1_remote.py")
m1 = importlib.util.module_from_spec(spec); spec.loader.exec_module(m1)
f = "/tmp/masklayout_zstd/part-00000.parquet"
buf = open(f, "rb").read()
md = pq.ParquetFile(pa.BufferReader(buf)).metadata
t = pq.read_table(f)
VT = {"ss_item_sk": ("<i8", 8), "ss_store_sk": ("<i8", 8), "ss_quantity": ("<i4", 4),
      "ss_sales_price": (">i4", 4), "ss_net_paid": (">i4", 4)}
def cents(col):
    return pc.cast(pc.cast(pc.multiply(col, 100), pa.decimal128(38, 0), safe=False), pa.int64(), safe=False).to_numpy()
for j, name in enumerate(VT):
    dt, w = VT[name]
    cc = md.row_group(0).column(j)
    pos = (getattr(cc, "dictionary_page_offset", 0) or 0) or cc.data_page_offset or cc.file_offset
    cur = 0; raw = b""
    while cur < cc.num_values:
        pt, nv, comp, hend = m1.parse_page_header(buf, pos)
        raw += bytes(pa.decompress(buf[hend:hend + comp], decompressed_size=nv * w, codec="zstd"))
        cur += nv; pos = hend + comp
    v = np.frombuffer(raw, dtype=dt).astype(np.int64)
    ref = cents(t.column(name)) if name in ("ss_sales_price", "ss_net_paid") else t.column(name).to_numpy().astype(np.int64)
    print(name, dt, "equal:", np.array_equal(v, ref))
