import json,time,random,os
from pathlib import Path
import pyarrow as pa
from streaming.client import GovernedClient,iter_batches
r=Path('/home/lyb/fgac-lab');d=r/'deploy/fgac-batch-compression';out=r/'runs/batch-compression-0915'
c=next(c for c in json.loads((d/'cases.json').read_text()) if c['name']=='bob_100')
client=GovernedClient('grpc://172.168.22.23:18820','Bearer bob-token')
try:
 info=client.describe(c['plan']);reader=client.client.do_get(info.endpoints[0].ticket,client.options)
 table=pa.Table.from_batches(list(iter_batches(reader))).combine_chunks()
finally:client.close()
assert table.num_rows==c['rows']
profiles=[(size,codec) for size in [8192,32768,65536] for codec in ['NONE','LZ4','ZSTD']]
batches={size:table.to_batches(max_chunksize=size) for size in [8192,32768,65536]}
options={codec:pa.ipc.IpcWriteOptions(compression=None if codec=='NONE' else 'lz4' if codec=='LZ4' else pa.Codec('zstd',compression_level=1)) for codec in ['NONE','LZ4','ZSTD']}
rng=random.Random(91505);records=[]
for rep in range(-4,36):
 order=rng.sample(profiles,len(profiles))
 for size,codec in order:
  bs=batches[size];cpu=time.process_time();t=time.perf_counter()
  sink=pa.BufferOutputStream()
  with pa.ipc.new_stream(sink,table.schema,options=options[codec]) as writer:
   for b in bs:writer.write_batch(b)
  buf=sink.getvalue();enc=(time.perf_counter()-t)*1000;enc_cpu=(time.process_time()-cpu)*1000
  cpu=time.process_time();t=time.perf_counter()
  with pa.ipc.open_stream(buf) as reader:decoded=reader.read_all()
  dec=(time.perf_counter()-t)*1000;dec_cpu=(time.process_time()-cpu)*1000
  assert decoded.equals(table)
  x={'rep':rep,'warmup':rep<0,'size':size,'codec':codec,'batches':len(bs),'nonempty_input_buffers':sum(sum(b is not None and b.size>0 for b in col.buffers()) for batch in bs for col in batch.columns),'ipc_bytes':buf.size,'encode_ms':enc,'decode_ms':dec,'encode_cpu_ms':enc_cpu,'decode_cpu_ms':dec_cpu}
  records.append(x)
  with (out/'ipc-microbench.jsonl').open('a') as f:f.write(json.dumps(x)+'\n')
  del decoded,buf,sink
 print('microbench',rep,flush=True)
(out/'microbench-done.json').write_text(json.dumps({'ok':True,'records':len(records),'source_rows':table.num_rows,'note':'in-memory IPC serialization/deserialization; validation outside timer; no Spark/Flight/digest timing; CPU includes Arrow allocations and codec work, not isolated codec function calls'}))
