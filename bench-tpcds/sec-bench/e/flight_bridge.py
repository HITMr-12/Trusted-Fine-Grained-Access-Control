"""Benchmark-only Flight -> Arrow IPC -> Spark columnar adapter, local[2]."""
import hashlib,json,secrets,socket,threading,time,os
import pyarrow as pa
from fgac_client import GovernedClient,iter_batches,iter_chunks

def verify_chunks(iterator, chunk_rows=900031):
    """SEC mode: recompute per-chunk sha256 and check server markers."""
    h = hashlib.sha256(); rows = cum = idx = 0
    for batch, meta in iterator:
        h.update(batch.serialize().to_pybytes())
        rows += batch.num_rows; cum += batch.num_rows
        boundary = cum >= (idx + 1) * chunk_rows
        if meta is not None:
            marker = json.loads(meta.to_pybytes().decode() if hasattr(meta, 'to_pybytes') else bytes(meta).decode())
            if marker['chunk'] != idx or marker['rows'] != rows or not (boundary or marker.get('last')):
                raise RuntimeError('SEC chunk marker mismatch: ' + str(marker))
            if h.hexdigest() != marker['sha256']:
                raise RuntimeError('SEC chunk sha256 mismatch at chunk %d' % idx)
            h = hashlib.sha256(); rows = 0; idx += 1
        yield batch
    if rows != 0:
        raise RuntimeError('SEC stream ended with unverified tail chunk')

class FlightBridge:
 def __init__(self,plan,token,prefetch=False,sec=False,tls_ca=None):
  self.sec=sec
  self.client=GovernedClient(os.getenv('FGAC_FLIGHT_URL','grpc://172.168.22.23:18815'),token,tls_ca=tls_ca if sec else None)
  self.info=self.client.describe(plan)
  assert len(self.info.endpoints)==1
  self.prefetched=self.client.prefetch(self.info,with_meta=sec) if prefetch else None
  self.secret=secrets.token_hex(32);self.listeners=[];self.connections=[];self.error=None
  self.next_wait_ms=0.;self.ipc_write_ms=0.;self.connect_ms=0.;self.doget_ms=0.;self.batches=0;self.rows=0;self.nbytes=0;self.first_batch_at=None;self.reader=None
  for _ in range(2):
   s=socket.socket();s.bind(('127.0.0.1',0));s.listen(1);s.settimeout(120);self.listeners.append(s)
  self.thread=threading.Thread(target=self.pump,daemon=True);self.thread.start()
 def pump(self):
  files=[];writers=[];pump_start=time.perf_counter()
  try:
   for listener in self.listeners:
    conn,_=listener.accept();conn.settimeout(120);self.connections.append(conn);got=b''
    while len(got)<64:
     part=conn.recv(64-len(got))
     if not part:raise RuntimeError('Socket authentication ended early')
     got+=part
    if not secrets.compare_digest(got,self.secret.encode()):raise RuntimeError('Bad local nonce')
    f=conn.makefile('wb');files.append(f);writers.append(pa.ipc.new_stream(f,self.info.schema));f.flush()
   self.connect_ms=(time.perf_counter()-pump_start)*1000
   q=time.perf_counter()
   if self.prefetched is None:self.reader=self.client.client.do_get(self.info.endpoints[0].ticket,self.client.options)
   self.doget_ms=(time.perf_counter()-q)*1000
   if self.prefetched is None:
    raw_iterator=iter_chunks(self.reader) if self.sec else iter(iter_batches(self.reader))
   else:
    raw_iterator=iter(self.prefetched)
   iterator=verify_chunks(raw_iterator) if self.sec else raw_iterator
   while True:
    q=time.perf_counter()
    try:batch=next(iterator)
    except StopIteration:
     self.next_wait_ms+=(time.perf_counter()-q)*1000;break
    self.next_wait_ms+=(time.perf_counter()-q)*1000
    if self.first_batch_at is None:self.first_batch_at=self.prefetched.first_batch_at if self.prefetched is not None else time.perf_counter()
    q=time.perf_counter();i=self.batches%2;writers[i].write_batch(batch);files[i].flush()
    self.ipc_write_ms+=(time.perf_counter()-q)*1000
    self.batches+=1;self.rows+=batch.num_rows;self.nbytes+=batch.nbytes
   for w in writers:w.close()
   for f in files:f.flush()
  except BaseException as exc:self.error=exc
  finally:
   if self.reader:self.reader.cancel()
   for f in files:
    try:f.close()
    except Exception:pass
   for s in self.connections+self.listeners:s.close()
 def dataframe(self,spark,arrow_schema=None,spark_schema=None):
  # TPC-DS store_sales 默认schema；其他受控关系可显式传入
  from pyspark.sql.types import StructType,StructField,IntegerType,LongType,DecimalType
  if arrow_schema is None:
   arrow_schema=pa.schema([('ss_item_sk',pa.int64()),('ss_store_sk',pa.int64()),('ss_quantity',pa.int32()),('ss_sales_price',pa.decimal128(7,2)),('ss_net_paid',pa.decimal128(7,2))])
   spark_schema=StructType([StructField('ss_item_sk',LongType()),StructField('ss_store_sk',LongType()),StructField('ss_quantity',IntegerType()),StructField('ss_sales_price',DecimalType(7,2)),StructField('ss_net_paid',DecimalType(7,2))])
  assert [(f.name,f.type) for f in self.info.schema]==[(f.name,f.type) for f in arrow_schema],(self.info.schema,arrow_schema)
  return spark.read.format('fgac.bench.ArrowSocketSource').option('schema',spark_schema.json()).option('ports',','.join(str(x.getsockname()[1]) for x in self.listeners)).option('secret',self.secret).load()
 def finish(self):
  self.thread.join(120)
  if self.thread.is_alive():raise RuntimeError('Flight bridge did not finish')
  if self.error:raise RuntimeError('Flight bridge failed') from self.error
 def close(self):
  if self.prefetched is not None:self.prefetched.close()
  if self.reader:self.reader.cancel()
  for s in self.connections+self.listeners:
   try:s.shutdown(socket.SHUT_RDWR)
   except OSError:pass
   s.close()
  self.thread.join(5);self.client.close()
