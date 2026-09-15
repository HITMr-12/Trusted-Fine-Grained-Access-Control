"""Spark integration checks for opt-in local partition delivery; no external data."""
import json,time,socket,sys
from pathlib import Path
from pyspark.sql import SparkSession,functions as F
from streaming.spark_batches import iter_partition_arrow_batches
def main(output_path):
    s=SparkSession.builder.appName('partition-stream-integration').getOrCreate();s.sparkContext.setLogLevel('ERROR');s.conf.set('spark.sql.execution.arrow.maxRecordsPerBatch','8192');sc=s.sparkContext;results={}
    def begin(name):sc.setJobGroup('partition-test-'+name,name,interruptOnCancel=True)
    def read(n,delay=0):
     begin('range');probe={};rows=total=0
     for b in iter_partition_arrow_batches(s.range(0,n,numPartitions=2),lambda x:probe.update(json.loads(x))):
      rows+=b.num_rows;total+=sum(b.column(0).to_pylist())
      if delay:time.sleep(delay)
     assert rows==n and total==n*(n-1)//2
     return probe
    try:
     probe=read(300000,.003);assert probe['first_written_ms']<probe['first_partition_done_ms'],probe;assert probe['queue_peak_batches']<=2;results['incremental_and_bounded']=probe
     results['empty']=read(0)
     begin('cancel');it=iter_partition_arrow_batches(s.range(0,10000000,numPartitions=2));next(it);it.close()
     for _ in range(100):
      if not sc.statusTracker().getActiveJobsIds():break
      time.sleep(.05)
     assert not sc.statusTracker().getActiveJobsIds();results['cancel']=True
     begin('failure');df=s.range(0,100000,numPartitions=1).select(F.when(F.col('id')>=25000,F.raise_error('injected producer failure')).otherwise(F.col('id')).alias('id'))
     failed=False
     try:list(iter_partition_arrow_batches(df))
     except Exception:failed=True
     assert failed;results['producer_failure_propagates']=True
     results['recovery']=read(10000)
     begin('bad-token');h=s._jvm.org.fgac.streaming.PartitionArrowStream.start(s.range(5)._jdf)
     try:
      with socket.create_connection(('127.0.0.1',h.port()),timeout=5) as c:c.sendall(b'0'*64)
      failed=False
      try:h.awaitComplete()
      except Exception:failed=True
      assert failed;results['bad_local_token_rejected']=True
     finally:h.close()
     results['ok']=True
    finally:s.stop()
    Path(output_path).write_text(json.dumps(results,indent=2));print(json.dumps(results))


if __name__ == "__main__":
    main(sys.argv[1])
