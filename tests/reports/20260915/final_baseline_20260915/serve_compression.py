import os,json,time,threading,signal
from pathlib import Path
import remote_service as service
from streaming.server import SparkBackend,GovernedFlightServer
r=Path('/data1/lyb/fgac-lab');out=r/'runs/final-baseline-0915';lock=threading.Lock();hz=os.sysconf('SC_CLK_TCK');jpid=None
def usage():
    stat=Path('/proc',str(jpid),'stat').read_text().split();return time.process_time()+(int(stat[13])+int(stat[14]))/hz
def emit(row):
    with lock,(out/'remote-stages.jsonl').open('a') as f:f.write(json.dumps(row)+'\n')
class Backend(SparkBackend):
    def prepare(self,payload,token):
        cpu=usage();t=time.perf_counter();x=super().prepare(payload,token)
        emit({'kind':'prepare','time':time.time(),'ms':(time.perf_counter()-t)*1000,'cpu_ms':(usage()-cpu)*1000,'principal':x.principal});return x
    def revalidate(self,prepared,token):
        cpu=usage();t=time.perf_counter();x=super().revalidate(prepared,token)
        emit({'kind':'revalidate','time':time.time(),'ms':(time.perf_counter()-t)*1000,'cpu_ms':(usage()-cpu)*1000,'ok':x});return x
    def batches(self,prepared,job_id):
        from streaming.spark_batches import iter_arrow_batches,iter_partition_arrow_batches
        sc=self.spark.sparkContext;sc.setJobGroup(job_id,'Flight governed scan',interruptOnCancel=True)
        cpu=usage();t=time.perf_counter();n=rows=0;probe={}
        iterator=(iter_partition_arrow_batches(prepared.frame,lambda value:probe.update(json.loads(value))) if self.delivery=='STREAM' else iter_arrow_batches(prepared.frame))
        try:
            for batch in iterator:
                n+=1;rows+=batch.num_rows;yield batch
        finally:
            if hasattr(iterator,'close'):iterator.close()
            sc.cancelJobGroup(job_id);sc.setLocalProperty('spark.jobGroup.id',None)
            emit({'kind':'stream','time':time.time(),'ms':(time.perf_counter()-t)*1000,'cpu_ms':(usage()-cpu)*1000,'principal':prepared.principal,'job_id':job_id,'batches':n,'rows':rows,'delivery':self.delivery,'probe':probe})
profiles=[('STREAM',18832)]
servers=[]
for name,port in profiles:
    backend=Backend(service,8192);backend.delivery=name
    servers.append(GovernedFlightServer('grpc://172.168.22.23:'+str(port),backend,audit=out/('audit-'+name+'.jsonl')))
slot=threading.Semaphore(1)
for server in servers:server.scan_slot=slot
pid={'python':os.getpid(),'java':int(backend.spark._jvm.java.lang.management.ManagementFactory.getRuntimeMXBean().getName().split('@')[0])}
jpid=pid['java']
(out/'remote-ready.json').write_text(json.dumps(dict(**pid,spark=backend.spark.version,profiles=profiles)))
(r/'manifests/fgac-final-baseline.pid').write_text(str(os.getpid()))
def stop(*_):
    for server in servers:threading.Thread(target=server.shutdown,daemon=True).start()
signal.signal(signal.SIGTERM,stop);signal.signal(signal.SIGINT,stop)
try:
    threads=[threading.Thread(target=server.serve) for server in servers]
    for thread in threads:thread.start()
    for thread in threads:thread.join()
finally:backend.spark.stop()
