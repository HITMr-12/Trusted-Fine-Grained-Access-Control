"""Persistent Spark worker. All measured paths end in the same full-column sink."""
import json,os,sys,time,traceback,socket
from pathlib import Path
from pyspark.sql import SparkSession,functions as F
from flight_bridge import FlightBridge
kind,user,outdir=sys.argv[1:];out=Path(outdir)
s=SparkSession.builder.appName('bench0915-'+kind+'-'+user).getOrCreate();s.sparkContext.setLogLevel('ERROR')
jpid=int(s._jvm.java.lang.management.ManagementFactory.getRuntimeMXBean().getName().split('@')[0]);hz=os.sysconf('SC_CLK_TCK')
def usage():
    stat=Path('/proc',str(jpid),'stat').read_text().split();return time.process_time()+(int(stat[13])+int(stat[14]))/hz
listener=socket.socket();listener.bind(('127.0.0.1',0));listener.listen(1);connection=None
def answer(x):
    data=json.dumps(x,default=str)
    if connection is None:print('BENCH_RESULT '+data,flush=True)
    else:connection.sendall((data+'\n').encode())
answer({'ready':True,'kind':kind,'user':user,'pid':os.getpid(),'java':jpid,'spark':s.version,'master':s.sparkContext.master,'cpu_affinity':sorted(os.sched_getaffinity(0)),'control_port':listener.getsockname()[1]})
def requests():
    global connection
    while True:
        connection,_=listener.accept();connection.settimeout(180)
        line=connection.makefile('r').readline()
        yield line
        connection.close()
try:
    for line in requests():
        cmd=json.loads(line)
        if cmd.get('op')=='stop':break
        case=cmd['case'];mode=cmd['mode'];ident=cmd['id'];bridge=None
        try:
            s.sparkContext.setJobGroup(ident,ident)
            columns='VendorID, trip_distance, fare_amount, payment_type'
            business=case.get('business_sql','true')
            where=business
            if mode=='INLINE' and case['principal'] in ['alice','bob']:
                where='VendorID = '+('1' if case['principal']=='alice' else '2')+' AND ('+business+')'
            sql='SELECT '+columns+' FROM fgac.nyc.taxi_trips WHERE '+where
            start_wall=time.time();cpu=usage();t=time.perf_counter()
            if mode in ('COLLECT','STREAM'):
                os.environ['FGAC_FLIGHT_URL']='grpc://172.168.22.23:'+str({'COLLECT':18840,'STREAM':18841}[mode])
                bridge=FlightBridge(case['plan'],'Bearer '+case['principal']+'-token',prefetch=True)
                df=bridge.dataframe(s)
            else:df=s.sql(sql)
            ready=time.perf_counter()
            cols=[F.col(c) for c in df.columns]
            final=df.select(F.xxhash64(*cols).cast('decimal(38,0)').alias('h1'),F.xxhash64(F.lit(9173),*cols).cast('decimal(38,0)').alias('h2')).agg(F.count('*').alias('rows'),F.sum('h1').alias('sum1'),F.sum('h2').alias('sum2'))
            action=time.perf_counter();value={k:str(v) for k,v in final.first().asDict().items()}
            if bridge:bridge.finish();bridge.close()
            end=time.perf_counter();cpu_ms=(usage()-cpu)*1000
            schema=[(f.name,f.dataType.simpleString()) for f in df.schema.fields]
            assert int(value['rows'])==case['rows'],(value,case['rows'])
            row={'ok':True,'id':ident,'case':case['name'],'mode':mode,'principal':case['principal'],'rep':cmd['rep'],'warmup':cmd['rep']<0,'start':start_wall,'end':time.time(),'total_ms':(end-t)*1000,'prepare_ms':(ready-t)*1000,'digest_setup_ms':(action-ready)*1000,'action_ms':(end-action)*1000,'e_cpu_ms':cpu_ms,'digest':value,'schema':schema,'sql':sql if mode not in ('COLLECT','STREAM') else None}
            if bridge:
                row.update(stream_rows=bridge.rows,stream_batches=bridge.batches,arrow_bytes=bridge.nbytes,first_arrow_ms=(bridge.first_batch_at-t)*1000 if bridge.first_batch_at else None,bridge_next_wait_ms=bridge.next_wait_ms,bridge_ipc_write_ms=bridge.ipc_write_ms,prefetch_peak_bytes=bridge.prefetched.peak_queued_bytes,prefetch_peak_batches=bridge.prefetched.peak_queued_batches)
            if cmd.get('capture_plan'):(out/(case['name']+'_'+mode+'_plan.txt')).write_text(final._jdf.queryExecution().toString())
            answer(row)
        except Exception:
            if bridge:bridge.close()
            answer({'ok':False,'id':ident,'error':traceback.format_exc()})
finally:
    if connection:connection.close()
    listener.close();s.stop()
