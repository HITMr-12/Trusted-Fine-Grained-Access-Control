"""Balanced, interleaved comparison of actual deployment architectures."""
import itertools,json,os,random,selectors,subprocess,sys,time,traceback,socket,threading,urllib.request,urllib.parse
from pathlib import Path
r=Path('/home/lyb/fgac-lab');d=r/'deploy/fgac-batch-compression';out=Path(sys.argv[1]);smoke='--smoke' in sys.argv;out.mkdir(parents=True,exist_ok=True)
workers={};records=[];readies=[];manifest={'ok':False}
def snapshot():
    c=json.loads((r/'deploy/polaris-credentials.json').read_text());base='http://172.168.22.23:18182/api/catalog/v1'
    data=urllib.parse.urlencode({'grant_type':'client_credentials','client_id':c['client_id'],'client_secret':c['client_secret'],'scope':'PRINCIPAL_ROLE:ALL'}).encode()
    with urllib.request.urlopen(urllib.request.Request(base+'/oauth/tokens',data=data),timeout=15) as f:token=json.load(f)['access_token']
    with urllib.request.urlopen(urllib.request.Request(base+'/fgac/namespaces/nyc/tables/taxi_trips',headers={'Authorization':'Bearer '+token}),timeout=15) as f:meta=json.load(f)
    assert meta['metadata']['current-snapshot-id']==6896028106384817739
    return {'snapshot':meta['metadata']['current-snapshot-id'],'metadata_location':meta['metadata-location'],'time':time.time()}
def save(name,x):(out/name).write_text(json.dumps(x,indent=2,default=str))
def read(p,timeout=180):
    deadline=time.monotonic()+timeout
    while time.monotonic()<deadline:
        sel=selectors.DefaultSelector();sel.register(p.stdout,selectors.EVENT_READ);events=sel.select(max(0,deadline-time.monotonic()));sel.close()
        if not events:raise TimeoutError('Worker response timeout')
        line=p.stdout.readline()
        if not line:raise RuntimeError('Worker ended with '+str(p.poll()))
        if line.startswith('BENCH_RESULT '):return json.loads(line[len('BENCH_RESULT '):])
    raise TimeoutError()
def call(p,x):
    with socket.create_connection(('127.0.0.1',p.control_port),timeout=180) as conn:
        conn.sendall((json.dumps(x)+'\n').encode());return json.loads(conn.makefile('r').readline())
def launch(kind,user):
    conf={'NATIVE':'native-conf','INLINE':'inline-conf','FGAC':'conf'}[kind]
    env=os.environ.copy();env.update({'SPARK_CONF_DIR':str(r/'deploy/fgac-current'/conf),'JAVA_HOME':'/usr/lib/jvm/java-11-openjdk-arm64','PYSPARK_PYTHON':'/home/lyb/fgac/venv/bin/python','PYTHONPATH':str(r/'envs/fgac-python')+':'+str(d),'HADOOP_USER_NAME':user,'SPARK_LOCAL_DIRS':str(r/'tmp'),'TMPDIR':str(r/'tmp')})
    p=subprocess.Popen(['/home/lyb/fgac/spark/spark-3.5.8-bin-hadoop3/bin/spark-submit','--conf','spark.eventLog.dir=file:/home/lyb/fgac-lab/runs/batch-compression-0915/events',str(d/'worker.py'),kind,user,str(out)],env=env,cwd=d,stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=(out/(kind+'-'+user+'.log')).open('w'),text=True,bufsize=1)
    workers[(kind,user)]=p;ready=read(p);assert ready.get('ready'),ready;p.control_port=ready['control_port'];readies.append(ready);save('workers.json',readies);print('ready',kind,user,flush=True)
    def drain():
        with (out/(kind+'-'+user+'-stdout.log')).open('w') as f:
            for line in p.stdout:f.write(line)
    threading.Thread(target=drain,daemon=True).start()

try:
    save('snapshot-before.json',snapshot())
    cases=[c for c in json.loads((d/'cases.json').read_text()) if c['name'] in ['bob_90','bob_100']]
    for c in cases:c['business_sql']='trip_distance >= '+repr(c['threshold']) if c['threshold'] is not None else 'true'
    modes=[f'{codec}_{size}' for size in [8192,32768,65536] for codec in ['NONE','LZ4','ZSTD']]
    save('cases.json',cases)
    save('protocol.json',{'modes':modes,'formal_repetitions':36,'warmup_repetitions':8,'seed':91504,'same_R_Spark':True,'same_E_Spark':True,'resource_policy':'no affinity or CPU quota','prefetch':'2 batches / 16 MiB unchanged; actual byte occupancy varies with batch size','purpose':'factorial batch size and codec experiment; not new Ranger latency comparison'})
    launch('FGAC','lyb')
    w=workers[('FGAC','lyb')]
    refs=json.loads((d/'reference-digests.json').read_text())
    negative=[]
    for mode in modes:
        x=call(w,{'id':'negative-'+mode,'case':dict(cases[0],principal='invalid_identity'),'mode':mode,'rep':-99})
        assert not x.get('ok') and 'Unauthenticated' in x['error'],x
        negative.append(x)
    save('negative-controls.json',negative)
    rng=random.Random(91504);orders={}
    for c in cases:
        seq=[]
        for block in range(4):
            base=rng.sample(modes,len(modes))
            chunk=[base[i:]+base[:i] for i in range(9)];rng.shuffle(chunk);seq.extend(chunk)
        orders[c['name']]=seq
    save('orders.json',orders)
    for rep in (range(1) if smoke else range(-8,36)):
        cycle=cases[:];rng.shuffle(cycle)
        for c in cycle:
            order=orders[c['name']][rep] if rep>=0 else rng.sample(modes,len(modes))
            for mode in order:
                x=call(w,{'id':f"batch-{c['name']}-{rep}-{mode}",'case':c,'mode':mode,'rep':rep,'capture_plan':rep==0})
                assert x.get('ok'),x
                assert x['digest']==refs[c['name']]['digest'] and x['schema']==refs[c['name']]['schema'],x
                records.append(x)
                with (out/'records.jsonl').open('a') as f:f.write(json.dumps(x)+'\n')
            print(json.dumps({'case':c['name'],'rep':rep,'records':len(records)}),flush=True)
        save('progress.json',{'rep':rep,'records':len(records),'time':time.time()})
        if rep>=0 and rep%9==0:save('snapshot-check-'+str(rep)+'.json',snapshot())
    save('snapshot-after.json',snapshot());manifest={'ok':True,'records':len(records),'formal_records':sum(not x['warmup'] for x in records),'all_results_match_prior_native_digest':True}
except Exception:
    manifest['error']=traceback.format_exc();print(manifest['error'],flush=True);raise
finally:
    save('done.json',manifest)
    for worker in workers.values():
        try:
            with socket.create_connection(('127.0.0.1',worker.control_port),timeout=5) as conn:conn.sendall(b'{"op":"stop"}\n')
        except Exception:pass
    for worker in workers.values():
        try:worker.wait(timeout=20)
        except subprocess.TimeoutExpired:worker.terminate()
