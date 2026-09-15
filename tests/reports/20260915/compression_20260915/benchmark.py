"""Balanced, interleaved comparison of actual deployment architectures."""
import itertools,json,os,random,selectors,subprocess,sys,time,traceback,socket,threading,urllib.request,urllib.parse
from pathlib import Path
r=Path('/home/lyb/fgac-lab');d=r/'deploy/fgac-compression';out=Path(sys.argv[1]);smoke='--smoke' in sys.argv;out.mkdir(parents=True,exist_ok=True)
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
    p=subprocess.Popen(['/home/lyb/fgac/spark/spark-3.5.8-bin-hadoop3/bin/spark-submit','--conf','spark.eventLog.dir=file:/home/lyb/fgac-lab/runs/compression-0915/events',str(d/'worker.py'),kind,user,str(out)],env=env,cwd=d,stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=(out/(kind+'-'+user+'.log')).open('w'),text=True,bufsize=1)
    workers[(kind,user)]=p;ready=read(p);assert ready.get('ready'),ready;p.control_port=ready['control_port'];readies.append(ready);save('workers.json',readies);print('ready',kind,user,flush=True)
    def drain():
        with (out/(kind+'-'+user+'-stdout.log')).open('w') as f:
            for line in p.stdout:f.write(line)
    threading.Thread(target=drain,daemon=True).start()

try:
    save('snapshot-before.json',snapshot())
    cases=[c for c in json.loads((d/'cases.json').read_text()) if c['name'] in ['bob_90','bob_100']]
    for c in cases:c['business_sql']='trip_distance >= '+repr(c['threshold']) if c['threshold'] is not None else 'true'
    modes=['NATIVE','NONE','LZ4','ZSTD'];save('cases.json',cases)
    save('protocol.json',{'modes':modes,'formal_repetitions':48,'warmup_repetitions':20,'extra_native_warmup':80,'seed':91503,'codec_levels':{'NONE':None,'LZ4':None,'ZSTD':1},'same_R_Spark':True,'same_E_FGAC_Spark':True,'E_native_and_FGAC_warmup_queries':120,'batch_rows':8192,'resource_policy':'no affinity or aggregate CPU quota','wire_probe':'separate from timed benchmark','smoke':smoke})
    launch('FGAC','lyb');launch('NATIVE','bob')
    if not smoke:
        launch('NATIVE','denied');negative=[]
        x=call(workers[('NATIVE','denied')],{'id':'negative-native','case':cases[0],'mode':'NATIVE','rep':-99})
        assert not x.get('ok') and 'AccessControlException' in x['error'],x;negative.append(x)
        denied=workers.pop(('NATIVE','denied'))
        with socket.create_connection(('127.0.0.1',denied.control_port),timeout=5) as conn:conn.sendall(b'{"op":"stop"}\n')
        denied.wait(timeout=20)
        for mode in modes[1:]:
            x=call(workers[('FGAC','lyb')],{'id':'negative-'+mode,'case':dict(cases[0],principal='invalid_identity'),'mode':mode,'rep':-99})
            assert not x.get('ok') and 'Unauthenticated' in x['error'],x;negative.append(x)
        save('negative-controls.json',negative)
    rng=random.Random(91503);orders={}
    for c in cases:
        seq=list(itertools.permutations(modes))*2;rng.shuffle(seq);orders[c['name']]=seq
    save('orders.json',orders)
    for rep in (range(1) if smoke else range(-20,48)):
        cycle=cases[:];rng.shuffle(cycle)
        for c in cycle:
            order=orders[c['name']][rep] if rep>=0 else rng.sample(modes,len(modes));pair=[]
            for mode in order:
                worker=workers[('NATIVE','bob') if mode=='NATIVE' else ('FGAC','lyb')]
                x=call(worker,{'id':f"compression-{c['name']}-{rep}-{mode}",'case':c,'mode':mode,'rep':rep,'capture_plan':rep==0})
                assert x.get('ok'),x
                records.append(x);pair.append(x)
                with (out/'records.jsonl').open('a') as f:f.write(json.dumps(x)+'\n')
            assert all(x['digest']==pair[0]['digest'] and x['schema']==pair[0]['schema'] for x in pair),(c['name'],rep)
            print(json.dumps({'case':c['name'],'rep':rep,**{x['mode']:round(x['total_ms'],2) for x in pair}}),flush=True)
        if rep==-1 and not smoke:
            extra=[]
            for i in range(80):
                c=cases[i%2];x=call(workers[('NATIVE','bob')],{'id':'extra-native-'+str(i),'case':c,'mode':'NATIVE','rep':-100})
                ref=next(y for y in records if y['case']==c['name'])
                assert x.get('ok') and x['digest']==ref['digest'] and x['schema']==ref['schema'],x;extra.append(x)
            save('extra-session-warmup.json',extra);print('Native warmup equalized: 120 queries per E session',flush=True)
        save('progress.json',{'rep':rep,'records':len(records),'time':time.time()})
        if rep>=0 and rep%10==0:save('snapshot-check-'+str(rep)+'.json',snapshot())
    save('snapshot-after.json',snapshot());manifest={'ok':True,'records':len(records),'formal_records':sum(not x['warmup'] for x in records),'all_paired_results_equal':True}
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
