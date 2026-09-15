"""Balanced, interleaved comparison of actual deployment architectures."""
import itertools,json,os,random,selectors,subprocess,sys,time,traceback,socket,threading,urllib.request,urllib.parse
from pathlib import Path
r=Path('/home/lyb/fgac-lab');d=r/'deploy/fgac-current';out=Path(sys.argv[1]);smoke='--smoke' in sys.argv;out.mkdir(parents=True,exist_ok=True)
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
    env=os.environ.copy();env.update({'SPARK_CONF_DIR':str(d/conf),'JAVA_HOME':'/usr/lib/jvm/java-11-openjdk-arm64','PYSPARK_PYTHON':'/home/lyb/fgac/venv/bin/python','PYTHONPATH':str(r/'envs/fgac-python')+':'+str(d),'HADOOP_USER_NAME':user,'SPARK_LOCAL_DIRS':str(r/'tmp'),'TMPDIR':str(r/'tmp')})
    p=subprocess.Popen(['/home/lyb/fgac/spark/spark-3.5.8-bin-hadoop3/bin/spark-submit',str(d/'worker.py'),kind,user,str(out)],env=env,cwd=d,stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=(out/(kind+'-'+user+'.log')).open('w'),text=True,bufsize=1)
    workers[(kind,user)]=p;ready=read(p);assert ready.get('ready'),ready;p.control_port=ready['control_port'];readies.append(ready);save('workers.json',readies);print('ready',kind,user,flush=True)
    def drain():
        with (out/(kind+'-'+user+'-stdout.log')).open('w') as f:
            for line in p.stdout:f.write(line)
    threading.Thread(target=drain,daemon=True).start()
try:
    save('snapshot-before.json',snapshot())
    cases=json.loads((d/'cases.json').read_text())
    for c in cases:c['business_sql']='fare_amount >= 50.0' if c['principal']=='alice' else ('trip_distance >= '+repr(c['threshold']) if c['threshold'] is not None else 'true')
    full=json.loads(json.dumps(cases[-2]));full.update(name='whole_source_100',principal='bench_full',rows=2964624,authorized_rows=2964624,actual_ratio=1.,business_sql='true');cases.append(full)
    save('cases.json',cases)
    save('protocol.json',{'modes':['NATIVE','FGAC','INLINE'],'snapshot':6896028106384817739,'formal_repetitions':30,'warmup_repetitions':5,'minimum_warmup_queries_per_session':40,'seed':915,'compute_budget':'User-selected deployment comparison: identical local[2] E Spark settings; FGAC adds R local[2]; no CPU affinity or aggregate quota','sink':'four-column double xxhash64 digest included in timer','cache':'Iceberg catalog cache enabled 30s, no data/result cache','timing':'start before SQL/Flight describe, end after full digest and bridge close','smoke':smoke})
    for kind,user in [('FGAC','lyb'),('INLINE','lyb'),('NATIVE','alice'),('NATIVE','bob'),('NATIVE','bench_full')]:launch(kind,user)
    if not smoke:
        launch('NATIVE','denied');negative=[]
        result=call(workers[('NATIVE','denied')],{'id':'negative-native','case':cases[0],'mode':'NATIVE','rep':-99})
        assert not result.get('ok') and 'AccessControlException' in result['error'],result;negative.append(result)
        denied=workers.pop(('NATIVE','denied'))
        with socket.create_connection(('127.0.0.1',denied.control_port),timeout=5) as conn:conn.sendall(b'{"op":"stop"}\n')
        denied.wait(timeout=20)
        bad=dict(cases[0],principal='invalid_identity')
        result=call(workers[('FGAC','lyb')],{'id':'negative-fgac','case':bad,'mode':'FGAC','rep':-99})
        assert not result.get('ok') and 'Unauthenticated' in result['error'],result;negative.append(result);save('negative-controls.json',negative)
    rng=random.Random(915);orders={}
    for c in cases:
        order=list(itertools.permutations(['NATIVE','FGAC','INLINE']))*5;rng.shuffle(order);orders[c['name']]=order
    save('orders.json',orders)
    for rep in (range(1) if smoke else range(-5,30)):
        cycle=cases[:];rng.shuffle(cycle)
        for c in cycle:
            pair=[];order=orders[c['name']][rep] if rep>=0 else list(itertools.permutations(['NATIVE','FGAC','INLINE']))[rep%6]
            for mode in order:
                worker=workers[(mode,c['principal'] if mode=='NATIVE' else 'lyb')]
                result=call(worker,{'id':f"{c['name']}-{rep}-{mode}",'case':c,'mode':mode,'rep':rep,'capture_plan':rep==0})
                assert result.get('ok'),result
                records.append(result);pair.append(result)
                with (out/'records.jsonl').open('a') as f:f.write(json.dumps(result)+'\n')
            assert all(x['digest']==pair[0]['digest'] and x['schema']==pair[0]['schema'] for x in pair),('RESULT MISMATCH',c['name'],rep)
            print(json.dumps({'case':c['name'],'rep':rep,**{x['mode']:round(x['total_ms'],2) for x in pair}}),flush=True)
        if rep==-1 and not smoke:
            extra=[]
            for user in ['alice','bob','bench_full']:
                own=[c for c in cases if c['principal']==user];existing=sum(x['mode']=='NATIVE' and x['principal']==user and x['warmup'] for x in records)
                for i in range(max(0,40-existing)):
                    c=own[i%len(own)];result=call(workers[('NATIVE',user)],{'id':f'sessionwarm-{user}-{i}','case':c,'mode':'NATIVE','rep':-100})
                    reference=next(x for x in records if x['mode']=='NATIVE' and x['case']==c['name'])
                    assert result.get('ok') and result['digest']==reference['digest'] and result['schema']==reference['schema'],result
                    extra.append(result)
                print('session warmup complete',user,40,flush=True)
            save('extra-session-warmup.json',extra)
        save('progress.json',{'rep':rep,'records':len(records),'last':time.time()})
        if rep>=0 and rep%10==0:save('snapshot-check-'+str(rep)+'.json',snapshot())
    save('snapshot-after.json',snapshot())
    manifest={'ok':True,'records':len(records),'formal_records':sum(not x['warmup'] for x in records),'cases':len(cases),'all_paired_results_equal':True}
except Exception:
    manifest['error']=traceback.format_exc();print(manifest['error'],flush=True);raise
finally:
    save('done.json',manifest)
    for p in workers.values():
        try:
            with socket.create_connection(('127.0.0.1',p.control_port),timeout=5) as conn:conn.sendall(b'{"op":"stop"}\n')
        except Exception:pass
    for p in workers.values():
        try:p.wait(timeout=20)
        except subprocess.TimeoutExpired:p.terminate()
