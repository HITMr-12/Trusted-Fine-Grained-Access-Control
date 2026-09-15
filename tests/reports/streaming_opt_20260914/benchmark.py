"""Unified end-to-end benchmark: runtime policy + full-column double digest."""
import ast,hashlib,json,os,random,re,sys,time,traceback
from pathlib import Path
import requests
from flight_bridge import FlightBridge
from pyspark.sql import functions as F
out=Path(sys.argv[1]);smoke='--smoke' in sys.argv
os.environ['POLARIS_URL']='http://172.168.22.25:8181'
os.environ['FGAC_ICEBERG_TABLE']='fgac.nyc.taxi_trips'
sys.path.insert(0,str(out))
import deployed_adapter as adapter
from deployed_plan_compiler import PlanCompiler
sys.path.insert(0,'/home/lyb/infra/Trusted-Fine-Grained-Access-Control/tests')
import phase1_baseline_benchmark as p1
spark=p1.build_spark(2);spark.conf.set('spark.sql.shuffle.partitions','200');spark.sparkContext.setLogLevel('ERROR')
active_bridges=[];records=[];uris=[];cleanups=[];validations=[];policy_versions={};base_snapshot=None
def save(name,value):
 (out/name).write_text(json.dumps(value,indent=2,default=str))
def snapshot():
 return sorted(r.snapshot_id for r in spark.sql('SELECT snapshot_id FROM fgac.nyc.taxi_trips.snapshots').collect())
def plan(who):
 node={'op':'governed_scan','relation':'lake.sales.orders'}
 if who=='alice':node={'op':'filter','condition':{'op':'gte','left':{'op':'column','name':'fare_amount'},'right':{'op':'literal','data_type':'double','value':50}},'input':node}
 return {'version':2,'schema_version':'1','root':{'op':'project','columns':['VendorID','trip_distance','fare_amount','payment_type'],'input':node}}
def direct(payload,token):
 times={};t=time.perf_counter()
 if not token.startswith('Bearer '):raise ValueError('Bearer token required')
 if payload['version']!=2:raise ValueError('Unsupported protocol')
 relation,ops,cols=adapter.inspect_plan(payload['root']);t1=time.perf_counter()
 contract=adapter.authorize(token,relation,ops,cols,payload['schema_version']);t2=time.perf_counter()
 source=adapter.fetch_source(contract);t3=time.perf_counter()
 df,audit=PlanCompiler(spark,Path('/unused'),policy_loader=lambda _:contract['policy'],source_loader=lambda _:source).compile(payload['root']);t4=time.perf_counter()
 times.update(inspect_ms=(t1-t)*1000,catalog_ms=(t2-t1)*1000,source_ms=(t3-t2)*1000,compile_ms=(t4-t3)*1000,catalog_requests=1)
 return df,contract,times
def digest(df):
 cols=[F.col(c) for c in df.columns]
 final=df.select(F.xxhash64(*cols).cast('decimal(38,0)').alias('h1'),F.xxhash64(F.lit(9173),*cols).cast('decimal(38,0)').alias('h2')).agg(F.count('*').alias('rows'),F.sum('h1').alias('sum1'),F.sum('h2').alias('sum2'))
 return final
def remove(uri):
 assert uri in uris and re.fullmatch(r's3a://fgac/results/[0-9a-f]{32}-lake\.sales\.orders',uri)
 p=spark._jvm.org.apache.hadoop.fs.Path(uri);fs=p.getFileSystem(spark._jsc.hadoopConfiguration());existed=fs.exists(p)
 if existed:fs.delete(p,True)
 row={'uri':uri,'existed':existed,'remaining':fs.exists(p)};cleanups.append(row);save('cleanup.json',cleanups);assert not row['remaining']
def run(case,rep,mode):
 who=case['principal'];payload=case['plan'];token='Bearer '+who+'-token';ident=f"{case['name']}-{rep}-{mode}";spark.sparkContext.setJobGroup(ident,ident)
 start=time.time();t=time.perf_counter();uri=None;extra={};bridge=None
 if mode=='A':
  df,body,extra=direct(payload,token);policy=body['policy']['version'];principal=body['principal']
 else:
  bridge=FlightBridge(payload,token,prefetch=mode=='O');active_bridges.append(bridge);extra['flight_metadata_ms']=(time.perf_counter()-t)*1000
  df=bridge.dataframe(spark);policy=None;principal=None
 before_setup=time.perf_counter();final=digest(df);before_action=time.perf_counter()
 try:
  result=final.first().asDict();value={k:str(v) for k,v in result.items()}
  if bridge:bridge.finish()
 finally:
  if bridge:bridge.close();active_bridges.remove(bridge)
 end=time.perf_counter()
 if bridge:extra.update(stream_rows=bridge.rows,stream_batches=bridge.batches,arrow_bytes=bridge.nbytes,first_arrow_ms=(bridge.first_batch_at-t)*1000 if bridge.first_batch_at else None)
 if bridge:extra.update(bridge_next_wait_ms=bridge.next_wait_ms,bridge_ipc_write_ms=bridge.ipc_write_ms,bridge_connect_ms=bridge.connect_ms,bridge_doget_ms=bridge.doget_ms)
 if bridge and bridge.prefetched is not None:
  q=bridge.prefetched;extra.update(prefetch_read_ms=q.read_ms,prefetch_backpressure_ms=q.backpressure_ms,prefetch_peak_batches=q.peak_queued_batches,prefetch_peak_bytes=q.peak_queued_bytes)
 extra['digest_setup_ms']=(before_action-before_setup)*1000
 row=dict(id=ident,case=case['name'],plan_hash=hashlib.sha256(json.dumps(payload,sort_keys=True).encode()).hexdigest(),principal=who,rep=rep,warmup=rep<0,mode=mode,start=start,end=time.time(),total_ms=(end-t)*1000,prepare_ms=(before_setup-t)*1000,action_ms=(end-before_action)*1000,policy_version=str(policy),resolved_principal=principal,result_uri=uri,**extra)
 records.append(row);save('records.json',records)
 # Both modes consume and validate the measured digest; no second data action.
 schema=[(f.name,f.dataType.simpleString()) for f in df.schema.fields]
 validations.append(dict(id=ident,digest=value,schema=schema));save('validation.json',validations)
 if mode=='A':
  if who in policy_versions:assert policy_versions[who]==str(policy)
  policy_versions[who]=str(policy)
  assert principal==who
 assert int(value['rows'])==case['rows']
 if rep==0:(out/(case['name']+'_'+mode+'_plan.txt')).write_text(final._jdf.queryExecution().toString())
 if uri:remove(uri)
 print(json.dumps({k:row[k] for k in ['id','total_ms','prepare_ms','action_ms']}),flush=True)
 return value,schema
try:
 save('pid.json',{'python':os.getpid(),'java':spark._jvm.java.lang.management.ManagementFactory.getRuntimeMXBean().getName().split('@')[0]})
 base_snapshot=snapshot();assert base_snapshot==[6896028106384817739]
 save('meta.json',{'spark':spark.version,'master':spark.sparkContext.master,'shuffle':spark.conf.get('spark.sql.shuffle.partitions'),'snapshot':base_snapshot,'seed':914,'smoke':smoke,'sink':'full-column-double-digest-included','stream_adapter':'same Arrow IPC, 2 Spark partitions; U=unoptimized, O=bounded early prefetch','objective':'mean of paired O/A - 1 < 0.10, equal seven cases','max_optimization_rounds':5,'compiler_sha256':hashlib.sha256((out/'deployed_plan_compiler.py').read_bytes()).hexdigest()})
 # A rejected identity must stop before source loading/compiler execution.
 rejected=[]
 for mode in ['A']:
  if mode=='A':
   try:direct(plan('alice'),'Bearer fair-invalid-token')
   except adapter.HTTPException as e:status=e.status_code
   else:raise AssertionError('A accepted invalid identity')
  else:
   response=requests.post('http://172.168.22.25:8002/v2/subplans',json=plan('alice'),headers={'Authorization':'Bearer fair-invalid-token'},timeout=10);status=response.status_code
  assert status in (401,403);rejected.append({'mode':mode,'status':status})
 save('rejection.json',rejected)
 # Isolated fixture verifies policy-before-user-filter and last4 masks.
 raw=spark.createDataFrame([(1,'12345678'),(2,'87654321')],['VendorID','card'])
 raw.createOrReplaceTempView('fair_mask_fixture')
 fixture_policy={'version':'fixture','row_filter':{'op':'eq','column':'VendorID','value':1},'masks':{'card':{'type':'last4','prefix':'****'}}}
 fixture,_=PlanCompiler(spark,Path('/unused'),lambda _:fixture_policy,lambda _:'fair_mask_fixture').compile({'op':'governed_scan','relation':'lake.sales.orders'})
 assert [tuple(r) for r in fixture.collect()]==[(1,'****5678')];save('mask_fixture.json',{'ok':True});spark.catalog.dropTempView('fair_mask_fixture')
 # Calibrate thresholds against the authorized bob relation, outside measurements.
 base,_,_=direct(plan('bob'),'Bearer bob-token')
 thresholds=base.approxQuantile('trip_distance',[.999,.99,.9,.5,.1],.00001)
 cases=[]
 for name,target,threshold in zip(['bob_001','bob_01','bob_10','bob_50','bob_90'],[.001,.01,.1,.5,.9],thresholds):
  payload=plan('bob');payload['root']['input']={'op':'filter','condition':{'op':'gte','left':{'op':'column','name':'trip_distance'},'right':{'op':'literal','data_type':'double','value':threshold}},'input':payload['root']['input']}
  cases.append(dict(name=name,principal='bob',target=target,threshold=threshold,plan=payload))
 counts=base.agg(*[F.sum(F.when(F.col('trip_distance')>=F.lit(c['threshold']),1).otherwise(0)).alias(c['name']) for c in cases],F.count('*').alias('base')).first().asDict()
 for c in cases:c.update(rows=counts[c['name']],authorized_rows=counts['base'],actual_ratio=counts[c['name']]/counts['base'])
 cases.append(dict(name='bob_100',principal='bob',target=1.,threshold=None,plan=plan('bob'),rows=counts['base'],authorized_rows=counts['base'],actual_ratio=1.))
 alice_base=plan('alice');alice_base['root']['input']={'op':'governed_scan','relation':'lake.sales.orders'}
 adf,_,_=direct(alice_base,'Bearer alice-token');alice_rows=adf.count()
 cases.append(dict(name='alice_original',principal='alice',target=None,threshold=50.,plan=plan('alice'),rows=36149,authorized_rows=alice_rows,actual_ratio=36149/alice_rows))
 save('cases.json',cases)
 import itertools
 rng=random.Random(914);orders={}
 for case in cases:
  seq=[list(x) for x in itertools.permutations(['A','U','O'])]*5;rng.shuffle(seq);orders[case['name']]=seq
 if '--diagnostic' in sys.argv:
  cases=[c for c in cases if c['name'] in ['bob_001','bob_100']]
  (out/'diagnostic_ready').write_text('ready')
  while not (out/'diagnostic_go').exists():time.sleep(.2)
 reps=range(1) if smoke else (range(-2,8) if '--diagnostic' in sys.argv else range(-5,30))
 for rep in reps:
  cycle=cases[:];rng.shuffle(cycle)
  for case in cycle:
   assert requests.get('http://172.168.22.25:8002/health',timeout=5).status_code==200
   order=orders[case['name']][rep] if rep>=0 else (['A','U','O'] if rep%2 else ['O','U','A'])
   if '--diagnostic' in sys.argv:order=['O']
   pair=[run(case,rep,mode) for mode in order];assert all(x==pair[0] for x in pair),(case['name'],rep,'mismatch')
  if rep%10==0:assert snapshot()==base_snapshot
 assert snapshot()==base_snapshot
 save('done.json',{'ok':True,'records':len(records),'validation':len(validations),'cleaned':len(cleanups),'snapshot_after':snapshot()})
except Exception:
 save('done.json',{'ok':False,'error':traceback.format_exc()});raise
finally:
 for bridge in active_bridges:bridge.close()
 for uri in uris:
  if not any(x['uri']==uri and not x['remaining'] for x in cleanups):remove(uri)
 spark.stop()
