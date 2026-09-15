import hashlib,json,os,tarfile,sys
from pathlib import Path
role=sys.argv[1];r=Path('/data1/lyb/fgac-lab' if role=='R' else '/home/lyb/fgac-lab');out=r/'runs/full-alignment-0915';d=r/'deploy/fgac-full-alignment'
secrets=[]
for f in (r/'deploy').glob('*credentials.json'):
 for k,v in json.loads(f.read_text()).items():
  if isinstance(v,str) and len(v)>3 and ('secret' in k.lower() or 'password' in k.lower() or k=='access_key'):secrets.append(v)
manifest={'role':role,'files':{},'cpu_affinity':sorted(os.sched_getaffinity(0))}
for pattern in ['*.py','*.java','*.jar','streaming/*.py']:
 for f in d.glob(pattern):manifest['files'][str(f.relative_to(d))]=hashlib.sha256(f.read_bytes()).hexdigest()
(out/'source-manifest.json').write_text(json.dumps(manifest,indent=2))
keep={'SparkListenerJobStart','SparkListenerJobEnd','SparkListenerTaskEnd','SparkListenerStageCompleted','org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart','org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd'}
for folder in out.rglob('events'):
 with (folder.parent/'spark-events-filtered.jsonl').open('w') as dest:
  for f in folder.glob('*'):
   if not f.is_file() or f.name.startswith('.'):continue
   for line in f.open():
    try:e=json.loads(line)
    except json.JSONDecodeError:continue
    if e.get('Event') in keep:
     e['_source_file']=f.name;line=json.dumps(e)
     for value in secrets:line=line.replace(value,'[REDACTED]')
     dest.write(line+'\n')
with tarfile.open(out/('evidence-'+role+'.tar.gz'),'w:gz') as tar:
 for f in out.rglob('*'):
  parts=f.relative_to(out).parts
  if not f.is_file() or any(t=='events' or t.endswith('tmp') for t in parts):continue
  if f.suffix not in ['.json','.jsonl','.txt']:continue
  data=f.read_bytes()
  assert not any(v.encode() in data for v in secrets),f.name
  tar.add(f,arcname=f.relative_to(out).as_posix())
print('Sanitized evidence ready',role)
