import hashlib,json,os,tarfile,sys
from pathlib import Path
role=sys.argv[1];r=Path('/data1/lyb/fgac-lab' if role=='R' else '/home/lyb/fgac-lab');out=r/'runs/batch-compression-0915';d=r/'deploy/fgac-batch-compression'
secrets=[]
for f in (r/'deploy').glob('*credentials.json'):
    for k,v in json.loads(f.read_text()).items():
        if isinstance(v,str) and ('secret' in k or 'password' in k or k=='access_key'):secrets.append(v)
manifest={'role':role,'files':{},'cpu_affinity':sorted(os.sched_getaffinity(0))}
for pattern in ['*.py','*.java','*.jar','streaming/*.py']:
    for p in d.glob(pattern):manifest['files'][str(p.relative_to(d))]=hashlib.sha256(p.read_bytes()).hexdigest()
# Deliberately exclude credential-bearing configuration and environment events.
(out/'source-manifest.json').write_text(json.dumps(manifest,indent=2))
keep={'SparkListenerJobStart','SparkListenerJobEnd','SparkListenerTaskEnd','SparkListenerStageCompleted','org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart','org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd'}
with (out/'spark-events-filtered.jsonl').open('w') as dest:
    for p in (out/'events').glob('*'):
        if not p.is_file() or p.name.startswith('.'):continue
        for line in p.open():
            try:e=json.loads(line)
            except json.JSONDecodeError:continue
            if e.get('Event') in keep:
                e['_source_file']=p.name;line=json.dumps(e)
                for value in secrets:line=line.replace(value,'[REDACTED]')
                dest.write(line+'\n')
with tarfile.open(out/('evidence-'+role+'.tar.gz'),'w:gz') as tar:
    for p in out.rglob('*'):
        if not p.is_file() or 'events' in p.relative_to(out).parts:continue
        if p.suffix not in ['.json','.jsonl','.txt']:continue
        if p.name=='resources-'+role+'.jsonl' or p.parent==out or 'formal' in p.relative_to(out).parts or 'smoke3' in p.relative_to(out).parts:
            tar.add(p,arcname=str(p.relative_to(out)))
print('Evidence ready',role)
