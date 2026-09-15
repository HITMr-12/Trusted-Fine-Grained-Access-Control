import json,subprocess,time,os
from pathlib import Path
r=Path('/home/lyb/fgac-lab');d=r/'deploy/fgac-batch-compression';out=r/'runs/batch-compression-0915'
while not (out/'formal/done.json').exists():time.sleep(3)
assert json.loads((out/'formal/done.json').read_text())['ok']
env=os.environ.copy();env['PYTHONPATH']=str(r/'envs/fgac-python')+':'+str(d)
for name,args in [('microbench',['ipc_microbench.py']),('wire',['wire_probe.py','2','wire-results.json'])]:
 with (out/(name+'.log')).open('w') as f:subprocess.run(['/home/lyb/fgac/venv/bin/python',str(d/args[0]),*args[1:]],env=env,cwd=d,stdout=f,stderr=subprocess.STDOUT,check=True)
(out/'followup-done.json').write_text(json.dumps({'ok':True}))
