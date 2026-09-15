import json
from pathlib import Path
p=Path(__file__).resolve().parent;workers=json.loads((p/'formal/workers.json').read_text());remote=json.loads((p/'r/remote-ready.json').read_text());result={}
for role,folder in [('E','e'),('R','r')]:
    samples=[json.loads(x) for x in (p/folder/('resources-'+role+'.jsonl')).read_text().splitlines()]
    groups={}
    if role=='E':
        for w in workers:
            if w['user']!='denied':groups[w['kind']+'-'+w['user']]=[w['pid'],w['java']]
    else:groups['FGAC-server']=[remote['python'],remote['java']]
    peaks={}
    for name,pids in groups.items():
        peaks[name]=max(sum(x['rss_bytes'] for x in s['processes'] if x['pid'] in pids) for s in samples)/1048576
    first=[int(x) for x in samples[0]['cpu'].split()[1:]];last=[int(x) for x in samples[-1]['cpu'].split()[1:]];delta=[b-a for a,b in zip(first,last)]
    result[role]={'peak_combined_python_jvm_rss_mib':peaks,'machine_cpu_busy_fraction':1-(delta[3]+delta[4])/sum(delta),'samples':len(samples),'scope':'whole observed run including worker startup and warmup; host CPU includes unrelated activity'}
(p/'resource-summary.json').write_text(json.dumps(result,indent=2));print(json.dumps(result,indent=2))
