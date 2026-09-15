import collections,csv,json,random,statistics
from pathlib import Path
p=Path(__file__).resolve().parent
read=lambda name:json.loads((p/name).read_text(encoding='utf-8-sig'))
rows=[json.loads(x) for x in (p/'formal/records.jsonl').read_text().splitlines()]
assert read('formal/done.json')['ok'] and len(rows)==544
cases=read('formal/cases.json');modes=['NATIVE','NONE','LZ4','ZSTD']
extra=read('formal/extra-session-warmup.json');assert len(extra)==80
for x in extra:
    ref=next(y for y in rows if y['case']==x['case'])
    assert x['ok'] and x['digest']==ref['digest'] and x['schema']==ref['schema']
assert len([x for x in rows if x['warmup'] and x['mode']!='NATIVE'])==120
assert len([x for x in rows if x['warmup'] and x['mode']=='NATIVE'])+len(extra)==120
off=read('r/formal-offsets.json')
audits=[json.loads(x) for x in (p/'r/remote-audit.jsonl').read_text().splitlines()][off['audit']:off['audit']+408]
stages=[json.loads(x) for x in (p/'r/remote-stages.jsonl').read_text().splitlines()][off['stages']:off['stages']+1224]
fgac=[x for x in rows if x['mode']!='NATIVE'];assert len(fgac)==len(audits)==408
prep=[x for x in stages if x['kind']=='prepare'];reval=[x for x in stages if x['kind']=='revalidate'];streams={x['job_id']:x for x in stages if x['kind']=='stream'}
assert len(prep)==len(reval)==len(streams)==408
for i,(x,a) in enumerate(zip(fgac,audits)):
    assert a['status']=='complete' and a['principal']==x['principal'] and a['compression']==x['mode'].lower()
    assert a['rows']==x['stream_rows']==int(x['digest']['rows']) and a['batches']==x['stream_batches']
    assert reval[i]['ok'] and x['prefetch_peak_batches']<=2 and x['prefetch_peak_bytes']<=16*1024*1024
    x['remote_job_id']=a['job_id'];x['r_cpu_ms']=prep[i]['cpu_ms']+reval[i]['cpu_ms']+streams[a['job_id']]['cpu_ms']
    x['total_process_cpu_ms']=x['r_cpu_ms']+x['e_cpu_ms'];x['r_stream_ms']=streams[a['job_id']]['ms']
for x in rows:
    if x['mode']=='NATIVE':x['total_process_cpu_ms']=x['e_cpu_ms']
for c in cases:
    for rep in range(-20,48):
        group=[x for x in rows if x['case']==c['name'] and x['rep']==rep]
        assert len(group)==4 and {x['mode'] for x in group}==set(modes)
        assert all(x['digest']==group[0]['digest'] and x['schema']==group[0]['schema'] for x in group)
        assert int(group[0]['digest']['rows'])==c['rows']
for seq in read('formal/orders.json').values():assert len(seq)==48 and set(collections.Counter(tuple(x) for x in seq).values())=={2}
assert len(read('formal/negative-controls.json'))==4
def q(v,f):
    a=sorted(v);pos=(len(a)-1)*f;i=int(pos);return a[i]+(a[min(i+1,len(a)-1)]-a[i])*(pos-i)
def ci(xs,block=False):
    rng=random.Random(91503);means=[]
    for _ in range(10000):
        sample=([xs[(i+j)%48] for i in [rng.randrange(48) for _ in range(12)] for j in range(4)] if block else rng.choices(xs,k=len(xs)))
        means.append(statistics.mean(sample))
    return [q(means,.025),q(means,.975)]
summary=[];pairs=[]
for case in cases:
    data={m:sorted([x for x in rows if x['case']==case['name'] and x['mode']==m and not x['warmup']],key=lambda x:x['rep']) for m in modes}
    assert all(len(xs)==48 for xs in data.values())
    for m,xs in data.items():
        entry={'case':case['name'],'mode':m,'rows':case['rows'],'n':48}
        keys=['total_ms','prepare_ms','digest_setup_ms','action_ms','e_cpu_ms','total_process_cpu_ms']
        if m!='NATIVE':keys+=['first_arrow_ms','bridge_next_wait_ms','bridge_ipc_write_ms','prefetch_read_ms','prefetch_backpressure_ms','r_cpu_ms','r_stream_ms','arrow_bytes','stream_batches']
        entry.update({k:statistics.median(x[k] for x in xs) for k in keys});entry['p95_ms']=q([x['total_ms'] for x in xs],.95)
        for ref in ['NATIVE','NONE']:
            values=[x['total_ms']/y['total_ms']-1 for x,y in zip(xs,data[ref])]
            entry['over_'+ref+'_mean']=statistics.mean(values);entry['over_'+ref+'_ci95']=ci(values);entry['over_'+ref+'_block_ci95']=ci(values,True)
            entry['over_'+ref+'_blocks']=[statistics.mean(values[i:i+16]) for i in [0,16,32]]
        summary.append(entry)
    for rep in range(48):
        item={'case':case['name'],'rep':rep,**{m+'_ms':data[m][rep]['total_ms'] for m in modes}}
        for m in modes[1:]:
            item[m+'_over_NATIVE_minus1']=item[m+'_ms']/item['NATIVE_ms']-1
            item[m+'_over_NONE_minus1']=item[m+'_ms']/item['NONE_ms']-1
        pairs.append(item)
for name,value in [('summary',summary),('pairs',pairs),('verified-records',rows)]:
    (p/(name+'.json')).write_text(json.dumps(value,indent=2))
    if name in ['summary','pairs']:
        fields=list(dict.fromkeys(k for x in value for k in x))
        with (p/(name+'.csv')).open('w',newline='',encoding='utf-8-sig') as f:
            w=csv.DictWriter(f,fields);w.writeheader();w.writerows(value)
(p/'validation.json').write_text(json.dumps({'ok':True,'formal_queries':384,'regular_warmup':160,'extra_native_warmup':80,'audited_FGAC_queries':408,'paired_results_equal':True,'negative_controls':4},indent=2))
print(json.dumps(summary,indent=2))
