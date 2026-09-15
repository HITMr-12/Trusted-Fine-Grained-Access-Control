import csv,json,random,statistics,sys
from pathlib import Path
p=Path(__file__).resolve().parent;d=p/'formal'
rows=[json.loads(x) for x in (d/'records.jsonl').read_text().splitlines()];cases=json.loads((d/'cases.json').read_text());done=json.loads((d/'done.json').read_text())
assert done['ok'] and len(rows)==840 and len(cases)==8
extra=json.loads((d/'extra-session-warmup.json').read_text());assert len(extra)==80
for user in ['alice','bob','bench_full']:
    normal=[x for x in rows if x['mode']=='NATIVE' and x['principal']==user and x['warmup']]
    additions=[x for x in extra if x['principal']==user]
    assert len(normal)+len(additions)==40
    for x in additions:
        ref=next(y for y in normal if y['case']==x['case'])
        assert x['ok'] and x['digest']==ref['digest'] and x['schema']==ref['schema']
offset=json.loads((p/'r/formal-offsets.json').read_text())
audits=[json.loads(x) for x in (p/'r/remote-audit.jsonl').read_text().splitlines()][offset['audit']:]
stages=[json.loads(x) for x in (p/'r/remote-stages.jsonl').read_text().splitlines()][offset['stages']:]
fgac=[x for x in rows if x['mode']=='FGAC'];assert len(fgac)==len(audits)==280
prep=[x for x in stages if x['kind']=='prepare'];reval=[x for x in stages if x['kind']=='revalidate'];streams={x['job_id']:x for x in stages if x['kind']=='stream'}
assert len(prep)==len(reval)==len(streams)==280
for i,(x,a) in enumerate(zip(fgac,audits)):
    assert a['status']=='complete' and a['principal']==x['principal'] and a['rows']==x['stream_rows']==int(x['digest']['rows'])
    assert a['batches']==x['stream_batches'] and x['prefetch_peak_batches']<=2 and x['prefetch_peak_bytes']<=16*1024*1024
    stream=streams[a['job_id']];x['r_cpu_ms']=prep[i]['cpu_ms']+reval[i]['cpu_ms']+stream['cpu_ms'];x['total_executor_cpu_ms']=x['r_cpu_ms']+x['e_cpu_ms'];x['remote_job_id']=a['job_id']
for x in rows:
    if x['mode']!='FGAC':x['total_executor_cpu_ms']=x['e_cpu_ms']
for c in cases:
    for rep in range(-5,30):
        triple=[x for x in rows if x['case']==c['name'] and x['rep']==rep]
        assert len(triple)==3 and {x['mode'] for x in triple}=={'INLINE','NATIVE','FGAC'}
        assert all(x['digest']==triple[0]['digest'] and x['schema']==triple[0]['schema'] for x in triple)
        assert int(triple[0]['digest']['rows'])==c['rows']
def q(v,f):
    a=sorted(v);pos=(len(a)-1)*f;i=int(pos);return a[i]+(a[min(i+1,len(a)-1)]-a[i])*(pos-i)
def ci(v):
    rng=random.Random(915);b=[statistics.mean(rng.choices(v,k=len(v))) for _ in range(10000)];return [q(b,.025),q(b,.975)]
formal=[x for x in rows if not x['warmup']];summary=[];pairs=[]
for c in cases:
    data={m:[x for x in formal if x['case']==c['name'] and x['mode']==m] for m in ['INLINE','NATIVE','FGAC']}
    entry={'case':c['name'],'rows':c['rows'],'authorized_return_ratio':c['actual_ratio'],'raw_return_ratio':c['rows']/2964624}
    for m,xs in data.items():
        assert len(xs)==30
        for k in ['total_ms','prepare_ms','action_ms','e_cpu_ms','total_executor_cpu_ms']:
            entry[m+'_'+k]=statistics.median(x[k] for x in xs)
        entry[m+'_p95_ms']=q([x['total_ms'] for x in xs],.95)
    ratios=[];inline_ratios=[]
    for rep in range(30):
        triple={m:next(x for x in xs if x['rep']==rep) for m,xs in data.items()};n=triple['NATIVE']['total_ms'];f=triple['FGAC']['total_ms'];u=triple['INLINE']['total_ms']
        pairs.append({'case':c['name'],'rep':rep,'FGAC_ms':f,'NATIVE_ms':n,'INLINE_ms':u,'FGAC_over_NATIVE_minus1':f/n-1,'FGAC_over_INLINE_minus1':f/u-1,'NATIVE_over_INLINE_minus1':n/u-1})
        ratios.append(f/n-1);inline_ratios.append(f/u-1)
    entry['FGAC_over_NATIVE_mean']=statistics.mean(ratios);entry['FGAC_over_NATIVE_ci95']=ci(ratios);entry['FGAC_over_INLINE_mean']=statistics.mean(inline_ratios)
    entry['FGAC_first_arrow_ms']=statistics.median(x['first_arrow_ms'] for x in data['FGAC']);entry['FGAC_arrow_bytes']=statistics.median(x['arrow_bytes'] for x in data['FGAC']);summary.append(entry)
def overall(selected):
    selected_names={c['name'] for c in selected};a=[x for x in pairs if x['case'] in selected_names]
    strata=[[x['FGAC_over_NATIVE_minus1'] for x in a if x['case']==c['name']] for c in selected];rng=random.Random(915)
    b=[statistics.mean(statistics.mean(rng.choices(s,k=len(s))) for s in strata) for _ in range(10000)]
    byrep=[statistics.mean(x['FGAC_over_NATIVE_minus1'] for x in a if x['rep']==rep) for rep in range(30)]
    block_boot=[]
    for _ in range(10000):
        indices=[(start+j)%30 for start in [rng.randrange(30) for _ in range(6)] for j in range(5)]
        block_boot.append(statistics.mean(byrep[i] for i in indices))
    return {'cases':len(selected),'paired_mean_degradation':statistics.mean(x['FGAC_over_NATIVE_minus1'] for x in a),'ci95':[q(b,.025),q(b,.975)],'moving_block_ci95':[q(block_boot,.025),q(block_boot,.975)],'ratio_of_total_times_minus1':sum(x['FGAC_ms'] for x in a)/sum(x['NATIVE_ms'] for x in a)-1,'FGAC_over_INLINE_mean':statistics.mean(x['FGAC_over_INLINE_minus1'] for x in a),'NATIVE_over_INLINE_mean':statistics.mean(x['NATIVE_over_INLINE_minus1'] for x in a),'blocks':[statistics.mean(x['FGAC_over_NATIVE_minus1'] for x in a if x['rep']//10==i) for i in range(3)]}
result={'formal_queries':720,'warmup_queries':120,'extra_session_warmup_queries':80,'legacy_seven':overall(cases[:7]),'bob_six':overall(cases[:6]),'all_eight':overall(cases),'all_digests_schemas_equal':True,'all_remote_audits_complete':True}
for name,value in [('summary',summary),('pairs',pairs),('overall',result),('verified-records',rows)]:
    (p/(name+'.json')).write_text(json.dumps(value,indent=2))
    if name in ['summary','pairs']:
        with (p/(name+'.csv')).open('w',newline='',encoding='utf-8-sig') as f:
            w=csv.DictWriter(f,fieldnames=list(value[0]));w.writeheader();w.writerows(value)
print(json.dumps(result,indent=2));print(json.dumps(summary,indent=2))
