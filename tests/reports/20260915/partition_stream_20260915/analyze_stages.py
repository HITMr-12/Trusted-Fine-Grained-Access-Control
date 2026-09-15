import collections,json,statistics
from pathlib import Path
p=Path(__file__).resolve().parent;rows=json.loads((p/'verified-records.json').read_text());wanted={x['id']:x for x in rows};wanted_r={x['remote_job_id']:x for x in rows if x['mode']!='NATIVE'}
summary=[]
for role,folder,groups in [('E',p/'e',wanted),('R',p/'r',wanted_r)]:
    events=[json.loads(x) for x in (folder/'spark-events-filtered.jsonl').read_text().splitlines()];stage_group={};job_group={};metric=collections.defaultdict(lambda:collections.defaultdict(float));failures=[]
    for e in events:
        src=e['_source_file'];kind=e['Event']
        if kind=='SparkListenerJobStart':
            group=e.get('Properties',{}).get('spark.jobGroup.id')
            if group not in groups:continue
            # Query ids repeat across preserved runs; select this run's time window.
            record=groups[group]
            if role=='E' and not record['start']*1000-5 <= e['Submission Time'] <= record['end']*1000+5:continue
            job_group[(src,e['Job ID'])]=group
            for st in e['Stage IDs']:stage_group[(src,st)]=group
        elif kind=='SparkListenerTaskEnd':
            group=stage_group.get((src,e['Stage ID']))
            if not group:continue
            m=e.get('Task Metrics',{});a=metric[group];a['tasks']+=1;a['executor_run_ms']+=m.get('Executor Run Time',0);a['executor_cpu_ms']+=m.get('Executor CPU Time',0)/1e6;a['gc_ms']+=m.get('JVM GC Time',0)
            a['input_bytes']+=m.get('Input Metrics',{}).get('Bytes Read',0);a['input_records']+=m.get('Input Metrics',{}).get('Records Read',0);a['memory_spill']+=m.get('Memory Bytes Spilled',0);a['disk_spill']+=m.get('Disk Bytes Spilled',0)
            if e.get('Task End Reason',{}).get('Reason')!='Success':failures.append({'group':group,'reason':e.get('Task End Reason')})
        elif kind=='SparkListenerJobEnd':
            group=job_group.get((src,e['Job ID']))
            if group and e.get('Job Result',{}).get('Result')!='JobSucceeded':failures.append({'group':group,'reason':e.get('Job Result')})
    assert not failures,failures
    assert len(metric)==len(groups),(role,len(metric),len(groups))
    for group,m in metric.items():
        record=groups[group];summary.append(dict(role=role,id=record['id'],case=record['case'],mode=record['mode'],warmup=record['warmup'],**m))
out=[]
for role,mode in [(role,mode) for role in ['E','R'] for mode in sorted({x['mode'] for x in rows})]:
    for case in sorted({x['case'] for x in rows}):
        xs=[x for x in summary if x['role']==role and x['mode']==mode and x['case']==case and not x['warmup']]
        assert len(xs)==60
        out.append(dict(role=role,mode=mode,case=case,**{k:statistics.median(x[k] for x in xs) for k in ['tasks','executor_run_ms','executor_cpu_ms','gc_ms','input_bytes','input_records','memory_spill','disk_spill']}))
(p/'stage-summary.json').write_text(json.dumps(out,indent=2));(p/'stage-validation.json').write_text(json.dumps({'ok':True,'query_groups':len(summary),'failed_tasks_or_jobs':0,'memory_spill_bytes':sum(x['memory_spill'] for x in summary),'disk_spill_bytes':sum(x['disk_spill'] for x in summary)},indent=2));print(json.dumps(out,indent=2))
