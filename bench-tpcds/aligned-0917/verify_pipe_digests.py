import json, sys, re
from pathlib import Path

base = Path(r'D:\Kimi\WorkSpace\可信细粒度访问控制\bench-tpcds')
CASE = {'s001':'bench_full_001','s01':'bench_full_01','s10':'bench_full_1','s477':'bench_full_5',
        's575':'bench_full_06','s718':'bench_full_075','s86':'bench_full_09','s100':'bench_full_1_0'}

# native digests from the 3-mode aligned run
ref = {}
for line in open(base / 'aligned-0917' / 'records3.jsonl'):
    r = json.loads(line)
    if r['mode'] == 'NATIVE' and not r.get('warmup'):
        ref[r['case']] = r['digest']

ok_all = True
for label, case in CASE.items():
    logdir = base / ('pipe_logs_' + label)
    pipe_line = None
    for l in open(logdir / 'e_pipe.log'):
        if l.startswith('PIPE '):
            pipe_line = l[5:]
    p = json.loads(pipe_line)
    d = p['digest']
    r = ref[case]
    ok = d['rows'] == r['rows'] and d['sum1'] == r['sum1'] and d['sum2'] == r['sum2']
    ok_all &= ok
    print('%-5s %-16s equal=%s rows=%s wall=%.2fs e_work=%.0fms' % (
        label, case, ok, d['rows'], p['wall_s'], p['e_work_ms']))
print('ALL_DIGEST_EQUAL:', ok_all)
