"""Untimed transport-only validation using live TCP socket receive counters."""
import json,os,re,subprocess,sys,time
from pathlib import Path
from streaming.client import GovernedClient,iter_batches
r=Path('/home/lyb/fgac-lab');out=r/'runs/batch-compression-0915';d=r/'deploy/fgac-batch-compression'
cases=[c for c in json.loads((d/'cases.json').read_text()) if c['name'] in ['bob_90','bob_100']]
reps=int(sys.argv[1]);name=sys.argv[2];rows=[]
for rep in range(reps):
    for case in cases:
        for mode,port in [(f'{codec}_{size}',18820+i*3+j) for i,size in enumerate([8192,32768,65536]) for j,codec in enumerate(['NONE','LZ4','ZSTD'])]:
            client=GovernedClient('grpc://172.168.22.23:'+str(port),'Bearer bob-token')
            try:
                info=client.describe(case['plan']);reader=client.client.do_get(info.endpoints[0].ticket,client.options)
                nr=nb=logical=0;t=time.perf_counter()
                for batch in iter_batches(reader):nr+=batch.num_rows;nb+=1;logical+=batch.nbytes
                elapsed=(time.perf_counter()-t)*1000
                stats=subprocess.check_output(['ss','-tinH','dst','172.168.22.23','dport','=',str(port)],text=True)
                values=[int(x) for x in re.findall(r'bytes_received:(\d+)',stats)]
                assert len(values)==1,(mode,stats)
                assert nr==case['rows']
                x={'mode':mode,'case':case['name'],'rep':rep,'rows':nr,'batches':nb,'arrow_logical_bytes':logical,'tcp_received_bytes':values[0],'read_ms':elapsed,'tcp_stats':stats}
                rows.append(x);print(json.dumps({k:v for k,v in x.items() if k!='tcp_stats'}),flush=True)
            finally:client.close()
(out/name).write_text(json.dumps(rows,indent=2))
