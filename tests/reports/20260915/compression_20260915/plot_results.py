import json,sys
from pathlib import Path
p=Path(__file__).resolve().parent
sys.path.insert(0,str(p.parent/'streaming_probe_20260914/plot_deps'))
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np
s=json.loads((p/'summary.json').read_text());wire=json.loads((p/'wire-summary.json').read_text())
modes=['NATIVE','NONE','LZ4','ZSTD'];colors=['#3478bd','#6b7787','#d29b21','#169977'];cases=['bob_90','bob_100'];x=np.arange(2)
fig,axes=plt.subplots(2,1,figsize=(9,8),layout='constrained')
for i,m in enumerate(modes):
    v=[next(z for z in s if z['case']==c and z['mode']==m)['total_ms'] for c in cases]
    axes[0].bar(x+(i-1.5)*.19,v,width=.18,color=colors[i],label=m)
axes[0].set_ylabel('Median end-to-end latency (ms)');axes[0].legend(frameon=False,ncols=4);axes[0].set_ylim(0,max(z['total_ms'] for z in s)*1.25)
for i,m in enumerate(modes[1:]):
    v=[next(z for z in wire if z['case']==c and z['mode']==m)['tcp_received_bytes_median']/1e6 for c in cases]
    axes[1].bar(x+(i-1)*.24,v,width=.23,color=colors[i+1],label=m)
axes[1].set_ylabel('TCP received payload (MB)');axes[1].legend(frameon=False,ncols=3)
for ax in axes:
    ax.set_xticks(x,['90% authorized rows','100% authorized rows']);ax.grid(axis='y',alpha=.2);ax.set_axisbelow(True)
fig.suptitle('Flight compression: latency vs. transferred bytes')
fig.savefig(p/'compression_comparison.png',dpi=180);fig.savefig(p/'compression_comparison.pdf');plt.close(fig)
