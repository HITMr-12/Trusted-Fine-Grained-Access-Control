import json,sys
from pathlib import Path
p=Path(__file__).resolve().parent
sys.path.insert(0,str(p.parent/'streaming_probe_20260914/plot_deps'))
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np
s=json.loads((p/'summary.json').read_text());x=np.arange(len(s));labels=['Bob 90%','Bob 100% (authorized rows)']
fig,axes=plt.subplots(2,1,figsize=(8,7),layout='constrained')
for i,(mode,color,label) in enumerate([('INLINE','#8b95a5','Inline policy (idealized reference)'),('NATIVE','#3478bd','Spark + Kyuubi AuthZ + Ranger'),('FGAC','#169977','Current FGAC + bounded prefetch')]):
    axes[0].bar(x+(i-1)*.24,[a[mode+'_total_ms'] for a in s],width=.23,color=color,label=label)
axes[0].set_ylabel('Median end-to-end latency (ms)');axes[0].legend(frameon=False,fontsize=9,loc='upper left');axes[0].set_ylim(0,1500);axes[0].set_xticks(x,labels,rotation=15,ha='right');axes[0].grid(axis='y',alpha=.2);axes[0].set_axisbelow(True)
ci={x['case']:x['current_block_ci95'] for x in json.loads((p/'comparison.json').read_text())}
mean=np.array([a['FGAC_over_NATIVE_mean']*100 for a in s]);lo=np.array([ci[a['case']][0]*100 for a in s]);hi=np.array([ci[a['case']][1]*100 for a in s])
axes[1].errorbar(x,mean,yerr=[mean-lo,hi-mean],fmt='o',color='#169977',capsize=4);axes[1].axhline(0,color='#4a5568',linewidth=1);axes[1].axhline(5,color='#b8860b',linestyle='--',linewidth=1,label='5% reference')
axes[1].set_ylabel('Paired mean FGAC / native - 1 (%)');axes[1].set_xticks(x,labels,rotation=15,ha='right');axes[1].grid(axis='y',alpha=.2);axes[1].legend(frameon=False)
fig.suptitle('Polaris-backed deployment comparison — 60 paired repetitions per case',fontsize=14)
fig.savefig(p/'performance_comparison.png',dpi=180);fig.savefig(p/'performance_comparison.pdf');plt.close(fig)
