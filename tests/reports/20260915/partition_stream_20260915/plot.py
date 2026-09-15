import json,sys
from pathlib import Path
p=Path(__file__).resolve().parent
sys.path.insert(0,str(p.parent/'streaming_probe_20260914/plot_deps'))
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
s=json.loads((p/'summary.json').read_text());fig,axes=plt.subplots(1,3,figsize=(13,4))
labels=['90% authorized','100% authorized'];x=[0,1]
for off,mode,color in [(-.18,'collect','#627386'),(.18,'stream','#138d91')]:
 axes[0].bar([v+off for v in x],[r[mode]['total_ms'] for r in s],width=.35,label=mode,color=color)
 axes[1].bar([v+off for v in x],[r[mode]['first_arrow_ms'] for r in s],width=.35,label=mode,color=color)
for ax,title in zip(axes[:2],['Median full-result latency','Median first Arrow batch arrival']):
 ax.set_xticks(x,labels);ax.set_ylabel('Milliseconds');ax.set_title(title);ax.legend();ax.grid(axis='y',alpha=.2)
y=[r['paired_mean_delta_pct'] for r in s];axes[2].errorbar(x,y,yerr=[[v-r['ci_block5'][0] for v,r in zip(y,s)],[r['ci_block5'][1]-v for v,r in zip(y,s)]],fmt='o',capsize=5,color='#138d91');axes[2].axhline(0,color='gray',ls='--');axes[2].set_xticks(x,labels);axes[2].set_ylabel('Paired mean change (%)');axes[2].set_title('STREAM vs COLLECT, block 95% CI');axes[2].grid(alpha=.2)
fig.suptitle('Partition-internal Arrow delivery: same R/E Spark, no compression');fig.tight_layout();fig.savefig(p/'partition_stream.png',dpi=180);fig.savefig(p/'partition_stream.pdf')
