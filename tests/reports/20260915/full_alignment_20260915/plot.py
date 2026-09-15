import sys,json
from pathlib import Path
p=Path(__file__).resolve().parent;sys.path.insert(0,str(p.parent/'streaming_probe_20260914/plot_deps'))
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
s=json.loads((p/'summary.json').read_text());cases=json.loads((p/'cases.json').read_text());names=[c['name'] for c in cases];labels=['Bob 0.1%','Bob 1%','Bob 10%','Bob 50%','Bob 90%','Bob auth 100%','Alice original','Whole source 100%']
fig,axes=plt.subplots(1,3,figsize=(16,7),sharey=True)
for ax,comp,title in zip(axes,['STREAM_vs_NATIVE','STREAM_vs_COLLECT','STREAM_vs_INLINE'],['STREAM / Ranger native','STREAM / old COLLECT','STREAM / INLINE']):
 for run,color in zip([1,2,3],['#2563eb','#16a34a','#d97706']):
  for j,case in enumerate(names):
   x=next(x for x in s if x['run']==run and x['case']==case and x['comparison']==comp);v=x['paired_mean_pct'];lo,hi=x['ci_block4']
   ax.errorbar(v,j+(run-2)*.18,xerr=[[v-lo],[hi-v]],fmt='o',markersize=4,color=color,capsize=2,label='Restart '+str(run) if j==0 else None)
 ax.axvline(0,color='#64748b',linewidth=1);ax.axvline(5,color='#dc2626',linewidth=1,linestyle=':');ax.set_title(title);ax.grid(axis='x',alpha=.2);ax.set_xlabel('Paired latency change (%)')
axes[0].set_yticks(range(len(names)),labels);axes[0].invert_yaxis();axes[0].legend(fontsize=8,loc='best')
fig.suptitle('Eight scenarios, independent processes, three restarts with five-minute gaps',fontsize=14)
fig.text(.5,.015,'48 balanced four-way groups per scenario per restart. Bars: within-restart 95% block-bootstrap intervals; negative = faster.',ha='center',fontsize=9)
fig.tight_layout(rect=[0,.04,1,.94]);fig.savefig(p/'full_comparison.png',dpi=170);fig.savefig(p/'full_comparison.pdf');plt.close(fig)
