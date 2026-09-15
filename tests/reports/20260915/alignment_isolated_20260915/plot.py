import sys,json
from pathlib import Path
p=Path(__file__).resolve().parent
sys.path.insert(0,str(p.parent/'streaming_probe_20260914/plot_deps'))
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
s=json.loads((p/'summary.json').read_text());pairs=json.loads((p/'pairs.json').read_text())
fig,axes=plt.subplots(1,2,figsize=(12,4.8),sharex=True)
colors=['#2563eb','#16a34a','#d97706']
comps=['STREAM_vs_COLLECT','STREAM_vs_NATIVE','COLLECT_vs_NATIVE']
for ax,case in zip(axes,['bob_001','bob_01']):
 for run,color in zip([1,2,3],colors):
  for j,comp in enumerate(comps):
   row=next(x for x in s if x['run']==run and x['case']==case and x['comparison']==comp)
   v=row['paired_mean_pct'];lo,hi=row['ci_block4'];y=j+(run-2)*.19
   ax.errorbar(v,y,xerr=[[v-lo],[hi-v]],fmt='o',color=color,capsize=3,label='Restart '+str(run) if j==0 else None)
 ax.axvline(0,color='#64748b',linewidth=1);ax.set_yticks(range(3),[c.replace('_vs_',' / ') for c in comps]);ax.invert_yaxis();ax.set_title('Authorized return '+('0.1%' if case=='bob_001' else '1%'));ax.grid(axis='x',alpha=.2);ax.set_xlabel('Paired latency change (%) | negative = faster')
axes[0].legend(loc='best',fontsize=8)
fig.suptitle('Independent-process comparison across three JVM restarts',fontsize=13)
fig.text(.5,.01,'48 balanced pairs per case per restart; 95% circular block bootstrap (block = 4).',ha='center',fontsize=9)
fig.tight_layout(rect=[0,.04,1,.94]);fig.savefig(p/'isolated_comparison.png',dpi=180);fig.savefig(p/'isolated_comparison.pdf');plt.close(fig)
