import json,sys
from pathlib import Path
p=Path(__file__).resolve().parent
sys.path.insert(0,str(p.parent/'streaming_probe_20260914/plot_deps'))
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
s=json.loads((p/'summary.json').read_text());labels=['Bob 0.1%','Bob 1%','Bob 10%','Bob 50%','Bob 90%','Bob 100%','Alice fare>=50','Whole source'];x=list(range(len(s)))
fig,axes=plt.subplots(2,1,figsize=(12,8),gridspec_kw={'height_ratios':[1.25,1]})
for off,mode,label,color in [(-.25,'native_ms','Spark + Kyuubi AuthZ + Ranger','#61738a'),(0,'fgac_ms','FGAC partition streaming','#168d91'),(.25,'inline_ms','Inline policy reference','#c88b41')]:axes[0].bar([v+off for v in x],[r[mode] for r in s],width=.24,label=label,color=color)
axes[0].set_ylabel('Median full-result latency (ms)');axes[0].set_xticks(x,labels);axes[0].legend();axes[0].grid(axis='y',alpha=.2)
y=[r['fgac_vs_native_pct'] for r in s];axes[1].errorbar(x,y,yerr=[[v-r['ci_block3'][0] for v,r in zip(y,s)],[r['ci_block3'][1]-v for v,r in zip(y,s)]],fmt='o',capsize=5,color='#168d91');axes[1].axhline(0,color='gray',ls='--');axes[1].set_xticks(x,labels);axes[1].set_ylabel('FGAC vs native, paired mean (%)');axes[1].grid(alpha=.2)
fig.suptitle('Unified comparison: same snapshot, full four-column digest, 36 paired rounds');fig.tight_layout();fig.savefig(p/'final_comparison.png',dpi=180);fig.savefig(p/'final_comparison.pdf')
