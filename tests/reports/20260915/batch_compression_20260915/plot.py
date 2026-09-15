import json,sys
from pathlib import Path
p=Path(__file__).resolve().parent
sys.path.insert(0,str(p.parent/'streaming_probe_20260914/plot_deps'))
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
s=json.loads((p/'summary.json').read_text());m=json.loads((p/'micro-summary.json').read_text())
fig,axes=plt.subplots(1,3,figsize=(14,4.2));colors={'NONE':'#516173','LZ4':'#168c9a','ZSTD':'#bf6831'}
for ax,case,title in zip(axes[:2],['bob_90','bob_100'],['90% of authorized rows','100% of authorized rows']):
 for codec,color in colors.items():
  rows=[next(x for x in s if x['case']==case and x['mode']==f'{codec}_{size}') for size in [8192,32768,65536]]
  vals=[x['vs_same_codec_8192_pct'] for x in rows]
  ax.errorbar([0,1,2],vals,yerr=[[x['vs_same_codec_8192_pct']-x['vs_same_codec_8192_ci'][0] for x in rows],[x['vs_same_codec_8192_ci'][1]-x['vs_same_codec_8192_pct'] for x in rows]],fmt='o-',capsize=3,label=codec,color=color)
 ax.set_title(title);ax.set_xticks([0,1,2],['8,192','32,768','65,536']);ax.set_xlabel('Maximum rows per batch');ax.set_ylabel('Paired mean latency change vs 8,192 (%)');ax.axhline(0,color='gray',lw=.8,ls='--');ax.grid(alpha=.2);ax.legend()
for codec,color in colors.items():
 vals=[next(x for x in m if x['codec']==codec and x['size']==size) for size in [8192,32768,65536]]
 axes[2].plot([0,1,2],[x['encode_cpu_ms']+x['decode_cpu_ms'] for x in vals],'o-',label=codec,color=color)
axes[2].set_title('In-memory IPC encode + decode CPU');axes[2].set_xticks([0,1,2],['8,192','32,768','65,536']);axes[2].set_xlabel('Maximum rows per batch');axes[2].set_ylabel('Sum of median CPU times (ms)');axes[2].grid(alpha=.2);axes[2].legend()
fig.suptitle('Batch size × compression: same data and persistent Spark instances',fontsize=13)
fig.tight_layout();fig.savefig(p/'batch_compression.png',dpi=180);fig.savefig(p/'batch_compression.pdf')
