import json,sys
from pathlib import Path
ROOT=Path('/home/coder/projects/PDF-proverka')
P=ROOT/'experiments/stage_comparison_vector_architecture_opus/probes'
sys.path.insert(0,str(ROOT)); sys.path.insert(0,str(P))
import relgraph_core as R
from relgraph_crop import coverage, renormalize, text_multiset, jitter_rect
from relgraph_granularity import project
from relgraph_rotfix import extract_rotation_correct
pairs={p['pair_id']:p for p in json.load(open(ROOT/'experiments/stage_comparison_vector_blocks/artifacts/block_pairs.json'))['pairs']}
s=pairs['eom_singleline_changed']['left']
base,named,_=extract_rotation_correct(s['pdf'],int(s['page_index']),s['bbox_norm'],'eom_left')
rect=base['bbox']; gb=R.build_relation_graph(base)
def row(name,nr):
    var=renormalize(base,nr); gv=R.build_relation_graph(var)
    c=coverage(base,var)
    return dict(variant=name, cov005=c['tol_0.005'], cov01=c['tol_0.01'],
                relG1=round(R.weighted_jaccard(project(gb['relations'],1),project(gv['relations'],1)),4),
                relG3=round(R.weighted_jaccard(gb['relations'],gv['relations']),4))
print("SIGNS as probe used (+,-,-,+) = anti-symmetric scale")
for f in (0.002,0.005,0.01,0.015,0.02,0.03,0.05):
    print(f"  frac={f:<6}", row(f'jitter_{f}', jitter_rect(rect,f)))
print("PURE TRANSLATION (+,+,+,+): same shift, no scale change")
for f in (0.005,0.01,0.02,0.03,0.05):
    print(f"  frac={f:<6}", row(f'trans_{f}', jitter_rect(rect,f,signs=(1,1,1,1))))
print("PURE UNIFORM OUTSET (-,-,+,+): symmetric grow")
for f in (0.005,0.01,0.02,0.05):
    print(f"  frac={f:<6}", row(f'outset_{f}', jitter_rect(rect,f,signs=(-1,-1,1,1))))
