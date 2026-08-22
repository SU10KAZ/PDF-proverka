import json,sys,copy
from pathlib import Path
ROOT=Path('/home/coder/projects/PDF-proverka')
P=ROOT/'experiments/stage_comparison_vector_architecture_opus/probes'
sys.path.insert(0,str(ROOT)); sys.path.insert(0,str(P))
import relgraph_core as R
from relgraph_crop import coverage, renormalize, jitter_rect, crop_edge_rect
from relgraph_granularity import project
from relgraph_rotfix import extract_rotation_correct
pairs={p['pair_id']:p for p in json.load(open(ROOT/'experiments/stage_comparison_vector_blocks/artifacts/block_pairs.json'))['pairs']}
s=pairs['eom_singleline_changed']['left']
base,named,content=extract_rotation_correct(s['pdf'],int(s['page_index']),s['bbox_norm'],'eom_left')
print('base[bbox] (probe set it to NAMED page-space rect):',[round(v,1) for v in base['bbox']])
print('content rect actually used for extraction        :',[round(v,1) for v in (content.x0,content.y0,content.x1,content.y1)])
xs=[p[0] for pr in base['geometry']['primitives'] for sg in pr['raw']['segments'] for p in sg]
ys=[p[1] for pr in base['geometry']['primitives'] for sg in pr['raw']['segments'] for p in sg]
print('RAW segment coord extent                         :',[round(min(xs),1),round(min(ys),1),round(max(xs),1),round(max(ys),1)])
nxs=[p[0] for pr in base['geometry']['primitives'] for sg in pr['normalized']['segments'] for p in sg]
nys=[p[1] for pr in base['geometry']['primitives'] for sg in pr['normalized']['segments'] for p in sg]
print('base NORMALIZED extent (rotation-mapped by probe):',[round(min(nxs),3),round(min(nys),3),round(max(nxs),3),round(max(nys),3)])
z=renormalize(base, base['bbox'])   # ZERO jitter -> must be identity if self-consistent
nzx=[p[0] for pr in z['geometry']['primitives'] for sg in pr['normalized']['segments'] for p in sg]
nzy=[p[1] for pr in z['geometry']['primitives'] for sg in pr['normalized']['segments'] for p in sg]
print('renormalize(base, base.bbox) NORMALIZED extent   :',[round(min(nzx),3),round(min(nzy),3),round(max(nzx),3),round(max(nzy),3)])
c=coverage(base,z)
print('ZERO-JITTER control coverage (should be 1.0):',c)
gb=R.build_relation_graph(base); gz=R.build_relation_graph(z)
print('ZERO-JITTER relG1:',round(R.weighted_jaccard(project(gb['relations'],1),project(gz['relations'],1)),4),
      ' relG3:',round(R.weighted_jaccard(gb['relations'],gz['relations']),4))
# correct control: jitter the CONTENT rect (the one extract_block normalized against)
crect=[content.x0,content.y0,content.x1,content.y1]
b2=copy.deepcopy(base); b2['bbox']=crect
b2=renormalize(b2,crect)   # rebuild base normalized in content frame
for f in (0.0,0.002,0.005,0.01,0.02,0.05):
    v=renormalize(b2, jitter_rect(crect,f) if f else crect)
    cc=coverage(b2,v); g2=R.build_relation_graph(b2); gv=R.build_relation_graph(v)
    print(f'  CONTENT-frame jitter {f:<6}: cov@0.005={cc["tol_0.005"]:.4f} cov@0.01={cc["tol_0.01"]:.4f} '
          f'relG1={R.weighted_jaccard(project(g2["relations"],1),project(gv["relations"],1)):.4f} '
          f'relG3={R.weighted_jaccard(g2["relations"],gv["relations"]):.4f}')
v=renormalize(b2, crop_edge_rect(crect))
cc=coverage(b2,v); g2=R.build_relation_graph(b2); gv=R.build_relation_graph(v)
print(f'  CONTENT-frame edge-crop 10%: cov@0.005={cc["tol_0.005"]:.4f} cov@0.01={cc["tol_0.01"]:.4f} '
      f'relG1={R.weighted_jaccard(project(g2["relations"],1),project(gv["relations"],1)):.4f}')
