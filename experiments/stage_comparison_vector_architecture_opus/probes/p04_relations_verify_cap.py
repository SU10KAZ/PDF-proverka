import json,sys
from pathlib import Path
ROOT=Path('/home/coder/projects/PDF-proverka')
sys.path.insert(0,str(ROOT)); sys.path.insert(0,str(ROOT/'experiments/stage_comparison_vector_architecture_opus/probes'))
import fitz
from experiments.stage_comparison_vector_blocks import extractor as E
pairs={p['pair_id']:p for p in json.load(open(ROOT/'experiments/stage_comparison_vector_blocks/artifacts/block_pairs.json'))['pairs']}
for pid in ('vk_plan',):
    for sn in ('left','right'):
        s=pairs[pid][sn]
        doc=fitz.open(ROOT/s['pdf']); pg=doc[int(s['page_index'])]; pr=pg.rect
        bn=s['bbox_norm']
        named=fitz.Rect(bn[0]*pr.width,bn[1]*pr.height,bn[2]*pr.width,bn[3]*pr.height)
        content=fitz.Rect(named)*pg.derotation_matrix; content.normalize()
        doc.close()
        nb=[content.x0/pr.width, content.y0/pr.height, content.x1/pr.width, content.y1/pr.height]
        d=E.extract_block(ROOT/s['pdf'], page_index=int(s['page_index']), bbox_norm=nb, block_id='cap')
        e=d['geometry']['extraction']
        print(pid,sn,'uncapped',e['primitives_uncapped'],'capped',e['storage_capped'],'kept',len(d['geometry']['primitives']),'quality',d['vector_quality'])
