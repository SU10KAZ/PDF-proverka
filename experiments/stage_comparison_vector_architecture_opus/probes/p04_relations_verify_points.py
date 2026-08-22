import json, fitz
from pathlib import Path
ROOT=Path('/home/coder/projects/PDF-proverka')
pairs={p['pair_id']:p for p in json.load(open(ROOT/'experiments/stage_comparison_vector_blocks/artifacts/block_pairs.json'))['pairs']}
def pts(pg):
    out=[]
    for d in pg.get_drawings():
        for it in d['items']:
            for v in it[1:]:
                if isinstance(v,fitz.Point): out.append((v.x,v.y))
                elif isinstance(v,fitz.Rect): out.extend([(v.x0,v.y0),(v.x1,v.y1)])
    return out
for pid in ('vk_node_plan','vk_plan','vk_nodes','eom_singleline_changed'):
    for sn in ('left',):
        s=pairs[pid][sn]
        doc=fitz.open(ROOT/s['pdf']); pg=doc[int(s['page_index'])]; pr=pg.rect; bn=s['bbox_norm']
        br=fitz.Rect(bn[0]*pr.width,bn[1]*pr.height,bn[2]*pr.width,bn[3]*pr.height)
        m=pg.rotation_matrix
        P=pts(pg); e=n=b=0
        for x,y in P:
            p=fitz.Point(x,y); ine=br.contains(p); inn=br.contains(p*m)
            e+=ine; n+=inn; b+= (ine and inn)
        print(f"{pid:24s} rot={pg.rotation:3d} total={len(P):8d} extracted={e:8d} named={n:8d} both={b:8d} agree={b/max(n,1):.4f}")
        doc.close()
