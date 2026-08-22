import json, fitz, sys
from pathlib import Path
ROOT=Path('/home/coder/projects/PDF-proverka')
pairs=json.load(open(ROOT/'experiments/stage_comparison_vector_blocks/artifacts/block_pairs.json'))['pairs']
for p in pairs:
    for sn in ('left','right'):
        s=p[sn]
        doc=fitz.open(ROOT/s['pdf']); pg=doc[int(s['page_index'])]
        rot=pg.rotation; pr=pg.rect
        bn=s['bbox_norm']
        br=fitz.Rect(bn[0]*pr.width, bn[1]*pr.height, bn[2]*pr.width, bn[3]*pr.height)
        # independent: use text spans (not drawings) to test region identity
        td=pg.get_text('dict')
        spans=[]
        for b in td['blocks']:
            for l in b.get('lines',[]):
                for sp in l['spans']:
                    spans.append((sp['bbox'], sp['text']))
        m=pg.rotation_matrix
        extracted=[t for bb,t in spans if br.contains(fitz.Point((bb[0]+bb[2])/2,(bb[1]+bb[3])/2))]
        named=[t for bb,t in spans if br.contains((fitz.Point((bb[0]+bb[2])/2,(bb[1]+bb[3])/2))*m)]
        # cropbox / mediabox dims
        print(f"{p['pair_id']:24s} {sn:5s} rot={rot:3d} page.rect={pr.width:.0f}x{pr.height:.0f} cropbox={pg.cropbox.width:.0f}x{pg.cropbox.height:.0f} bbox_norm={[round(v,3) for v in bn]}")
        print(f"    block_rect={[round(v,1) for v in br]}  n_spans_extracted={len(extracted)} n_spans_named={len(named)}")
        print(f"    extracted sample={extracted[:6]}")
        print(f"    named     sample={named[:6]}")
        doc.close()
