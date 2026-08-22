import json, fitz, numpy as np
from pathlib import Path
ROOT=Path('/home/coder/projects/PDF-proverka')
OUT=Path('/tmp/claude-1001/-home-coder-projects-PDF-proverka/7be66dd6-80e8-4c87-9aef-d5834ab15302/scratchpad')
pairs={p['pair_id']:p for p in json.load(open(ROOT/'experiments/stage_comparison_vector_blocks/artifacts/block_pairs.json'))['pairs']}
def arr(pix):
    a=np.frombuffer(pix.samples,dtype=np.uint8).reshape(pix.height,pix.width,pix.n)
    return a[:,:,:3]
for pid,sn in [('eom_singleline_changed','left'),('vk_node_plan','left')]:
    s=pairs[pid][sn]
    doc=fitz.open(ROOT/s['pdf']); pg=doc[int(s['page_index'])]
    pr=pg.rect; bn=s['bbox_norm']
    br=fitz.Rect(bn[0]*pr.width,bn[1]*pr.height,bn[2]*pr.width,bn[3]*pr.height)
    print(pid,sn,'rot',pg.rotation,'page.rect',pr,'block_rect',br)
    A=pg.get_pixmap(matrix=fitz.Matrix(0.5,0.5),clip=br,alpha=False)  # what save_description renders
    A.save(str(OUT/f'{pid}_{sn}_A_pixmap_clip.png'))
    pg.set_rotation(0)
    print('  after set_rotation(0): page.rect',pg.rect)
    B=pg.get_pixmap(matrix=fitz.Matrix(0.5,0.5),clip=br,alpha=False)  # same numeric rect in unrotated space
    B.save(str(OUT/f'{pid}_{sn}_B_unrotated_clip.png'))
    a,b=arr(A),arr(B)
    print('  A shape',a.shape,'B shape',b.shape)
    # compare A vs rotations of B
    for k in (0,1,2,3):
        rb=np.rot90(b,k)
        if rb.shape==a.shape:
            print(f'   rot90x{k}: identical={np.array_equal(a,rb)} meanabsdiff={np.abs(a.astype(int)-rb.astype(int)).mean():.2f}')
    doc.close()
