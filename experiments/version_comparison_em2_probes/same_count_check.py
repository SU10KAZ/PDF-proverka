"""Переходы (0)→(1), (2)→(3), (3)→(4) по страницам: текст, изображения, растр 36 dpi без аннотаций; (8)→(9) — страницы без пары.

Только чтение PDF; запуск: python -I <скрипт> <папка с PDF корпуса ЭМ2>.
См. docs/version_comparison_plan.md §16.
"""
import sys, fitz, numpy as np, hashlib
P=sys.argv[1]
def op(i): return fitz.open(f"{P}/133_23-ГК-ЭМ2 ({i}).pdf")
def img_sig(pg, doc):
    out=[]
    for im in pg.get_images(full=True):
        try: r=pg.get_image_rects(im[0])
        except Exception: r=[]
        out.append((im[2],im[3], hashlib.md5(doc.xref_stream_raw(im[0]) or b'').hexdigest()[:8], [tuple(round(v) for v in x) for x in r]))
    return sorted(out)
def ras(pg, dpi=36):
    pm=pg.get_pixmap(matrix=fitz.Matrix(dpi/72,dpi/72), colorspace=fitz.csGRAY, annots=False)
    return np.frombuffer(pm.samples,np.uint8).reshape(pm.h,pm.w)
for a,b in [(0,1),(2,3),(3,4),(8,9)]:
    A,B=op(a),op(b)
    print(f"=== ({a})->({b}) pages {len(A)}->{len(B)}")
    if len(A)!=len(B):
        # map by exact text
        tb={}
        for j,pg in enumerate(B): tb.setdefault(pg.get_text(),[]).append(j+1)
        unmatched=[i+1 for i,pg in enumerate(A) if pg.get_text() not in tb]
        print("  old pages whose exact text not found in new:", unmatched)
        continue
    for i in range(len(A)):
        pa,pb=A[i],B[i]
        te = pa.get_text()==pb.get_text()
        ia,ib=img_sig(pa,A),img_sig(pb,B)
        ra,rb=ras(pa),ras(pb)
        if ra.shape!=rb.shape: print(f"  p{i+1}: shape differs"); continue
        d=np.abs(ra.astype(int)-rb.astype(int))>32
        n=int(d.sum())
        bbox=None
        if n:
            ys,xs=np.where(d); k=72/36
            bbox=(int(xs.min()*k),int(ys.min()*k),int(xs.max()*k),int(ys.max()*k))
        if (not te) or ia!=ib or n:
            print(f"  p{i+1} {pa.rect.width:.0f}x{pa.rect.height:.0f}: text_eq={te} img_eq={ia==ib} diffpx36={n} bbox_pt={bbox}")
            if ia!=ib: print("     imgsA:",ia[:3]); print("     imgsB:",ib[:3])
