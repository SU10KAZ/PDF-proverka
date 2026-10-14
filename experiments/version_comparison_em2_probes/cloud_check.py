"""Листы с облаками (9)→(10): растровые отличия вне штампа.

Только чтение PDF; запуск: python -I <скрипт> <папка с PDF корпуса ЭМ2>.
См. docs/version_comparison_plan.md §16.
"""
import sys, fitz, numpy as np
P=sys.argv[1]
def op(i): return fitz.open(f"{P}/133_23-ГК-ЭМ2 ({i}).pdf")
A,B=op(9),op(10)
def ras(pg,dpi=72,annots=False):
    pm=pg.get_pixmap(matrix=fitz.Matrix(dpi/72,dpi/72), colorspace=fitz.csGRAY, annots=annots)
    return np.frombuffer(pm.samples,np.uint8).reshape(pm.h,pm.w).astype(int)
for a,b in [(55,58),(67,69),(68,70),(74,76),(82,84)]:
    pa,pb=A[a-1],B[b-1]
    ra,rb=ras(pa),ras(pb)
    d=np.abs(ra-rb)>48
    ys,xs=np.where(d)
    # mask out bottom-right stamp zone (190x60mm) and report remaining
    k=72/25.4; h,w=ra.shape
    m=d.copy(); m[int(h-62*k):, int(w-192*k):]=False
    ys2,xs2=np.where(m)
    te = pa.get_text()==pb.get_text()
    print(f"(9)p{a}->(10)p{b}: text_eq={te} diff_px72={int(d.sum())} outside_stamp={int(m.sum())}",
          f"bbox_out={ (int(xs2.min()),int(ys2.min()),int(xs2.max()),int(ys2.max())) if len(xs2) else None}")
