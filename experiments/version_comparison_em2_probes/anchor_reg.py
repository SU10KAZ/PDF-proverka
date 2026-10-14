"""Выравнивание по уникальным общим словам (RANSAC подобия): тождество, сдвиг разделённых листов, масштаб при смене формата.

Только чтение PDF; запуск: python -I <скрипт> <папка с PDF корпуса ЭМ2>.
См. docs/version_comparison_plan.md §16.
"""
import sys, fitz, numpy as np, cv2
from collections import Counter
P=sys.argv[1]
def op(i): return fitz.open(f"{P}/133_23-ГК-ЭМ2 ({i}).pdf")
def uniq(pg):
    ws=pg.get_text("words"); c=Counter(w[4] for w in ws)
    return {w[4]:((w[0]+w[2])/2,(w[1]+w[3])/2) for w in ws if c[w[4]]==1 and len(w[4])>=3}
def reg(a,pa,b,pb):
    A,B=op(a)[pa-1],op(b)[pb-1]
    ua,ub=uniq(A),uniq(B); keys=[k for k in ua if k in ub]
    src=np.float32([ua[k] for k in keys]); dst=np.float32([ub[k] for k in keys])
    M,inl=cv2.estimateAffinePartial2D(src,dst,method=cv2.RANSAC,ransacReprojThreshold=2.0)
    s=np.hypot(M[0,0],M[1,0]); ang=np.degrees(np.arctan2(M[1,0],M[0,0]))
    res=np.abs((src@M[:,:2].T+M[:,2])-dst).max(1)[inl.ravel()==1]
    print(f"({a})p{pa} {A.rect.width:.0f}x{A.rect.height:.0f} -> ({b})p{pb} {B.rect.width:.0f}x{B.rect.height:.0f}: anchors={len(keys)} inliers={int(inl.sum())} scale={s:.4f} rot={ang:.3f}deg shift=({M[0,2]:.1f},{M[1,2]:.1f})pt maxres={res.max():.2f}pt")
reg(10,12,11,13)   # changed VRU-1, same format
reg(10,13,11,14)
reg(4,20,5,24)     # split group plan -> 2nd floor
reg(4,21,5,27)
reg(1,6,2,8)       # VRU-1 split
reg(5,39,6,40)     # pavilion plan with format change
