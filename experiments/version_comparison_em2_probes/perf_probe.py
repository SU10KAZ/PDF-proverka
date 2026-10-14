"""Время и память рендера и разности самого большого листа (10) стр. 12 / (11) стр. 13 при 100/150/200 dpi.

Только чтение PDF; запуск: python -I <скрипт> <папка с PDF корпуса ЭМ2>.
См. docs/version_comparison_plan.md §16.
"""
import sys, time, fitz, numpy as np, cv2, resource
P=sys.argv[1]
d=fitz.open(f"{P}/133_23-ГК-ЭМ2 (10).pdf"); e=fitz.open(f"{P}/133_23-ГК-ЭМ2 (11).pdf")
for dpi in (100,150,200):
    t=time.time()
    pa=d[11].get_pixmap(matrix=fitz.Matrix(dpi/72,dpi/72), colorspace=fitz.csGRAY, annots=False)
    pb=e[12].get_pixmap(matrix=fitz.Matrix(dpi/72,dpi/72), colorspace=fitz.csGRAY, annots=False)
    t1=time.time()-t
    a=np.frombuffer(pa.samples,np.uint8).reshape(pa.h,pa.w); b=np.frombuffer(pb.samples,np.uint8).reshape(pb.h,pb.w)
    t=time.time()
    ia=a<200; ib=b<200
    k=np.ones((3,3),np.uint8)
    da=cv2.dilate(ib.astype(np.uint8),k); db=cv2.dilate(ia.astype(np.uint8),k)
    old_only=ia & (da==0); new_only=ib & (db==0)
    n,lab,st,_=cv2.connectedComponentsWithStats((old_only|new_only).astype(np.uint8)*255)
    t2=time.time()-t
    print(f"dpi={dpi}: {pa.w}x{pa.h} = {pa.w*pa.h/1e6:.1f} Mpx; render 2 pages {t1:.1f}s; diff+components {t2:.1f}s; components={n-1}; old_only={int(old_only.sum())} new_only={int(new_only.sum())}")
print("maxrss MB:", resource.getrusage(resource.RUSAGE_SELF).ru_maxrss/1024)
