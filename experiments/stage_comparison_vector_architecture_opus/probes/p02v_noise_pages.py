"""p02v_: is 'Число примитивов: 814 -> 815' caused by the removed wall, or is it
packaging noise that fires on pages with NO change at all?"""
import json, fitz
from pathlib import Path
from experiments.stage_comparison_vector_blocks import comparator, extractor
ROOT=Path('.'); ART=Path("experiments/stage_comparison_vector_architecture_opus/artifacts")
L="projects_v2/objects/214_Alia_ASTERUS/disciplines/SS/documents/13AB-РД-СОТ-К7 V1/versions/v001/02_work/document.pdf"
R="projects_v2/objects/214_Alia_ASTERUS/disciplines/SS/documents/13AB-РД-СОТ-К7 V1/versions/v002/02_work/document.pdf"
dl,dr=fitz.open(ROOT/L),fitz.open(ROOT/R)
n=min(len(dl),len(dr)); print("pages",len(dl),len(dr))
rows=[]
for pg in range(n):
    if dl[pg].rect.round()!=dr[pg].rect.round(): continue
    # raster diff at 110 dpi
    z=110/72.0
    a=dl[pg].get_pixmap(matrix=fitz.Matrix(z,z),colorspace=fitz.csGRAY)
    b=dr[pg].get_pixmap(matrix=fitz.Matrix(z,z),colorspace=fitz.csGRAY)
    if (a.width,a.height)!=(b.width,b.height): continue
    changed=sum(1 for x,y in zip(a.samples,b.samples) if abs(x-y)>16)
    bb=[0.001,0.001,0.999,0.999]
    Ld=extractor.extract_block(ROOT/L,page_index=pg,bbox_norm=bb,block_id=f"p{pg}L")
    Rd=extractor.extract_block(ROOT/R,page_index=pg,bbox_norm=bb,block_id=f"p{pg}R")
    c=comparator.compare_descriptions(Ld,Rd)
    row={"page":pg,"changed_pixels":changed,"status":c["status"],
         "prims":[Ld["primitive_summary"]["primitive_count"],Rd["primitive_summary"]["primitive_count"]],
         "segs":[Ld["primitive_summary"]["total_segment_count"],Rd["primitive_summary"]["total_segment_count"]],
         "diffs":c["differences"][:4]}
    rows.append(row); print(json.dumps(row,ensure_ascii=False),flush=True)
(ART/"p02v_noise_pages.json").write_text(json.dumps(rows,ensure_ascii=False,indent=1),encoding="utf-8")
