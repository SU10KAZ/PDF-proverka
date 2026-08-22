"""p02v_: the same real revision has a SECOND changed region (45 dirty px).
Does the tightest possible crop detect it?  Tests 'detection depends entirely
on how tightly the block is cropped'."""
import json
from pathlib import Path
from experiments.stage_comparison_vector_blocks import comparator, extractor
ROOT=Path('.'); ART=Path("experiments/stage_comparison_vector_architecture_opus/artifacts")
L="projects_v2/objects/214_Alia_ASTERUS/disciplines/SS/documents/13AB-РД-СОТ-К7 V1/versions/v001/02_work/document.pdf"
R="projects_v2/objects/214_Alia_ASTERUS/disciplines/SS/documents/13AB-РД-СОТ-К7 V1/versions/v002/02_work/document.pdf"
REG2=[0.1260827625504557,0.192806057609153,0.1817937506541454,0.2736602108000882]
def grow(b,k):
    cx,cy=(b[0]+b[2])/2,(b[1]+b[3])/2; w,h=(b[2]-b[0])*k/2,(b[3]-b[1])*k/2
    return [max(0,cx-w),max(0,cy-h),min(1,cx+w),min(1,cy+h)]
rows=[]
for name,k in (("tight",1.0),("x0.5_tighter",0.5),("x3",3.0),("x8",8.0)):
    bb=grow(REG2,k)
    a=extractor.extract_block(ROOT/L,page_index=8,bbox_norm=bb,block_id="r2L")
    b=extractor.extract_block(ROOT/R,page_index=8,bbox_norm=bb,block_id="r2R")
    c=comparator.compare_descriptions(a,b)
    row={"scale":name,"bbox":[round(v,5) for v in bb],"status":c["status"],
         "geom":round(c["geometry"]["similarity"],6),"topo":round(c["topology"]["similarity"],6),
         "segs":[a["primitive_summary"]["total_segment_count"],b["primitive_summary"]["total_segment_count"]],
         "prims":[a["primitive_summary"]["primitive_count"],b["primitive_summary"]["primitive_count"]],
         "diffs":c["differences"][:5]}
    rows.append(row); print(json.dumps(row,ensure_ascii=False),flush=True)
(ART/"p02v_region2_ladder.json").write_text(json.dumps(rows,ensure_ascii=False,indent=1),encoding="utf-8")
