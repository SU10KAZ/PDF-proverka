"""p02v_: does v0.1 catch a LARGER real revision at whole-page scale?
The falsify_ probe ran its crop ladder on exactly one real change (395 dirty px).
Two much larger real changes exist in its own falsify_visual/ output.
"""
import json, time
from pathlib import Path
from experiments.stage_comparison_vector_blocks import comparator, extractor
ROOT = Path(__file__).resolve().parents[3]
ART = Path(__file__).resolve().parents[1] / "artifacts"

CASES = [
 ("ar12k4_p11", "projects_v2/objects/214_Alia_ASTERUS/disciplines/AR/documents/13АВ-РД-АР1.2-К4/versions/v002/02_work/document.pdf",
  "projects_v2/objects/214_Alia_ASTERUS/disciplines/AR/documents/13АВ-РД-АР1.2-К4/versions/v003/02_work/document.pdf", 11, 86775),
 ("ar12k4_p12", "projects_v2/objects/214_Alia_ASTERUS/disciplines/AR/documents/13АВ-РД-АР1.2-К4/versions/v002/02_work/document.pdf",
  "projects_v2/objects/214_Alia_ASTERUS/disciplines/AR/documents/13АВ-РД-АР1.2-К4/versions/v003/02_work/document.pdf", 12, 180830),
 ("aps_k4_p8_zero_change", "projects_v2/objects/214_Alia_ASTERUS/disciplines/SS/documents/13АВ-РД-АПЗ.АПС-К4/versions/v002/02_work/document.pdf",
  "projects_v2/objects/214_Alia_ASTERUS/disciplines/SS/documents/13АВ-РД-АПЗ.АПС-К4/versions/v003/02_work/document.pdf", 8, 0),
]
rows=[]
for tag,l,r,pg,px in CASES:
    t0=time.time()
    bb=[0.001,0.001,0.999,0.999]
    L=extractor.extract_block(ROOT/l,page_index=pg,bbox_norm=bb,block_id=tag+"_L")
    R=extractor.extract_block(ROOT/r,page_index=pg,bbox_norm=bb,block_id=tag+"_R")
    c=comparator.compare_descriptions(L,R)
    row={"tag":tag,"dirty_pixels":px,"status":c["status"],
         "geom":round(c["geometry"]["similarity"],6),"topo":round(c["topology"]["similarity"],6),
         "text_eff":round(c["text"]["effective_similarity"],4),"text_reliable":c["text"]["reliable"],
         "prims":[L["primitive_summary"]["primitive_count"],R["primitive_summary"]["primitive_count"]],
         "segs":[L["primitive_summary"]["total_segment_count"],R["primitive_summary"]["total_segment_count"]],
         "quality":[L["vector_quality"],R["vector_quality"]],
         "diffs":c["differences"][:8],"secs":round(time.time()-t0,1)}
    rows.append(row); print(json.dumps(row,ensure_ascii=False), flush=True)
(ART/"p02v_bigger_change_wholepage.json").write_text(json.dumps(rows,ensure_ascii=False,indent=1),encoding="utf-8")
