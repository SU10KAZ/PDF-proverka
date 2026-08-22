"""p07v — VERIFIER: render ONLY the socket layer, keep vs drop, at symbol scale.
Tests whether the filter makes socket symbols uncountable (claim 4) or merely thins them.
"""
from __future__ import annotations
import sys
from experiments.stage_comparison_vector_architecture_opus.probes import hatchnoise_core as C
from experiments.stage_comparison_vector_architecture_opus.probes import hatchnoise_filter as F
from experiments.stage_comparison_vector_architecture_opus.probes.hatchnoise_p2b_zoom import clip

LAYER = "08_Розетки и выводы"

def main():
    box = [float(v) for v in sys.argv[1:5]] or [0.30,0.40,0.46,0.66]
    tag = sys.argv[5]
    pdf, pi, bbox = C.BLOCKS["ar_layered_plan"]["left"]
    payload = C.load_primitives(pdf, pi, bbox)
    rows = C.segment_table(payload)["rows"]
    flags, records, pflags = F.classify(rows)
    sel = [i for i,r in enumerate(rows) if r["layer"]==LAYER]
    sub_rows = [rows[i] for i in sel]; sub_flags=[flags[i] for i in sel]
    keep_rows = clip([r for r,f in zip(sub_rows,sub_flags) if not f], box)
    drop_rows = clip([r for r,f in zip(sub_rows,sub_flags) if f], box)
    all_rows  = clip(sub_rows, box)
    w=bbox[2]-bbox[0]; h=bbox[3]-bbox[1]
    aspect=((box[3]-box[1])*h*payload["page_size"][1])/((box[2]-box[0])*w*payload["page_size"][0])
    out=C.OUT/"ar_layered_plan"
    C.render_segments(all_rows, out/f"p07v_{tag}_socketlayer_ALL.png", aspect=aspect, width_px=1500,
                      title=f"socket layer ALL {len(all_rows)}")
    C.render_segments(keep_rows, out/f"p07v_{tag}_socketlayer_KEEP.png", aspect=aspect, width_px=1500,
                      title=f"socket layer KEPT {len(keep_rows)}/{len(all_rows)}")
    C.render_segments(drop_rows, out/f"p07v_{tag}_socketlayer_DROP.png", aspect=aspect, color=(0.85,0,0), width_px=1500,
                      title=f"socket layer DROPPED {len(drop_rows)}/{len(all_rows)}")
    print("all",len(all_rows),"keep",len(keep_rows),"drop",len(drop_rows))

if __name__=="__main__":
    main()
