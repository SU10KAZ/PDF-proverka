# -*- coding: utf-8 -*-
"""advC: is the heavy payload a property of the drawing, or of the S fallback?"""
from __future__ import annotations
import json, sys, time
from pathlib import Path
HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import ctr_common as C
import grp_common as GC
import v03_objects as O

targets = json.load(open(HERE.parent / "artifacts" / "advC_heavy_targets.json"))
out = []
for t in targets:
    pb = GC.prepared_block(t["doc_id"], t["version"], t["block_id"])
    if pb is None:
        continue
    ex = GC.extract(pb)
    ptx = C.page_text_lines(pb.pdf_path, pb.page_index)
    row = {"block_id": t["block_id"], "n_seg": ex.inked_segments_count, "variants": {}}
    for name, ov in (("native", None), ("S=1pt", 1.0), ("S=2pt", 2.0), ("S=5pt", 5.0)):
        t0 = time.time()
        layer = O.build_objects(ex, **({"S_override": ov} if ov else {}))
        pay = C.describe(pb, ex, layer, None, t["cls"], ptx, with_family=False)
        row["variants"][name] = {"S": pay["scale"]["S"], "S_source": pay["scale"]["S_source"],
                                 "n_obj": len(pay["objects"]),
                                 "tokens": C.tokens(pay),
                                 "secs": round(time.time() - t0, 1)}
    out.append(row)
    print(json.dumps(row, ensure_ascii=False), flush=True)
(C.ART / "advC_sfix.json").write_text(json.dumps(out, ensure_ascii=False, indent=1),
                                      encoding="utf-8")
