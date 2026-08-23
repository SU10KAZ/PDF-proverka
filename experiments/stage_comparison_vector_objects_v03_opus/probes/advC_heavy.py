# -*- coding: utf-8 -*-
"""advC: real payload cost of the v0.3 contract on the HEAVY tail the ctr sample excluded (>60k segments)."""
from __future__ import annotations
import json, random, sys, time
from pathlib import Path
HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import ctr_common as C
import grp_common as GC
import v03_objects as O
import fam_family as FAM

lo = int(sys.argv[1]); hi = int(sys.argv[2]); n = int(sys.argv[3]); tag = sys.argv[4]
rows = [r for r in GC.block_records() if lo <= (r.get("n_seg") or 0) < hi]
rnd = random.Random(20260823)
sample = rnd.sample(rows, min(n, len(rows)))
out = []
for r in sample:
    rec = {"block_id": r["block_id"], "doc_id": r["doc_id"], "version": r["version"],
           "cls": r["cls"], "disc": r["discipline"], "n_seg_census": r.get("n_seg")}
    try:
        t0 = time.time()
        pb = GC.prepared_block(r["doc_id"], r["version"], r["block_id"])
        if pb is None:
            rec["error"] = "no prepared block"; out.append(rec); continue
        ex = GC.extract(pb); t_ex = time.time() - t0
        layer = O.build_objects(ex); t_obj = time.time() - t0 - t_ex
        fam = FAM.build_families(layer)
        ptx = C.page_text_lines(pb.pdf_path, pb.page_index)
        pay = C.describe(pb, ex, layer, fam, r["cls"], ptx)
        head = {k: v for k, v in pay.items() if k not in ("objects", "families")}
        objs = pay.get("objects") or []
        desc_bytes = sum(C.nbytes(o.get("desc")) for o in objs)
        fam_bytes = C.nbytes(pay.get("families")) + sum(C.nbytes(o.get("fam")) for o in objs)
        rec.update({"n_seg": ex.inked_segments_count, "n_obj": len(objs),
                    "route": pay["quality"]["route"], "bytes": C.nbytes(pay),
                    "tokens": C.tokens(pay), "head_tokens": C.tokens(head),
                    "desc_bytes": desc_bytes, "fam_bytes": fam_bytes,
                    "t_extract": round(t_ex, 2), "t_objects": round(t_obj, 2),
                    "secs": round(time.time() - t0, 2)})
    except Exception as e:
        rec["error"] = type(e).__name__ + ": " + str(e)[:120]
    out.append(rec)
    print(json.dumps(rec, ensure_ascii=False), flush=True)
(C.ART / ("advC_heavy_%s.json" % tag)).write_text(json.dumps(out, ensure_ascii=False, indent=1), encoding="utf-8")
