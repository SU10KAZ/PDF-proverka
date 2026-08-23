# -*- coding: utf-8 -*-
"""fam smoke: does the family layer produce anything sane, and at what cost?"""
from __future__ import annotations
import json, sys, time, statistics
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
import grp_common as G
import fam_family as FAM

def main():
    sample = json.load(open(G.ART / "grp_sample.json", encoding="utf-8"))["blocks"]
    rows = []
    for b in sample[:40]:
        pb = G.prepared_block(b["doc_id"], b["version"], b["block_id"])
        if pb is None:
            continue
        t0 = time.time()
        try:
            ex = G.extract(pb)
        except Exception as e:
            print("ERR", b["block_id"], repr(e)); continue
        if not ex.segments or len(ex.segments) > 60000:
            continue
        L = G.layer_of(ex.segments, ex.texts)
        t1 = time.time()
        F = FAM.build_families(L)
        t2 = time.time()
        top = sorted(F.families, key=lambda f: -len(f["members"]))[:4]
        rows.append({"blk": b["block_id"][:12], "disc": b["discipline"], "cls": b["cls"],
                     "n_seg": len(ex.segments), "n_obj": len(L.objects),
                     "n_fam": F.stats["n_families"], "rep": F.stats["n_repeated_families"],
                     "sh_obj": F.stats["share_objects_in_repeated"],
                     "sh_ink": F.stats["share_ink_in_repeated"],
                     "largest": F.stats["largest_family"],
                     "t_obj": round(t1 - t0, 2), "t_fam": round(t2 - t1, 3),
                     "top": [(len(f["members"]), f["cls"], f["diag_med"]) for f in top]})
        print(rows[-1], flush=True)
    print("---- fam time median", statistics.median([r["t_fam"] for r in rows]))
    print("share_obj_in_repeated median", statistics.median([r["sh_obj"] for r in rows]))

main()
