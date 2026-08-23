# -*- coding: utf-8 -*-
"""F9 — what does the family layer cost on top of the object layer?"""
from __future__ import annotations
import json, statistics, sys, time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
import grp_common as G
import fam_family as FAM

def main():
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 120
    out = sys.argv[2] if len(sys.argv) > 2 else str(G.ART / "fam_f9_cost.json")
    shard = int(sys.argv[3]) if len(sys.argv) > 3 else 0
    of = int(sys.argv[4]) if len(sys.argv) > 4 else 1
    smp = [b for i, b in enumerate(json.load(open(G.ART / "grp_sample.json", encoding="utf-8"))["blocks"][:n])
           if i % of == shard]
    rows = []
    for k, b in enumerate(smp):
        pb = G.prepared_block(b["doc_id"], b["version"], b["block_id"])
        if pb is None:
            continue
        try:
            ex = G.extract(pb)
        except Exception:
            continue
        if not ex.segments or len(ex.segments) > 200000:
            continue
        t0 = time.time(); L = G.layer_of(ex.segments, ex.texts); t1 = time.time()
        F = FAM.build_families(L); t2 = time.time()
        FP = FAM.build_families_pair(L, L); t3 = time.time()
        rows.append({"block_id": b["block_id"], "discipline": b["discipline"],
                     "n_seg": len(ex.segments), "n_obj": len(L.objects),
                     "n_fam": F.stats["n_families"], "n_rep": F.stats["n_repeated_families"],
                     "t_obj": round(t1 - t0, 4), "t_fam": round(t2 - t1, 4),
                     "t_fam_pair": round(t3 - t2, 4)})
        if k % 20 == 0:
            print(shard, k, len(smp), flush=True)
    json.dump({"rows": rows}, open(out, "w", encoding="utf-8"), ensure_ascii=False, indent=1)

main()
