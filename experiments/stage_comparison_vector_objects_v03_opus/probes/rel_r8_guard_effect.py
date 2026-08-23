# -*- coding: utf-8 -*-
"""R8 — does the guard against the rectangle->circle defect change the VERDICTS?

R6 counted how many bbox relations vanish under ``arc_min_pts=6``.  That alone does not
say whether the surviving contract is better: a type could lose its false edges and its
usefulness together.  So the two decision measures are re-run on the same carriers with
the guard on and off:

  * R1-style STABILITY under the two rewrites that actually move numbers (A6_round_0.25,
    A3_curve_resample_down) — object correspondence by exact segment provenance;
  * R3-style ADDRESSING (share of changed objects that get a usable address) for
    C1_remove_object / C3_move_object.

Usage:  rel_r8_guard_effect.py [shard nshards]
"""
from __future__ import annotations
import json, sys, time, traceback
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
import rel_common as C
import rel_relations as R
import rel_r3_address as A3
import v03_counterfactual as CF
import cf_build_set as CB

GUARD = {"arc_min_pts": 6}
REWRITES = ["A6_round_0.25", "A3_curve_resample_down"]
PLAN = [("C1_remove_object", {"bucket": b}) for b in ("small", "large")] + \
       [("C3_move_object", {"bucket": b, "frac": 0.02}) for b in ("small", "large")]
MAX_OBJ = 4000


def build(ex, guard, **kw):
    p = dict(kw)
    if guard:
        p.update(GUARD)
    return C.O.build_objects(ex, **p)


def one(rec, guard):
    row = {"guard": guard, "stab": [], "addr": [], "error": None}
    try:
        pb = C.G.prepared_block(rec["doc_id"], rec["version"], rec["block_id"])
        ex = C.G.extract(pb)
        if not ex.segments:
            row["error"] = "no geometry"; return row
        L0 = build(ex, guard)
        if not (4 <= len(L0.objects) <= MAX_OBJ):
            row["error"] = f"n_obj {len(L0.objects)}"; return row
        rel0 = R.build_relations(L0, ex)
        row["n_obj"] = len(L0.objects)
        row["counts0"] = R.relation_counts(rel0)
        for cf_id in REWRITES:
            try:
                ex2, man = CF.apply(ex, L0, cf_id)
            except Exception as e:
                row["stab"].append({"cf": cf_id, "skipped": repr(e)[:80]}); continue
            L2 = build(ex2, guard, S_override=L0.S)
            rel2 = R.build_relations(L2, ex2)
            a2b, _, _ = C.match_by_provenance(L0, ex.segments, L2, ex2.segments)
            row["stab"].append({"cf": cf_id, "surv": C.survival(rel0, rel2, a2b, L0, L2)})
        mult0 = A3.motif_multiplicity(L0)
        tc0 = A3.text_multiplicity(ex.texts)
        id2ix = {o["object_id"]: i for i, o in enumerate(L0.objects)}
        for cf_id, kw in PLAN:
            tag = cf_id + "".join(f"@{v}" for v in kw.values())
            try:
                ex2, man = CF.apply(ex, L0, cf_id, **kw)
            except Exception as e:
                row["addr"].append({"tag": tag, "skipped": repr(e)[:80]}); continue
            L2 = build(ex2, guard, S_override=L0.S)
            a2b, _, _ = C.match_by_provenance(L0, ex.segments, L2, ex2.segments)
            oid = man["touched_objects"][0]["object_id"] if man["touched_objects"] else None
            x = id2ix.get(oid, -1)
            if x < 0:
                row["addr"].append({"tag": tag, "skipped": "touched not found"}); continue
            row["addr"].append({"tag": tag,
                                "addr": A3.address_stats(L0, rel0, x, a2b, mult0, tc0)})
    except Exception:
        row["error"] = traceback.format_exc()[-300:]
    return row


def main(shard, n):
    carriers = [r for i, r in enumerate(CB.pick_carriers()) if i % n == shard]
    out = []
    for k, rec in enumerate(carriers):
        t0 = time.time()
        pair = {"carrier": rec, "off": one(rec, False), "on": one(rec, True)}
        print(f"[{shard}] {k+1}/{len(carriers)} {rec['block_id']} {round(time.time()-t0,1)}s",
              flush=True)
        out.append(pair)
        C.F.clear_caches()
    json.dump(out, open(C.ART / f"rel_r8_{shard}.json", "w", encoding="utf-8"),
              ensure_ascii=False)


if __name__ == "__main__":
    main(int(sys.argv[1]) if len(sys.argv) > 1 else 0,
         int(sys.argv[2]) if len(sys.argv) > 2 else 1)
