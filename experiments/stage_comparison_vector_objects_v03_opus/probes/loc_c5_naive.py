# -*- coding: utf-8 -*-
"""Falsification arm: ink-based ledger vs OBJECT-MATCHING ledger, same pairs.

    python probes/loc_c5_naive.py <shard> <nshards>
"""
from __future__ import annotations
import json, sys, time
from pathlib import Path
HERE = Path(__file__).resolve().parent
ART = HERE.parent / "artifacts"
sys.path.insert(0, str(HERE))
import grp_common as G          # noqa: E402
import v03_objects as O         # noqa: E402
import v03_counterfactual as C  # noqa: E402
import loc_common as L          # noqa: E402
from cf_build_set import pick_carriers  # noqa: E402

INST = [("NEG", None, {}), ("C1_remove_object@small", "C1_remove_object", {"bucket": "small"}),
        ("C3_move_object@small@0.01", "C3_move_object", {"bucket": "small", "frac": 0.01})]
NOISES = ("none", "round025")


def main():
    shard, nsh = int(sys.argv[1]), int(sys.argv[2])
    out = open(ART / "loc_runs" / f"naive_{shard}.jsonl", "w", encoding="utf-8")
    for ci, r in enumerate(pick_carriers()):
        if ci % nsh != shard:
            continue
        try:
            pb = G.prepared_block(r["doc_id"], r["version"], r["block_id"])
            ex = G.extract(pb)
            if not ex.segments:
                continue
            ol = O.build_objects(ex)
        except Exception as e:
            print("fail", r["block_id"], repr(e), flush=True)
            continue
        for inst, cf, kw in INST:
            try:
                ex2, man = ((ex, {"change_bbox_pt": None, "expected_ledger": []})
                            if cf is None else C.apply(ex, ol, cf, **kw))
            except Exception as e:
                continue
            for noise in NOISES:
                try:
                    exB = L.noisy(ex2, noise, seed=20260823)
                    LA, LB, meta = L.layers(ex, exB)
                    t0 = time.time()
                    led = L.ledger(ex, exB, LA=LA, LB=LB, meta=meta)
                    t_ink = time.time() - t0
                    t0 = time.time()
                    nled = L.object_ledger(LA, LB, S=meta["S_shared"])
                    t_obj = time.time() - t0
                    out.write(json.dumps({
                        "block_id": r["block_id"], "discipline": r["discipline"],
                        "bucket": r["bucket"], "n_seg": len(ex.segments),
                        "n_obj": len(LA.objects), "inst": inst, "noise": noise,
                        "ink_records": led["n_records"],
                        "ink_L2": L.score_against_manifest(led, man)["L2_localised"],
                        "obj_records": nled["n_records"], "obj_removed": nled["n_removed"],
                        "obj_added": nled["n_added"], "obj_matched": nled["n_matched"],
                        "obj_L2": L.score_against_manifest(nled, man)["L2_localised"],
                        "t_ink": round(t_ink, 2), "t_obj": round(t_obj, 2),
                    }, ensure_ascii=False) + "\n")
                except Exception as e:
                    out.write(json.dumps({"block_id": r["block_id"], "inst": inst,
                                          "noise": noise, "error": repr(e)},
                                         ensure_ascii=False) + "\n")
            out.flush()
        print("done", r["block_id"], flush=True)
    out.close()


if __name__ == "__main__":
    main()
