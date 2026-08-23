# -*- coding: utf-8 -*-
"""Export-noise arm the first grid did not cover: RE-PACKAGING (A1 path split).

The realistic noise between two revisions is not coordinate jitter but a different
decomposition of the same ink (mine M11: 5 845 against 12 921 segments at 0.08 % visible
difference).  The ledger is built on ink, so it should be invariant; that is a claim and
therefore has to be measured.

    python probes/loc_c6_repack.py <shard> <nshards>
"""
from __future__ import annotations
import json
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
ART = HERE.parent / "artifacts"
sys.path.insert(0, str(HERE))

import grp_common as G          # noqa: E402
import v03_objects as O         # noqa: E402
import v03_counterfactual as C  # noqa: E402
import loc_common as L          # noqa: E402
from cf_build_set import pick_carriers  # noqa: E402
from loc_c1_sens import touched_ink     # noqa: E402

INST = [("NEG", None, {}),
        ("C1_remove_object@small", "C1_remove_object", {"bucket": "small"}),
        ("C1_remove_object@tiny", "C1_remove_object", {"bucket": "tiny"}),
        ("C3_move_object@small@0.01", "C3_move_object", {"bucket": "small", "frac": 0.01})]


NOISE = "repack"


def main():
    global NOISE
    shard, nsh = int(sys.argv[1]), int(sys.argv[2])
    NOISE = sys.argv[3] if len(sys.argv) > 3 else "repack"
    out = open(ART / "loc_runs" / f"repack_{NOISE}_{shard}.jsonl", "w", encoding="utf-8")
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
        ink_total = sum(s["len"] for s in ex.segments)
        for inst, cf, kw in INST:
            try:
                ex2, man = ((ex, {"change_bbox_pt": None, "expected_ledger": [],
                                  "expected_verdict": "NO_GRAPHIC_CHANGE"})
                            if cf is None else C.apply(ex, ol, cf, **kw))
            except Exception as e:
                out.write(json.dumps({"block_id": r["block_id"], "inst": inst,
                                      "skip": repr(e)}, ensure_ascii=False) + "\n")
                continue
            t0 = time.time()
            try:
                exB = L.noisy(ex2, NOISE, seed=20260823)
                LA, LB, meta = L.layers(ex, exB)
                led = L.ledger(ex, exB, LA=LA, LB=LB, meta=meta)
                sc = L.score_against_manifest(led, man)
            except Exception as e:
                out.write(json.dumps({"block_id": r["block_id"], "inst": inst,
                                      "error": repr(e)}, ensure_ascii=False) + "\n")
                continue
            tk_len, tk_n = touched_ink(ex, man)
            out.write(json.dumps({
                "block_id": r["block_id"], "discipline": r["discipline"],
                "bucket": r["bucket"], "n_seg": len(ex.segments),
                "n_seg_b": len(exB.segments), "n_obj_a": len(LA.objects),
                "n_obj_b": len(LB.objects), "inst": inst, "noise": NOISE,
                "cf_id": man.get("cf_id"), "cf_class": man.get("cf_class"),
                "touched_ink_pt": tk_len,
                "touched_ink_frac": round(tk_len / max(ink_total, 1e-9), 8),
                "size_bucket": (man.get("params") or {}).get("size_bucket"),
                "scalar": led["scalar"], "counts": led["counts"],
                "verdict_scalar_999": L.scalar_verdict(led, 0.999),
                "verdict_counts": L.counts_verdict(led),
                "score": sc, "t_sec": round(time.time() - t0, 2),
            }, ensure_ascii=False) + "\n")
            out.flush()
        print("done", r["block_id"], flush=True)
    out.close()


if __name__ == "__main__":
    main()
