# -*- coding: utf-8 -*-
"""Replay of the sensitivity grid for the rows the FIXED scorer can change.

Two defects were found in `loc_common.score_against_manifest` after the first grid was
run (both are described in loc_FINDINGS):

  DEFECT-1  `_ov` returned 0 for a degenerate (zero-area) bbox, so a record whose bbox
            was IDENTICAL to the ground-truth bbox scored "not localised";
  DEFECT-2  two expected types (ADDED_BRANCH, REMOVED_OPENING) were missing from the
            type dictionary, so C9/C10 scored L3 = L4 = 0 by omission.

Both fixes are MONOTONE: they can only turn a False into a True (`_ov` takes the max of
the padded and unpadded value; the dictionary only gains keys).  Therefore only the rows
that failed at least one level need replaying — the all-True rows cannot change.  The
replay also recomputes the ledger, so the old scalar/counts fields are re-derived and
compared: a free determinism check.

    python probes/loc_c1_rescore.py <shard> <nshards>
"""
from __future__ import annotations
import glob
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
from loc_c1_sens import plan, touched_ink  # noqa: E402


def failures():
    want = {}
    for f in sorted(glob.glob(str(ART / "loc_runs" / "sens_*.jsonl"))):
        for line in open(f, encoding="utf-8"):
            r = json.loads(line)
            if "score" not in r or r.get("cf_class") != "C":
                continue
            s = r["score"]
            if s["L2_localised"] and s["L3_right_type"] and s["L4_right_object"]:
                continue
            want.setdefault(r["block_id"], set()).add((r["inst"], r["noise"]))
    return want


def main():
    shard, nsh = int(sys.argv[1]), int(sys.argv[2])
    want = failures()
    carriers = pick_carriers()
    P = {(f"{c}@{tag}" if tag else c): (c, kw) for (c, kw, tag) in plan()}
    out = open(ART / "loc_runs" / f"rescore_{shard}.jsonl", "w", encoding="utf-8")
    for ci, r in enumerate(carriers):
        if ci % nsh != shard or r["block_id"] not in want:
            continue
        todo = want[r["block_id"]]
        try:
            pb = G.prepared_block(r["doc_id"], r["version"], r["block_id"])
            ex = G.extract(pb)
            ol = O.build_objects(ex)
        except Exception as e:
            print("CARRIER FAIL", r["block_id"], repr(e), flush=True)
            continue
        if not ex.segments:
            continue
        ink_total = sum(s["len"] for s in ex.segments)
        base = {"block_id": r["block_id"], "doc_id": r["doc_id"], "version": r["version"],
                "discipline": r["discipline"], "bucket": r["bucket"], "cls": r["cls"],
                "n_seg": len(ex.segments), "ink_total_pt": round(ink_total, 2),
                "n_obj": len(ol.objects), "S": round(ol.S, 3)}
        made = {}
        for inst in sorted({i for (i, _n) in todo}):
            cf, kw = P[inst]
            try:
                made[inst] = C.apply(ex, ol, cf, **kw)
            except Exception as e:
                out.write(json.dumps({**base, "inst": inst, "skip": repr(e)},
                                     ensure_ascii=False) + "\n")
        for inst, noise in sorted(todo):
            if inst not in made:
                continue
            ex2, man = made[inst]
            t0 = time.time()
            try:
                exB = L.noisy(ex2, noise, seed=20260823)
                LA, LB, meta = L.layers(ex, exB)
                led = L.ledger(ex, exB, LA=LA, LB=LB, meta=meta)
                sc = L.score_against_manifest(led, man)
            except Exception as e:
                out.write(json.dumps({**base, "inst": inst, "noise": noise, "error": repr(e)},
                                     ensure_ascii=False) + "\n")
                continue
            tk_len, tk_n = touched_ink(ex, man)
            to = (man.get("touched_objects") or [{}])[0]
            out.write(json.dumps({
                **base, "inst": inst, "noise": noise,
                "cf_id": man.get("cf_id"), "cf_class": man.get("cf_class"),
                "expected": man.get("expected_verdict"),
                "size_bucket": (man.get("params") or {}).get("size_bucket"),
                "frac_of_diag": (man.get("params") or {}).get("frac_of_diag"),
                "obj_area_frac": to.get("area_frac_of_block"),
                "obj_n_seg": to.get("n_seg"), "obj_diag_pt": to.get("diag_pt"),
                "touched_ink_pt": tk_len, "touched_n_seg": tk_n,
                "touched_ink_frac": round(tk_len / max(ink_total, 1e-9), 8),
                "counters_invariant": bool(man.get("counters_invariant")),
                "scalar": led["scalar"], "counts": led["counts"],
                "verdict_scalar_999": L.scalar_verdict(led, 0.999),
                "verdict_scalar_9999": L.scalar_verdict(led, 0.9999),
                "verdict_counts": L.counts_verdict(led),
                "score": sc, "t_sec": round(time.time() - t0, 2),
            }, ensure_ascii=False) + "\n")
            out.flush()
        print("done", r["block_id"], len(todo), flush=True)
    out.close()


if __name__ == "__main__":
    main()
