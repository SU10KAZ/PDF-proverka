# -*- coding: utf-8 -*-
"""C1/C3/C5 driver: sensitivity curve, same-counts test and the false-positive arm.

    python probes/loc_c1_sens.py <shard> <nshards>

One row per (carrier, noise, counterfactual instance).  Negatives (no counterfactual,
same export noise) are emitted with cf_id = "NEG" so positives and negatives differ by
exactly one thing.
"""
from __future__ import annotations
import json
import sys
import time
import traceback
from pathlib import Path

HERE = Path(__file__).resolve().parent
ART = HERE.parent / "artifacts"
sys.path.insert(0, str(HERE))

import grp_common as G          # noqa: E402
import v03_objects as O         # noqa: E402
import v03_counterfactual as C  # noqa: E402
import loc_common as L          # noqa: E402
from cf_build_set import pick_carriers  # noqa: E402

NOISES = ("none", "round025", "jitter03", "jitter10")


def plan():
    P = []
    for b in ("tiny", "small", "large"):
        P.append(("C1_remove_object", {"bucket": b}, b))
        P.append(("C2_add_object", {"bucket": b}, b))
        P.append(("C6_reshape_object", {"bucket": b}, b))
        for f in (0.0025, 0.005, 0.01, 0.05):
            P.append(("C3_move_object", {"bucket": b, "frac": f}, f"{b}@{f}"))
    for cf in ("C4_swap_objects", "C5_swap_unlike", "C7_split_object",
               "C8_merge_objects", "C9_add_branch", "C10_remove_opening"):
        P.append((cf, {}, ""))
    return P


def touched_ink(ex, man):
    cp = man.get("changed_primitives") or {}
    ix = list(cp.get("removed_segment_ix") or []) + list(cp.get("moved_segment_ix") or [])
    tot = sum(ex.segments[k]["len"] for k in ix if k < len(ex.segments))
    n = len(ix)
    add = cp.get("added_segment_ix") or []
    return round(tot, 3), n + len(add)


def main():
    shard, nsh = int(sys.argv[1]), int(sys.argv[2])
    carriers = pick_carriers()
    out = open(ART / "loc_runs" / f"sens_{shard}.jsonl", "w", encoding="utf-8")
    P = plan()
    for ci, r in enumerate(carriers):
        if ci % nsh != shard:
            continue
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
        instances = [("NEG", None, {}, "")] + [(f"{c}@{tag}" if tag else c, c, kw, tag)
                                               for (c, kw, tag) in P]
        made = {}
        for inst_id, cf, kw, tag in instances:
            if cf is None:
                made[inst_id] = (ex, {"change_bbox_pt": None, "expected_ledger": [],
                                      "expected_verdict": "NO_GRAPHIC_CHANGE"})
                continue
            try:
                made[inst_id] = C.apply(ex, ol, cf, **kw)
            except C.CFNotApplicable as e:
                out.write(json.dumps({**base, "inst": inst_id, "skip": str(e)},
                                     ensure_ascii=False) + "\n")
            except Exception as e:
                out.write(json.dumps({**base, "inst": inst_id, "error": repr(e)},
                                     ensure_ascii=False) + "\n")
        for noise in NOISES:
            for inst_id, (ex2, man) in made.items():
                t0 = time.time()
                try:
                    exB = L.noisy(ex2, noise, seed=20260823)
                    LA, LB, meta = L.layers(ex, exB)
                    led = L.ledger(ex, exB, LA=LA, LB=LB, meta=meta)
                    sc = L.score_against_manifest(led, man)
                except Exception as e:
                    out.write(json.dumps({**base, "inst": inst_id, "noise": noise,
                                          "error": repr(e),
                                          "tb": traceback.format_exc()[-400:]},
                                         ensure_ascii=False) + "\n")
                    continue
                tk_len, tk_n = touched_ink(ex, man)
                to = (man.get("touched_objects") or [{}])[0]
                row = {**base, "inst": inst_id, "noise": noise,
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
                       "score": sc, "t_sec": round(time.time() - t0, 2)}
                out.write(json.dumps(row, ensure_ascii=False) + "\n")
            out.flush()
        print("done", r["block_id"], r["bucket"], len(ex.segments), flush=True)
    out.close()


if __name__ == "__main__":
    main()
