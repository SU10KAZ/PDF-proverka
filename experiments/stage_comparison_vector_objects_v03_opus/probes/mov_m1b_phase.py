# -*- coding: utf-8 -*-
"""M1b — is the object layer phase-locked to the absolute page origin? [CF]

Translating a block by a pure offset changes NOTHING about the drawing, so an ideal
object layer would return the identical partition.  This measures how much of it is
rewritten as a function of the offset, in units of the block's own characteristic
scale S.  It is the justification for the two-pass design (regroup B inside A's frame)
in mov_common.align_pair.

    python probes/mov_m1b_phase.py [--limit 14]
Writes artifacts/mov_phase.json
"""
from __future__ import annotations
import argparse, json, math, sys, time
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import mov_common as MC       # noqa: E402
import mov_align as MA        # noqa: E402
import grp_common as G        # noqa: E402
import v03_objects as O       # noqa: E402
import cf_build_set as CB     # noqa: E402

ART = MC.ART
FRACS = [0.0, 0.001, 0.01, 0.1, 0.25, 0.5, 0.75, 1.0, 2.0, 3.0]


def partition(layer, n_seg):
    """segment index -> object index"""
    out = [-1] * n_seg
    for oi, o in enumerate(layer.objects):
        for gi in o["segments"]:
            if 0 <= gi < n_seg:
                out[gi] = oi
    return out


def churn(pa, pb, segs):
    """Share of INK LENGTH whose object neighbourhood changed (partition disagreement).

    Two segments are 'together' if they share an object.  A segment is rewritten when the
    set of segments it shares an object with is not the same on both sides.  Comparing the
    full sets is quadratic, so the comparison is done on the object's segment-id signature.
    """
    sig_a, sig_b = {}, {}
    for i, oi in enumerate(pa):
        sig_a.setdefault(oi, []).append(i)
    for i, oi in enumerate(pb):
        sig_b.setdefault(oi, []).append(i)
    key_a = {oi: tuple(v) for oi, v in sig_a.items()}
    key_b = {frozenset(v) for v in sig_b.values()}
    same = 0.0
    tot = 0.0
    for oi, v in key_a.items():
        L = sum(segs[i]["len"] for i in v)
        tot += L
        if frozenset(v) in key_b:
            same += L
    return round(1.0 - same / max(tot, 1e-9), 5)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=14)
    ap.add_argument("--max_seg", type=int, default=6000)
    a = ap.parse_args()
    carriers = [c for c in CB.pick_carriers() if c["n_seg"] <= a.max_seg][: a.limit]
    rows = []
    for k, rec in enumerate(carriers):
        pb = G.prepared_block(rec["doc_id"], rec["version"], rec["block_id"])
        if pb is None:
            continue
        ex = G.extract(pb)
        if not ex.segments:
            continue
        t0 = time.time()
        L0 = O.build_objects(ex)
        S = L0.S
        p0 = partition(L0, len(ex.segments))
        row = {"block_id": rec["block_id"], "discipline": rec["discipline"],
               "n_seg": len(ex.segments), "n_obj": len(L0.objects), "S": round(S, 3),
               "by_frac": {}}
        for f in FRACS:
            d = f * S
            T = MA.Sim(1.0, 0, d, d * 0.5)      # not axis-aligned, so both axes are probed
            ex2 = MC.apply_sim(ex, T)
            L1 = O.build_objects(ex2)
            p1 = partition(L1, len(ex2.segments))
            row["by_frac"][str(f)] = {"offset_pt": round(math.hypot(d, d * 0.5), 4),
                                      "n_obj": len(L1.objects),
                                      "churn": churn(p0, p1, ex.segments)}
        row["t_sec"] = round(time.time() - t0, 1)
        rows.append(row)
        print(f"[{k+1}/{len(carriers)}] {rec['block_id'][:12]} {rec['discipline']} "
              f"{row['n_seg']} seg S={row['S']} "
              f"churn@0.5S={row['by_frac']['0.5']['churn']} "
              f"churn@1S={row['by_frac']['1.0']['churn']} {row['t_sec']}s", flush=True)
    agg = {}
    for f in FRACS:
        v = sorted(r["by_frac"][str(f)]["churn"] for r in rows)
        o = sorted(r["by_frac"][str(f)]["n_obj"] / max(1, r["n_obj"]) for r in rows)
        agg[str(f)] = {"n": len(v), "churn_median": v[len(v) // 2] if v else None,
                       "churn_max": max(v) if v else None,
                       "n_zero": sum(1 for x in v if x == 0.0),
                       "nobj_ratio_median": round(o[len(o) // 2], 4) if o else None}
    json.dump({"schema": "mov-phase-1", "fracs": FRACS, "agg": agg, "rows": rows},
              open(ART / "mov_phase.json", "w", encoding="utf-8"), ensure_ascii=False, indent=1)
    print(json.dumps(agg, ensure_ascii=False))


if __name__ == "__main__":
    main()
