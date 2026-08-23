# -*- coding: utf-8 -*-
"""Aggregate G1 shards into artifacts/grp_repack_stability.json."""
from __future__ import annotations
import json, statistics, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
import grp_common as G

def q(v, p):
    return None if not v else G.pct(v, p)

def main():
    rows = []
    for f in sorted((G.ART / "grp_runs").glob("g1_*.jsonl")):
        for line in open(f, encoding="utf-8"):
            if line.strip():
                rows.append(json.loads(line))
    main_rows = [r for r in rows if not r.get("arc_ablation") and "rewrites" in r]
    abl_rows = [r for r in rows if r.get("arc_ablation") and "rewrites" in r]

    names = sorted({n for r in main_rows for n in r["rewrites"]})
    per_rw = {}
    for n in names:
        eff = []          # blocks where the rewrite actually bit
        noop = 0
        for r in main_rows:
            d = r["rewrites"].get(n)
            if not d or "error" in d:
                continue
            if d["bite"] <= 0:
                noop += 1
                continue
            eff.append((r, d))
        if not eff:
            per_rw[n] = {"n_blocks_effective": 0, "n_blocks_noop": noop}
            continue
        d_obj = [d["d_obj"] for _r, d in eff]
        rel = [abs(d["d_obj"]) / max(1, _r["n_obj"]) for _r, d in eff]
        bid = [d["boundary_identical"] for _r, d in eff]
        o2o = [d["churn"]["one_to_one"] for _r, d in eff]
        conf = [d["desc_confused"] for _r, d in eff if d["desc_confused"] is not None]
        per_rw[n] = {
            "n_blocks_effective": len(eff), "n_blocks_noop": noop,
            "bite_median_segments": statistics.median([d["bite"] for _r, d in eff]),
            "blocks_with_zero_change": sum(1 for _r, d in eff
                                           if d["d_obj"] == 0 and d["boundary_identical"] >= 1.0
                                           and d["churn"]["one_to_one"] >= 0.999999),
            "d_obj_abs_median": statistics.median([abs(x) for x in d_obj]),
            "d_obj_abs_max": max(abs(x) for x in d_obj),
            "d_obj_rel_median": round(statistics.median(rel), 6),
            "d_obj_rel_p90": round(q(rel, 0.90), 6),
            "boundary_identical_median": round(statistics.median(bid), 5),
            "boundary_identical_p10": round(q(bid, 0.10), 5),
            "boundary_identical_min": round(min(bid), 5),
            "churn_1to1_median": round(statistics.median(o2o), 5),
            "churn_1to1_p10": round(q(o2o, 0.10), 5),
            "churn_1to1_min": round(min(o2o), 5),
            "churn_mean_by_kind": {k: round(statistics.mean([d["churn"][k] for _r, d in eff]), 5)
                                   for k in ("one_to_one", "split", "merge", "mixed", "lost")},
            "n_desc_pairs_used": sum(d.get("n_desc") or 0 for _r, d in eff),
            "n_desc_degenerate_twins": sum(d.get("n_desc_degenerate") or 0 for _r, d in eff),
            "desc_drift_median": round(statistics.median([d["desc_drift_median"] for _r, d in eff if d.get("desc_drift_median") is not None]), 5),
            "desc_nn_median": round(statistics.median([d["desc_nn_median"] for _r, d in eff if d.get("desc_nn_median") is not None]), 5),
            "desc_confused_median": (round(statistics.median(conf), 5) if conf else None),
            "desc_confused_p90": (round(q(conf, 0.90), 5) if conf else None),
            "desc_confused_max": (round(max(conf), 5) if conf else None),
        }

    # arc-closure ablation: same blocks, same rewrites, arc_enable on/off
    abl_by_block = {r["block_id"]: r for r in abl_rows}
    abl = {}
    for n in ("A4_circle_to_bezier", "A4b_circle_to_chords5", "A4c_circle_to_chords24",
              "A3_curve_resample_down", "A3_curve_resample_up"):
        on, off = [], []
        for r in main_rows:
            b = abl_by_block.get(r["block_id"])
            if not b:
                continue
            d1, d0 = r["rewrites"].get(n), b["rewrites"].get(n)
            if not d1 or not d0 or d1.get("bite", 0) <= 0:
                continue
            on.append(d1); off.append(d0)
        if not on:
            continue
        abl[n] = {
            "n_blocks": len(on),
            "arc_on": {"boundary_identical_median": round(statistics.median([d["boundary_identical"] for d in on]), 5),
                       "churn_1to1_median": round(statistics.median([d["churn"]["one_to_one"] for d in on]), 5),
                       "desc_drift_median": round(statistics.median([d["desc_drift_median"] for d in on if d.get("desc_drift_median") is not None]), 5),
                       "desc_nn_median": round(statistics.median([d["desc_nn_median"] for d in on if d.get("desc_nn_median") is not None]), 5),
                       "desc_confused_median": round(statistics.median([d["desc_confused"] for d in on if d["desc_confused"] is not None]), 5),
                       "d_obj_abs_median": statistics.median([abs(d["d_obj"]) for d in on])},
            "arc_off": {"boundary_identical_median": round(statistics.median([d["boundary_identical"] for d in off]), 5),
                        "churn_1to1_median": round(statistics.median([d["churn"]["one_to_one"] for d in off]), 5),
                        "desc_drift_median": round(statistics.median([d["desc_drift_median"] for d in off if d.get("desc_drift_median") is not None]), 5),
                        "desc_nn_median": round(statistics.median([d["desc_nn_median"] for d in off if d.get("desc_nn_median") is not None]), 5),
                        "desc_confused_median": round(statistics.median([d["desc_confused"] for d in off if d["desc_confused"] is not None]), 5),
                        "d_obj_abs_median": statistics.median([abs(d["d_obj"]) for d in off])},
        }

    # churn vs density (the CF density curve for G2)
    dens = {}
    for r in main_rows:
        b = r["bucket"]
        for n in ("A6_round_0.25", "A6_round_0.5", "A6_round_0.1"):
            d = r["rewrites"].get(n)
            if not d or d.get("bite", 0) <= 0:
                continue
            dens.setdefault(n, {}).setdefault(b, []).append(d["churn"]["one_to_one"])
    dens_out = {n: {b: {"n": len(v), "median": round(statistics.median(v), 5),
                        "p10": round(q(v, 0.10), 5), "min": round(min(v), 5)}
                    for b, v in sorted(bb.items())} for n, bb in dens.items()}

    out = {
        "source": "class A rewrites of REAL prepared blocks [CF]",
        "n_blocks": len(main_rows),
        "disciplines": sorted({r["discipline"] for r in main_rows}),
        "n_disciplines": len({r["discipline"] for r in main_rows}),
        "buckets": {b: sum(1 for r in main_rows if r["bucket"] == b)
                    for b in {r["bucket"] for r in main_rows}},
        "seg_total": sum(r["n_seg"] for r in main_rows),
        "obj_total": sum(r["n_obj"] for r in main_rows),
        "ink_coverage_min": min(r["ink_coverage"] for r in main_rows),
        "scale_source": {s: sum(1 for r in main_rows if r["scale_source"] == s)
                         for s in {r["scale_source"] for r in main_rows}},
        "per_rewrite": per_rw,
        "arc_closure_ablation": abl,
        "churn_vs_density_cf": dens_out,
        "blocks": [{k: v for k, v in r.items() if k != "rewrites"} for r in main_rows],
    }
    json.dump(out, open(G.ART / "grp_repack_stability.json", "w", encoding="utf-8"),
              ensure_ascii=False, indent=1)
    print(json.dumps({k: v for k, v in out.items() if k not in ("blocks",)},
                     ensure_ascii=False, indent=1)[:6000])


if __name__ == "__main__":
    main()
