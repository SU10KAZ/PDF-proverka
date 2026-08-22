#!/usr/bin/env python3
"""dim_* probe: residual analysis of value-vs-measured-span across blocks.

For every bound dimension we compare the PRINTED value against
    measured_span_pt * fitted_scale(mm/pt)
The residual distribution is the evidence for (a) how tightly the drawn geometry
corroborates the printed number and (b) whether that cross-check can be used as
a guard against false "value changed" reports.

Run from repo root:
    python -m experiments.stage_comparison_vector_architecture_opus.probes.dim_eval \
        --out-dir <detector out dir> --model slash_object --variant C_scale_arbitration \
        --json <artifact.json>
"""
from __future__ import annotations

import argparse
import json
import statistics
from pathlib import Path

BUCKETS = [0.002, 0.005, 0.01, 0.02, 0.05, 0.10, 0.25, 1e9]
NAMES = ["<=0.2%", "<=0.5%", "<=1%", "<=2%", "<=5%", "<=10%", "<=25%", ">25%"]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out-dir", required=True)
    ap.add_argument("--blocks", default="experiments/stage_comparison_vector_architecture_opus/probes/dim_blocks.json")
    ap.add_argument("--models", default="raw_endpoint,slash_object")
    ap.add_argument("--variants", default="A_nearest_line,B_side_convention,C_scale_arbitration")
    ap.add_argument("--json", required=True)
    a = ap.parse_args()
    pairs = json.loads(Path(a.blocks).read_text(encoding="utf-8"))["pairs"]
    rows = []
    for p in pairs:
        for side in ("left", "right"):
            for model in a.models.split(","):
                for variant in a.variants.split(","):
                    f = Path(a.out_dir) / f"{p['pair_id']}__{side}__{model}__{variant}.json"
                    if not f.exists():
                        continue
                    res = json.loads(f.read_text(encoding="utf-8"))
                    det = [d for d in res["dimensions"] if d.get("detected")]
                    errs = sorted(d["rel_err"] for d in det if "rel_err" in d)
                    hist = dict.fromkeys(NAMES, 0)
                    for e in errs:
                        for b, n in zip(BUCKETS, NAMES):
                            if e <= b:
                                hist[n] += 1
                                break
                    rows.append({
                        "pair": p["pair_id"], "side": side, "model": model, "variant": variant,
                        "candidates": len(res["dimensions"]),
                        "bound": len(det),
                        "scale_mm_per_pt": res["scale"].get("scale_mm_per_pt"),
                        "implied_scale_1_to": res["scale"].get("implied_drawing_scale_1_to"),
                        "median_rel_err": round(statistics.median(errs), 5) if errs else None,
                        "rel_err_hist_cumulative": {
                            n: sum(hist[k] for k in NAMES[:i + 1]) for i, n in enumerate(NAMES)
                        },
                    })
    Path(a.json).write_text(json.dumps(rows, ensure_ascii=False, indent=1), encoding="utf-8")
    hdr = f"{'pair':<18}{'side':<6}{'model':<14}{'variant':<22}{'cand':>5}{'bound':>6}{'scale1:':>8}{'med.err':>9}  cumulative rel-err"
    print(hdr)
    for r in rows:
        c = r["rel_err_hist_cumulative"]
        print(f"{r['pair']:<18}{r['side']:<6}{r['model']:<14}{r['variant']:<22}"
              f"{r['candidates']:>5}{r['bound']:>6}{str(r['implied_scale_1_to']):>8}"
              f"{str(r['median_rel_err']):>9}  "
              f"0.2%:{c['<=0.2%']:>4} 1%:{c['<=1%']:>4} 2%:{c['<=2%']:>4} 5%:{c['<=5%']:>4} 25%:{c['<=25%']:>4}")


if __name__ == "__main__":
    main()
