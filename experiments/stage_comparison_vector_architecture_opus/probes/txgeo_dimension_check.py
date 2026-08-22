#!/usr/bin/env python3
"""Self-verification of the dimension_interval relation — no human labels needed.

If the relation is right, `text value / measured interval length` must collapse onto ONE
number per block: the drawing scale.  One PDF point = 25.4/72 mm on paper, so a 1:100
drawing gives 35.2778 mm per point.  Landing on a standard scale is independent evidence
that the relation recovered the true referent.

    python -m experiments.stage_comparison_vector_architecture_opus.probes.txgeo_dimension_check
"""
from __future__ import annotations

import collections
import json
import math
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
REL = ROOT / "experiments/stage_comparison_vector_architecture_opus/artifacts/txgeo_relations/line"
ART = ROOT / "experiments/stage_comparison_vector_architecture_opus/artifacts"

MM_PER_PT = 25.4 / 72.0
STANDARD = [1, 2, 5, 10, 20, 25, 50, 75, 100, 200, 250, 400, 500]
PURE_INT = re.compile(r"^\d{2,5}$")


def modal_scale(ratios: list[float], bin_factor: float = 1.02) -> tuple[float, int]:
    if not ratios:
        return 0.0, 0
    logs = [math.log(r) / math.log(bin_factor) for r in ratios]
    counts = collections.Counter(int(round(l)) for l in logs)
    best_bin, _ = counts.most_common(1)[0]
    members = [r for r, l in zip(ratios, logs) if abs(l - best_bin) <= 1.0]
    return (sum(members) / len(members), len(members))


def main() -> None:
    out = []
    for path in sorted(REL.glob("*/left.json")):
        res = json.loads(path.read_text(encoding="utf-8"))
        rows = []
        for u in res["units"]:
            text = u["text"].strip()
            if not PURE_INT.match(text):
                continue
            rel = u["relations"].get("dimension_interval")
            if not rel or not rel.get("hit"):
                rows.append({"text": text, "state": "no_relation"})
                continue
            span = rel["measured_len_pt"]
            rows.append({
                "text": text,
                "state": "relation",
                "value": float(text),
                "span_pt": span,
                "ratio": float(text) / span,
                "centred": bool(rel.get("centred_on_interval")),
                "candidates": rel.get("candidates"),
                "alt_ratios": [float(text) / v for v in (rel.get("alt_spans_pt") or []) if v > 1e-6],
            })
        with_rel = [r for r in rows if r["state"] == "relation"]
        scale, n_mode = modal_scale([r["ratio"] for r in with_rel])
        centred = [r for r in with_rel if r["centred"]]
        scale_c, n_mode_c = modal_scale([r["ratio"] for r in centred])

        def share_any(subset: list[dict], sc: float, tol: float) -> float:
            if not subset or sc <= 0:
                return 0.0
            ok = 0
            for r in subset:
                cands = [r["ratio"]] + list(r.get("alt_ratios") or [])
                if any(abs(c / sc - 1.0) <= tol for c in cands):
                    ok += 1
            return round(ok / len(subset), 3)

        def share(subset: list[dict], sc: float, tol: float) -> float:
            if not subset or sc <= 0:
                return 0.0
            return round(sum(1 for r in subset if abs(r["ratio"] / sc - 1.0) <= tol) / len(subset), 3)

        implied = scale / MM_PER_PT if scale else 0.0
        nearest = min(STANDARD, key=lambda s: abs(math.log((implied or 1e-9) / s))) if implied else None
        out.append({
            "block": path.parent.name,
            "pure_integer_texts": len(rows),
            "with_dimension_relation": len(with_rel),
            "recall_relation": round(len(with_rel) / len(rows), 3) if rows else None,
            "modal_mm_per_pt": round(scale, 4),
            "implied_scale_1_to": round(implied, 2) if implied else None,
            "nearest_standard_scale": nearest,
            "scale_error_vs_standard_pct": round(abs(implied / nearest - 1.0) * 100, 2) if nearest else None,
            "share_within_2pct_all": share(with_rel, scale, 0.02),
            "share_within_5pct_all": share(with_rel, scale, 0.05),
            "centred_subset": len(centred),
            "share_within_2pct_centred": share(centred, scale_c or scale, 0.02),
            "share_within_5pct_centred": share(centred, scale_c or scale, 0.05),
            "share_any_candidate_within_2pct": share_any(with_rel, scale, 0.02),
            "verified_dimensions": int(round(share(with_rel, scale, 0.02) * len(with_rel))),
        })
    (ART / "txgeo_dimension_check.json").write_text(json.dumps(out, ensure_ascii=False, indent=1), encoding="utf-8")
    hdr = f"{'block':24s} {'pureInt':>7s} {'rel':>5s} {'recall':>7s} {'mm/pt':>8s} {'1:X':>7s} {'std':>5s} {'err%':>6s} {'≤2%':>6s} {'≤5%':>6s} {'cntrd':>6s} {'c≤2%':>6s} {'c≤5%':>6s} {'any2%':>6s}"
    print(hdr)
    for r in out:
        print(f"{r['block']:24s} {r['pure_integer_texts']:7d} {r['with_dimension_relation']:5d} "
              f"{str(r['recall_relation']):>7s} {r['modal_mm_per_pt']:8.3f} {str(r['implied_scale_1_to']):>7s} "
              f"{str(r['nearest_standard_scale']):>5s} {str(r['scale_error_vs_standard_pct']):>6s} "
              f"{r['share_within_2pct_all']:6.2f} {r['share_within_5pct_all']:6.2f} "
              f"{r['centred_subset']:6d} {r['share_within_2pct_centred']:6.2f} {r['share_within_5pct_centred']:6.2f} {r['share_any_candidate_within_2pct']:6.2f}")


if __name__ == "__main__":
    main()
