#!/usr/bin/env python3
"""Which cheap signals actually predict that a relation is CORRECT?

Uses the drawing-scale self-check as ground truth for `dimension_interval` (a hit is
correct iff value/span reproduces the block scale within 2 %), then measures precision
stratified by the signals a v0.2 contract could carry as `confidence`.

    python -m experiments.stage_comparison_vector_architecture_opus.probes.txgeo_confidence
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
PURE_INT = re.compile(r"^\d{2,5}$")

# blocks where the modal ratio landed on a standard scale within 1 % (txgeo_dimension_check.json)
TRUSTED = {
    "ar_plan": 35.283,
    "ar_wall_sections": 17.644,
    "fresh_ar_lintels": 3.546,
    "fresh_kj_sections": 17.612,
    "ss_plan_dense": 35.269,
    "vk_nodes": 7.073,
}


def main() -> None:
    samples = []
    for block, scale in TRUSTED.items():
        res = json.loads((REL / block / "left.json").read_text(encoding="utf-8"))
        for u in res["units"]:
            if not PURE_INT.match(u["text"].strip()):
                continue
            rel = u["relations"].get("dimension_interval")
            if not rel or not rel.get("hit"):
                continue
            ratio = float(u["text"]) / rel["measured_len_pt"]
            samples.append({
                "block": block,
                "text": u["text"],
                "correct": abs(ratio / scale - 1.0) <= 0.02,
                "ticks_in_reach": rel.get("ticks_in_reach", 0),
                "candidates": rel.get("candidates", 0),
                "centred": bool(rel.get("centred_on_interval")),
                "perp": rel.get("perp", 0.0),
                "nearest_unique": bool(u["relations"].get("nearest_geometry", {}).get("unique")),
                "v01_conf": u["relations"].get("nearest_geometry", {}).get("v01_confidence"),
                "n_relation_types": sum(
                    1 for k, v in u["relations"].items()
                    if v.get("hit") and k not in ("nearest_geometry", "text_alignment", "enclosure_loose")
                ),
            })

    def strat(name, keyfn, order=None):
        buckets = collections.defaultdict(lambda: [0, 0])
        for s in samples:
            k = keyfn(s)
            buckets[k][1] += 1
            if s["correct"]:
                buckets[k][0] += 1
        keys = order or sorted(buckets)
        rows = []
        for k in keys:
            if k not in buckets:
                continue
            ok, n = buckets[k]
            rows.append({"bucket": str(k), "n": n, "correct": ok, "precision": round(ok / n, 3)})
        return {"signal": name, "rows": rows}

    def tick_bucket(s):
        t = s["ticks_in_reach"]
        return "2" if t <= 2 else "3-4" if t <= 4 else "5-8" if t <= 8 else "9-16" if t <= 16 else ">16"

    report = {
        "n_samples": len(samples),
        "overall_precision": round(sum(1 for s in samples if s["correct"]) / len(samples), 3),
        "stratifications": [
            strat("ticks_in_reach", tick_bucket, ["2", "3-4", "5-8", "9-16", ">16"]),
            strat("dimension_line_candidates", lambda s: min(s["candidates"], 4)),
            strat("centred_on_interval", lambda s: s["centred"]),
            strat("v0.1_anchor_confidence", lambda s: s["v01_conf"]),
            strat("v0.1_anchor_unique", lambda s: s["nearest_unique"]),
            strat("competing_relation_types", lambda s: min(s["n_relation_types"], 5)),
            strat("GATE ticks==2 & centred", lambda s: (s["ticks_in_reach"] <= 2 and s["centred"])),
            strat("GATE ticks==2 & centred & 1 line", lambda s: (s["ticks_in_reach"] <= 2 and s["centred"] and s["candidates"] == 1)),
        ],
        "gate_coverage": {},
    }
    gate = [s for s in samples if s["ticks_in_reach"] <= 2 and s["centred"]]
    gate2 = [s for s in gate if s["candidates"] == 1]
    report["gate_coverage"] = {
        "all_hits": len(samples),
        "gate_ticks2_centred": len(gate),
        "gate_ticks2_centred_precision": round(sum(1 for s in gate if s["correct"]) / len(gate), 3) if gate else None,
        "gate_plus_single_line": len(gate2),
        "gate_plus_single_line_precision": round(sum(1 for s in gate2 if s["correct"]) / len(gate2), 3) if gate2 else None,
    }
    (ART / "txgeo_confidence.json").write_text(json.dumps(report, ensure_ascii=False, indent=1), encoding="utf-8")
    print(f"samples={report['n_samples']}  overall precision={report['overall_precision']}")
    for st in report["stratifications"]:
        print(f"\n-- {st['signal']}")
        for r in st["rows"]:
            print(f"   {r['bucket']:>8s}  n={r['n']:4d}  precision={r['precision']:.3f}")


if __name__ == "__main__":
    main()
