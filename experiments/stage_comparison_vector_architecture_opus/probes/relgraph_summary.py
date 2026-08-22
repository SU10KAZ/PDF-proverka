#!/usr/bin/env python3
"""relgraph_summary -- consolidates the relgraph probe artifacts into one
derived-metrics table (artifacts/relgraph_summary.{json,md}).

Run from repo root AFTER the other relgraph probes:
    python experiments/stage_comparison_vector_architecture_opus/probes/relgraph_summary.py
"""
from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
OUT = ROOT / "experiments/stage_comparison_vector_architecture_opus/artifacts"

METRICS = [
    ("cov@0.005", lambda m: m["segment_coverage"]["tol_0.005"]),
    ("cov@0.01", lambda m: m["segment_coverage"]["tol_0.01"]),
    ("relG3", lambda m: m["rel_jaccard_G3"]),
    ("relG1", lambda m: m["rel_jaccard_G1"]),
    ("relG0", lambda m: m["rel_jaccard_G0"]),
    ("entity", lambda m: m["entity_jaccard"]),
    ("text", lambda m: m["text_jaccard"]),
]


def main() -> None:
    crop = json.load(open(OUT / "relgraph_crop_invariance.json"))["results"]
    gran = json.load(open(OUT / "relgraph_granularity.json"))
    ctrl = json.load(open(OUT / "relgraph_frame_control.json"))["controls"]
    size = json.load(open(OUT / "relgraph_size.json"))
    rot = json.load(open(OUT / "relgraph_rotation_pointcheck.json"))["rows"]

    # 1. crop noise floors (excluding the 0.5% variant, which nothing breaks on)
    floors = {}
    for blk in sorted({r["block"] for r in crop}):
        rs = [r for r in crop if r["block"] == blk and r["variant"] != "jitter_0.5%"]
        floors[blk] = {name: round(min(fn(r) for r in rs), 6) for name, fn in METRICS}

    # 2. eom operating margin: crop noise floor on the SAME block vs the real pair value
    eom_real = {
        "cov@0.005": 0.069800, "cov@0.01": 0.173922,
        "relG3": [r for r in gran["rows"] if r["pair_id"] == "eom_singleline_changed"][0]["G3_jaccard"],
        "relG1": [r for r in gran["rows"] if r["pair_id"] == "eom_singleline_changed"][0]["G1_jaccard"],
        "relG0": [r for r in gran["rows"] if r["pair_id"] == "eom_singleline_changed"][0]["G0_jaccard"],
        "entity": None, "text": 0.062900,
    }
    margins = {}
    for name in ("cov@0.005", "cov@0.01", "relG3", "relG1", "relG0"):
        f = floors["eom_singleline_changed"][name]
        r = eom_real[name]
        margins[name] = {
            "crop_noise_floor": f, "real_change_value": r,
            "operating_margin": round(f - r, 6),
            "headroom_ratio": round((f - r) / (1 - f), 3) if f < 1 else None,
        }

    # 3. frame-attributable fraction of the observed drop
    frame_frac = {}
    for row in ctrl:
        real, c = row["real_pair"], row["aspect_control_same_content"]

        def g(d, k):
            return d["segment_coverage"]["tol_" + k.split("@")[1]] if k.startswith("cov@") else d[k]
        frame_frac[row["pair_id"]] = {
            "aspect_distortion": row["aspect_distortion"],
            **{k: (round((1 - g(c, k)) / (1 - g(real, k)), 4) if g(real, k) < 1 else None)
               for k in ("cov@0.005", "cov@0.01", "rel_G3", "rel_G1", "rel_G0", "entity", "text")}}

    # 4. rotation validity of the benchmark
    valid, invalid = [], []
    seen = {}
    for r in rot:
        seen.setdefault(r["pair"], []).append(r)
    for pid, rs in seen.items():
        ok = all(x["agreement_fraction"] >= 0.999 for x in rs)
        (valid if ok else invalid).append(pid)

    summary = {
        "research_only": True,
        "crop_noise_floors": floors,
        "eom_operating_margins": margins,
        "frame_attributable_fraction": frame_frac,
        "granularity_separation": gran["separation"],
        "size_totals_estimated_tokens": size["totals"],
        "benchmark_pairs_with_correct_coordinates": sorted(valid),
        "benchmark_pairs_corrupted_by_page_rotation": sorted(invalid),
    }
    (OUT / "relgraph_summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=1))

    lines = ["# relgraph derived summary (all numbers measured)", ""]
    lines += ["## Crop noise floor per block", "",
              "min similarity over {jitter 2 %, jitter 5 %, crop-edge 10 %} x {frame-only, re-extract}",
              "", "| block | " + " | ".join(n for n, _ in METRICS) + " |",
              "|---|" + "---|" * len(METRICS)]
    for blk, vals in floors.items():
        lines.append(f"| {blk} | " + " | ".join(f"{vals[n]:.4f}" for n, _ in METRICS) + " |")
    lines += ["", "## eom operating margin (same block: crop noise floor vs real pair)", "",
              "| metric | crop noise floor | real changed pair | margin | headroom ratio |",
              "|---|---:|---:|---:|---:|"]
    for k, v in margins.items():
        lines.append(f"| {k} | {v['crop_noise_floor']:.4f} | {v['real_change_value']:.4f} | "
                     f"{v['operating_margin']:+.4f} | {v['headroom_ratio']} |")
    lines += ["", "## Frame-attributable fraction of the observed drop", "",
              "(1 - control) / (1 - real); control = identical content, frame stretched by the "
              "pair's own aspect mismatch", "",
              "| pair | distortion | cov@0.005 | cov@0.01 | rel_G3 | rel_G1 | rel_G0 | entity | text |",
              "|---|---:|---:|---:|---:|---:|---:|---:|---:|"]
    for pid, v in frame_frac.items():
        lines.append(f"| {pid} | x{v['aspect_distortion']} | " + " | ".join(
            f"{v[k]}" for k in ("cov@0.005", "cov@0.01", "rel_G3", "rel_G1", "rel_G0",
                                "entity", "text")) + " |")
    lines += ["", "## Payload size, 20 blocks, estimated tokens", ""]
    for k, v in size["totals"].items():
        lines.append(f"- {k}: {v}")
    lines += ["", "## Benchmark coordinate validity", "",
              f"- pairs with correct coordinates: {sorted(valid)}",
              f"- pairs corrupted by page rotation: {sorted(invalid)}"]
    (OUT / "relgraph_summary.md").write_text("\n".join(lines) + "\n")
    print("\n".join(lines))


if __name__ == "__main__":
    main()
