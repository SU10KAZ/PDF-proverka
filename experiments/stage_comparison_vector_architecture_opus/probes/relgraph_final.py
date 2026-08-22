#!/usr/bin/env python3
"""relgraph_final -- Track-B: separation margins and crop noise floors recomputed
on ROTATION-CORRECTED data.

Run from repo root (after relgraph_rotfix.py):
    python experiments/stage_comparison_vector_architecture_opus/probes/relgraph_final.py
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))
import relgraph_core as R  # noqa: E402
from relgraph_crop import coverage, jitter_rect, crop_edge_rect, renormalize, text_multiset  # noqa: E402
from relgraph_granularity import project  # noqa: E402
from relgraph_rotfix import extract_rotation_correct  # noqa: E402

A = ROOT / "experiments/stage_comparison_vector_blocks/artifacts"
OUT = ROOT / "experiments/stage_comparison_vector_architecture_opus/artifacts"

CHANGED_TEXT = {"ss_scheme_text_changed", "vk_nodes"}
CHANGED_GEOM = {"eom_singleline_changed"}


def main() -> None:
    gran = {r["pair_id"]: r for r in json.load(open(OUT / "relgraph_granularity.json"))["rows"]}
    stab = {r["pair_id"]: r for r in json.load(open(OUT / "relgraph_stability.json"))["rows"]}
    fix = {r["pair_id"]: r for r in json.load(open(OUT / "relgraph_rotfix.json"))["pairs"]}

    table = {}
    for pid in gran:
        if pid in fix:
            a = fix[pid]["after_rotation_fix"]
            table[pid] = {"cov@0.01": a["segment_coverage"]["tol_0.01"],
                          "cov@0.005": a["segment_coverage"]["tol_0.005"],
                          "relG1": a["rel_G1"], "relG3": a["rel_G3"], "relG0": a["rel_G0"],
                          "text": a["text"], "source": "rotation_corrected"}
        else:
            table[pid] = {"cov@0.01": stab[pid]["v01_geometry_similarity"],
                          "cov@0.005": None,
                          "relG1": gran[pid]["G1_jaccard"], "relG3": gran[pid]["G3_jaccard"],
                          "relG0": gran[pid]["G0_jaccard"],
                          "text": stab[pid]["v01_text_similarity"], "source": "unrotated_page"}

    print(f"{'pair':24s} {'cov@0.01':>9s} {'relG1':>8s} {'relG3':>8s} {'text':>8s}  source")
    for pid, v in table.items():
        print(f"{pid:24s} {v['cov@0.01']:9.4f} {v['relG1']:8.4f} {v['relG3']:8.4f} "
              f"{(v['text'] if v['text'] is not None else -1):8.4f}  {v['source']}")

    def margin(key, changed):
        ch = [v[key] for p, v in table.items() if p in changed]
        un = [(v[key], p) for p, v in table.items() if p not in changed]
        return {"max_changed": max(ch), "min_unchanged": min(un)[0],
                "min_unchanged_pair": min(un)[1],
                "margin": round(min(un)[0] - max(ch), 6), "separated": min(un)[0] > max(ch)}

    seps = {}
    for key in ("cov@0.01", "relG1", "relG3", "relG0", "text"):
        seps[key + "::geometry_change_only"] = margin(key, CHANGED_GEOM)
        seps[key + "::any_change"] = margin(key, CHANGED_GEOM | CHANGED_TEXT)
    print("\n--- separation margins on rotation-corrected data ---")
    for k, v in seps.items():
        print(f"{k:34s} margin={v['margin']:+.4f} max_changed={v['max_changed']:.4f} "
              f"min_unchanged={v['min_unchanged']:.4f}({v['min_unchanged_pair']})")

    # crop noise floor recomputed on the ROTATION-CORRECTED eom left block
    pairs = {p["pair_id"]: p for p in json.loads((A / "block_pairs.json").read_text())["pairs"]}
    s = pairs["eom_singleline_changed"]["left"]
    base, named, _ = extract_rotation_correct(s["pdf"], int(s["page_index"]),
                                              s["bbox_norm"], "eom_left_rotfix")
    rect = base["bbox"]
    gb = R.build_relation_graph(base)
    floors = {}
    per_variant = []
    for name, frac in (("jitter_2%", 0.02), ("jitter_5%", 0.05), ("crop_edge_10%", None)):
        nr = crop_edge_rect(rect) if frac is None else jitter_rect(rect, frac)
        var = renormalize(base, nr)
        gv = R.build_relation_graph(var)
        row = {"variant": name,
               "cov@0.005": coverage(base, var)["tol_0.005"],
               "cov@0.01": coverage(base, var)["tol_0.01"],
               "relG1": round(R.weighted_jaccard(project(gb["relations"], 1),
                                                 project(gv["relations"], 1)), 6),
               "relG3": round(R.weighted_jaccard(gb["relations"], gv["relations"]), 6),
               "relG0": round(R.weighted_jaccard(project(gb["relations"], 0),
                                                 project(gv["relations"], 0)), 6),
               "text": round(R.weighted_jaccard(text_multiset(base), text_multiset(var)), 6)}
        per_variant.append(row)
        print(f"  crop {name:14s} " + "  ".join(f"{k}={row[k]:.4f}" for k in
              ("cov@0.005", "cov@0.01", "relG1", "relG3", "relG0", "text")))
    for k in ("cov@0.005", "cov@0.01", "relG1", "relG3", "relG0", "text"):
        floors[k] = min(r[k] for r in per_variant)

    signal = table["eom_singleline_changed"]
    verdict = {}
    for k in ("cov@0.005", "cov@0.01", "relG1", "relG3", "relG0", "text"):
        sg = signal[k]
        if sg is None:
            continue
        verdict[k] = {"change_signal": sg, "crop_noise_floor": floors[k],
                      "operating_margin": round(floors[k] - sg, 6),
                      "separable": floors[k] > sg}
    print("\n--- eom (rotation-corrected): change signal vs crop noise floor on the SAME block ---")
    for k, v in verdict.items():
        print(f"  {k:10s} signal={v['change_signal']:.4f} crop_floor={v['crop_noise_floor']:.4f} "
              f"margin={v['operating_margin']:+.4f} separable={v['separable']}")

    (OUT / "relgraph_final.json").write_text(json.dumps(
        {"research_only": True, "corrected_pair_table": table, "separation": seps,
         "eom_crop_variants_rotation_corrected": per_variant,
         "eom_signal_vs_crop_noise": verdict}, ensure_ascii=False, indent=1))
    print("\nwrote", OUT / "relgraph_final.json")


if __name__ == "__main__":
    main()
