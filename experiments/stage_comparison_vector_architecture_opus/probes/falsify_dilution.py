"""falsify_ probe, attack A (dilution): how big must a real engineering change be
before v0.1's geometry score drops out of NEAR_IDENTICAL?

comparator.compare_descriptions calls a pair NEAR_IDENTICAL when
    selected geometry similarity >= 0.985  AND  topology similarity >= 0.85
    AND (text unreliable OR text similarity >= 0.92).

Geometry similarity is a symmetric F1 over matched *segments*. Adding or deleting
k segments in a block that has N segments moves that score by roughly k/N.
So the number of whole symbol instances an engineer may add or delete while the
comparator keeps answering "NEAR_IDENTICAL" is about

    0.015 * N / (segments per instance)

This probe reads Track A's own 20 real descriptions and reports, per block:
  * N = total_segment_count
  * the segment size of the block's most common repeated motif (a real symbol)
  * how many such instances can change before the 0.985 gate trips
  * the same for the topology 0.85 gate (component count)
It then verifies the k/N model by rebuilding descriptions with k primitives
removed and running the real comparator.

Run:
  python -m experiments.stage_comparison_vector_architecture_opus.probes.falsify_dilution
"""
from __future__ import annotations

import copy
import json
from pathlib import Path

from experiments.stage_comparison_vector_blocks import comparator, extractor

ROOT = Path(__file__).resolve().parents[3]
TRACK_A = ROOT / "experiments/stage_comparison_vector_blocks/artifacts/descriptions"
ART = Path(__file__).resolve().parents[1] / "artifacts"

GEOM_GATE = 0.985
TOPO_GATE = 0.85


def analytic_rows():
    rows = []
    for left_file in sorted(TRACK_A.glob("*/left/vector_block.json")):
        pair = left_file.parent.parent.name
        d = json.loads(left_file.read_text(encoding="utf-8"))
        n = d["primitive_summary"]["total_segment_count"]
        motifs = d["repeated_elements"]
        motif = max(motifs, key=lambda m: m["count"]) if motifs else None
        seg_per_instance = motif["segment_count"] if motif else None
        comps = d["topology"]["connected_components"]
        rows.append(
            {
                "pair": pair,
                "block_id": d["block_id"],
                "quality": d["vector_quality"],
                "total_segments": n,
                "connected_components": comps,
                "top_motif": None
                if not motif
                else {
                    "primitive_type": motif["primitive_type"],
                    "segment_count": motif["segment_count"],
                    "instances_present": motif["count"],
                },
                "segments_budget_before_geometry_gate_trips": round((1 - GEOM_GATE) * n, 1),
                "motif_instances_before_geometry_gate_trips": None
                if not seg_per_instance
                else round((1 - GEOM_GATE) * n / seg_per_instance, 1),
                "components_budget_before_topology_gate_trips": round((1 - TOPO_GATE) * comps, 1),
            }
        )
    return rows


def rebuild(base: dict, keep_primitives: list) -> dict:
    """Rebuild every derived layer of a v0.1 description from a reduced primitive
    list, using Track A's own extractor functions, so the mutated description is
    internally consistent (summary, topology, signatures, size metrics)."""
    d = copy.deepcopy(base)
    d["geometry"]["primitives"] = keep_primitives
    texts = d["texts"]
    d["topology"] = extractor._topology(keep_primitives, 0.0025, extractor.DEFAULT_TOPOLOGY_CAP)
    d["anchors"] = extractor._anchors(texts, keep_primitives)
    d["repeated_elements"] = extractor._repeated_elements(keep_primitives)
    d["hatch_like_structures"] = extractor._hatch_like_structures(keep_primitives)
    d["primitive_summary"] = extractor._summary(keep_primitives, texts, d["topology"])
    d["structural_signature"] = extractor._signatures(keep_primitives, texts, d["topology"])
    d["size_metrics"] = extractor._size_metrics(d)
    return d


def empirical_check(pair: str, ks=(1, 2, 5, 10, 20, 50, 100, 200, 400)):
    """Delete the k SMALLEST-segment primitives (the symbol-sized ones, not the
    background linework) from a real description and run the real comparator
    against the untouched original."""
    base = json.loads((TRACK_A / pair / "left" / "vector_block.json").read_text(encoding="utf-8"))
    prims = base["geometry"]["primitives"]
    # symbol-like primitives first: small segment count, in the middle of the block
    order = sorted(range(len(prims)), key=lambda i: (prims[i]["segment_count"], i))
    out = []
    for k in ks:
        if k > len(prims):
            break
        drop = set(order[:k])
        removed_segments = sum(prims[i]["segment_count"] for i in drop)
        mutated = rebuild(base, [p for i, p in enumerate(prims) if i not in drop])
        cmp_ = comparator.compare_descriptions(base, mutated)
        out.append(
            {
                "primitives_removed": k,
                "segments_removed": removed_segments,
                "segments_removed_pct": round(
                    100 * removed_segments / max(base["primitive_summary"]["total_segment_count"], 1), 3
                ),
                "status": cmp_["status"],
                "geometry_similarity": cmp_["geometry"]["similarity"],
                "topology_similarity": cmp_["topology"]["similarity"],
                "text_similarity": cmp_["text"]["effective_similarity"],
                "differences": cmp_["differences"][:4],
            }
        )
        if cmp_["status"] not in ("IDENTICAL", "NEAR_IDENTICAL"):
            break
    return out


def localized_check(pair: str, sides=(0.02, 0.05, 0.1, 0.15, 0.2, 0.3, 0.4, 0.5)):
    """Erase every primitive whose normalized centroid falls inside a square
    window of the given side, centred on the block. This models one real
    localized revision (a device removed, an opening bricked up) rather than
    scattered noise, and asks how big the window must get before the v0.1
    comparator stops answering NEAR_IDENTICAL."""
    base = json.loads((TRACK_A / pair / "left" / "vector_block.json").read_text(encoding="utf-8"))
    prims = base["geometry"]["primitives"]

    def centroid(p):
        pts = [pt for seg in p["normalized"]["segments"] for pt in seg]
        return (sum(x for x, _ in pts) / len(pts), sum(y for _, y in pts) / len(pts))

    cents = [centroid(p) for p in prims]
    out = []
    for side in sides:
        lo, hi = 0.5 - side / 2, 0.5 + side / 2
        keep, removed_segments, removed_n = [], 0, 0
        for p, (cx, cy) in zip(prims, cents):
            if lo <= cx <= hi and lo <= cy <= hi:
                removed_segments += p["segment_count"]
                removed_n += 1
                continue
            keep.append(p)
        if removed_n == 0 or not keep:
            continue
        mutated = rebuild(base, keep)
        cmp_ = comparator.compare_descriptions(base, mutated)
        out.append(
            {
                "window_side_norm": side,
                "window_area_pct_of_block": round(100 * side * side, 2),
                "primitives_removed": removed_n,
                "segments_removed": removed_segments,
                "segments_removed_pct": round(
                    100 * removed_segments / max(base["primitive_summary"]["total_segment_count"], 1), 3
                ),
                "status": cmp_["status"],
                "geometry_similarity": cmp_["geometry"]["similarity"],
                "topology_similarity": cmp_["topology"]["similarity"],
                "differences": cmp_["differences"][:5],
            }
        )
        if cmp_["status"] not in ("IDENTICAL", "NEAR_IDENTICAL"):
            break
    return out


def main() -> None:
    rows = analytic_rows()
    checks = {}
    for pair in ("ss_plan_dense", "ar_plan", "vk_nodes", "ss_scheme_text_changed"):
        if (TRACK_A / pair / "left" / "vector_block.json").exists():
            checks[pair] = empirical_check(pair)
    localized = {}
    for pair in ("ss_plan_dense", "ar_plan", "vk_nodes", "vk_plan", "ar_wall_sections"):
        if (TRACK_A / pair / "left" / "vector_block.json").exists():
            localized[pair] = localized_check(pair)
    payload = {
        "localized": localized,
        "geometry_gate": GEOM_GATE,
        "topology_gate": TOPO_GATE,
        "note": "analytic budget = (1 - gate) * N; empirical check deletes real primitives "
        "from a real description and runs the unmodified Track A comparator.",
        "analytic": rows,
        "empirical": checks,
    }
    ART.mkdir(parents=True, exist_ok=True)
    (ART / "falsify_dilution.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=1), encoding="utf-8"
    )
    print(
        "%-24s %8s %8s %10s %10s"
        % ("pair", "segments", "comps", "seg_budget", "motif_inst")
    )
    for r in rows:
        print(
            "%-24s %8d %8d %10.1f %10s"
            % (
                r["pair"],
                r["total_segments"],
                r["connected_components"],
                r["segments_budget_before_geometry_gate_trips"],
                r["motif_instances_before_geometry_gate_trips"],
            )
        )
    for pair, rowset in checks.items():
        print("\n--", pair)
        for row in rowset:
            print(
                "  drop %4d prims / %6d segs (%5.2f%%) -> %-22s geom=%.4f topo=%.3f"
                % (
                    row["primitives_removed"],
                    row["segments_removed"],
                    row["segments_removed_pct"],
                    row["status"],
                    row["geometry_similarity"],
                    row["topology_similarity"],
                )
            )
    for pair, rowset in localized.items():
        print("\n== localized window --", pair)
        for row in rowset:
            print(
                "  window %4.0f%% of width (%5.2f%% of area): %4d prims / %6d segs (%5.2f%%) -> %-20s geom=%.4f topo=%.3f"
                % (
                    100 * row["window_side_norm"],
                    row["window_area_pct_of_block"],
                    row["primitives_removed"],
                    row["segments_removed"],
                    row["segments_removed_pct"],
                    row["status"],
                    row["geometry_similarity"],
                    row["topology_similarity"],
                )
            )
    print("wrote", ART / "falsify_dilution.json")


if __name__ == "__main__":
    main()
