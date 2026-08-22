#!/usr/bin/env python3
"""signoise probe 2 — field-group ablation over the 10 Track A pairs.

For each field group of VectorBlockDescription v0.1 we neutralise it on BOTH sides and
re-run experiments.stage_comparison_vector_blocks.comparator.compare_descriptions,
then record whether the emitted status and the user-visible `differences` list change.

Run from repo root:
    python -m experiments.stage_comparison_vector_architecture_opus.probes.signoise_02_ablation

Writes artifacts/signoise_02_ablation.{json,md}. Research only; nothing outside this dir is written.
"""
from __future__ import annotations

import copy
import json
import time
from pathlib import Path
from typing import Any, Callable

from experiments.stage_comparison_vector_blocks import comparator as C

ROOT = Path(__file__).resolve().parents[3]
DESC = ROOT / "experiments/stage_comparison_vector_blocks/artifacts/descriptions"
OUT = Path(__file__).resolve().parents[1] / "artifacts"

PAIRS = [
    "ss_scheme_text_changed", "ss_plan_dense", "ss_simple_node", "ss_table_graphic",
    "ar_plan", "ar_wall_sections", "vk_plan", "vk_nodes", "vk_node_plan",
    "eom_singleline_changed",
]


def shallow(description: dict[str, Any], **overrides: Any) -> dict[str, Any]:
    out = dict(description)
    out.update(overrides)
    return out


def strip_primitive_key(description: dict[str, Any], key: str, value: Any) -> dict[str, Any]:
    primitives = [dict(p, **{key: value}) for p in description["geometry"]["primitives"]]
    return shallow(description, geometry=dict(description["geometry"], primitives=primitives))


def flatten_categories(description: dict[str, Any]) -> dict[str, Any]:
    return shallow(description, texts=[dict(t, category="label") for t in description["texts"]])


def zero_text_positions(description: dict[str, Any]) -> dict[str, Any]:
    return shallow(
        description, texts=[dict(t, x_norm=0.0, y_norm=0.0) for t in description["texts"]]
    )


def blank_source_item_counts(description: dict[str, Any]) -> dict[str, Any]:
    extraction = dict(description["geometry"]["extraction"], source_item_counts={})
    return shallow(description, geometry=dict(description["geometry"], extraction=extraction))


# name -> (mutator(left, right) -> (left', right'), note)
ABLATIONS: dict[str, tuple[Callable[[dict, dict], tuple[dict, dict]], str]] = {
    "baseline": (lambda l, r: (l, r), "unmodified Track A descriptions"),
    "anchors_blank": (
        lambda l, r: (shallow(l, anchors=[]), shallow(r, anchors=[])),
        "anchors = [] on both sides",
    ),
    "repeated_elements_blank": (
        lambda l, r: (shallow(l, repeated_elements=[]), shallow(r, repeated_elements=[])),
        "repeated_elements = [] on both sides",
    ),
    "hatch_blank": (
        lambda l, r: (shallow(l, hatch_like_structures=[]), shallow(r, hatch_like_structures=[])),
        "hatch_like_structures = []",
    ),
    "dimensions_labels_blank": (
        lambda l, r: (shallow(l, dimensions=[], labels=[]), shallow(r, dimensions=[], labels=[])),
        "dimensions = labels = []",
    ),
    "size_metrics_blank": (
        lambda l, r: (shallow(l, size_metrics={}), shallow(r, size_metrics={})),
        "size_metrics = {}",
    ),
    "primitive_summary_equalized": (
        lambda l, r: (
            l,
            shallow(r, primitive_summary=dict(r["primitive_summary"],
                                              primitive_count=l["primitive_summary"]["primitive_count"])),
        ),
        "right.primitive_summary.primitive_count forced equal to left",
    ),
    "texts_blank": (
        lambda l, r: (shallow(l, texts=[]), shallow(r, texts=[])),
        "texts = [] on both sides",
    ),
    "texts_categories_flattened": (
        lambda l, r: (flatten_categories(l), flatten_categories(r)),
        "every text category forced to 'label'",
    ),
    "texts_positions_zeroed": (
        lambda l, r: (zero_text_positions(l), zero_text_positions(r)),
        "text x_norm/y_norm forced to 0",
    ),
    "topology_forced_1.0": (lambda l, r: (l, r), "monkeypatch _topology_diff -> similarity 1.0"),
    "topology_forced_0.0": (lambda l, r: (l, r), "monkeypatch _topology_diff -> similarity 0.0"),
    "topology_equalized": (
        lambda l, r: (l, shallow(r, topology=dict(r["topology"], **{
            k: l["topology"][k] for k in (
                "node_count", "edge_count", "connected_components", "endpoints", "branch_points",
                "t_junctions", "x_crossings_unconnected", "closed_contours", "nested_contours")
        }))),
        "right topology counts copied from left (9 comparator keys)",
    ),
    "signature_l1_broken": (
        lambda l, r: (
            l,
            shallow(r, structural_signature=dict(r["structural_signature"],
                                                 level_1_exact_vector="ABLATED")),
        ),
        "level_1_exact_vector forced unequal",
    ),
    "signature_l2_l3_blank": (
        lambda l, r: (
            shallow(l, structural_signature=dict(l["structural_signature"],
                                                 level_2_normalized_geometry="X",
                                                 level_3_structural_topology="X",
                                                 level_3_payload={})),
            shallow(r, structural_signature=dict(r["structural_signature"],
                                                 level_2_normalized_geometry="X",
                                                 level_3_structural_topology="X",
                                                 level_3_payload={})),
        ),
        "level_2 / level_3 signatures forced equal-and-meaningless",
    ),
    "primitive_style_blank": (
        lambda l, r: (
            strip_primitive_key(l, "style", {}), strip_primitive_key(r, "style", {})),
        "every primitive.style = {} (27.0 % of corpus bytes)",
    ),
    "primitive_raw_blank": (
        lambda l, r: (strip_primitive_key(l, "raw", {}), strip_primitive_key(r, "raw", {})),
        "every primitive.raw = {} (22.8 % of corpus bytes)",
    ),
    "primitive_provenance_blank": (
        lambda l, r: (
            strip_primitive_key(strip_primitive_key(l, "source_kinds", []), "item_indexes", []),
            strip_primitive_key(strip_primitive_key(r, "source_kinds", []), "item_indexes", []),
        ),
        "primitive.source_kinds / item_indexes = [] (6.6 % of corpus bytes)",
    ),
    "extraction_item_counts_blank": (
        lambda l, r: (blank_source_item_counts(l), blank_source_item_counts(r)),
        "geometry.extraction.source_item_counts = {} (kills encoding-rewrite heuristic)",
    ),
    "quality_notes_ambiguities_blank": (
        lambda l, r: (
            shallow(l, quality_notes=[], ambiguities=[], coordinate_system={}, source={}),
            shallow(r, quality_notes=[], ambiguities=[], coordinate_system={}, source={}),
        ),
        "quality_notes / ambiguities / coordinate_system / source emptied",
    ),
}


def summarise(result: dict[str, Any]) -> dict[str, Any]:
    return {
        "status": result["status"],
        "differences": result["differences"],
        "geometry_similarity": result["geometry"]["similarity"],
        "text_similarity": result["text"]["effective_similarity"],
        "topology_similarity": result["topology"]["similarity"],
        "patterns_similarity": result["repeated_patterns"]["similarity"],
        "exact_sig_equal": result["exact_vector_signature_equal"],
        "normalized_sig_equal": result["normalized_signature_equal"],
        "structural_sig_equal": result["structural_signature_equal"],
        "encoding_rewrite_suspected": result["geometry"]["encoding_rewrite_suspected"],
    }


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    results: dict[str, dict[str, Any]] = {name: {} for name in ABLATIONS}
    original_topology_diff = C._topology_diff

    for pair in PAIRS:
        left = json.loads((DESC / pair / "left" / "vector_block.json").read_text(encoding="utf-8"))
        right = json.loads((DESC / pair / "right" / "vector_block.json").read_text(encoding="utf-8"))
        for name, (mutate, _note) in ABLATIONS.items():
            started = time.time()
            C._topology_diff = original_topology_diff
            if name == "topology_forced_1.0":
                C._topology_diff = lambda a, b: {"similarity": 1.0, "counts": {}}
            elif name == "topology_forced_0.0":
                C._topology_diff = lambda a, b: {"similarity": 0.0, "counts": {}}
            mutated_left, mutated_right = mutate(left, right)
            summary = summarise(C.compare_descriptions(mutated_left, mutated_right))
            summary["seconds"] = round(time.time() - started, 2)
            results[name][pair] = summary
            print(f"{pair:24s} {name:32s} {summary['status']:30s} {summary['seconds']:6.1f}s")
        C._topology_diff = original_topology_diff
        del left, right

    base = results["baseline"]
    table = []
    for name in ABLATIONS:
        if name == "baseline":
            continue
        status_changed = [p for p in PAIRS if results[name][p]["status"] != base[p]["status"]]
        diff_changed = [
            p for p in PAIRS if results[name][p]["differences"] != base[p]["differences"]
        ]
        score_changed = [
            p for p in PAIRS
            if any(results[name][p][k] != base[p][k] for k in
                   ("geometry_similarity", "text_similarity", "topology_similarity",
                    "patterns_similarity", "exact_sig_equal", "normalized_sig_equal",
                    "structural_sig_equal", "encoding_rewrite_suspected"))
        ]
        table.append({
            "ablation": name,
            "note": ABLATIONS[name][1],
            "status_changed_pairs": status_changed,
            "status_changed_count": len(status_changed),
            "differences_changed_pairs": diff_changed,
            "differences_changed_count": len(diff_changed),
            "any_score_changed_pairs": score_changed,
            "any_score_changed_count": len(score_changed),
        })

    payload = {
        "probe": "signoise_02_ablation",
        "research_only": True,
        "pairs": PAIRS,
        "baseline_statuses": {p: base[p]["status"] for p in PAIRS},
        "ablation_summary": table,
        "raw": results,
    }
    (OUT / "signoise_02_ablation.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=1), encoding="utf-8")

    lines = [
        "# signoise probe 2 — ablation of field groups against the Track A comparator",
        "",
        "Reproduce: `python -m experiments.stage_comparison_vector_architecture_opus.probes.signoise_02_ablation`",
        "",
        "Baseline statuses: " + ", ".join(f"{p}={base[p]['status']}" for p in PAIRS),
        "",
        "| ablation | what was neutralised | pairs whose STATUS changed | pairs whose `differences` text changed | pairs whose any score changed |",
        "|---|---|---:|---:|---:|",
    ]
    for row in table:
        lines.append(
            f"| `{row['ablation']}` | {row['note']} | **{row['status_changed_count']}/10** "
            f"{','.join(row['status_changed_pairs'])} | {row['differences_changed_count']}/10 "
            f"{','.join(row['differences_changed_pairs'])} | {row['any_score_changed_count']}/10 |"
        )
    (OUT / "signoise_02_ablation.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    print("written:", OUT / "signoise_02_ablation.json")


if __name__ == "__main__":
    main()
