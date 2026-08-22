"""P3 — payoff and risk of the background filter.

Measures, per pair, with and without the filter:
  * primitive / segment counts and whether Track A's caps (20 000 primitives,
    8 000 topology segments, 12 000 comparator segments) still fire
  * comparator status, geometry similarity, topology similarity, text similarity
  * for CAD-layered blocks: how much genuine foreground the filter ate

Run from repo root:
    python -m experiments.stage_comparison_vector_architecture_opus.probes.hatchnoise_p3_payoff
"""
from __future__ import annotations

import collections
import hashlib
import json
import sys
import time
from pathlib import Path
from typing import Any, Sequence

from experiments.stage_comparison_vector_blocks import comparator as tc
from experiments.stage_comparison_vector_blocks import extractor as ta
from experiments.stage_comparison_vector_architecture_opus.probes import hatchnoise_core as C
from experiments.stage_comparison_vector_architecture_opus.probes import hatchnoise_filter as F

OUT = C.ART / "hatchnoise_p3_payoff.json"

PAIRS: dict[str, dict[str, Any]] = {}
for _name, _spec in C.BLOCKS.items():
    PAIRS[_name] = {"discipline": _spec["discipline"], "left": _spec["left"], "right": _spec["right"]}
for _name, _spec in C.EXTRA_PAIRS.items():
    PAIRS[_name] = {"discipline": _spec["discipline"], "left": _spec["left"], "right": _spec["right"]}


def build_description(payload: dict[str, Any], primitives: Sequence[dict[str, Any]], block_id: str) -> dict[str, Any]:
    """Assemble a Track-A-shaped VectorBlockDescription from a chosen primitive subset,
    applying Track A's own caps so the comparison is like-for-like."""
    primitives = list(primitives)
    uncapped = len(primitives)
    storage_capped = uncapped > ta.DEFAULT_STORAGE_CAP
    if storage_capped:
        primitives = sorted(
            primitives,
            key=lambda item: (item["type"] not in {"line", "polyline"}, item["closed"], item["length_norm"]),
            reverse=True,
        )[: ta.DEFAULT_STORAGE_CAP]
    primitives = [dict(p) for p in primitives]
    for index, primitive in enumerate(primitives, 1):
        primitive["id"] = f"primitive-{index}"
    topology = ta._topology(primitives, 0.0025, ta.DEFAULT_TOPOLOGY_CAP)
    texts = payload["texts"]
    if not primitives or topology["segments_total"] < 3:
        quality = "VECTOR_DATA_INSUFFICIENT"
    elif storage_capped or topology["segments_capped"]:
        quality = "LIMITED_CAPPED"
    elif topology["segments_total"] < 30:
        quality = "LIMITED"
    else:
        quality = "GOOD"
    description = {
        "block_id": block_id,
        "vector_quality": quality,
        "geometry": {
            "extraction": {
                **payload["extraction"],
                "primitives_uncapped": uncapped,
                "storage_capped": storage_capped,
            },
            "primitives": primitives,
        },
        "texts": texts,
        "topology": topology,
        "repeated_elements": ta._repeated_elements(primitives),
        "hatch_like_structures": ta._hatch_like_structures(primitives),
        "structural_signature": ta._signatures(primitives, texts, topology),
        "primitive_summary": ta._summary(primitives, texts, topology),
    }
    return description


def side(spec, block_id: str) -> dict[str, Any]:
    payload = C.load_primitives(*spec)
    rows = C.segment_table(payload)["rows"]
    flags, records, prim_flags = F.classify(rows)
    kept_pi = {record["pi"] for record, flag in zip(records, prim_flags) if not flag}
    primitives = payload["primitives"]
    kept = [primitives[i] for i in sorted(kept_pi)]
    result = {
        "baseline": build_description(payload, primitives, block_id),
        "filtered": build_description(payload, kept, block_id),
        "segments_total": len(rows),
        "segments_kept": sum(1 for f in flags if not f),
        "primitives_total": len(primitives),
        "primitives_kept": len(kept),
    }
    del rows, records, flags, prim_flags, kept, primitives, payload
    return result


def summarize(comparison: dict[str, Any]) -> dict[str, Any]:
    return {
        "status": comparison["status"],
        "geometry_similarity": comparison["geometry"]["similarity"],
        "selected_tolerance": comparison["geometry"]["selected_tolerance"],
        "geometry_capped": comparison["geometry"]["tolerance_experiment"][0]["capped"],
        "left_used": comparison["geometry"]["tolerance_experiment"][0]["left_used"],
        "left_total": comparison["geometry"]["tolerance_experiment"][0]["left_total"],
        "topology_similarity": comparison["topology"]["similarity"],
        "text_similarity": comparison["text"]["effective_similarity"],
        "n_differences": len(comparison["differences"]),
        "differences_head": comparison["differences"][:4],
    }


def main() -> None:
    names = sys.argv[1:] or list(PAIRS)
    results = {}
    if OUT.exists():
        results = json.loads(OUT.read_text(encoding="utf-8")).get("pairs", {})
    for name in names:
        spec = PAIRS[name]
        t0 = time.time()
        print("...", name, flush=True)
        left = side(spec["left"], f"{name}_left")
        right = side(spec["right"], f"{name}_right")
        row = {
            "pair": name,
            "discipline": spec["discipline"],
            "left_pdf": spec["left"][0],
            "right_pdf": spec["right"][0],
            "left": {
                "segments_total": left["segments_total"], "segments_kept": left["segments_kept"],
                "primitives_total": left["primitives_total"], "primitives_kept": left["primitives_kept"],
                "baseline_quality": left["baseline"]["vector_quality"],
                "filtered_quality": left["filtered"]["vector_quality"],
                "baseline_storage_capped": left["baseline"]["geometry"]["extraction"]["storage_capped"],
                "filtered_storage_capped": left["filtered"]["geometry"]["extraction"]["storage_capped"],
                "baseline_topology_capped": left["baseline"]["topology"]["segments_capped"],
                "filtered_topology_capped": left["filtered"]["topology"]["segments_capped"],
            },
            "right": {
                "segments_total": right["segments_total"], "segments_kept": right["segments_kept"],
                "primitives_total": right["primitives_total"], "primitives_kept": right["primitives_kept"],
                "baseline_quality": right["baseline"]["vector_quality"],
                "filtered_quality": right["filtered"]["vector_quality"],
            },
            "baseline_comparison": summarize(tc.compare_descriptions(left["baseline"], right["baseline"])),
            "filtered_comparison": summarize(tc.compare_descriptions(left["filtered"], right["filtered"])),
            "elapsed_s": round(time.time() - t0, 1),
        }
        results[name] = row
        print(json.dumps({k: row[k] for k in ("pair", "baseline_comparison", "filtered_comparison")}, ensure_ascii=False)[:700], flush=True)
        C.write_json(OUT, {"probe": "hatchnoise_p3_payoff", "filter_defaults": F.DEFAULTS, "pairs": results})
    C.write_json(OUT, {"probe": "hatchnoise_p3_payoff", "filter_defaults": F.DEFAULTS, "pairs": results})


if __name__ == "__main__":
    main()
