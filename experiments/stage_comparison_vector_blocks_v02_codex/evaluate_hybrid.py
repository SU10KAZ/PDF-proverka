#!/usr/bin/env python3
"""Manual, traceable evaluation of the three fixed-model input architectures."""
from __future__ import annotations

import json
import statistics
from pathlib import Path
from typing import Any


ARTIFACT = Path(__file__).resolve().parent / "artifacts/hybrid_results.json"
PROMPT_ARTIFACT = Path(__file__).resolve().parent / "artifacts/prompt_size_results.json"
GROUND_TRUTH = {
    "ss_scheme_text_changed": "STRUCTURE_SAME_VALUES_CHANGED",
    "ss_plan_dense": "NEAR_IDENTICAL",
    "ss_crop_mismatch_page07": "CROP_MISMATCH",
    "ar_plan": "NEAR_IDENTICAL",
    "ar_plan_page08": "NEAR_IDENTICAL",
    "vk_plan": "NEAR_IDENTICAL",
    "vk_nodes": "STRUCTURE_SAME_VALUES_CHANGED",
    "vk_axono_page17": "STRUCTURE_SAME_VALUES_CHANGED",
    "eom_singleline_changed": "STRUCTURE_CHANGED",
    "ov_plan_floor07": "STRUCTURE_CHANGED",
}

# Values were assigned after inspecting each model claim against the original
# side-by-side crops.  1.0 means all important facts were found, 0.5 means a
# material subset, and 0.0 means the major change was missed.
MANUAL = {
    "vector": {
        "completeness": [1, 1, 1, 1, 1, 1, 0.5, 0, 0, 0],
        "false_structural": {"ar_plan", "ar_plan_page08", "vk_axono_page17"},
        "missed": {"vk_nodes", "vk_axono_page17", "eom_singleline_changed", "ov_plan_floor07"},
    },
    "vision": {
        "completeness": [1, 1, 1, 1, 1, 1, 0, 1, 1, 1],
        "false_structural": {"vk_plan", "vk_axono_page17"},
        "missed": {"vk_nodes"},
    },
    "hybrid": {
        "completeness": [1, 1, 1, 1, 1, 1, 0, 1, 1, 1],
        "false_structural": set(),
        "missed": {"vk_nodes"},
    },
}


def evaluate() -> dict[str, Any]:
    data = json.loads(ARTIFACT.read_text(encoding="utf-8")); evaluations = {}
    for arm in ("vector", "vision", "hybrid"):
        outputs = data["arms"][arm]["output"]["pairs"]; metadata = data["arms"][arm]["metadata"]
        rows = []
        completeness = MANUAL[arm]["completeness"]
        for index, row in enumerate(outputs):
            pair_id = row["pair_id"]
            rows.append({
                "pair_id": pair_id, "expected": GROUND_TRUTH[pair_id], "actual": row["classification"],
                "classification_correct": row["classification"] == GROUND_TRUTH[pair_id],
                "completeness": completeness[index],
                "false_structural_change": pair_id in MANUAL[arm]["false_structural"],
                "missed_change": pair_id in MANUAL[arm]["missed"],
                "traceability_claims": len(row["traceability"]),
                "traceability_sources": sorted({item["source"] for item in row["traceability"]}),
            })
        evaluations[arm] = {
            "correctness": round(sum(row["classification_correct"] for row in rows) / len(rows), 6),
            "completeness": round(statistics.mean(row["completeness"] for row in rows), 6),
            "false_structural_change": sum(row["false_structural_change"] for row in rows),
            "missed_change": sum(row["missed_change"] for row in rows),
            "traceability_claims": sum(row["traceability_claims"] for row in rows),
            "traceability_source_tag_coverage": round(sum(bool(row["traceability_sources"]) for row in rows) / len(rows), 6),
            "input_tokens": metadata["usage"]["input_tokens"], "output_tokens": metadata["usage"]["output_tokens"],
            "latency_seconds": metadata["latency_seconds"], "pairs": rows,
        }
    data["human_evaluation"] = {
        "method": "Every output claim was checked against the manually reviewed source crops; the model was not the judge.",
        "ground_truth": GROUND_TRUTH, "arms": evaluations,
        "conclusion": "Hybrid removed false structural calls and raised classification correctness, but cost more tokens than either single arm and still missed the subtle VK notes/value case.",
    }
    ARTIFACT.write_text(json.dumps(data, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    prompt = json.loads(PROMPT_ARTIFACT.read_text(encoding="utf-8"))
    prompt["aggregate"]["real_model_tokens_l3_change_only_10_pairs"] = {
        "input_tokens": evaluations["vector"]["input_tokens"],
        "output_tokens": evaluations["vector"]["output_tokens"],
        "pairs": 10,
        "note": "Includes fixed system/schema overhead reported by codex JSON events.",
    }
    prompt["aggregate"]["real_model_tokens_pending_hybrid_run"] = False
    PROMPT_ARTIFACT.write_text(json.dumps(prompt, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return evaluations


if __name__ == "__main__":
    print(json.dumps(evaluate(), ensure_ascii=False, indent=2))
