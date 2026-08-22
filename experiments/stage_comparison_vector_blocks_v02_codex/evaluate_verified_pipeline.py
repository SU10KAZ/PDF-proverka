#!/usr/bin/env python3
"""Retained human evaluation of the fixed Pipeline B model output."""
from __future__ import annotations

import json
import statistics
from pathlib import Path
from typing import Any


ARTIFACT = Path(__file__).resolve().parent / "artifacts/verification_results.json"
COMPLETENESS = {
    "ss_scheme_text_changed": 1.0,
    "ss_plan_dense": 1.0,
    "ss_crop_mismatch_page07": 1.0,
    "ar_plan": 1.0,
    "ar_plan_page08": 1.0,
    "vk_plan": 1.0,
    "vk_nodes": 0.0,
    "vk_axono_page17": 0.0,
    "eom_singleline_changed": 1.0,
    "ov_plan_floor07": 0.0,
}
FALSE_STRUCTURAL = {"ar_plan_page08", "vk_axono_page17"}
MISSED_IMPORTANT_CHANGE = {"vk_nodes", "vk_axono_page17", "ov_plan_floor07"}
FOUND_BUT_MISCLASSIFIED = {"eom_singleline_changed"}


def evaluate() -> dict[str, Any]:
    data = json.loads(ARTIFACT.read_text(encoding="utf-8"))
    experiment = data["analysis"]["pipeline_B_hard_pair_experiment"]
    automatic_rows = experiment["automatic_evaluation"]["rows"]
    rows = []
    for row in automatic_rows:
        pair_id = row["pair_id"]
        rows.append(
            {
                **row,
                "completeness": COMPLETENESS[pair_id],
                "false_structural_change": pair_id in FALSE_STRUCTURAL,
                "missed_important_change": pair_id in MISSED_IMPORTANT_CHANGE,
                "found_change_but_misclassified": pair_id in FOUND_BUT_MISCLASSIFIED,
            }
        )
    existing_metadata = data["metadata"][0]
    reused = experiment["reused_verification_count"]
    existing_batch_size = existing_metadata["image_count"]
    existing_fraction = reused / existing_batch_size
    verifier = experiment["new_verifier_metadata"]
    comparator = experiment["comparison_metadata"]
    normalized_input = round(
        (existing_metadata["usage"]["input_tokens"] or 0) * existing_fraction
        + (verifier["usage"]["input_tokens"] or 0)
        + (comparator["usage"]["input_tokens"] or 0)
    )
    normalized_latency = round(
        existing_metadata["latency_seconds"] * existing_fraction
        + verifier["latency_seconds"]
        + comparator["latency_seconds"],
        6,
    )
    manual = {
        "method": "Every major-change claim was checked against the same fixed human ground truth and source-crop review used for the three original arms.",
        "correctness": round(sum(row["classification_correct"] for row in rows) / len(rows), 6),
        "completeness": round(statistics.mean(row["completeness"] for row in rows), 6),
        "false_structural_change": sum(row["false_structural_change"] for row in rows),
        "missed_important_change": sum(row["missed_important_change"] for row in rows),
        "found_change_but_misclassified": sum(row["found_change_but_misclassified"] for row in rows),
        "classification_misses": sum(not row["classification_correct"] for row in rows),
        "conceptual_vision_calls": 30,
        "actual_batched_model_invocations": 3,
        "normalized_relevant_input_tokens": normalized_input,
        "normalized_relevant_latency_seconds": normalized_latency,
        "normalization_note": "Eight reused block verifications are charged as 8/12 of their original batch; the other 12 verifications and ten-pair comparator use measured totals.",
        "rows": rows,
        "conclusion": "A separate verifier anchored the comparator on crop/style/vector-count warnings. It found the EOM change but misclassified it, missed the OV removal, and regressed exact class accuracy versus the single fused Hybrid arm.",
    }
    experiment["manual_evaluation"] = manual
    experiment["automatic_evaluation"]["manual_claim_review_pending"] = False
    data["analysis"]["interpretation"] = (
        "The separate verifier is useful for error localization but unsafe as a permission gate. "
        "On the same ten hard pairs it regressed Hybrid exact-class accuracy from 0.9 to 0.5; "
        "use deterministic gates and one fused Hybrid call for risky comparisons."
    )
    ARTIFACT.write_text(json.dumps(data, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return manual


if __name__ == "__main__":
    print(json.dumps(evaluate(), ensure_ascii=False, indent=2))
