#!/usr/bin/env python3
"""Run Pipeline B: per-block Vision verification before Hybrid comparison."""
from __future__ import annotations

import json
import tempfile
from pathlib import Path
from typing import Any

from .benchmark_data import REPOSITORY_ROOT, benchmark_manifest
from .evaluate_hybrid import GROUND_TRUTH
from .extractor import PageCache, extract_block
from .run_description_verification import _compact, _invoke as invoke_verifier, _render
from .run_hybrid_experiment import PAIR_IDS, _invoke as invoke_comparator


EXPERIMENT_DIR = Path(__file__).resolve().parent
ARTIFACT = EXPERIMENT_DIR / "artifacts/verification_results.json"
ROUTING_ARTIFACT = EXPERIMENT_DIR / "artifacts/routing_results.json"


def _verification_projection(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "status": row.get("actual_status") or row["status"],
        "verified_facts": row["verified_facts"],
        "missing_facts": row["missing_facts"],
        "suspicious_facts": row["suspicious_facts"],
        "limitations": row["limitations"],
        "policy": "May downgrade or add bounded facts; never overrides failed deterministic gates or vector coordinates.",
    }


def _prompt(
    routing_rows: dict[str, dict[str, Any]],
    verifications: dict[str, dict[str, Any]],
) -> str:
    parts = [
        """Ты сравниваешь 10 пар инженерных блоков, LEFT — старая версия, RIGHT — новая.
Это Pipeline B: каждый VectorBlockDescription предварительно проверен Vision verifier; raster crops также приложены для difficult diff.
Verifier не имеет права отменять failed deterministic gate или переписывать vector coordinates. Его VERIFIED/PARTIAL/FAILED,
missing_facts и suspicious_facts — дополнительное evidence, а не источник точной геометрии. Не считай padding/crop реальным
изменением конструкции. Для каждой пары верни ровно один classification, только фактические major_changes, evidence,
uncertainties и traceability source. Не открывай файлы и не вызывай инструменты. Верни только JSON по схеме.
Images идут по две на пару: LEFT, RIGHT, в порядке записей ниже."""
    ]
    for pair_id in PAIR_IDS:
        parts.append(
            json.dumps(
                {
                    "pair_id": pair_id,
                    "vector_diff": routing_rows[pair_id]["l3_change_only"],
                    "left_verification": _verification_projection(verifications[f"real_{pair_id}_left"]),
                    "right_verification": _verification_projection(verifications[f"real_{pair_id}_right"]),
                },
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            )
        )
    return "\n\n".join(parts) + "\n"


def _automatic_evaluation(output: dict[str, Any]) -> dict[str, Any]:
    rows = []
    for row in output["pairs"]:
        expected = GROUND_TRUTH[row["pair_id"]]
        actual = row["classification"]
        false_structural = actual == "STRUCTURE_CHANGED" and expected in {
            "IDENTICAL", "NEAR_IDENTICAL", "STRUCTURE_SAME_VALUES_CHANGED",
        }
        missed = expected in {"STRUCTURE_SAME_VALUES_CHANGED", "STRUCTURE_CHANGED"} and actual != expected
        rows.append(
            {
                "pair_id": row["pair_id"],
                "expected": expected,
                "actual": actual,
                "classification_correct": actual == expected,
                "false_structural_change": false_structural,
                "missed_change_class": missed,
                "major_changes": row["major_changes"],
                "traceability_sources": sorted({claim["source"] for claim in row["traceability"]}),
            }
        )
    return {
        "correctness": round(sum(row["classification_correct"] for row in rows) / len(rows), 6),
        "false_structural_change": sum(row["false_structural_change"] for row in rows),
        "missed_change_class": sum(row["missed_change_class"] for row in rows),
        "rows": rows,
        "manual_claim_review_pending": True,
    }


def run() -> dict[str, Any]:
    artifact = json.loads(ARTIFACT.read_text(encoding="utf-8"))
    existing = {row["sample_id"]: row for row in artifact["samples"]}
    manifest = {pair["pair_id"]: pair for pair in benchmark_manifest()["pairs"]}
    routing = json.loads(ROUTING_ARTIFACT.read_text(encoding="utf-8"))
    routing_rows = {row["pair_id"]: row for row in routing["pairs"]}
    cache = PageCache(EXPERIMENT_DIR / ".page_cache")

    with tempfile.TemporaryDirectory(prefix="vector-verified-pipeline-") as directory:
        work = Path(directory)
        missing_samples = []
        all_images = []
        for pair_id in PAIR_IDS:
            pair = manifest[pair_id]
            for side_name in ("left", "right"):
                side = pair[side_name]
                sample_id = f"real_{pair_id}_{side_name}"
                pdf = Path(side["pdf"])
                pdf = pdf if pdf.is_absolute() else REPOSITORY_ROOT / pdf
                image = work / f"{pair_id}-{side_name}.png"
                _render(pdf, side["page_index"], side["bbox_norm"], image)
                all_images.append(image)
                if sample_id in existing:
                    continue
                description = extract_block(
                    pdf,
                    page_index=side["page_index"],
                    bbox_norm=side["bbox_norm"],
                    block_id=side["block_id"],
                    page_cache=cache,
                )
                missing_samples.append(
                    {
                        "sample_id": sample_id,
                        "group": "hard_pair_extension",
                        "expected_status": "NOT_SCORED_HERE",
                        "expected_reason": "used as Pipeline B input",
                        "description": _compact(description),
                        "image": image,
                    }
                )

        verifier_metadata = None
        if missing_samples:
            verifier_output, verifier_metadata = invoke_verifier(missing_samples, work)
            for row in verifier_output["samples"]:
                existing[row["sample_id"]] = row
        prompt = _prompt(routing_rows, existing)
        comparison_output, comparison_metadata = invoke_comparator(prompt, all_images, work)

    result = {
        "schema_version": "vector-verified-pipeline-v0.2-codex",
        "pair_ids": list(PAIR_IDS),
        "block_verification_count": len(PAIR_IDS) * 2,
        "reused_verification_count": len(PAIR_IDS) * 2 - len(missing_samples),
        "new_verification_count": len(missing_samples),
        "block_verifications": {
            sample_id: _verification_projection(existing[sample_id])
            for pair_id in PAIR_IDS
            for sample_id in (f"real_{pair_id}_left", f"real_{pair_id}_right")
        },
        "new_verifier_metadata": verifier_metadata,
        "comparison_metadata": comparison_metadata,
        "comparison_output": comparison_output,
        "automatic_evaluation": _automatic_evaluation(comparison_output),
    }
    artifact["analysis"]["pipeline_B_hard_pair_experiment"] = result
    ARTIFACT.write_text(json.dumps(artifact, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return result


if __name__ == "__main__":
    result = run()
    print(json.dumps({
        "new_verifier_metadata": result["new_verifier_metadata"],
        "comparison_metadata": result["comparison_metadata"],
        "automatic_evaluation": result["automatic_evaluation"],
    }, ensure_ascii=False, indent=2))
