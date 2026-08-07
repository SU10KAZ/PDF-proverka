from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

import pytest


_SCRIPTS_DIR = Path(__file__).resolve().parents[1] / "scripts"
sys.path.insert(0, str(_SCRIPTS_DIR))
_MODULE_PATH = _SCRIPTS_DIR / "merge_rejected_audit_runs.py"
_SPEC = importlib.util.spec_from_file_location("merge_rejected_audit_runs", _MODULE_PATH)
merger = importlib.util.module_from_spec(_SPEC)
assert _SPEC.loader is not None
_SPEC.loader.exec_module(merger)


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "".join(json.dumps(row, ensure_ascii=False) + "\n" for row in rows),
        encoding="utf-8",
    )


def _case(case_id: str, input_hash: str) -> dict:
    return {
        "case_id": case_id,
        "input_hash": input_hash,
        "object_id": "object-1",
        "object_name": "Объект",
        "discipline": "AR",
        "document": "DOC-1",
        "version_id": "v001",
        "item_id": case_id,
        "finding_problem": f"Замечание {case_id}",
        "expert_reason": f"Причина {case_id}",
    }


def _result(case: dict, verdict: str) -> dict:
    return {
        **case,
        "status": "success",
        "verdict": verdict,
        "recommended_action": (
            "collect_context" if verdict == "insufficient_evidence" else "keep_rejected"
        ),
    }


def test_incomplete_pilot_base_requires_explicit_flag_and_merges_only_successes(
    tmp_path: Path,
) -> None:
    base_dir = tmp_path / "base"
    overlay_dir = tmp_path / "overlay"
    cases = [_case("RF-A", "base-a"), _case("RF-B", "base-b"), _case("RF-C", "base-c")]
    _write_jsonl(base_dir / "manifest.jsonl", cases)
    (base_dir / "inventory.json").write_text(
        json.dumps(
            {
                "period": "2026-07",
                "timezone": "Europe/Moscow",
                "filters": {"reviewers": ["Кульдяев Ф. С."]},
                "counts": {"selected_cases": 3},
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    _write_jsonl(
        base_dir / "results.jsonl",
        [_result(cases[0], "expert_correct"), _result(cases[1], "insufficient_evidence")],
    )

    overlay_case = {**cases[1], "input_hash": "retry-b"}
    _write_jsonl(overlay_dir / "manifest.jsonl", [overlay_case])
    _write_jsonl(overlay_dir / "results.jsonl", [_result(overlay_case, "expert_correct")])

    with pytest.raises(ValueError, match="successful current result for every case"):
        merger.merge_runs(base_dir, overlay_dir, tmp_path / "blocked")

    output_dir = tmp_path / "merged"
    receipt = merger.merge_runs(
        base_dir,
        overlay_dir,
        output_dir,
        allow_incomplete_base=True,
    )

    assert receipt["total_cases"] == 2
    assert receipt["source_base_manifest_cases"] == 3
    assert receipt["source_base_successful_cases"] == 2
    assert receipt["updated_from_overlay"] == 1
    assert receipt["retained_from_base"] == 1
    merged_results = [
        json.loads(line)
        for line in (output_dir / "results.jsonl").read_text(encoding="utf-8").splitlines()
    ]
    assert [row["case_id"] for row in merged_results] == ["RF-A", "RF-B"]
    assert [row["verdict"] for row in merged_results] == ["expert_correct", "expert_correct"]
    summary = json.loads((output_dir / "summary.json").read_text(encoding="utf-8"))
    assert summary["selected_cases"] == 2
    assert summary["completed"] == 2
    assert summary["remaining"] == 0
    assert summary["merge"]["base_results_missing"] == 1
    assert summary["merge"]["incomplete_base_explicitly_allowed"] is True
    inventory = json.loads((output_dir / "inventory.json").read_text(encoding="utf-8"))
    assert inventory["period"] == "2026-07"
    assert inventory["timezone"] == "Europe/Moscow"
    assert inventory["filters"] == {"reviewers": ["Кульдяев Ф. С."]}
    assert inventory["counts"]["selected_cases"] == 2
    assert inventory["counts"]["source_base_manifest_cases"] == 3
