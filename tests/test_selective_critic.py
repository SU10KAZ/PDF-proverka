"""Тесты для Selective Critic фильтрации."""
import json
import pytest
from pathlib import Path
from unittest.mock import patch

from webapp.services.pipeline_service import PipelineManager


def _write_findings(tmp_path: Path, findings: list[dict]):
    output_dir = tmp_path / "_output"
    output_dir.mkdir(parents=True, exist_ok=True)
    data = {"findings": findings}
    (output_dir / "03_findings.json").write_text(
        json.dumps(data, ensure_ascii=False), encoding="utf-8"
    )
    return output_dir


def test_all_well_grounded(tmp_path):
    findings = [
        {
            "id": "F-001",
            "evidence": [{"type": "image", "block_id": "b1"}],
            "related_block_ids": ["b1"],
            "norm_confidence": 0.95,
        },
        {
            "id": "F-002",
            "evidence": [{"type": "image", "block_id": "b2"}],
            "related_block_ids": ["b2"],
            "norm_confidence": 0.9,
        },
    ]
    output_dir = _write_findings(tmp_path, findings)
    result = PipelineManager._build_selective_review_input(output_dir)
    assert result["risky"] == 0
    assert result["skipped"] == 2
    assert result["total"] == 2


def test_no_evidence_is_risky(tmp_path):
    findings = [
        {
            "id": "F-001",
            "evidence": [],
            "related_block_ids": [],
        },
    ]
    output_dir = _write_findings(tmp_path, findings)
    result = PipelineManager._build_selective_review_input(output_dir)
    assert result["risky"] == 1
    assert "F-001" in result["risky_ids"]


def test_low_confidence_is_risky(tmp_path):
    findings = [
        {
            "id": "F-001",
            "evidence": [{"type": "image", "block_id": "b1"}],
            "related_block_ids": ["b1"],
            "norm_confidence": 0.5,
        },
    ]
    output_dir = _write_findings(tmp_path, findings)
    result = PipelineManager._build_selective_review_input(output_dir)
    assert result["risky"] == 1


def test_grounding_only_evidence_is_risky(tmp_path):
    """Evidence from grounding_service only (not original) → risky."""
    findings = [
        {
            "id": "F-001",
            "evidence": [{"type": "image", "block_id": "b1", "source": "grounding_service"}],
            "related_block_ids": [],
        },
    ]
    output_dir = _write_findings(tmp_path, findings)
    result = PipelineManager._build_selective_review_input(output_dir)
    assert result["risky"] == 1


def test_review_input_file_created(tmp_path):
    findings = [
        {"id": "F-001", "evidence": [], "related_block_ids": []},
        {"id": "F-002", "evidence": [{"type": "image", "block_id": "b1"}], "related_block_ids": ["b1"]},
    ]
    output_dir = _write_findings(tmp_path, findings)
    PipelineManager._build_selective_review_input(output_dir)

    input_path = output_dir / "03_findings_review_input.json"
    assert input_path.exists()

    data = json.loads(input_path.read_text(encoding="utf-8"))
    assert data["meta"]["risky_count"] == 1
    assert len(data["findings"]) == 1
    assert data["findings"][0]["id"] == "F-001"


def test_missing_findings_file(tmp_path):
    output_dir = tmp_path / "_output"
    output_dir.mkdir(parents=True, exist_ok=True)
    result = PipelineManager._build_selective_review_input(output_dir)
    assert result["total"] == 0
    assert result["risky"] == 0
