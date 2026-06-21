from __future__ import annotations

import json
from pathlib import Path

from backend.app.pipeline.stages.gemma_enrichment.gemma_enrichment_contract import (
    GEMMA_BASE_CROP_POLICY,
    build_gemma_summary,
    gemma_blocks_dir,
    gemma_output_root,
    utc_now_iso,
    validate_gemma_summary,
)

_WMODE = "AUDIT_PROJECTS_V2_WRITE_MODE"


def _write(path: Path, data: str = "x") -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(data, encoding="utf-8")
    return path


def test_gemma_output_root_switches_to_latest_for_v2_primary(monkeypatch, tmp_path):
    version_dir = tmp_path / "versions" / "v001"
    (version_dir / "02_work").mkdir(parents=True)
    monkeypatch.setenv(_WMODE, "projects_v2_primary")

    assert gemma_output_root(version_dir) == version_dir / "03_analysis" / "latest"
    assert gemma_blocks_dir(version_dir) == version_dir / "03_analysis" / "latest" / "blocks_gemma_100"


def test_validate_gemma_summary_reads_from_v2_latest(monkeypatch, tmp_path):
    version_dir = tmp_path / "versions" / "v001"
    md_path = _write(version_dir / "02_work" / "document.md", "md")
    monkeypatch.setenv(_WMODE, "projects_v2_primary")
    index_path = version_dir / "03_analysis" / "latest" / "blocks_gemma_100" / "index.json"
    index_path.parent.mkdir(parents=True)
    index_payload = {**GEMMA_BASE_CROP_POLICY, "blocks": []}
    index_path.write_text(json.dumps(index_payload, ensure_ascii=False), encoding="utf-8")

    summary = build_gemma_summary(
        status="no_blocks",
        project_dir=version_dir,
        md_path=md_path,
        model="gemma-test",
        blocks_total=0,
        blocks_ok=0,
        blocks_failed=0,
        blocks_skipped=0,
        extra={
            "blocks": [],
            "timestamp": utc_now_iso(),
            "uncovered_block_ids": [],
            "uncovered_blocks": [],
            "high_detail_blocks_index_hash": None,
        },
    )
    summary_path = version_dir / "03_analysis" / "latest" / "gemma_enrichment_summary.json"
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(json.dumps(summary, ensure_ascii=False), encoding="utf-8")

    result = validate_gemma_summary(version_dir, md_path=md_path)

    assert result["valid"] is True
    assert result["summary"]["model"] == "gemma-test"
