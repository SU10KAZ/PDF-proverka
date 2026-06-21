"""B6: production read-path cutover contract tests for projects_v2."""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

_STORAGE = "AUDIT_STORAGE_BACKEND"
_READ = "AUDIT_PROJECTS_V2_READ_DEFAULT_ENABLED"
_V2DIR = "AUDIT_PROJECTS_V2_DIR"


def _write_json(path: Path, data) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")


def _make_v2_project(v2: Path, code: str = "DOC-B6") -> Path:
    doc = v2 / "objects" / "OBJ_B6" / "disciplines" / "KJ" / "documents" / code
    _write_json(v2 / "objects" / "OBJ_B6" / "object.json", {
        "object_id": "obj-b6",
        "display_name": "Object B6",
        "folder_name": "OBJ_B6",
    })
    _write_json(doc / "document.json", {
        "schema_version": 1,
        "document_code": code,
        "object_id": "obj-b6",
        "discipline": "KJ",
        "versions": [{"version_id": "v001", "version_no": 1, "label": "V1"}],
        "current_version": "v001",
    })
    (doc / "current_version.txt").write_text("v001\n", encoding="utf-8")
    vdir = doc / "versions" / "v001"
    _write_json(vdir / "version.json", {
        "version_id": "v001",
        "version_no": 1,
        "label": "V1",
        "analysis_status": "complete",
        "project_info": {
            "name": "DOC B6",
            "description": "shape contract",
            "section": "KJ",
            "pipeline_version": "ocr",
        },
    })
    (vdir / "01_input").mkdir(parents=True, exist_ok=True)
    (vdir / "01_input" / f"{code}.pdf").write_bytes(b"%PDF-b6")
    (vdir / "01_input" / f"{code}_document.md").write_text("# md", encoding="utf-8")
    (vdir / "02_work").mkdir(parents=True, exist_ok=True)
    (vdir / "02_work" / "document.md").write_text("# normalized", encoding="utf-8")
    latest = vdir / "03_analysis" / "latest"
    _write_json(latest / "01_text_analysis.json", {"ok": True})
    _write_json(latest / "02_blocks_analysis.json", {"blocks": []})
    _write_json(latest / "03_findings.json", {
        "audit_date": "2026-06-21",
        "findings": [
            {"id": "F-001", "severity": "КРИТИЧЕСКОЕ", "category": "ЭОМ", "sheet": "Лист 1", "problem": "A"},
            {"id": "F-002", "severity": "РЕКОМЕНДАТЕЛЬНОЕ", "category": "КЖ", "sheet": "Лист 2", "problem": "B"},
        ],
    })
    _write_json(latest / "optimization.json", {
        "meta": {"total_items": 1, "estimated_savings_pct": 12.5, "top3_summary": "save"},
        "items": [{"id": "O-001", "type": "cable", "savings_pct": 12.5}],
    })
    _write_json(latest / "optimization_review.json", {
        "meta": {"total_reviewed": 1, "verdicts": {"pass": 1}},
    })
    _write_json(latest / "pipeline_log.json", {
        "stages": {
            "text_analysis": {"status": "done"},
            "block_analysis": {"status": "done"},
            "findings_merge": {"status": "done"},
            "optimization": {"status": "done"},
        }
    })
    return doc


def _enable_v2(monkeypatch, v2: Path) -> None:
    monkeypatch.setenv(_STORAGE, "projects_v2")
    monkeypatch.setenv(_READ, "true")
    monkeypatch.setenv(_V2DIR, str(v2))


def test_production_uses_v2_flag(monkeypatch):
    from backend.app.services.storage.storage_read_facade import production_uses_v2

    monkeypatch.delenv(_STORAGE, raising=False)
    monkeypatch.delenv(_READ, raising=False)
    assert production_uses_v2() is False
    monkeypatch.setenv(_STORAGE, "projects_v2")
    monkeypatch.setenv(_READ, "false")
    assert production_uses_v2() is False
    monkeypatch.setenv(_STORAGE, "legacy")
    monkeypatch.setenv(_READ, "true")
    assert production_uses_v2() is False
    monkeypatch.setenv(_STORAGE, "projects_v2")
    monkeypatch.setenv(_READ, "true")
    assert production_uses_v2() is True


def test_adapter_findings_contract_and_by_id(monkeypatch, tmp_path):
    from backend.app.services.findings import findings_service

    v2 = tmp_path / "projects_v2"
    _make_v2_project(v2)
    _enable_v2(monkeypatch, v2)

    response = findings_service.get_findings("DOC-B6", severity="крит")
    payload = response.model_dump()

    assert set(payload) == {"project_id", "total", "filtered_total", "by_severity", "findings", "audit_date"}
    assert "documents" not in payload and "count" not in payload
    assert payload["project_id"] == "DOC-B6"
    assert payload["total"] == 2
    assert payload["filtered_total"] == 1
    assert payload["by_severity"] == {"КРИТИЧЕСКОЕ": 1, "РЕКОМЕНДАТЕЛЬНОЕ": 1}
    assert payload["findings"][0]["id"] == "F-001"
    assert findings_service.get_finding_by_id("DOC-B6", "F-002")["problem"] == "B"


def test_adapter_project_status_contract(monkeypatch, tmp_path):
    from backend.app.models.project import ProjectStatus
    from backend.app.services.common import project_service

    v2 = tmp_path / "projects_v2"
    _make_v2_project(v2)
    _enable_v2(monkeypatch, v2)

    status = project_service.get_project_status("DOC-B6")
    projects = project_service.list_projects()

    assert isinstance(status, ProjectStatus)
    assert status.project_id == "DOC-B6"
    assert status.version_id == "v001"
    assert status.has_pdf is True
    assert status.has_md_file is True
    assert status.findings_count == 2
    assert status.findings_by_severity["КРИТИЧЕСКОЕ"] == 1
    assert status.optimization_count == 1
    assert status.pipeline.text_analysis == "done"
    assert status.pipeline.findings == "done"
    assert [p.project_id for p in projects] == ["DOC-B6"]
    assert set(status.model_dump()).issuperset({"project_id", "pipeline", "findings_count", "optimization_count"})


@pytest.mark.asyncio
async def test_optimization_payload_and_status_contract(monkeypatch, tmp_path):
    from backend.app.api.routers import optimization

    v2 = tmp_path / "projects_v2"
    _make_v2_project(v2)
    _enable_v2(monkeypatch, v2)

    payload = await optimization.get_optimization("DOC-B6")
    status = await optimization.get_optimization_status("DOC-B6")

    assert set(payload) == {"project_id", "version_id", "has_data", "data"}
    assert payload["has_data"] is True
    assert payload["version_id"] == "v001"
    assert payload["data"]["items"][0]["id"] == "O-001"
    assert set(status) == {"project_id", "version_id", "pipeline_status", "is_running", "has_results"}
    assert status["pipeline_status"] == "done"
    assert status["has_results"] is True


def test_production_read_fallback_to_legacy(monkeypatch, tmp_path):
    from backend.app.services.findings import findings_service

    v2 = tmp_path / "empty_projects_v2"
    (v2 / "objects").mkdir(parents=True)
    legacy_output = tmp_path / "legacy" / "_output"
    _write_json(legacy_output / "03_findings.json", {
        "findings": [{"id": "L-001", "severity": "LEGACY", "problem": "fallback"}],
    })
    _enable_v2(monkeypatch, v2)
    monkeypatch.setattr(findings_service, "_get_version_output_dir", lambda project_id, version_id=None: legacy_output)

    response = findings_service.get_findings("legacy-doc")

    assert response.project_id == "legacy-doc"
    assert response.total == 1
    assert response.findings[0]["id"] == "L-001"
