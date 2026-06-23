from __future__ import annotations

import json
from pathlib import Path

import pytest

from backend.app.services.common import project_service

_WMODE = "AUDIT_PROJECTS_V2_WRITE_MODE"
_V2DIR = "AUDIT_PROJECTS_V2_DIR"


def _make_v2_doc(v2_root: Path, code: str = "DOC-CLEAN") -> Path:
    doc = v2_root / "objects" / "OBJ" / "disciplines" / "EOM" / "documents" / code
    doc.mkdir(parents=True, exist_ok=True)
    (doc / "document.json").write_text(json.dumps({
        "schema_version": 1,
        "document_code": code,
        "object_id": "obj-clean",
        "current_version": "v001",
        "versions": [{"version_id": "v001", "version_no": 1}],
    }), encoding="utf-8")
    vdir = doc / "versions" / "v001"
    (vdir / "01_input").mkdir(parents=True, exist_ok=True)
    (vdir / "03_analysis" / "latest" / "blocks").mkdir(parents=True, exist_ok=True)
    (vdir / "03_analysis" / "runs" / "run-1").mkdir(parents=True, exist_ok=True)
    (vdir / "01_input" / f"{code}.pdf").write_bytes(b"%PDF-1.4 clean")
    (vdir / "03_analysis" / "latest" / "03_findings.json").write_text(
        json.dumps({"findings": [{"id": "F-1"}]}), encoding="utf-8",
    )
    (vdir / "03_analysis" / "latest" / "blocks" / "index.json").write_text(
        json.dumps({"blocks": [1]}), encoding="utf-8",
    )
    (vdir / "03_analysis" / "runs" / "run-1" / "pipeline_log.json").write_text(
        json.dumps({"status": "done"}), encoding="utf-8",
    )
    (vdir / "version.json").write_text(json.dumps({
        "version_id": "v001",
        "project_info": {
            "name": code,
            "tile_config_source": "auto",
            "tile_config": {"old": True},
        },
    }), encoding="utf-8")
    return vdir


def test_clean_project_data_v2_primary_requires_confirmation(monkeypatch, tmp_path):
    v2 = tmp_path / "projects_v2"
    vdir = _make_v2_doc(v2)
    monkeypatch.setenv(_WMODE, "projects_v2_primary")
    monkeypatch.setenv(_V2DIR, str(v2))

    with pytest.raises(ValueError, match="_confirmed"):
        project_service.clean_project_data("DOC-CLEAN")

    assert (vdir / "03_analysis" / "latest" / "03_findings.json").exists()
    assert not (v2 / "_system" / "destructive_confirmations.jsonl").exists()


def test_clean_project_data_v2_primary_backs_up_logs_and_cleans(monkeypatch, tmp_path):
    v2 = tmp_path / "projects_v2"
    vdir = _make_v2_doc(v2)
    monkeypatch.setenv(_WMODE, "projects_v2_primary")
    monkeypatch.setenv(_V2DIR, str(v2))

    result = project_service.clean_project_data("DOC-CLEAN", _confirmed=True)

    assert result["backup_id"]
    backup_dir = v2 / "_system" / "destructive_backups" / result["backup_id"]
    assert (backup_dir / "03_analysis" / "latest" / "03_findings.json").exists()
    assert (vdir / "01_input" / "DOC-CLEAN.pdf").exists()
    assert (vdir / "03_analysis" / "latest").is_dir()
    assert not any((vdir / "03_analysis" / "latest").iterdir())
    assert not (vdir / "03_analysis" / "runs").exists()

    log = (v2 / "_system" / "destructive_confirmations.jsonl").read_text(encoding="utf-8").splitlines()
    assert len(log) == 1
    row = json.loads(log[0])
    assert row["op"] == "clean_project_data"
    assert row["backup_id"] == result["backup_id"]
    assert row["document_code"] == "DOC-CLEAN"

    vj = json.loads((vdir / "version.json").read_text(encoding="utf-8"))
    assert "tile_config_source" not in vj["project_info"]
    assert vj["project_info"]["tile_config"] == {}
