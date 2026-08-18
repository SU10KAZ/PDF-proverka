"""Привязка проекта как версии в projects_v2-primary.

Регресс живого инцидента 18.08.2026: окно «Изменить выбранные проекты» →
«Привязать выбранные пары» отвечало 404 «Source проект '<...>' не найден» для
любой пары. Причина — вся ветка `merge_project_as_version` работала только с
legacy-раскладкой `projects/<объект>/<id>`, которой после cutover на
projects_v2 на диске нет вовсе.

Отличие v2-ветки от legacy: артефакты аудита source не отбрасываются, а
переезжают в новую версию target (решение оператора 18.08.2026).
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

_PDF_BYTES = b"%PDF-1.4\n%v2-merge\n%%EOF\n"
_MD_BYTES = "## СТРАНИЦА 1\n\n### [TEXT b1]\nSource MD.\n".encode("utf-8")


def _set_v2_env(monkeypatch, v2_root: Path) -> None:
    monkeypatch.setenv("AUDIT_STORAGE_BACKEND", "projects_v2")
    monkeypatch.setenv("AUDIT_PROJECTS_V2_WRITE_MODE", "projects_v2_primary")
    monkeypatch.setenv("AUDIT_PROJECTS_V2_READ_DEFAULT_ENABLED", "true")
    monkeypatch.setenv("AUDIT_PROJECTS_V2_DIR", str(v2_root))


def _reset_project_cache(monkeypatch, legacy_root: Path) -> None:
    import backend.app.services.common.project_service as ps

    legacy_root.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(ps, "_get_projects_dir", lambda: legacy_root)
    monkeypatch.setattr(ps, "_PROJECT_DIRS_CACHE", [])
    monkeypatch.setattr(ps, "_PROJECT_DIRS_CACHE_TIME", 0.0)
    monkeypatch.setattr(ps, "_document_cache", {})


def _make_v2_doc(
    v2_root: Path,
    doc_code: str,
    *,
    discipline: str = "AR",
    with_sources: bool = False,
    with_artifacts: bool = False,
) -> Path:
    doc_dir = (
        v2_root / "objects" / "OBJ" / "disciplines" / discipline / "documents" / doc_code
    )
    version_dir = doc_dir / "versions" / "v001"
    for subdir in (
        version_dir / "01_input",
        version_dir / "02_work",
        version_dir / "03_analysis" / "latest",
        version_dir / "04_review",
        version_dir / "05_export",
    ):
        subdir.mkdir(parents=True, exist_ok=True)
    (doc_dir / "document.json").write_text(json.dumps({
        "schema_version": 1,
        "document_code": doc_code,
        "object_folder": "OBJ",
        "discipline": discipline,
        "current_version": "v001",
        "version_ids": ["v001"],
        "versions": [{
            "version_id": "v001",
            "version_no": 1,
            "label": "V1",
            "status": "source_only",
            "source": "test",
        }],
    }, ensure_ascii=False, indent=2), encoding="utf-8")
    (doc_dir / "current_version.txt").write_text("v001", encoding="utf-8")

    info = {
        "project_id": doc_code,
        "document_code": doc_code,
        "name": doc_code,
        "section": discipline,
        "pdf_file": "",
        "pdf_files": [],
        "md_files": [],
        "version_id": "v001",
    }
    if with_sources:
        (version_dir / "01_input" / f"{doc_code}.pdf").write_bytes(_PDF_BYTES)
        (version_dir / "01_input" / f"{doc_code}_document.md").write_bytes(_MD_BYTES)
        (version_dir / "01_input" / "input_manifest.json").write_text("{}", encoding="utf-8")
        (version_dir / "02_work" / "document.pdf").write_bytes(_PDF_BYTES)
        info["pdf_file"] = f"{doc_code}.pdf"
        info["pdf_files"] = [f"{doc_code}.pdf"]
        info["md_files"] = [f"{doc_code}_document.md"]
        info["md_file"] = f"{doc_code}_document.md"
    (version_dir / "01_input" / "project_info.json").write_text(
        json.dumps(info, ensure_ascii=False, indent=2), encoding="utf-8",
    )

    version_json = {
        "schema_version": 1,
        "version_id": "v001",
        "version_no": 1,
        "label": "V1",
        "analysis_status": "source_only",
        "project_info": info,
    }
    if with_artifacts:
        (version_dir / "03_analysis" / "latest" / "03_findings.json").write_text(
            json.dumps({"findings": [{"id": "F-001", "problem": "тест"}]}, ensure_ascii=False),
            encoding="utf-8",
        )
        (version_dir / "04_review" / "03_findings_review.json").write_text(
            json.dumps({"verdicts": []}, ensure_ascii=False), encoding="utf-8",
        )
        version_json["analysis_status"] = "complete"
        version_json["analysis_run_id"] = "run_test"
    (version_dir / "version.json").write_text(
        json.dumps(version_json, ensure_ascii=False, indent=2), encoding="utf-8",
    )
    return doc_dir


def test_merge_v2_primary_creates_version_and_carries_artifacts(monkeypatch, tmp_path):
    v2_root = tmp_path / "projects_v2"
    tgt_dir = _make_v2_doc(v2_root, "DOC-TGT", with_sources=True)
    src_dir = _make_v2_doc(v2_root, "DOC-TGT_V2", with_sources=True, with_artifacts=True)
    _set_v2_env(monkeypatch, v2_root)
    _reset_project_cache(monkeypatch, tmp_path / "legacy_projects")

    from backend.app.services.common import version_service

    result = version_service.merge_project_as_version(
        "DOC-TGT_V2", "DOC-TGT", comment="привязка", source="edit_projects_modal",
    )

    assert result["status"] == "ok"
    assert result["storage_layout"] == "projects_v2"
    assert result["version_id"] == "v002"

    v002 = tgt_dir / "versions" / "v002"
    # исходники source переехали в новую версию target
    assert (v002 / "01_input" / "DOC-TGT_V2.pdf").read_bytes() == _PDF_BYTES
    assert (v002 / "01_input" / "DOC-TGT_V2_document.md").read_bytes() == _MD_BYTES
    assert (v002 / "02_work" / "document.pdf").read_bytes() == _PDF_BYTES
    # служебный манифест входа source не тащим
    assert not (v002 / "01_input" / "input_manifest.json").exists()

    # артефакты аудита source не потеряны
    findings = json.loads(
        (v002 / "03_analysis" / "latest" / "03_findings.json").read_text(encoding="utf-8")
    )
    assert findings["findings"][0]["id"] == "F-001"
    assert (v002 / "04_review" / "03_findings_review.json").is_file()
    assert "03_analysis" in result["carried_artifacts"]

    # статус анализа перенесён, иначе карточка покажет «аудит не запускался»
    vj = json.loads((v002 / "version.json").read_text(encoding="utf-8"))
    assert vj["analysis_status"] == "complete"
    assert vj["analysis_run_id"] == "run_test"
    assert vj["merged_from_project_id"] == "DOC-TGT_V2"

    # V1 target не тронут, документ source удалён
    assert (tgt_dir / "versions" / "v001" / "01_input" / "DOC-TGT.pdf").is_file()
    assert not src_dir.exists()

    doc_json = json.loads((tgt_dir / "document.json").read_text(encoding="utf-8"))
    assert doc_json["current_version"] == "v002"
    assert [v["version_id"] for v in doc_json["versions"]] == ["v001", "v002"]


def test_merge_v2_primary_keeps_source_when_delete_disabled(monkeypatch, tmp_path):
    v2_root = tmp_path / "projects_v2"
    _make_v2_doc(v2_root, "KEEP-TGT", with_sources=True)
    src_dir = _make_v2_doc(v2_root, "KEEP-SRC", with_sources=True)
    _set_v2_env(monkeypatch, v2_root)
    _reset_project_cache(monkeypatch, tmp_path / "legacy_projects")

    from backend.app.services.common import version_service

    version_service.merge_project_as_version(
        "KEEP-SRC", "KEEP-TGT", delete_source=False,
    )
    assert src_dir.exists()


def test_merge_v2_primary_rejects_cross_section(monkeypatch, tmp_path):
    v2_root = tmp_path / "projects_v2"
    _make_v2_doc(v2_root, "SEC-TGT", discipline="AR", with_sources=True)
    _make_v2_doc(v2_root, "SEC-SRC", discipline="EOM", with_sources=True)
    _set_v2_env(monkeypatch, v2_root)
    _reset_project_cache(monkeypatch, tmp_path / "legacy_projects")

    from backend.app.services.common import version_service

    with pytest.raises(ValueError, match="Раздел source"):
        version_service.merge_project_as_version("SEC-SRC", "SEC-TGT")


def test_merge_v2_primary_requires_pdf(monkeypatch, tmp_path):
    v2_root = tmp_path / "projects_v2"
    _make_v2_doc(v2_root, "NOPDF-TGT", with_sources=True)
    _make_v2_doc(v2_root, "NOPDF-SRC", with_sources=False)
    _set_v2_env(monkeypatch, v2_root)
    _reset_project_cache(monkeypatch, tmp_path / "legacy_projects")

    from backend.app.services.common import version_service

    with pytest.raises(ValueError, match="нет PDF"):
        version_service.merge_project_as_version("NOPDF-SRC", "NOPDF-TGT")


def test_flat_from_project_endpoint_v2_primary(monkeypatch, tmp_path):
    """Живой сценарий кнопки «Привязать выбранные пары»: было 404, стало 200."""
    v2_root = tmp_path / "projects_v2"
    tgt_dir = _make_v2_doc(v2_root, "API-TGT", with_sources=True)
    _make_v2_doc(v2_root, "API-TGT_V2", with_sources=True)
    _set_v2_env(monkeypatch, v2_root)
    _reset_project_cache(monkeypatch, tmp_path / "legacy_projects")

    from backend.app.main import app

    client = TestClient(app)
    resp = client.post("/api/projects/versions/from-project", json={
        "target_project_id": "API-TGT",
        "source_project_id": "API-TGT_V2",
        "comment": "Привязано из окна Изменить выбранные проекты",
        "source": "edit_projects_modal",
        "delete_source": True,
        "discard_source_output": False,
    })

    assert resp.status_code == 200, resp.text
    assert resp.json()["version_id"] == "v002"
    assert (tgt_dir / "versions" / "v002" / "01_input" / "API-TGT_V2.pdf").is_file()


def test_flat_from_project_endpoint_404_when_document_absent(monkeypatch, tmp_path):
    """404 остаётся 404, когда документа действительно нет ни в v2, ни в legacy."""
    v2_root = tmp_path / "projects_v2"
    _make_v2_doc(v2_root, "ONLY-TGT", with_sources=True)
    _set_v2_env(monkeypatch, v2_root)
    _reset_project_cache(monkeypatch, tmp_path / "legacy_projects")

    from backend.app.main import app

    client = TestClient(app)
    resp = client.post("/api/projects/versions/from-project", json={
        "target_project_id": "ONLY-TGT",
        "source_project_id": "GHOST-SRC",
    })
    assert resp.status_code == 404
    assert "GHOST-SRC" in resp.text
