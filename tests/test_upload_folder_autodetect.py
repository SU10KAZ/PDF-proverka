"""Тесты авто-определения дисциплины + предложения версии + upload как версия
для browser folder upload.

Покрывает:
  - detect_discipline_detailed: folder_name / pdf_name / document_text / fallback;
  - precheck: detected_discipline + source; явная дисциплина имеет приоритет;
  - precheck: точное совпадение норм. имени → suggested_target + version label;
  - precheck: нет совпадения → suggested_target=None (new_project по умолчанию);
  - upload как новая версия: candidate_files (ocr в extra), expected_section,
    re-mirror target; маппинг ошибок (conflict/section/not-found); no/multi PDF.

Run:
    python -m pytest tests/test_upload_folder_autodetect.py -v
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from backend.app.services.common import (  # noqa: E402
    object_service, project_service, discipline_service, version_service,
)
import backend.app.services.storage.storage_write_facade as swf  # noqa: E402

_PDF = b"%PDF-1.4\n%a\n%%EOF\n"
_PDF2 = b"%PDF-1.4\n%b-diff\n%%EOF\n"
_MD = "## STR\nфасад кладка фасад\n".encode("utf-8")
_OCR = b"<html>ocr</html>"

_FAKE_REGISTRY = {"disciplines": {
    "AR":  {"folder_patterns": ["АР"], "text_keywords": ["фасад", "кладка"]},
    "EOM": {"folder_patterns": ["ЭМ", "ЭО"], "text_keywords": ["кабель", "щит"]},
}}


@pytest.fixture(autouse=True)
def fake_registry(monkeypatch):
    monkeypatch.setattr(discipline_service, "_load_registry", lambda: _FAKE_REGISTRY)


@pytest.fixture
def env(tmp_path, monkeypatch):
    projects_dir = tmp_path / "projects" / "OBJ"
    projects_dir.mkdir(parents=True)
    obj = {"id": "obj-1", "name": "Объект 1", "projects_dir": str(projects_dir)}
    monkeypatch.setattr(object_service, "get_object_by_id",
                        lambda oid: obj if oid == "obj-1" else None)
    monkeypatch.setattr(object_service, "get_projects_dir_for",
                        lambda oid: projects_dir if oid == "obj-1" else None)
    monkeypatch.setattr(project_service, "_v2_document_exists", lambda *a, **k: False)
    monkeypatch.setattr(swf, "shadow_mirror_project_path_safe", lambda p: None)
    return projects_dir


# ─── detect_discipline_detailed ──────────────────────────────────────────────


def test_detect_by_folder_name():
    d = discipline_service.detect_discipline_detailed("133-АР1", "x.pdf", "")
    assert d["code"] == "AR" and d["source"] == "folder_name"


def test_detect_by_pdf_name():
    d = discipline_service.detect_discipline_detailed("zzz", "doc-ЭМ-1.pdf", "")
    assert d["code"] == "EOM" and d["source"] == "pdf_name"


def test_detect_by_document_text():
    d = discipline_service.detect_discipline_detailed("zzz", "zzz.pdf", "тут кабель и щит рядом")
    assert d["code"] == "EOM" and d["source"] == "document_text"


def test_detect_fallback():
    d = discipline_service.detect_discipline_detailed("zzz", "yyy.pdf", "ничего")
    assert d["code"] == "EOM" and d["source"] == "fallback"


# ─── precheck: auto discipline + suggestion ──────────────────────────────────


def _pre(env, name, files, discipline=None, folder=None):
    return project_service.precheck_uploaded_project_folder(
        object_id="obj-1", discipline=discipline, project_name=name,
        files=files, folder_name=folder)


def test_precheck_autodetects_discipline_from_folder(env):
    v = _pre(env, "NEWPROJ", [("doc.pdf", _PDF)], folder="папка-АР-7")
    assert v["detected_discipline"] == "AR"
    assert v["discipline_source"] == "folder_name"
    assert v["discipline"] == "AR"            # effective = detected
    assert v["discipline_was_provided"] is False


def test_precheck_explicit_discipline_wins(env):
    v = _pre(env, "NEWPROJ", [("doc.pdf", _PDF)], discipline="EOM", folder="папка-АР-7")
    assert v["detected_discipline"] == "AR"   # всё равно сообщаем
    assert v["discipline"] == "EOM"           # но эффективная — заданная
    assert v["discipline_was_provided"] is True


def test_precheck_suggests_version_on_name_match(env):
    # существующий проект в AR
    project_service.save_uploaded_project_folder(
        object_id="obj-1", discipline="AR", project_name="13АВ-АР-X",
        files=[("doc.pdf", _PDF)])
    # та же норм. форма имени → предложение версии
    v = _pre(env, "13АВ-АР-X (1)", [("doc.pdf", _PDF2)], discipline="AR")
    assert v["suggested_target_project"] == "AR/13АВ-АР-X"
    assert v["suggested_version_label"]            # «V2» и т.п.
    assert any(w["code"] == "similar_name" for w in v["warnings"])


def test_precheck_no_match_no_suggestion(env):
    v = _pre(env, "UNIQUE-NAME-123", [("doc.pdf", _PDF)], discipline="AR")
    assert v["suggested_target_project"] is None
    assert v["status"] == "ready"


# ─── upload as new version (version_service mocked) ───────────────────────────


@pytest.fixture
def mock_vs(monkeypatch):
    calls = {}
    def fake_create(target, candidate_files, *, expected_section=None, comment=None,
                    source=None, allowed_roots=None, resolve_project_dir_fn=None):
        # файлы должны реально существовать во временной папке на момент вызова
        assert Path(candidate_files["pdf"]).exists()
        calls["target"] = target
        calls["candidate_files"] = candidate_files
        calls["expected_section"] = expected_section
        return {"version": {"version_id": "v002", "label": "V2"},
                "versions_summary": {"version_count": 2}, "saved": ["doc.pdf"], "warnings": []}
    monkeypatch.setattr(version_service, "create_version_from_existing_files", fake_create)
    mirrors = []
    monkeypatch.setattr(swf, "shadow_mirror_project_id_safe", lambda pid, **k: mirrors.append(pid))
    return calls, mirrors


def test_upload_new_version_maps_files_and_remirrors(env, mock_vs):
    calls, mirrors = mock_vs
    res = project_service.save_uploaded_project_folder(
        object_id="obj-1", discipline="AR", project_name="src-name",
        upload_mode="new_version", target_project_id="AR/TARGET",
        files=[("doc.pdf", _PDF), ("doc_document.md", _MD), ("doc_ocr.html", _OCR)])
    assert res["mode"] == "new_version"
    assert res["project_id"] == "AR/TARGET"
    assert calls["target"] == "AR/TARGET"
    assert calls["expected_section"] == "AR"
    # ocr.html уехал в extra (сохраняется в версии)
    assert any(p.endswith("doc_ocr.html") for p in calls["candidate_files"]["extra"])
    assert calls["candidate_files"]["md"].endswith("doc_document.md")
    assert res["has_ocr"] is True
    assert mirrors == ["AR/TARGET"]            # target пере-зеркалирован


def test_upload_new_version_section_mismatch_rejected(env, monkeypatch):
    def fake(*a, **k):
        raise ValueError("Раздел target проекта 'OV' не совпадает с ожидаемым 'AR'")
    monkeypatch.setattr(version_service, "create_version_from_existing_files", fake)
    monkeypatch.setattr(swf, "shadow_mirror_project_id_safe", lambda *a, **k: None)
    with pytest.raises(project_service.UploadFolderError, match="не совпадает"):
        project_service.save_uploaded_project_folder(
            object_id="obj-1", discipline="AR", project_name="x",
            upload_mode="new_version", target_project_id="OV/OTHER",
            files=[("doc.pdf", _PDF)])


def test_upload_new_version_conflict_maps_to_conflict(env, monkeypatch):
    def fake(*a, **k):
        raise version_service.VersionFileConflictError("файл уже есть")
    monkeypatch.setattr(version_service, "create_version_from_existing_files", fake)
    monkeypatch.setattr(swf, "shadow_mirror_project_id_safe", lambda *a, **k: None)
    with pytest.raises(project_service.UploadFolderConflict):
        project_service.save_uploaded_project_folder(
            object_id="obj-1", discipline="AR", project_name="x",
            upload_mode="new_version", target_project_id="AR/T",
            files=[("doc.pdf", _PDF)])


def test_upload_new_version_target_not_found_propagates(env, monkeypatch):
    def fake(*a, **k):
        raise FileNotFoundError("target не найден")
    monkeypatch.setattr(version_service, "create_version_from_existing_files", fake)
    monkeypatch.setattr(swf, "shadow_mirror_project_id_safe", lambda *a, **k: None)
    with pytest.raises(FileNotFoundError):   # endpoint → 404
        project_service.save_uploaded_project_folder(
            object_id="obj-1", discipline="AR", project_name="x",
            upload_mode="new_version", target_project_id="AR/GHOST",
            files=[("doc.pdf", _PDF)])


def test_upload_new_version_requires_pdf(env, monkeypatch):
    monkeypatch.setattr(version_service, "create_version_from_existing_files",
                        lambda *a, **k: pytest.fail("не должен вызываться без PDF"))
    monkeypatch.setattr(swf, "shadow_mirror_project_id_safe", lambda *a, **k: None)
    with pytest.raises(project_service.UploadFolderError, match="не найден PDF"):
        project_service.save_uploaded_project_folder(
            object_id="obj-1", discipline="AR", project_name="x",
            upload_mode="new_version", target_project_id="AR/T",
            files=[("doc_document.md", _MD)])


def test_upload_new_version_multi_pdf_rejected(env, monkeypatch):
    monkeypatch.setattr(version_service, "create_version_from_existing_files",
                        lambda *a, **k: pytest.fail("не должен вызываться с 2 PDF"))
    monkeypatch.setattr(swf, "shadow_mirror_project_id_safe", lambda *a, **k: None)
    with pytest.raises(project_service.UploadFolderError, match="несколько PDF"):
        project_service.save_uploaded_project_folder(
            object_id="obj-1", discipline="AR", project_name="x",
            upload_mode="new_version", target_project_id="AR/T",
            files=[("a.pdf", _PDF), ("b.pdf", _PDF2)])
