"""Тесты duplicate-precheck и fingerprint при загрузке папки проекта.

Покрывает:
  - precheck нового проекта → ready;
  - дубль по project_name (legacy) → hard block (status=duplicate);
  - дубль по pdf checksum → warning;
  - дубль по bundle_fingerprint → hard block (status=duplicate);
  - похожее имя → warning;
  - фактическая загрузка повторяет проверку дубля (save → UploadFolderConflict);
  - fingerprint сохраняется в legacy project_info.json + input_manifest.json;
  - `*_ocr.html` учитывается в bundle_fingerprint (меняет bundle, не pdf_sha256);
  - HTTP /upload-folder/precheck возвращает verdict.

Run:
    python -m pytest tests/test_upload_folder_precheck.py -v
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

from backend.app.services.common import object_service, project_service  # noqa: E402
import backend.app.services.storage.storage_write_facade as swf  # noqa: E402

_PDF = b"%PDF-1.4\n%alpha\n%%EOF\n"
_PDF2 = b"%PDF-1.4\n%beta-different\n%%EOF\n"
_MD = b"## STR 1\n"
_RESULT = json.dumps({"pages": []}).encode()
_OCR = b"<html>ocr</html>"


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


def _bundle(pdf=_PDF, md=True, result=True, ocr=True):
    files = [("doc.pdf", pdf)]
    if md:
        files.append(("doc_document.md", _MD))
    if result:
        files.append(("doc_result.json", _RESULT))
    if ocr:
        files.append(("doc_ocr.html", _OCR))
    return files


def _save(name, files):
    return project_service.save_uploaded_project_folder(
        object_id="obj-1", discipline="EOM", project_name=name, files=files)


def _pre(name, files, discipline="EOM"):
    return project_service.precheck_uploaded_project_folder(
        object_id="obj-1", discipline=discipline, project_name=name, files=files)


# ─── precheck verdicts ───────────────────────────────────────────────────────


def test_precheck_new_project_ready(env):
    v = _pre("BRAND-NEW", _bundle())
    assert v["status"] == "ready"
    assert v["blocks"] == [] and v["warnings"] == []
    assert v["pdf_sha256"] and v["bundle_fingerprint"]
    assert v["has_md"] and v["has_result"] and v["has_ocr"]


def test_precheck_duplicate_by_name_hard_block(env):
    _save("DUPNAME", _bundle())
    v = _pre("DUPNAME", _bundle(pdf=_PDF2, md=False, result=False, ocr=False))
    assert v["status"] == "duplicate"
    assert any(b["code"] == "legacy_name_exists" for b in v["blocks"])


def test_precheck_duplicate_by_pdf_checksum_warning(env):
    _save("ORIG", _bundle())                       # хранит pdf_sha256
    # другой name, другой bundle (только pdf), но тот же pdf-контент
    v = _pre("OTHER", [("copy.pdf", _PDF)])
    assert v["status"] == "warning"
    assert any(w["code"] == "pdf_checksum_duplicate" for w in v["warnings"])
    assert not any(b["code"] == "bundle_exact_duplicate" for b in v["blocks"])


def test_precheck_duplicate_by_bundle_fingerprint_hard_block(env):
    _save("ORIG2", _bundle())
    # тот же комплект (имена+контент), другое имя проекта
    v = _pre("DIFFERENT-NAME", _bundle())
    assert v["status"] == "duplicate"
    assert any(b["code"] == "bundle_exact_duplicate" for b in v["blocks"])


def test_precheck_similar_name_warning(env):
    _save("13АВ-РД-X", _bundle())
    # «13АВ-РД-X (1)» нормализуется в то же → similar; другой pdf/bundle
    v = _pre("13АВ-РД-X (1)", _bundle(pdf=_PDF2, md=False, result=False, ocr=False))
    assert v["status"] == "warning"
    assert any(w["code"] == "similar_name" for w in v["warnings"])


def test_precheck_no_pdf_error(env):
    v = _pre("NOPDF", [("doc_document.md", _MD)])
    assert v["status"] == "error"
    assert any(b["code"] == "no_pdf" for b in v["blocks"])


def test_precheck_multiple_pdf_error(env):
    v = _pre("MULTI", [("a.pdf", _PDF), ("b.pdf", _PDF2)])
    assert v["status"] == "error"
    assert any(b["code"] == "multiple_pdf" for b in v["blocks"])


# ─── save re-checks + fingerprint storage ────────────────────────────────────


def test_save_rechecks_bundle_duplicate(env):
    _save("FIRST", _bundle())
    with pytest.raises(project_service.UploadFolderConflict, match="Точный комплект"):
        _save("SECOND-NAME", _bundle())   # тот же bundle → блок при фактической загрузке


def test_fingerprint_stored_in_legacy(env):
    res = _save("FP", _bundle())
    dest = Path(res["dest"])
    info = json.loads((dest / "project_info.json").read_text(encoding="utf-8"))
    assert info.get("pdf_sha256") and info.get("bundle_fingerprint")
    manifest = json.loads((dest / "input_manifest.json").read_text(encoding="utf-8"))
    assert manifest["pdf_sha256"] == info["pdf_sha256"]
    assert manifest["bundle_fingerprint"] == info["bundle_fingerprint"]
    roles = {f["role"] for f in manifest["files"]}
    assert {"pdf", "md", "result", "ocr"} <= roles   # ocr учтён в манифесте


def test_ocr_changes_bundle_not_pdf_sha(env):
    cls_with = project_service._classify_upload_files(_bundle())
    cls_without = project_service._classify_upload_files(_bundle(ocr=False))
    f_with = project_service._compute_upload_fingerprint(cls_with)
    f_without = project_service._compute_upload_fingerprint(cls_without)
    assert f_with["pdf_sha256"] == f_without["pdf_sha256"]
    assert f_with["bundle_fingerprint"] != f_without["bundle_fingerprint"]


# ─── HTTP endpoint ───────────────────────────────────────────────────────────


def test_http_precheck_returns_verdict(env):
    from backend.app.main import app
    client = TestClient(app)
    parts = [("files", ("doc.pdf", _PDF, "application/pdf")),
             ("files", ("doc_ocr.html", _OCR, "text/html"))]
    r = client.post("/api/projects/upload-folder/precheck",
                    data={"object_id": "obj-1", "discipline": "EOM", "project_name": "HTTP-NEW"},
                    files=parts)
    assert r.status_code == 200, r.text
    v = r.json()["precheck"]
    assert v["status"] == "ready"
    assert v["has_ocr"] is True and v["bundle_fingerprint"]
