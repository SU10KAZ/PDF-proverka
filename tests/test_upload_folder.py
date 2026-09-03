"""Тесты загрузки папки проекта через браузер (Добавить проект → Из папки на компьютере).

Покрывает:
  - save_uploaded_project_folder: полный комплект (pdf+md+result+ocr) → legacy + shadow-вызов;
  - сохранение *_ocr.html;
  - отсутствие ocr/result не блокирует, даёт warning;
  - нет PDF → UploadFolderError (422); несколько PDF → UploadFolderError (422);
  - дубль в legacy → UploadFolderConflict (409); дубль в projects_v2 → UploadFolderConflict (409);
  - имя проекта с префиксом '_' → UploadFolderError (422);
  - path-traversal в имени файла отклоняется / webkitRelativePath санитизируется до basename;
  - сбой v2-shadow fail-soft, legacy остаётся авторитетным;
  - register_external_project теперь копирует *_ocr.html;
  - HTTP-маппинг POST /api/projects/upload-folder (422/409/200) + projects_v2/ не в git.

Run:
    python -m pytest tests/test_upload_folder.py -v
"""
from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from backend.app.services.common import object_service, project_service  # noqa: E402
import backend.app.services.storage.storage_write_facade as swf  # noqa: E402


_PDF = b"%PDF-1.4\n%fake\n%%EOF\n"
_MD = b"## STR 1\n\n**List:** 1\n"
_RESULT = json.dumps({"pages": []}).encode("utf-8")
_OCR = b"<html><body>ocr</body></html>"


@pytest.fixture
def env(tmp_path, monkeypatch):
    """projects_dir объекта + захват shadow-вызовов; v2 dup-check выключен."""
    projects_dir = tmp_path / "projects" / "OBJ"
    projects_dir.mkdir(parents=True)
    obj = {"id": "obj-1", "name": "Объект 1", "projects_dir": str(projects_dir)}

    monkeypatch.setattr(object_service, "get_object_by_id",
                        lambda oid: obj if oid == "obj-1" else None)
    monkeypatch.setattr(object_service, "get_projects_dir_for",
                        lambda oid: projects_dir if oid == "obj-1" else None)
    monkeypatch.setattr(project_service, "_v2_document_exists", lambda *a, **k: False)

    shadow_calls = []
    monkeypatch.setattr(swf, "shadow_mirror_project_path_safe",
                        lambda p: shadow_calls.append(str(p)))

    return projects_dir, shadow_calls


def _bundle(pdf=True, md=True, result=True, ocr=True, pdf_count=1):
    files = []
    if pdf:
        for i in range(pdf_count):
            files.append((f"doc{i if pdf_count > 1 else ''}.pdf", _PDF))
    if md:
        files.append(("doc_document.md", _MD))
    if result:
        files.append(("doc_result.json", _RESULT))
    if ocr:
        files.append(("doc_ocr.html", _OCR))
    return files


# ─── happy path ──────────────────────────────────────────────────────────────


def test_full_bundle_creates_legacy_and_triggers_v2(env):
    projects_dir, shadow_calls = env
    res = project_service.save_uploaded_project_folder(
        object_id="obj-1", discipline="EOM", project_name="PRJ-1", files=_bundle())

    dest = projects_dir / "EOM" / "PRJ-1"
    assert res["project_id"] == "EOM/PRJ-1"
    assert (dest / "doc.pdf").read_bytes() == _PDF
    assert (dest / "doc_document.md").exists()
    assert (dest / "doc_result.json").exists()
    assert (dest / "doc_ocr.html").read_bytes() == _OCR
    assert (dest / "_output").is_dir()
    info = json.loads((dest / "project_info.json").read_text(encoding="utf-8"))
    assert info["section"] == "EOM"
    assert info["pdf_file"] == "doc.pdf"
    assert info["md_file"] == "doc_document.md"
    assert info["source"] == "upload-folder"
    assert res["has_pdf"] and res["has_md"] and res["has_result"] and res["has_ocr"]
    # v2-shadow зеркало вызвано на dest
    assert shadow_calls == [str(dest)]


def test_v2_primary_upload_stages_to_temp_and_skips_legacy(env, monkeypatch):
    """В v2-primary загрузка НЕ пишет в legacy projects/, а мигрирует из temp-staging."""
    projects_dir, shadow_calls = env
    monkeypatch.setattr(swf, "v2_is_primary", lambda: True)

    mirrored = {"done": False}

    mirror_identity = {}

    def fake_shadow(p, **identity):
        shadow_calls.append(str(p))
        mirror_identity.update(identity)
        mirrored["done"] = True

    monkeypatch.setattr(swf, "shadow_mirror_project_path_safe", fake_shadow)
    # _v2_document_exists: False до миграции (dup-check), True после (verify)
    monkeypatch.setattr(project_service, "_v2_document_exists",
                        lambda *a, **k: mirrored["done"])

    res = project_service.save_uploaded_project_folder(
        object_id="obj-1", discipline="EOM", project_name="PRJ-V2", files=_bundle())

    # legacy projects/ НЕ тронут
    assert not (projects_dir / "EOM" / "PRJ-V2").exists()
    # зеркалирование вызвано на staging-путь ВНЕ projects_dir, который затем удалён
    assert len(shadow_calls) == 1
    staged = Path(shadow_calls[0])
    assert projects_dir not in staged.parents
    assert not staged.exists()  # staging вычищен после миграции
    # basename папки объекта сохранён в staging-пути (object_id_for by_name)
    assert staged.parent.parent.name == projects_dir.name
    assert mirror_identity == {
        "object_id": "obj-1",
        "display_name": "Объект 1",
    }
    assert res["project_id"] == "EOM/PRJ-V2"
    assert res["dest"] == "EOM/PRJ-V2"


def test_project_name_with_pdf_suffix_normalized_like_v2(env, monkeypatch):
    """Имя с хвостовым `.pdf` не должно давать ложное «Не удалось создать документ».

    Портал отдаёт комплект с двойным расширением (`X.pdf.pdf`,
    `X.pdf_results.md`) — фронт снимает одно `.pdf` и присылает `X.pdf`.
    Миграция в v2 снимает второе (`document_code_for` → `strip_pdf_suffix`),
    поэтому имя нормализуем ещё на приёме: project_id == document_code.
    """
    projects_dir, shadow_calls = env
    monkeypatch.setattr(swf, "v2_is_primary", lambda: True)

    migrated: set[str] = set()

    def fake_shadow(p, **identity):
        shadow_calls.append(str(p))
        # v2 хранит документ под кодом БЕЗ хвостового `.pdf`
        migrated.add(Path(p).name.removesuffix(".pdf"))

    monkeypatch.setattr(swf, "shadow_mirror_project_path_safe", fake_shadow)
    monkeypatch.setattr(project_service, "_v2_document_exists",
                        lambda oid, name: name in migrated)

    res = project_service.save_uploaded_project_folder(
        object_id="obj-1", discipline="EOM", project_name="PRJ-DBL_V1.pdf",
        files=_bundle())

    assert res["project_id"] == "EOM/PRJ-DBL_V1"
    assert res["name"] == "PRJ-DBL_V1"
    assert res["project_info"]["project_id"] == "EOM/PRJ-DBL_V1"


def test_precheck_normalizes_pdf_suffix_in_project_name(env):
    pc = project_service.precheck_uploaded_project_folder(
        object_id="obj-1", discipline="EOM", folder_name="PRJ-PC_V1.pdf",
        project_name="PRJ-PC_V1.pdf", files=_bundle())
    assert pc["project_id"] == "EOM/PRJ-PC_V1"


def test_ocr_html_is_saved(env):
    projects_dir, _ = env
    res = project_service.save_uploaded_project_folder(
        object_id="obj-1", discipline="EOM", project_name="PRJ-OCR", files=_bundle())
    assert "doc_ocr.html" in res["saved_files"]
    assert (projects_dir / "EOM" / "PRJ-OCR" / "doc_ocr.html").exists()


# ─── недостающие файлы — не блокируют, дают warning ───────────────────────────


def test_missing_ocr_does_not_block(env):
    res = project_service.save_uploaded_project_folder(
        object_id="obj-1", discipline="EOM", project_name="NO-OCR",
        files=_bundle(ocr=False))
    assert res["has_ocr"] is False
    assert any("ocr" in w.lower() for w in res["warnings"])


def test_missing_result_warns_not_block(env):
    res = project_service.save_uploaded_project_folder(
        object_id="obj-1", discipline="EOM", project_name="NO-RES",
        files=_bundle(result=False))
    assert res["has_result"] is False
    assert any("result" in w.lower() for w in res["warnings"])


# ─── PDF-валидация ───────────────────────────────────────────────────────────


def test_no_pdf_raises(env):
    with pytest.raises(project_service.UploadFolderError, match="не найден PDF"):
        project_service.save_uploaded_project_folder(
            object_id="obj-1", discipline="EOM", project_name="NOPDF",
            files=_bundle(pdf=False))


def test_multiple_pdf_raises(env):
    with pytest.raises(project_service.UploadFolderError, match="несколько PDF"):
        project_service.save_uploaded_project_folder(
            object_id="obj-1", discipline="EOM", project_name="MULTIPDF",
            files=_bundle(pdf_count=2))


# ─── дубли ───────────────────────────────────────────────────────────────────


def test_duplicate_legacy_raises_conflict(env):
    project_service.save_uploaded_project_folder(
        object_id="obj-1", discipline="EOM", project_name="DUP", files=_bundle())
    with pytest.raises(project_service.UploadFolderConflict, match="projects/"):
        project_service.save_uploaded_project_folder(
            object_id="obj-1", discipline="EOM", project_name="DUP", files=_bundle())


def test_duplicate_v2_raises_conflict(env, monkeypatch):
    monkeypatch.setattr(project_service, "_v2_document_exists", lambda *a, **k: True)
    with pytest.raises(project_service.UploadFolderConflict, match="projects_v2"):
        project_service.save_uploaded_project_folder(
            object_id="obj-1", discipline="EOM", project_name="DUPV2", files=_bundle())


# ─── имена / path-traversal ──────────────────────────────────────────────────


def test_project_name_underscore_prefix_rejected(env):
    with pytest.raises(project_service.UploadFolderError, match="не может начинаться"):
        project_service.save_uploaded_project_folder(
            object_id="obj-1", discipline="EOM", project_name="_hidden", files=_bundle())


def test_project_name_with_slash_rejected(env):
    with pytest.raises(project_service.UploadFolderError, match="Недопустимое название"):
        project_service.save_uploaded_project_folder(
            object_id="obj-1", discipline="EOM", project_name="a/b", files=_bundle())


def test_unknown_object_rejected(env):
    with pytest.raises(project_service.UploadFolderError, match="Объект не найден"):
        project_service.save_uploaded_project_folder(
            object_id="nope", discipline="EOM", project_name="X", files=_bundle())


def test_path_traversal_degenerate_name_rejected(env):
    """Имя, схлопывающееся в '..'/пусто (нет валидного basename) — отклоняется."""
    for bad in ("..", "foo/..", "C:\\\\"):
        with pytest.raises(project_service.UploadFolderError, match="Небезопасное имя"):
            project_service.save_uploaded_project_folder(
                object_id="obj-1", discipline="EOM", project_name="TRAV",
                files=[(bad, _PDF)])


def test_path_traversal_filename_sanitized_to_basename(env):
    """`../../evil.pdf` не выходит за dest — пишется плоско как evil.pdf."""
    projects_dir, _ = env
    files = [("../../evil.pdf", _PDF)]
    project_service.save_uploaded_project_folder(
        object_id="obj-1", discipline="EOM", project_name="SANI", files=files)
    dest = projects_dir / "EOM" / "SANI"
    assert (dest / "evil.pdf").exists()
    # ничего не записано выше dest
    assert not (projects_dir / "EOM" / "evil.pdf").exists()
    assert not (projects_dir / "evil.pdf").exists()


def test_webkit_relative_path_sanitized_to_basename(env):
    projects_dir, _ = env
    files = [("PRJ/sub/doc.pdf", _PDF), ("PRJ/sub/doc_document.md", _MD)]
    res = project_service.save_uploaded_project_folder(
        object_id="obj-1", discipline="EOM", project_name="REL", files=files)
    dest = projects_dir / "EOM" / "REL"
    assert (dest / "doc.pdf").exists()      # записан плоско, без подпапок
    assert not (dest / "PRJ").exists()
    assert res["has_pdf"]


# ─── fail-soft v2-shadow ─────────────────────────────────────────────────────


def test_v2_shadow_failure_is_failsoft_legacy_authoritative(env, monkeypatch):
    projects_dir, _ = env

    def boom(_p):
        raise RuntimeError("v2 down")

    monkeypatch.setattr(swf, "shadow_mirror_project_path_safe", boom)
    res = project_service.save_uploaded_project_folder(
        object_id="obj-1", discipline="EOM", project_name="FAILSOFT", files=_bundle())
    # legacy записан несмотря на сбой v2
    assert (projects_dir / "EOM" / "FAILSOFT" / "doc.pdf").exists()
    assert res["project_id"] == "EOM/FAILSOFT"


# ─── register_external_project теперь копирует *_ocr.html ─────────────────────


def test_register_external_copies_ocr_html(tmp_path, monkeypatch):
    projects_dir = tmp_path / "projects"
    projects_dir.mkdir()
    src = tmp_path / "src"
    src.mkdir()
    (src / "doc.pdf").write_bytes(_PDF)
    (src / "doc_document.md").write_bytes(_MD)
    (src / "doc_result.json").write_bytes(_RESULT)
    (src / "doc_ocr.html").write_bytes(_OCR)

    monkeypatch.setattr(project_service, "_get_projects_dir", lambda: projects_dir)
    monkeypatch.setattr(swf, "shadow_mirror_project_path_safe", lambda p: None)

    project_service.register_external_project(
        source_path=str(src), pdf_file="doc.pdf", md_file="doc_document.md",
        name="EXT-OCR", section="EOM")
    dest = projects_dir / "EXT-OCR"
    assert (dest / "doc_ocr.html").read_bytes() == _OCR  # фикс: раньше не копировался
    assert (dest / "doc_result.json").exists()


# ─── HTTP endpoint mapping ───────────────────────────────────────────────────


@pytest.fixture
def http(env):
    from backend.app.main import app
    return TestClient(app), env[0]


def _multipart(pdf=True, md=True, result=True, ocr=True, pdf_count=1):
    parts = []
    if pdf:
        for i in range(pdf_count):
            parts.append(("files", (f"doc{i if pdf_count > 1 else ''}.pdf", _PDF, "application/pdf")))
    if md:
        parts.append(("files", ("doc_document.md", _MD, "text/markdown")))
    if result:
        parts.append(("files", ("doc_result.json", _RESULT, "application/json")))
    if ocr:
        parts.append(("files", ("doc_ocr.html", _OCR, "text/html")))
    return parts


def test_http_success(http):
    client, projects_dir = http
    r = client.post("/api/projects/upload-folder",
                    data={"object_id": "obj-1", "discipline": "EOM", "project_name": "HTTP-OK"},
                    files=_multipart())
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["status"] == "ok"
    assert body["project_id"] == "EOM/HTTP-OK"
    assert (projects_dir / "EOM" / "HTTP-OK" / "doc.pdf").exists()


def test_http_no_pdf_422(http):
    client, _ = http
    r = client.post("/api/projects/upload-folder",
                    data={"object_id": "obj-1", "discipline": "EOM", "project_name": "HTTP-NOPDF"},
                    files=_multipart(pdf=False))
    assert r.status_code == 422, r.text


def test_http_multiple_pdf_422(http):
    client, _ = http
    r = client.post("/api/projects/upload-folder",
                    data={"object_id": "obj-1", "discipline": "EOM", "project_name": "HTTP-MULTI"},
                    files=_multipart(pdf_count=2))
    assert r.status_code == 422, r.text


def test_http_duplicate_409(http):
    client, _ = http
    payload = dict(data={"object_id": "obj-1", "discipline": "EOM", "project_name": "HTTP-DUP"},
                   files=_multipart())
    assert client.post("/api/projects/upload-folder", **payload).status_code == 200
    # второй раунд нужен новый files (UploadFile исчерпан) — пересобираем
    r = client.post("/api/projects/upload-folder",
                    data={"object_id": "obj-1", "discipline": "EOM", "project_name": "HTTP-DUP"},
                    files=_multipart())
    assert r.status_code == 409, r.text


def test_http_missing_object_422(http):
    client, _ = http
    r = client.post("/api/projects/upload-folder",
                    data={"discipline": "EOM", "project_name": "X"},
                    files=_multipart())
    assert r.status_code == 422, r.text


# ─── projects_v2/ не отслеживается git ───────────────────────────────────────


def test_projects_v2_not_tracked_in_git():
    out = subprocess.run(
        ["git", "ls-files", "projects_v2/"], cwd=str(_ROOT),
        capture_output=True, text=True)
    assert out.stdout.strip() == "", f"projects_v2/ не должен быть в git: {out.stdout!r}"
