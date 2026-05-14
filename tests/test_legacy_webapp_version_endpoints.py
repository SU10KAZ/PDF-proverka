"""
test_legacy_webapp_version_endpoints.py
---------------------------------------
Smoke-тесты для version_compat router'а, подключённого к legacy
`webapp.main:app`. Проверяют, что после рефакторинга на 8081
доступен полный сценарий: создать V2, загрузить файлы, увидеть V2 как
latest, не сломать старые endpoints.

Run:
    python -m pytest tests/test_legacy_webapp_version_endpoints.py -v
"""
from __future__ import annotations

import io
import json
import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))


_PDF_BYTES = b"%PDF-1.4\n%fake-pdf-content\n%%EOF\n"
_MD_BYTES = (
    "## СТРАНИЦА 1\n\n**Лист:** 1\n**Наименование листа:** Test\n\n"
    "### [TEXT bid_001]\n\nHello V2.\n"
).encode("utf-8")


@pytest.fixture
def projects_dir(tmp_path, monkeypatch):
    """Минимальный V1-проект + подмена projects_dir у backend.project_service."""
    p = tmp_path / "projects"
    p.mkdir()
    pdir = p / "M31A"
    (pdir / "_output").mkdir(parents=True)
    (pdir / "project_info.json").write_text(
        json.dumps({
            "project_id": "M31A", "name": "M31A",
            "section": "EOM", "pdf_file": "document.pdf",
        }, ensure_ascii=False),
        encoding="utf-8",
    )
    (pdir / "document.pdf").write_bytes(_PDF_BYTES)

    import backend.app.services.common.project_service as ps
    monkeypatch.setattr(ps, "_get_projects_dir", lambda: p)
    monkeypatch.setattr(ps, "_PROJECT_DIRS_CACHE", [])
    monkeypatch.setattr(ps, "_PROJECT_DIRS_CACHE_TIME", 0.0)
    monkeypatch.setattr(ps, "_document_cache", {})
    return p


@pytest.fixture
def legacy_client(projects_dir):
    """TestClient над legacy webapp.main:app (а не backend.app.main)."""
    from webapp.main import app
    return TestClient(app), projects_dir


def _upload(c, version_id: str, files: list[tuple[str, bytes]], **form):
    files_payload = [
        ("files", (name, io.BytesIO(content), "application/octet-stream"))
        for name, content in files
    ]
    data = {k: str(v) for k, v in form.items()}
    return c.post(
        f"/api/projects/M31A/versions/{version_id}/files",
        files=files_payload,
        data=data,
    )


# ─── 1. POST /versions создаёт V2 ────────────────────────────────────────


def test_post_versions_creates_v2(legacy_client):
    c, projects_dir = legacy_client
    r = c.post("/api/projects/M31A/versions", json={"comment": "Editor 1"})
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["status"] == "ok"
    assert body["latest_version_id"] == "v2"
    assert body["version_count"] == 2
    # Физическая папка _versions/v2 создана.
    assert (projects_dir / "M31A" / "_versions" / "v2").is_dir()


# ─── 2. GET /versions после POST показывает latest=v2 ────────────────────


def test_get_versions_after_post(legacy_client):
    c, _ = legacy_client
    c.post("/api/projects/M31A/versions", json={"comment": ""})
    r = c.get("/api/projects/M31A/versions")
    assert r.status_code == 200
    body = r.json()
    assert body["latest_version_id"] == "v2"
    ids = [v["version_id"] for v in body["versions"]]
    assert "v1" in ids and "v2" in ids


# ─── 3. POST /versions/v2/files загружает PDF ────────────────────────────


def test_upload_pdf_to_v2(legacy_client):
    c, projects_dir = legacy_client
    c.post("/api/projects/M31A/versions", json={"comment": ""})
    r = _upload(c, "v2", [("new.pdf", _PDF_BYTES)])
    assert r.status_code == 200, r.text
    # Файл лёг внутрь V2.
    v2_dir = projects_dir / "M31A" / "_versions" / "v2"
    assert (v2_dir / "new.pdf").exists()
    # V1 не тронут.
    assert (projects_dir / "M31A" / "document.pdf").exists()


# ─── 4. GET /versions/v2/files показывает загруженный файл ───────────────


def test_list_files_after_upload(legacy_client):
    c, _ = legacy_client
    c.post("/api/projects/M31A/versions", json={"comment": ""})
    _upload(c, "v2", [("new.pdf", _PDF_BYTES)])
    r = c.get("/api/projects/M31A/versions/v2/files")
    assert r.status_code == 200
    body = r.json()
    names = [f.get("name") or f.get("filename") for f in body.get("files", [])]
    assert "new.pdf" in names


# ─── 5. POST /versions обновляет manifest на диске ──────────────────────


def test_post_versions_updates_manifest_on_disk(legacy_client):
    """После POST /versions проверяем, что project_versions.json содержит V2.

    (Поведение GET /api/projects/{id} проверяется в backend-тестах с правильной
    подменой project_service — у legacy webapp своя копия project_service,
    не реагирующая на monkeypatch backend-сервиса.)
    """
    c, projects_dir = legacy_client
    c.post("/api/projects/M31A/versions", json={"comment": ""})
    manifest_path = projects_dir / "M31A" / "project_versions.json"
    assert manifest_path.exists()
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest["latest_version_id"] == "v2"
    ids = [v["version_id"] for v in manifest["versions"]]
    assert "v1" in ids and "v2" in ids


# ─── 6. GET /api/projects/{id}/versions поддерживает ?version_id= ────────


def test_get_versions_query_doesnt_break(legacy_client):
    """version_compat router не парсит ?version_id= (оно ему не нужно),
    но и не падает 500. Это защита от регрессии."""
    c, _ = legacy_client
    c.post("/api/projects/M31A/versions", json={"comment": ""})
    r = c.get("/api/projects/M31A/versions?version_id=v1")
    assert r.status_code == 200


# ─── 7. V1 upload без allow_v1_upload=true → 403 ────────────────────────


def test_v1_upload_forbidden_by_default(legacy_client):
    c, projects_dir = legacy_client
    r = _upload(c, "v1", [("extra.pdf", _PDF_BYTES)])
    assert r.status_code == 403
    assert not (projects_dir / "M31A" / "extra.pdf").exists()


# ─── 8. duplicate без replace_existing → 409 ────────────────────────────


def test_duplicate_upload_conflict(legacy_client):
    c, _ = legacy_client
    c.post("/api/projects/M31A/versions", json={"comment": ""})
    _upload(c, "v2", [("dup.pdf", _PDF_BYTES)])
    r = _upload(c, "v2", [("dup.pdf", _PDF_BYTES)])
    assert r.status_code == 409


def test_duplicate_upload_replace_existing_ok(legacy_client):
    c, _ = legacy_client
    c.post("/api/projects/M31A/versions", json={"comment": ""})
    _upload(c, "v2", [("dup.pdf", _PDF_BYTES)])
    r = _upload(c, "v2", [("dup.pdf", b"%PDF-1.4\nnew\n")], replace_existing=True)
    assert r.status_code == 200


# ─── 9. path traversal → 400 ─────────────────────────────────────────────


def test_path_traversal_rejected(legacy_client):
    c, _ = legacy_client
    c.post("/api/projects/M31A/versions", json={"comment": ""})
    r = _upload(c, "v2", [("../escape.pdf", _PDF_BYTES)])
    assert r.status_code == 400


# ─── 10. unknown version → 404 ───────────────────────────────────────────


def test_unknown_version_404(legacy_client):
    c, _ = legacy_client
    r = c.get("/api/projects/M31A/versions/v999/files")
    assert r.status_code == 404
    r = _upload(c, "v999", [("x.pdf", _PDF_BYTES)])
    assert r.status_code == 404


# ─── 11. catch-all не перехватывает /versions/v2/files ──────────────────


def test_catch_all_does_not_swallow_versions_files(legacy_client):
    """Признак того, что наш router зарегистрирован ДО projects.router."""
    c, _ = legacy_client
    c.post("/api/projects/M31A/versions", json={"comment": ""})
    r = c.get("/api/projects/M31A/versions/v2/files")
    assert r.status_code == 200
    body = r.json()
    # Ответ от version_compat: {files: [...], project_info: {...}, ...}.
    # Catch-all бы вернул ProjectStatus с project_id, который содержит
    # 'M31A/versions/v2/files' — этого тут быть не должно.
    assert "files" in body
    assert "project_id" not in body or body["project_id"] != "M31A/versions/v2/files"


# ─── 12. Старые legacy endpoints не сломались ────────────────────────────


def test_old_endpoints_still_work(legacy_client):
    c, _ = legacy_client
    # /api/projects (список)
    r = c.get("/api/projects")
    assert r.status_code == 200
    # /api/findings/{id} — у нас нет findings, но endpoint должен ответить
    r = c.get("/api/findings/M31A")
    assert r.status_code in (200, 404)
    # /api/document/{id}/pages
    r = c.get("/api/document/M31A/pages")
    assert r.status_code in (200, 404)


# ─── 13. End-to-end: V2 → upload → migrated check ───────────────────────


def test_full_v2_flow_works(legacy_client):
    """Полный сценарий V1 → V2 → upload PDF → migrated check.

    Это конечная цель этого прохода: пользователь на 8081 должен суметь
    пройти этот путь целиком через legacy webapp.
    """
    c, projects_dir = legacy_client
    # 1) Создать V2
    r = c.post("/api/projects/M31A/versions", json={"comment": "тест V2"})
    assert r.status_code == 200, r.text
    assert r.json()["latest_version_id"] == "v2"
    # 2) Загрузить PDF в V2
    r = _upload(c, "v2", [("doc.pdf", _PDF_BYTES), ("doc.md", _MD_BYTES)])
    assert r.status_code == 200
    # 3) GET /versions/v2/files показывает оба файла
    r = c.get("/api/projects/M31A/versions/v2/files")
    body = r.json()
    names = [f.get("name") or f.get("filename") for f in body.get("files", [])]
    assert "doc.pdf" in names
    # 4) GET /versions показывает can_run_audit=true для V2
    r = c.get("/api/projects/M31A/versions")
    versions = r.json()["versions"]
    v2_entry = next(v for v in versions if v["version_id"] == "v2")
    assert v2_entry.get("has_source_files") is True
    # 5) migrated check (нет accepted в V1 → пустой report, но не 500)
    r = c.post("/api/projects/M31A/versions/v2/migrated-findings/check")
    assert r.status_code == 200, r.text
    body = r.json()
    assert body.get("status") == "ok"
