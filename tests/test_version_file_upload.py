"""
test_version_file_upload.py
---------------------------
Тесты загрузки исходных файлов в версию проекта (POST .../versions/{vid}/files)
и валидации перед запуском аудита V2 без исходников.

Run:
    python -m pytest tests/test_version_file_upload.py -v
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
    p = tmp_path / "projects"
    p.mkdir()
    pdir = p / "M31A"
    out = pdir / "_output"
    out.mkdir(parents=True)
    (pdir / "project_info.json").write_text(
        json.dumps({
            "project_id": "M31A",
            "name": "M31A",
            "section": "EOM",
            "pdf_file": "document.pdf",
        }, ensure_ascii=False),
        encoding="utf-8",
    )
    (pdir / "document.pdf").write_bytes(_PDF_BYTES)
    # Имитируем «обработанную» V1 — пустой 03_findings.json, чтобы было видно,
    # что V2 их не подтягивает.
    (out / "03_findings.json").write_text(
        json.dumps({"findings": [{"id": "F-001", "severity": "КРИТИЧЕСКОЕ"}]}),
        encoding="utf-8",
    )
    import backend.app.services.common.project_service as ps
    monkeypatch.setattr(ps, "_get_projects_dir", lambda: p)
    monkeypatch.setattr(ps, "_PROJECT_DIRS_CACHE", [])
    monkeypatch.setattr(ps, "_PROJECT_DIRS_CACHE_TIME", 0.0)
    monkeypatch.setattr(ps, "_document_cache", {})
    return p


@pytest.fixture
def client(projects_dir):
    from backend.app.main import app
    return TestClient(app), projects_dir


@pytest.fixture
def v2_created(client):
    c, projects_dir = client
    r = c.post("/api/projects/M31A/versions", json={"comment": "V2"})
    assert r.status_code == 200, r.text
    return c, projects_dir


# ─── 1. upload PDF в V2 ─────────────────────────────────────────────────────


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


def test_upload_pdf_to_v2(v2_created):
    c, projects_dir = v2_created
    r = _upload(c, "v2", [("document.pdf", _PDF_BYTES)])
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["version_id"] == "v2"
    assert body["saved"] == ["document.pdf"]

    # Файл на диске в V2
    v2_dir = projects_dir / "M31A" / "_versions" / "v2"
    assert (v2_dir / "document.pdf").read_bytes() == _PDF_BYTES

    # В КОРНЕ V1 файл существовал и до этого, но не должен был дублироваться
    # (то есть: V1 контент не тронут — там был свой document.pdf).
    v1_pdf = projects_dir / "M31A" / "document.pdf"
    assert v1_pdf.read_bytes() == _PDF_BYTES  # тот же исходный V1, не перезаписан

    # project_info.json V2 обновился
    info = json.loads((v2_dir / "project_info.json").read_text(encoding="utf-8"))
    assert "document.pdf" in info["pdf_files"]
    assert info["pdf_file"] == "document.pdf"
    assert info["last_uploaded_files"] == ["document.pdf"]
    assert "updated_at" in info


def test_upload_md_updates_info(v2_created):
    c, projects_dir = v2_created
    r = _upload(c, "v2", [("document.md", _MD_BYTES)])
    assert r.status_code == 200, r.text

    v2_dir = projects_dir / "M31A" / "_versions" / "v2"
    info = json.loads((v2_dir / "project_info.json").read_text(encoding="utf-8"))
    assert info["md_files"] == ["document.md"]
    assert info["md_file"] == "document.md"


def test_upload_md_makes_document_endpoint_work(v2_created):
    c, _ = v2_created
    _upload(c, "v2", [("document.md", _MD_BYTES)])
    r = c.get("/api/document/M31A/pages", params={"version_id": "v2"})
    assert r.status_code == 200
    assert r.json()["total_pages"] == 1


# ─── 2. validation & errors ─────────────────────────────────────────────────


def test_upload_to_unknown_version_404(v2_created):
    c, _ = v2_created
    r = _upload(c, "v999", [("a.pdf", _PDF_BYTES)])
    assert r.status_code == 404


def test_upload_to_v1_forbidden_by_default(v2_created):
    c, projects_dir = v2_created
    r = _upload(c, "v1", [("new.pdf", _PDF_BYTES)])
    assert r.status_code == 403
    # Файл в корне V1 не появился
    assert not (projects_dir / "M31A" / "new.pdf").exists()


def test_upload_to_v1_allowed_with_flag(v2_created):
    c, projects_dir = v2_created
    r = _upload(c, "v1", [("extra.pdf", _PDF_BYTES)], allow_v1_upload="true")
    assert r.status_code == 200, r.text
    assert (projects_dir / "M31A" / "extra.pdf").read_bytes() == _PDF_BYTES


def test_duplicate_filename_conflict(v2_created):
    c, _ = v2_created
    r1 = _upload(c, "v2", [("a.pdf", _PDF_BYTES)])
    assert r1.status_code == 200
    r2 = _upload(c, "v2", [("a.pdf", _PDF_BYTES + b"x")])
    assert r2.status_code == 409


def test_duplicate_filename_with_replace(v2_created):
    c, projects_dir = v2_created
    _upload(c, "v2", [("a.pdf", _PDF_BYTES)])
    new_bytes = _PDF_BYTES + b"REPLACED"
    r = _upload(c, "v2", [("a.pdf", new_bytes)], replace_existing="true")
    assert r.status_code == 200, r.text
    v2_dir = projects_dir / "M31A" / "_versions" / "v2"
    assert (v2_dir / "a.pdf").read_bytes() == new_bytes
    # pdf_files не дублируется
    info = json.loads((v2_dir / "project_info.json").read_text(encoding="utf-8"))
    assert info["pdf_files"].count("a.pdf") == 1


def test_path_traversal_filename_rejected(v2_created):
    c, projects_dir = v2_created
    r = _upload(c, "v2", [("../evil.pdf", _PDF_BYTES)])
    assert r.status_code == 400
    # Файл вне версии не создан
    v2_dir = projects_dir / "M31A" / "_versions" / "v2"
    assert not (v2_dir / "evil.pdf").exists()
    assert not (projects_dir / "evil.pdf").exists()
    assert not (projects_dir.parent / "evil.pdf").exists()


def test_disallowed_extension(v2_created):
    c, _ = v2_created
    r = _upload(c, "v2", [("malware.exe", _PDF_BYTES)])
    assert r.status_code == 400


def test_empty_file_rejected(v2_created):
    c, _ = v2_created
    r = _upload(c, "v2", [("empty.pdf", b"")])
    assert r.status_code == 400


def test_batch_atomic_on_conflict(v2_created):
    """При конфликте в одном из файлов partial save не должен происходить."""
    c, projects_dir = v2_created
    _upload(c, "v2", [("a.pdf", _PDF_BYTES)])
    # Загружаем сразу две: одна новая, вторая конфликтная
    r = _upload(c, "v2", [
        ("b.pdf", _PDF_BYTES),
        ("a.pdf", _PDF_BYTES),
    ])
    assert r.status_code == 409
    v2_dir = projects_dir / "M31A" / "_versions" / "v2"
    assert not (v2_dir / "b.pdf").exists()  # batch атомарен


# ─── 3. GET files & versions enrichment ────────────────────────────────────


def test_get_version_files_listing(v2_created):
    c, _ = v2_created
    _upload(c, "v2", [("document.pdf", _PDF_BYTES), ("document.md", _MD_BYTES)])
    r = c.get("/api/projects/M31A/versions/v2/files")
    assert r.status_code == 200
    body = r.json()
    names = sorted(f["name"] for f in body["files"])
    assert names == ["document.md", "document.pdf"]
    assert body["project_info"]["pdf_files"] == ["document.pdf"]


def test_versions_summary_enriched(v2_created):
    c, _ = v2_created
    _upload(c, "v2", [("document.pdf", _PDF_BYTES)])
    r = c.get("/api/projects/M31A/versions")
    assert r.status_code == 200
    by_id = {v["version_id"]: v for v in r.json()["versions"]}
    assert by_id["v2"]["has_source_files"] is True
    assert by_id["v2"]["pdf_count"] == 1
    assert by_id["v2"]["md_count"] == 0
    assert by_id["v2"]["can_run_audit"] is True

    # V1 (legacy без manifest-загрузки) — у неё в корне есть document.pdf
    assert by_id["v1"]["pdf_count"] == 1
    assert by_id["v1"]["can_run_audit"] is True


def test_v2_empty_can_run_audit_false(v2_created):
    c, _ = v2_created
    # V2 ещё пуста
    r = c.get("/api/projects/M31A/versions")
    by_id = {v["version_id"]: v for v in r.json()["versions"]}
    assert by_id["v2"]["pdf_count"] == 0
    assert by_id["v2"]["can_run_audit"] is False


# ─── 4. audit start validation ─────────────────────────────────────────────


def test_audit_start_v2_without_files_returns_409(v2_created):
    c, projects_dir = v2_created
    r = c.post("/api/audit/M31A/full-audit", params={"version_id": "v2"})
    assert r.status_code == 409
    # Подсказка про upload должна быть в detail
    assert "PDF" in r.json()["detail"] or "MD" in r.json()["detail"]

    # ничего не записано в V2 _output
    v2_out = projects_dir / "M31A" / "_versions" / "v2" / "_output"
    assert sorted(v2_out.iterdir()) == []


def test_audit_start_v2_with_pdf_passes_validation(v2_created):
    c, _ = v2_created
    _upload(c, "v2", [("document.pdf", _PDF_BYTES)])
    r = c.post("/api/audit/M31A/full-audit", params={"version_id": "v2"})
    # 200 (enqueued) ИЛИ 409 (worker уже что-то делает) — главное, не 400/404
    assert r.status_code in (200, 409)
    if r.status_code == 200:
        job = r.json()["job"]
        assert job["version_id"] == "v2"


def test_audit_start_v1_without_pdf_returns_400(client, projects_dir):
    c, _ = client
    # Удалим единственный PDF — V1 не должна быть способна запускаться
    (projects_dir / "M31A" / "document.pdf").unlink()
    r = c.post("/api/audit/M31A/full-audit")
    assert r.status_code == 400


def test_v1_has_pdf_v2_empty_audit_v2_blocked(v2_created):
    c, _ = v2_created
    # V1 имеет PDF (создано в фикстуре), V2 пуста.
    r = c.post("/api/audit/M31A/full-audit", params={"version_id": "v2"})
    assert r.status_code == 409  # V2 не fallback на V1
    # Без version_id (latest=V2) — тоже блокирован
    r2 = c.post("/api/audit/M31A/full-audit")
    assert r2.status_code == 409


def test_optimization_start_v2_without_files_409(v2_created):
    c, _ = v2_created
    r = c.post("/api/optimization/M31A/run", params={"version_id": "v2"})
    assert r.status_code == 409


# ─── 5. legacy ничего не сломала ──────────────────────────────────────────


def test_legacy_no_manifest_versions_summary_works(client, projects_dir):
    """Проект без manifest должен корректно отдавать versions с has_source_files
    исходя из файлов в корне."""
    c, _ = client
    r = c.get("/api/projects/M31A/versions")
    assert r.status_code == 200
    body = r.json()
    assert body["latest_version_id"] == "v1"
    v1 = body["versions"][0]
    assert v1["pdf_count"] == 1
    assert v1["can_run_audit"] is True

    # И manifest на диск всё ещё не пишется
    assert not (projects_dir / "M31A" / "project_versions.json").exists()


def test_v3_after_upload_isolated_from_v2(v2_created):
    """Загрузка в V2, потом создание V3, не должна тянуть V2 файлы в V3."""
    c, projects_dir = v2_created
    _upload(c, "v2", [("a.pdf", _PDF_BYTES)])
    c.post("/api/projects/M31A/versions", json={"comment": "V3"})

    v3_dir = projects_dir / "M31A" / "_versions" / "v3"
    # В V3 нет PDF (мы их явно не загружали)
    assert not (v3_dir / "a.pdf").exists()
    info = json.loads((v3_dir / "project_info.json").read_text(encoding="utf-8"))
    assert info.get("pdf_files", []) == []

    # versions API: V3 latest, can_run_audit=false
    r = c.get("/api/projects/M31A/versions")
    by_id = {v["version_id"]: v for v in r.json()["versions"]}
    assert by_id["v3"]["is_latest"] is True
    assert by_id["v3"]["can_run_audit"] is False
    # V2 не тронули
    assert by_id["v2"]["pdf_count"] == 1
