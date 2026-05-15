"""
test_project_version_from_candidate.py
--------------------------------------
Тесты эндпоинта POST /api/projects/{target_project_id}/versions/from-candidate:
candidate (PDF + опционально MD/result.json) из projects/ или внешней папки
добавляется как новая версия (V2, V3, ...) существующего проекта.

Run:
    python -m pytest tests/test_project_version_from_candidate.py -v
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


_PDF_BYTES = b"%PDF-1.4\n%fake-pdf\n%%EOF\n"
_MD_BYTES = (
    "## СТРАНИЦА 1\n\n**Лист:** 1\n**Наименование листа:** Test\n\n"
    "### [TEXT bid_001]\n\nHello version.\n"
).encode("utf-8")


@pytest.fixture
def projects_dir(tmp_path, monkeypatch):
    """Фикстура: projects/ с двумя проектами в разных разделах и одной candidate-папкой."""
    p = tmp_path / "projects"
    p.mkdir()

    # Target проект (раздел KJ) — будем добавлять к нему версию
    kj_dir = p / "M31A"
    (kj_dir / "_output").mkdir(parents=True)
    (kj_dir / "project_info.json").write_text(
        json.dumps({
            "project_id": "M31A",
            "name": "M31A",
            "section": "KJ",
            "pdf_file": "document.pdf",
        }, ensure_ascii=False),
        encoding="utf-8",
    )
    (kj_dir / "document.pdf").write_bytes(_PDF_BYTES)
    # V1 _output content — должен остаться нетронутым
    (kj_dir / "_output" / "03_findings.json").write_text(
        json.dumps({"findings": [{"id": "F-V1"}]}),
        encoding="utf-8",
    )

    # Проект в другом разделе — для теста запрета cross-section
    ar_dir = p / "OTHER_AR"
    (ar_dir / "_output").mkdir(parents=True)
    (ar_dir / "project_info.json").write_text(
        json.dumps({
            "project_id": "OTHER_AR",
            "name": "OTHER_AR",
            "section": "AR",
            "pdf_file": "document.pdf",
        }, ensure_ascii=False),
        encoding="utf-8",
    )
    (ar_dir / "document.pdf").write_bytes(_PDF_BYTES)

    # Candidate папка внутри projects/ (без project_info.json)
    cand_dir = p / "candidate_folder"
    cand_dir.mkdir()
    (cand_dir / "13АВ-РД-КЖ5.22.pdf").write_bytes(_PDF_BYTES + b"NEW")
    (cand_dir / "13АВ-РД-КЖ5.22_document.md").write_bytes(_MD_BYTES)

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


def _candidate_paths(projects_dir: Path):
    cand = projects_dir / "candidate_folder"
    return {
        "pdf": str(cand / "13АВ-РД-КЖ5.22.pdf"),
        "md": str(cand / "13АВ-РД-КЖ5.22_document.md"),
    }


# ─── 1. happy path ─────────────────────────────────────────────────────────


def test_candidate_added_as_v2_to_existing_project(client):
    c, projects_dir = client
    paths = _candidate_paths(projects_dir)

    r = c.post(
        "/api/projects/M31A/versions/from-candidate",
        json={
            "candidate_pdf_path": paths["pdf"],
            "candidate_md_path": paths["md"],
            "expected_section": "KJ",
            "comment": "Добавлено из окна Добавить проект",
            "source": "section_add_project_modal",
        },
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["version_id"] == "v2"
    assert "13АВ-РД-КЖ5.22.pdf" in body["saved"]
    assert "13АВ-РД-КЖ5.22_document.md" in body["saved"]

    # 2. Файлы скопированы в _versions/v2
    v2_dir = projects_dir / "M31A" / "_versions" / "v2"
    assert (v2_dir / "13АВ-РД-КЖ5.22.pdf").read_bytes() == _PDF_BYTES + b"NEW"
    assert (v2_dir / "13АВ-РД-КЖ5.22_document.md").read_bytes() == _MD_BYTES

    # 3. latest_version_id == v2
    manifest = json.loads((projects_dir / "M31A" / "project_versions.json").read_text(encoding="utf-8"))
    assert manifest["latest_version_id"] == "v2"

    # 4. project_info.json V2 содержит pdf_files/md_files
    info = json.loads((v2_dir / "project_info.json").read_text(encoding="utf-8"))
    assert info["pdf_files"] == ["13АВ-РД-КЖ5.22.pdf"]
    assert info["md_files"] == ["13АВ-РД-КЖ5.22_document.md"]
    assert info["version_id"] == "v2"
    assert info["version_source"] == "section_add_project_modal"
    assert info.get("version_comment") == "Добавлено из окна Добавить проект"

    # 5. V1 _output не изменён
    v1_findings = json.loads(
        (projects_dir / "M31A" / "_output" / "03_findings.json").read_text(encoding="utf-8")
    )
    assert v1_findings["findings"][0]["id"] == "F-V1"

    # 6. V1 PDF в корне проекта не тронут (своё содержимое)
    assert (projects_dir / "M31A" / "document.pdf").read_bytes() == _PDF_BYTES

    # 7. Версий стало 2
    assert body["versions_summary"]["version_count"] == 2


def test_no_new_project_card_created(client):
    """После добавления как версии — candidate папка НЕ становится новым проектом."""
    c, projects_dir = client
    paths = _candidate_paths(projects_dir)
    c.post(
        "/api/projects/M31A/versions/from-candidate",
        json={"candidate_pdf_path": paths["pdf"], "candidate_md_path": paths["md"]},
    )
    # candidate_folder не получает project_info.json — это не самостоятельный проект
    cand_info = projects_dir / "candidate_folder" / "project_info.json"
    assert not cand_info.exists()


# ─── 2. cross-section guard ────────────────────────────────────────────────


def test_cross_section_rejected(client):
    """Нельзя добавить candidate к проекту другого раздела (expected_section проверка)."""
    c, projects_dir = client
    paths = _candidate_paths(projects_dir)
    r = c.post(
        "/api/projects/M31A/versions/from-candidate",
        json={
            "candidate_pdf_path": paths["pdf"],
            "expected_section": "AR",  # M31A — KJ, не AR
        },
    )
    assert r.status_code == 400
    # Версия v2 не создана
    assert not (projects_dir / "M31A" / "_versions" / "v2").exists()


# ─── 3. security: path traversal ───────────────────────────────────────────


def test_path_traversal_rejected(client, tmp_path):
    """Путь вне PROJECTS_DIR / external_root отклоняется."""
    c, projects_dir = client
    outside = tmp_path / "evil"
    outside.mkdir()
    evil_pdf = outside / "evil.pdf"
    evil_pdf.write_bytes(_PDF_BYTES)

    r = c.post(
        "/api/projects/M31A/versions/from-candidate",
        json={"candidate_pdf_path": str(evil_pdf)},
    )
    assert r.status_code == 400
    assert not (projects_dir / "M31A" / "_versions" / "v2").exists()


def test_external_root_allowlist(client, tmp_path):
    """external_root явно расширяет allowlist (Из другой папки в UI)."""
    c, projects_dir = client
    ext = tmp_path / "ext_scan"
    ext.mkdir()
    pdf = ext / "ext.pdf"
    pdf.write_bytes(_PDF_BYTES)

    r = c.post(
        "/api/projects/M31A/versions/from-candidate",
        json={
            "candidate_pdf_path": str(pdf),
            "external_root": str(ext),
        },
    )
    assert r.status_code == 200, r.text


# ─── 4. валидация ──────────────────────────────────────────────────────────


def test_candidate_without_pdf_rejected(client):
    c, _ = client
    r = c.post(
        "/api/projects/M31A/versions/from-candidate",
        json={"candidate_pdf_path": ""},
    )
    assert r.status_code == 400


def test_candidate_without_md_produces_warning(client):
    """Без MD версия создаётся, но в warnings — предупреждение."""
    c, projects_dir = client
    paths = _candidate_paths(projects_dir)
    r = c.post(
        "/api/projects/M31A/versions/from-candidate",
        json={"candidate_pdf_path": paths["pdf"]},  # без md
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert any("MD" in w for w in body.get("warnings", []))


def test_v3_created_after_v2(client):
    c, projects_dir = client
    paths = _candidate_paths(projects_dir)
    # V2
    r1 = c.post(
        "/api/projects/M31A/versions/from-candidate",
        json={"candidate_pdf_path": paths["pdf"], "candidate_md_path": paths["md"]},
    )
    assert r1.status_code == 200
    assert r1.json()["version_id"] == "v2"

    # V3
    r2 = c.post(
        "/api/projects/M31A/versions/from-candidate",
        json={"candidate_pdf_path": paths["pdf"], "candidate_md_path": paths["md"]},
    )
    assert r2.status_code == 200
    assert r2.json()["version_id"] == "v3"
    assert r2.json()["versions_summary"]["version_count"] == 3
    assert r2.json()["versions_summary"]["latest_version_id"] == "v3"

    # Файлы V3 лежат в _versions/v3
    v3_dir = projects_dir / "M31A" / "_versions" / "v3"
    assert (v3_dir / "13АВ-РД-КЖ5.22.pdf").exists()


def test_unknown_target_project_returns_404(client):
    c, _ = client
    r = c.post(
        "/api/projects/NO_SUCH_PROJECT/versions/from-candidate",
        json={"candidate_pdf_path": "anything.pdf"},
    )
    assert r.status_code == 404
