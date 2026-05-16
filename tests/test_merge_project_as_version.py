"""
test_merge_project_as_version.py
--------------------------------
Тесты merge-as-version: source-проект становится новой версией target-проекта.
Эндпоинт: POST /api/projects/{target_project_id}/versions/from-project.

Run:
    python -m pytest tests/test_merge_project_as_version.py -v
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


_PDF_BYTES = b"%PDF-1.4\n%target-pdf\n%%EOF\n"
_PDF_SRC = b"%PDF-1.4\n%source-pdf\n%%EOF\n"
_MD_SRC = (
    "## СТРАНИЦА 1\n\n**Лист:** 1\n**Наименование листа:** Src\n\n"
    "### [TEXT bid_001]\n\nHello src.\n"
).encode("utf-8")


@pytest.fixture
def projects_dir(tmp_path, monkeypatch):
    p = tmp_path / "projects"
    p.mkdir()

    # Target: уже зарегистрированный проект раздела KJ
    tgt = p / "TARGET"
    (tgt / "_output").mkdir(parents=True)
    (tgt / "project_info.json").write_text(
        json.dumps({"project_id": "TARGET", "name": "TARGET", "section": "KJ", "pdf_file": "document.pdf"}, ensure_ascii=False),
        encoding="utf-8",
    )
    (tgt / "document.pdf").write_bytes(_PDF_BYTES)
    (tgt / "_output" / "03_findings.json").write_text(
        json.dumps({"findings": [{"id": "F-V1-TARGET"}]}),
        encoding="utf-8",
    )

    # Source: тоже зарегистрированный, того же раздела. _output пуст —
    # отдельный кейс `has_audit_artifacts=True` тестируем явно ниже.
    src = p / "SOURCE"
    (src / "_output").mkdir(parents=True)
    (src / "project_info.json").write_text(
        json.dumps({"project_id": "SOURCE", "name": "SOURCE", "section": "KJ", "pdf_file": "src.pdf"}, ensure_ascii=False),
        encoding="utf-8",
    )
    (src / "src.pdf").write_bytes(_PDF_SRC)
    (src / "src.md").write_bytes(_MD_SRC)

    # Другой раздел — для теста запрета cross-section
    ar = p / "AR_PROJ"
    (ar / "_output").mkdir(parents=True)
    (ar / "project_info.json").write_text(
        json.dumps({"project_id": "AR_PROJ", "name": "AR_PROJ", "section": "AR", "pdf_file": "doc.pdf"}, ensure_ascii=False),
        encoding="utf-8",
    )
    (ar / "doc.pdf").write_bytes(_PDF_BYTES)

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


def test_merge_creates_v2_and_removes_source(client):
    c, projects_dir = client
    r = c.post(
        "/api/projects/TARGET/versions/from-project",
        json={"source_project_id": "SOURCE", "comment": "test merge"},
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["version_id"] == "v2"
    assert body["source_project_id"] == "SOURCE"
    assert "src.pdf" in body["saved"]
    assert "src.md" in body["saved"]

    # PDF/MD скопированы в _versions/v2
    v2 = projects_dir / "TARGET" / "_versions" / "v2"
    assert (v2 / "src.pdf").read_bytes() == _PDF_SRC
    assert (v2 / "src.md").read_bytes() == _MD_SRC

    # Source-папка удалена
    assert not (projects_dir / "SOURCE").exists()

    # V1 target не тронут
    assert (projects_dir / "TARGET" / "document.pdf").read_bytes() == _PDF_BYTES
    v1_findings = json.loads(
        (projects_dir / "TARGET" / "_output" / "03_findings.json").read_text(encoding="utf-8")
    )
    assert v1_findings["findings"][0]["id"] == "F-V1-TARGET"

    # V2 _output пуст — _output source НЕ скопирован
    assert not (v2 / "_output" / "03_findings.json").exists()

    # project_info.json V2 содержит merged_from_project_id
    info = json.loads((v2 / "project_info.json").read_text(encoding="utf-8"))
    assert info["merged_from_project_id"] == "SOURCE"
    assert info["pdf_files"] == ["src.pdf"]
    assert info["md_files"] == ["src.md"]


def test_merge_keeps_source_when_delete_source_false(client):
    c, projects_dir = client
    r = c.post(
        "/api/projects/TARGET/versions/from-project",
        json={"source_project_id": "SOURCE", "delete_source": False},
    )
    assert r.status_code == 200
    # Source-папка осталась
    assert (projects_dir / "SOURCE").exists()
    # А V2 у target всё равно создана
    assert (projects_dir / "TARGET" / "_versions" / "v2" / "src.pdf").exists()


def test_merge_same_project_rejected(client):
    c, _ = client
    r = c.post(
        "/api/projects/TARGET/versions/from-project",
        json={"source_project_id": "TARGET"},
    )
    assert r.status_code == 400


def test_merge_cross_section_rejected(client):
    c, projects_dir = client
    r = c.post(
        "/api/projects/TARGET/versions/from-project",
        json={"source_project_id": "AR_PROJ"},
    )
    assert r.status_code == 400
    # AR_PROJ не удалён
    assert (projects_dir / "AR_PROJ").exists()
    # V2 у target не создана
    assert not (projects_dir / "TARGET" / "_versions" / "v2").exists()


def test_merge_unknown_source_404(client):
    c, _ = client
    r = c.post(
        "/api/projects/TARGET/versions/from-project",
        json={"source_project_id": "NO_SUCH"},
    )
    assert r.status_code == 404


def test_merge_unknown_target_404(client):
    c, _ = client
    r = c.post(
        "/api/projects/NO_SUCH/versions/from-project",
        json={"source_project_id": "SOURCE"},
    )
    assert r.status_code == 404


def test_merge_v3_after_v2(client):
    c, projects_dir = client
    # V2 из SOURCE → исходник удалён
    r1 = c.post(
        "/api/projects/TARGET/versions/from-project",
        json={"source_project_id": "SOURCE"},
    )
    assert r1.status_code == 200

    # Подкладываем второй source-проект
    src2 = projects_dir / "SRC2"
    (src2 / "_output").mkdir(parents=True)
    (src2 / "project_info.json").write_text(
        json.dumps({"project_id": "SRC2", "name": "SRC2", "section": "KJ", "pdf_file": "src2.pdf"}, ensure_ascii=False),
        encoding="utf-8",
    )
    (src2 / "src2.pdf").write_bytes(_PDF_SRC + b"V3")

    r2 = c.post(
        "/api/projects/TARGET/versions/from-project",
        json={"source_project_id": "SRC2"},
    )
    assert r2.status_code == 200
    body = r2.json()
    assert body["version_id"] == "v3"
    assert body["versions_summary"]["latest_version_id"] == "v3"
    assert (projects_dir / "TARGET" / "_versions" / "v3" / "src2.pdf").exists()


def test_merge_source_without_pdf_rejected(client, projects_dir):
    """Source без PDF не может стать версией."""
    c, _ = client
    # Подменяем SOURCE: удаляем PDF
    (projects_dir / "SOURCE" / "src.pdf").unlink()
    r = c.post(
        "/api/projects/TARGET/versions/from-project",
        json={"source_project_id": "SOURCE"},
    )
    assert r.status_code == 400
    # SOURCE не удалён (merge не сработал)
    assert (projects_dir / "SOURCE").exists()


# ─── Переиспользование пустой latest-версии ────────────────────────────────


def test_merge_reuses_empty_v2_instead_of_creating_v3(client, projects_dir):
    """Если у target уже есть пустая V2 — merge кладёт файлы в неё, без V3."""
    c, _ = client
    # Создаём пустую V2 у TARGET через прямой endpoint
    r0 = c.post("/api/projects/TARGET/versions", json={"comment": "manual V2"})
    assert r0.status_code == 200
    assert r0.json()["latest_version_id"] == "v2"
    # V2 пуста
    v2 = projects_dir / "TARGET" / "_versions" / "v2"
    assert v2.exists()
    assert not (v2 / "src.pdf").exists()

    # Привязываем SOURCE
    r = c.post(
        "/api/projects/TARGET/versions/from-project",
        json={"source_project_id": "SOURCE"},
    )
    assert r.status_code == 200, r.text
    body = r.json()
    # Должна быть переиспользована V2, а НЕ создана V3
    assert body["version_id"] == "v2", body
    assert body["reused_empty_latest"] is True
    assert body["versions_summary"]["latest_version_id"] == "v2"
    assert body["versions_summary"]["version_count"] == 2

    # Файлы попали в V2
    assert (v2 / "src.pdf").read_bytes() == _PDF_SRC
    assert (v2 / "src.md").read_bytes() == _MD_SRC

    # V3 НЕ создана
    assert not (projects_dir / "TARGET" / "_versions" / "v3").exists()

    # Source удалён
    assert not (projects_dir / "SOURCE").exists()


def test_flat_endpoint_merge_with_slash_project_id(client, projects_dir):
    """Flat-endpoint /versions/from-project работает с project_id со слешами.

    Воспроизводит ситуацию production: project_id = 'KJ/M31A'. URL-form
    `/api/projects/{p:path}/versions/from-project` после encodeURIComponent
    даёт `%2F`, что блокируется Cloudflare. Flat-endpoint решает это.
    """
    c, projects_dir = client
    # Подкладываем проекты с project_id со слешами: KJ/TGT и KJ/SRC
    kj = projects_dir / "KJ"
    kj.mkdir()
    tgt2 = kj / "TGT2"
    (tgt2 / "_output").mkdir(parents=True)
    (tgt2 / "project_info.json").write_text(
        json.dumps({"project_id": "KJ/TGT2", "name": "TGT2", "section": "KJ", "pdf_file": "doc.pdf"}, ensure_ascii=False),
        encoding="utf-8",
    )
    (tgt2 / "doc.pdf").write_bytes(_PDF_BYTES)

    src2 = kj / "SRC2"
    (src2 / "_output").mkdir(parents=True)
    (src2 / "project_info.json").write_text(
        json.dumps({"project_id": "KJ/SRC2", "name": "SRC2", "section": "KJ", "pdf_file": "src2.pdf"}, ensure_ascii=False),
        encoding="utf-8",
    )
    (src2 / "src2.pdf").write_bytes(_PDF_SRC)

    # Сбросим кеш проектов
    import backend.app.services.common.project_service as ps
    ps._PROJECT_DIRS_CACHE = []
    ps._PROJECT_DIRS_CACHE_TIME = 0.0

    r = c.post(
        "/api/projects/versions/from-project",
        json={"target_project_id": "KJ/TGT2", "source_project_id": "KJ/SRC2"},
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["version_id"] == "v2"
    assert "src2.pdf" in body["saved"]
    assert (tgt2 / "_versions" / "v2" / "src2.pdf").exists()
    # Source удалён
    assert not src2.exists()


def test_flat_endpoint_from_candidate_with_slash(client, projects_dir):
    """Flat-endpoint /versions/from-candidate тоже работает с slash-project_id."""
    c, projects_dir = client
    kj = projects_dir / "KJ"
    kj.mkdir(exist_ok=True)
    tgt3 = kj / "TGT3"
    (tgt3 / "_output").mkdir(parents=True)
    (tgt3 / "project_info.json").write_text(
        json.dumps({"project_id": "KJ/TGT3", "name": "TGT3", "section": "KJ", "pdf_file": "doc.pdf"}, ensure_ascii=False),
        encoding="utf-8",
    )
    (tgt3 / "doc.pdf").write_bytes(_PDF_BYTES)

    # Candidate-файл в projects/
    cand = projects_dir / "candidate3.pdf"
    cand.write_bytes(_PDF_SRC + b"C3")

    import backend.app.services.common.project_service as ps
    ps._PROJECT_DIRS_CACHE = []
    ps._PROJECT_DIRS_CACHE_TIME = 0.0

    r = c.post(
        "/api/projects/versions/from-candidate",
        json={
            "target_project_id": "KJ/TGT3",
            "candidate_pdf_path": str(cand),
        },
    )
    assert r.status_code == 200, r.text
    assert r.json()["version_id"] == "v2"
    assert (tgt3 / "_versions" / "v2" / "candidate3.pdf").exists()


def test_merge_creates_new_version_when_latest_not_empty(client, projects_dir):
    """Если latest у target уже заполнена — создаётся новая версия."""
    c, _ = client
    # V2 у TARGET, заполняем PDF
    r0 = c.post("/api/projects/TARGET/versions", json={})
    assert r0.status_code == 200
    import io
    r0b = c.post(
        "/api/projects/TARGET/versions/v2/files",
        files=[("files", ("doc.pdf", io.BytesIO(_PDF_BYTES), "application/pdf"))],
    )
    assert r0b.status_code == 200, r0b.text

    # Привязываем SOURCE — должна появиться V3
    r = c.post(
        "/api/projects/TARGET/versions/from-project",
        json={"source_project_id": "SOURCE"},
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["version_id"] == "v3"
    assert body["reused_empty_latest"] is False
    # V2 не тронута — там по-прежнему doc.pdf, а не src.pdf
    v2_files = sorted(p.name for p in (projects_dir / "TARGET" / "_versions" / "v2").iterdir() if p.is_file())
    assert "doc.pdf" in v2_files
    assert "src.pdf" not in v2_files


# ─── Source `_output` guard: запрещаем merge без discard_source_output ─────


def test_merge_rejects_source_with_audit_artifacts_without_flag(client, projects_dir):
    """Если source имеет непустой _output — без discard_source_output это 409."""
    c, _ = client
    # Кладём findings в source — имитация уже обработанного проекта
    (projects_dir / "SOURCE" / "_output" / "03_findings.json").write_text(
        json.dumps({"findings": [{"id": "F-SRC"}]}), encoding="utf-8",
    )
    r = c.post(
        "/api/projects/TARGET/versions/from-project",
        json={"source_project_id": "SOURCE"},
    )
    assert r.status_code == 409, r.text
    detail = r.json()["detail"]
    assert isinstance(detail, dict), detail
    assert detail["code"] == "source_output_not_empty"
    assert detail["needs_flag"] == "discard_source_output"
    # Source не удалён, V2 не создана
    assert (projects_dir / "SOURCE").exists()
    assert not (projects_dir / "TARGET" / "_versions" / "v2").exists()


def test_merge_allows_source_with_artifacts_when_discard_flag_set(client, projects_dir):
    """С `discard_source_output=True` source мерджится, _output теряется."""
    c, _ = client
    (projects_dir / "SOURCE" / "_output" / "03_findings.json").write_text(
        json.dumps({"findings": [{"id": "F-SRC"}]}), encoding="utf-8",
    )
    r = c.post(
        "/api/projects/TARGET/versions/from-project",
        json={"source_project_id": "SOURCE", "discard_source_output": True},
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["version_id"] == "v2"
    # Source удалён, V2 _output пуст
    assert not (projects_dir / "SOURCE").exists()
    v2 = projects_dir / "TARGET" / "_versions" / "v2"
    assert (v2 / "src.pdf").exists()
    assert not (v2 / "_output" / "03_findings.json").exists()


def test_merge_allows_source_with_empty_output(client):
    """`_output` существует, но пуст → разрешено без discard."""
    c, projects_dir = client
    # Фикстурный SOURCE/_output уже пуст
    assert (projects_dir / "SOURCE" / "_output").exists()
    assert not any((projects_dir / "SOURCE" / "_output").iterdir())
    r = c.post(
        "/api/projects/TARGET/versions/from-project",
        json={"source_project_id": "SOURCE"},
    )
    assert r.status_code == 200, r.text


# ─── Target active audit guard ──────────────────────────────────────────────


def test_merge_rejects_when_target_audit_running(client, projects_dir, monkeypatch):
    """Если target сейчас в активном аудите — flat и path endpoint отдают 409."""
    c, _ = client
    from backend.app.pipeline import manager as pipeline_mod

    calls = []
    def fake_is_running(pid):
        calls.append(pid)
        return pid == "TARGET"

    monkeypatch.setattr(pipeline_mod.pipeline_manager, "is_running", fake_is_running)

    # Path endpoint
    r1 = c.post(
        "/api/projects/TARGET/versions/from-project",
        json={"source_project_id": "SOURCE"},
    )
    assert r1.status_code == 409, r1.text
    assert "находится в обработке" in r1.json()["detail"]

    # Flat endpoint
    r2 = c.post(
        "/api/projects/versions/from-project",
        json={"target_project_id": "TARGET", "source_project_id": "SOURCE"},
    )
    assert r2.status_code == 409, r2.text
    assert "находится в обработке" in r2.json()["detail"]

    # Никакой merge не произошёл
    assert (projects_dir / "SOURCE").exists()
    assert not (projects_dir / "TARGET" / "_versions" / "v2").exists()


def test_merge_rejects_when_source_audit_running_still_works(client, projects_dir, monkeypatch):
    """Sanity-check: source-guard остался на месте после правок."""
    c, _ = client
    from backend.app.pipeline import manager as pipeline_mod
    monkeypatch.setattr(
        pipeline_mod.pipeline_manager, "is_running",
        lambda pid: pid == "SOURCE",
    )
    r = c.post(
        "/api/projects/TARGET/versions/from-project",
        json={"source_project_id": "SOURCE"},
    )
    assert r.status_code == 409
    assert "Аудит source" in r.json()["detail"]


# ─── Кеш списка проектов инвалидируется после merge ─────────────────────────


def test_merge_invalidates_project_dirs_cache(client, projects_dir):
    """После merge `/api/projects` не показывает удалённый source.

    Без явной инвалидации TTL-кеш `_PROJECT_DIRS_CACHE` держал бы SOURCE до
    30 секунд (см. project_service.iter_project_dirs). После фикса
    `merge_project_as_version` сам вызывает `invalidate_project_cache`.
    """
    c, _ = client
    import backend.app.services.common.project_service as ps

    # «Прогреваем» кеш — он должен содержать SOURCE и TARGET
    r0 = c.get("/api/projects")
    assert r0.status_code == 200
    ids_before = {p["project_id"] for p in r0.json()["projects"]}
    assert "SOURCE" in ids_before
    assert "TARGET" in ids_before

    # До merge кеш не пуст
    assert ps._PROJECT_DIRS_CACHE, "Кеш должен быть прогрет"

    r = c.post(
        "/api/projects/TARGET/versions/from-project",
        json={"source_project_id": "SOURCE"},
    )
    assert r.status_code == 200, r.text

    # Кеш сброшен (mtime=0 → следующий GET перестроит)
    assert ps._PROJECT_DIRS_CACHE == []
    assert ps._PROJECT_DIRS_CACHE_TIME == 0.0

    # GET /api/projects сразу видит актуальное состояние без TTL ожидания
    r2 = c.get("/api/projects")
    ids_after = {p["project_id"] for p in r2.json()["projects"]}
    assert "SOURCE" not in ids_after
    assert "TARGET" in ids_after


def test_invalidate_project_cache_helper():
    """`invalidate_project_cache()` сбрасывает оба внутренних поля кеша."""
    import backend.app.services.common.project_service as ps
    ps._PROJECT_DIRS_CACHE = [("X", Path("/tmp/X"))]
    ps._PROJECT_DIRS_CACHE_TIME = 12345.0
    ps.invalidate_project_cache()
    assert ps._PROJECT_DIRS_CACHE == []
    assert ps._PROJECT_DIRS_CACHE_TIME == 0.0


# ─── has_audit_artifacts ────────────────────────────────────────────────────


def test_has_audit_artifacts_detects_files_and_subdirs(tmp_path):
    from backend.app.services.common.version_service import has_audit_artifacts
    proj = tmp_path / "p"
    proj.mkdir()
    assert has_audit_artifacts(proj) is False  # нет _output

    (proj / "_output").mkdir()
    assert has_audit_artifacts(proj) is False  # _output пуст

    # Пустой файл (0 байт) тоже не считается артефактом
    (proj / "_output" / "empty.txt").write_text("")
    assert has_audit_artifacts(proj) is False

    # Непустой файл → артефакт
    (proj / "_output" / "03_findings.json").write_text("{}")
    assert has_audit_artifacts(proj) is True

    # Чистим, проверяем поддиректорию
    (proj / "_output" / "03_findings.json").unlink()
    (proj / "_output" / "empty.txt").unlink()
    sub = proj / "_output" / "blocks_gemma_100"
    sub.mkdir()
    assert has_audit_artifacts(proj) is False  # пустая подпапка
    (sub / "page_1.png").write_text("PNG")
    assert has_audit_artifacts(proj) is True
