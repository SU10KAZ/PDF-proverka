"""
test_pre_cutover_fixes.py
-------------------------
Регресс-тесты для двух pre-cutover дефектов, найденных по итогам Pass B
(2026-05-14):

A. prepare/runner.py передавал `--quality standard` в process_project.py,
   который этот аргумент не понимает (argparse → exit 2). Endpoint
   /api/audit/{id}/prepare на backend.app.main падал.
   Fix: убрать `--quality` из argv. Smart-pipeline priority_pages branch
   получает controlled failure (раньше тихо падал на subprocess exit 2).

B. GET/DELETE /api/audit/{id}/log endpoints не принимали `version_id`.
   Для V2 audit пользователь видел V1-лог (опасно: создаёт впечатление,
   что V2 что-то делал). Fix: query parameter version_id с контрактом
   resolve_version_output_dir + 404 на unknown.

Run:
    python -m pytest tests/test_pre_cutover_fixes.py -v
"""
from __future__ import annotations

import json
import sys
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))


# ─── Fixtures ───────────────────────────────────────────────────────────


def _write_pdf(p: Path) -> None:
    p.write_bytes(b"%PDF-1.4\nfake\n%%EOF\n")


def _write_md(p: Path, body: str = "## STR 1\n\n[TEXT b1]\n\nMD body.\n") -> None:
    p.write_text(body, encoding="utf-8")


@pytest.fixture
def v1_v2_with_logs(tmp_path, monkeypatch):
    """V1 + V2 проект с distinct audit_log.jsonl в обоих _output."""
    projects_dir = tmp_path / "projects"
    projects_dir.mkdir()
    pdir = projects_dir / "M31A"
    (pdir / "_output").mkdir(parents=True)
    (pdir / "project_info.json").write_text(
        json.dumps({
            "project_id": "M31A", "name": "M31A", "section": "EOM",
            "pdf_file": "doc.pdf", "md_file": "v1_document.md",
        }, ensure_ascii=False),
        encoding="utf-8",
    )
    _write_pdf(pdir / "doc.pdf")
    _write_md(pdir / "v1_document.md", "V1 MD content")

    # V1 sentinel log
    v1_log = pdir / "_output" / "audit_log.jsonl"
    v1_log.write_text(
        json.dumps({"v": "V1", "stage": "prepare", "message": "V1 entry"}) + "\n",
        encoding="utf-8",
    )

    import backend.app.services.common.project_service as ps
    monkeypatch.setattr(ps, "_get_projects_dir", lambda: projects_dir)
    monkeypatch.setattr(ps, "_PROJECT_DIRS_CACHE", [])
    monkeypatch.setattr(ps, "_PROJECT_DIRS_CACHE_TIME", 0.0)
    monkeypatch.setattr(ps, "_document_cache", {})

    from backend.app.services.common import version_service
    version_service.create_next_version(
        pdir, "M31A", source="manual", status="new", comment="V2",
    )
    v2_dir = pdir / "_versions" / "v2"
    _write_pdf(v2_dir / "v2.pdf")
    _write_md(v2_dir / "v2_document.md", "V2 MD content")
    v2_info_path = v2_dir / "project_info.json"
    v2_info = json.loads(v2_info_path.read_text(encoding="utf-8"))
    v2_info["md_file"] = "v2_document.md"
    v2_info["pdf_file"] = "v2.pdf"
    v2_info_path.write_text(json.dumps(v2_info, ensure_ascii=False), encoding="utf-8")

    # V2 distinct log (in _versions/v2/_output/audit_log.jsonl)
    (v2_dir / "_output").mkdir(parents=True, exist_ok=True)
    v2_log = v2_dir / "_output" / "audit_log.jsonl"
    v2_log.write_text(
        json.dumps({"v": "V2", "stage": "prepare", "message": "V2 entry"}) + "\n",
        encoding="utf-8",
    )

    return projects_dir, pdir, v2_dir, v1_log, v2_log


# ─── A. prepare/runner.py не передаёт --quality ─────────────────────────


@pytest.mark.asyncio
async def test_prepare_runner_does_not_pass_quality_arg():
    """run_prepare должен вызывать process_project.py с argv=[project_rel],
    без --quality (process_project.py его не понимает → exit 2)."""
    from backend.app.pipeline.stages.prepare.runner import run_prepare
    from backend.app.pipeline.context import PipelineStageContext

    captured = {}

    async def _fake_subprocess(*args, **kwargs):
        captured["script"] = args[0]
        captured["argv"] = args[1] if len(args) >= 2 else []
        return (0, "OK", "")

    async def _noop_log(*a, **k):
        pass

    def _noop_update(*a, **k):
        pass

    ctx = PipelineStageContext(
        project_dir=Path("/tmp/fake"),
        project_id="X",
        output_dir=Path("/tmp/fake/_output"),
        log=_noop_log,
        check_before_launch=AsyncMock(return_value=True),
        check_pause=AsyncMock(return_value=True),
        wait_for_rate_limit=AsyncMock(return_value=True),
        record_cli_usage=lambda *a, **k: None,
        update_pipeline_log=_noop_update,
        run_subprocess=_fake_subprocess,
    )

    result = await run_prepare(ctx)
    assert result.success
    argv = captured["argv"]
    assert "--quality" not in argv, (
        f"run_prepare всё ещё передаёт --quality в process_project.py: argv={argv}"
    )
    assert "--pages" not in argv
    # Должен быть только path к проекту.
    assert len(argv) == 1


def test_prepare_runner_imports_no_default_tile_quality():
    """Регрессия: prepare/runner.py больше не импортирует DEFAULT_TILE_QUALITY."""
    src = (_ROOT / "backend" / "app" / "pipeline" / "stages" / "prepare" / "runner.py").read_text(
        encoding="utf-8"
    )
    assert "DEFAULT_TILE_QUALITY" not in src, (
        "prepare/runner.py всё ещё импортирует/использует DEFAULT_TILE_QUALITY"
    )


def test_manager_smart_pipeline_priority_pages_is_controlled_failure():
    """Регрессия: smart-pipeline priority_pages branch не должен вызывать
    process_project.py с --pages (это не работает). Он должен делать
    controlled failure через RuntimeError."""
    src = (_ROOT / "backend" / "app" / "pipeline" / "manager.py").read_text(
        encoding="utf-8"
    )
    # Старый паттерн запуска subprocess с --pages для PROCESS_PROJECT_SCRIPT
    # больше не должен встречаться.
    bad_pattern = (
        '[self._project_path_for_job(job), "--pages", pages_str, '
        '"--quality", DEFAULT_TILE_QUALITY]'
    )
    assert bad_pattern not in src, (
        "smart-pipeline всё ещё передаёт --pages/--quality в process_project.py"
    )
    # И сам импорт DEFAULT_TILE_QUALITY должен быть удалён.
    assert "DEFAULT_TILE_QUALITY" not in src


# ─── B. GET/DELETE /log version-aware ───────────────────────────────────


@pytest.fixture
def backend_client(v1_v2_with_logs, monkeypatch):
    """TestClient для backend.app.main:app."""
    # Очищаем bind_version, если предыдущий тест оставил его.
    from backend.app.services.common import version_service
    try:
        version_service.unbind_version(version_service.bind_version(None))
    except Exception:
        pass
    from fastapi.testclient import TestClient
    from backend.app.main import app
    return TestClient(app)


def test_get_log_v1_returns_v1_entry(backend_client, v1_v2_with_logs):
    """GET /log?version_id=v1 → V1 entry."""
    r = backend_client.get("/api/audit/M31A/log", params={"version_id": "v1"})
    assert r.status_code == 200
    data = r.json()
    assert data["total"] == 1
    assert data["entries"][0]["v"] == "V1"


def test_get_log_v2_returns_v2_entry_not_v1(backend_client, v1_v2_with_logs):
    """GET /log?version_id=v2 → V2 entry. Не должно показать V1."""
    r = backend_client.get("/api/audit/M31A/log", params={"version_id": "v2"})
    assert r.status_code == 200
    data = r.json()
    assert data["total"] == 1
    assert data["entries"][0]["v"] == "V2", (
        f"V2 log запрос вернул не V2 entry: {data['entries']}"
    )


def test_get_log_v2_empty_does_not_fallback_to_v1(backend_client, v1_v2_with_logs):
    """Если в V2 нет audit_log.jsonl — GET вернёт пустой результат, НЕ V1."""
    _projects_dir, _pdir, v2_dir, _v1_log, v2_log = v1_v2_with_logs
    v2_log.unlink()
    r = backend_client.get("/api/audit/M31A/log", params={"version_id": "v2"})
    assert r.status_code == 200
    data = r.json()
    assert data["total"] == 0
    assert data["entries"] == []


def test_get_log_unknown_version_404(backend_client, v1_v2_with_logs):
    r = backend_client.get("/api/audit/M31A/log", params={"version_id": "v999"})
    assert r.status_code == 404


def test_delete_log_v2_does_not_touch_v1(backend_client, v1_v2_with_logs):
    """DELETE /log?version_id=v2 → V2 log удаляется, V1 log остаётся."""
    _projects_dir, _pdir, _v2_dir, v1_log, v2_log = v1_v2_with_logs
    v1_before = v1_log.read_bytes()

    r = backend_client.delete("/api/audit/M31A/log", params={"version_id": "v2"})
    assert r.status_code == 200
    assert r.json() == {"status": "ok"}

    # V2 log удалён, V1 log жив с тем же содержимым
    assert not v2_log.exists()
    assert v1_log.exists()
    assert v1_log.read_bytes() == v1_before


def test_delete_log_v1_does_not_touch_v2(backend_client, v1_v2_with_logs):
    """DELETE /log?version_id=v1 → V1 удаляется, V2 остаётся."""
    _projects_dir, _pdir, _v2_dir, v1_log, v2_log = v1_v2_with_logs
    v2_before = v2_log.read_bytes()

    r = backend_client.delete("/api/audit/M31A/log", params={"version_id": "v1"})
    assert r.status_code == 200
    assert not v1_log.exists()
    assert v2_log.exists()
    assert v2_log.read_bytes() == v2_before


def test_delete_log_unknown_version_404(backend_client, v1_v2_with_logs):
    r = backend_client.delete("/api/audit/M31A/log", params={"version_id": "v999"})
    assert r.status_code == 404


def test_get_log_no_version_id_picks_latest(backend_client, v1_v2_with_logs):
    """Без version_id endpoint должен выбрать latest version (V2)."""
    r = backend_client.get("/api/audit/M31A/log")
    assert r.status_code == 200
    data = r.json()
    # Latest = V2 (создана в fixture)
    assert data["total"] == 1
    assert data["entries"][0]["v"] == "V2"


# ─── B'. Регрессия: webapp /log тоже version-aware ──────────────────────


@pytest.fixture
def legacy_webapp_client(v1_v2_with_logs):
    """TestClient для legacy webapp.main:app."""
    from backend.app.services.common import version_service
    try:
        version_service.unbind_version(version_service.bind_version(None))
    except Exception:
        pass
    from fastapi.testclient import TestClient
    from webapp.main import app
    return TestClient(app)


def test_legacy_webapp_get_log_v2_does_not_show_v1(legacy_webapp_client, v1_v2_with_logs):
    """Legacy webapp GET /log?version_id=v2 → V2 entry, не V1 fallback."""
    r = legacy_webapp_client.get("/api/audit/M31A/log", params={"version_id": "v2"})
    assert r.status_code == 200
    data = r.json()
    assert data["total"] == 1
    assert data["entries"][0]["v"] == "V2"


def test_legacy_webapp_delete_log_v2_does_not_touch_v1(legacy_webapp_client, v1_v2_with_logs):
    _projects_dir, _pdir, _v2_dir, v1_log, v2_log = v1_v2_with_logs
    v1_before = v1_log.read_bytes()
    r = legacy_webapp_client.delete(
        "/api/audit/M31A/log", params={"version_id": "v2"},
    )
    assert r.status_code == 200
    assert not v2_log.exists()
    assert v1_log.read_bytes() == v1_before
