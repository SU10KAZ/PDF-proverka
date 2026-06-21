"""reserc.md #10 — crash-recovery version-aware (сканирует все версии контейнера).

Раньше _recover_stale_pipelines брал только primary _output → V2-аудит оставался
вечным 'running' после рестарта. Теперь сканируются и сиблинги контейнера (main).
"""
from __future__ import annotations

import json

from backend.app.pipeline import manager as mgr
from backend.app.services.common import project_service, version_service


def _write_log(vdir, status):
    (vdir / "_output").mkdir(parents=True, exist_ok=True)
    (vdir / "_output" / "pipeline_log.json").write_text(
        json.dumps({"stages": {"block_analysis": {"status": status}}}),
        encoding="utf-8",
    )


def _read_status(vdir):
    d = json.loads((vdir / "_output" / "pipeline_log.json").read_text(encoding="utf-8"))
    return d["stages"]["block_analysis"]["status"]


def test_recover_scans_version_siblings(monkeypatch, tmp_path):
    container = tmp_path / "base(main)"
    v1 = container / "base"           # primary
    v2 = container / "base V2"        # сиблинг-версия
    _write_log(v1, "done")
    _write_log(v2, "running")         # завис после рестарта

    # iter_project_dirs отдаёт только primary (как в проде); is_version_container
    # помечает наш контейнер.
    monkeypatch.setattr(project_service, "iter_project_dirs", lambda *a, **k: [("base", v1)])
    monkeypatch.setattr(version_service, "is_version_container", lambda p: p == container)
    monkeypatch.setattr(mgr.pipeline_manager, "active_jobs", {})

    mgr.pipeline_manager._recover_stale_pipelines()

    assert _read_status(v2) == "interrupted"   # V2-сиблинг восстановлен (#10)
    assert _read_status(v1) == "done"          # завершённый этап не трогаем


def test_recover_skips_active_container(monkeypatch, tmp_path):
    container = tmp_path / "base(main)"
    v1 = container / "base"
    v2 = container / "base V2"
    _write_log(v1, "running")
    _write_log(v2, "running")

    monkeypatch.setattr(project_service, "iter_project_dirs", lambda *a, **k: [("base", v1)])
    monkeypatch.setattr(version_service, "is_version_container", lambda p: p == container)
    # base — активный аудит → не трогаем ни одну версию контейнера
    monkeypatch.setattr(mgr.pipeline_manager, "active_jobs", {"base": object()})

    mgr.pipeline_manager._recover_stale_pipelines()

    assert _read_status(v1) == "running"   # активный — не тронут
    assert _read_status(v2) == "running"
