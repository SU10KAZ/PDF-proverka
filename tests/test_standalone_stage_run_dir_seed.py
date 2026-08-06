"""
test_standalone_stage_run_dir_seed.py
-------------------------------------
Одиночный этап должен видеть артефакты выполненного аудита.

Инцидент 06.08.2026: POST /api/audit/{id}/verify-norms по V2-проекту отвечал
«Файл 03_findings.json не найден. Сначала выполните основной аудит», хотя аудит
был выполнен часом ранее. Причина не в резолве проекта, а в раскладке
projects_v2: каждое задание исполняется в свежей `03_analysis/runs/<job_id>`, а
готовые артефакты лежат в `03_analysis/latest`. Для resume засев уже был (баг
B1), для одиночных этапов — нет.

Тот же класс задевал и пересмотр оптимизации: «optimization.json не найден».

Run: python -m pytest tests/test_standalone_stage_run_dir_seed.py -v
"""
from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from backend.app.pipeline.manager import PipelineManager  # noqa: E402


class _Job:
    project_id = "KJ/ТЕСТ-ПРОЕКТ_V1"
    version_id = "v001"
    job_id = "run-new"


@pytest.fixture()
def version_dir(tmp_path):
    """Раскладка v2: latest с артефактами аудита + пустой свежий run."""
    analysis = tmp_path / "03_analysis"
    latest = analysis / "latest"
    latest.mkdir(parents=True)
    (latest / "03_findings.json").write_text(
        json.dumps({"findings": [{"id": "F-001", "norm": "ГОСТ 21.110-2013"}]}, ensure_ascii=False),
        encoding="utf-8",
    )
    (latest / "optimization.json").write_text('{"items": []}', encoding="utf-8")
    (latest / "norm_checks.json").write_text('{"checks": []}', encoding="utf-8")
    (latest / "pipeline_log.json").write_text('{"stages": {"norm_verify": {"status": "error"}}}', encoding="utf-8")
    (latest / "audit_log.jsonl").write_text('{"message": "прошлый прогон"}\n', encoding="utf-8")
    (latest / "blocks_stage02_100").mkdir()
    (latest / "blocks_stage02_100" / "index.json").write_text("[]", encoding="utf-8")
    (latest / "blocks_stage02_100" / "block_1.png").write_bytes(b"PNG")
    (analysis / "runs" / "run-new").mkdir(parents=True)
    return tmp_path


@pytest.fixture()
def manager(monkeypatch, version_dir):
    mgr = PipelineManager.__new__(PipelineManager)
    output_dir = version_dir / "03_analysis" / "runs" / "run-new"
    monkeypatch.setattr(
        PipelineManager, "_resolve_job_paths",
        lambda self, job: (version_dir, version_dir, output_dir),
        raising=False,
    )
    logged: list[str] = []

    async def _log(self, job, msg, level="info", **kw):
        logged.append(msg)

    monkeypatch.setattr(PipelineManager, "_log", _log, raising=False)
    mgr._logged = logged
    return mgr


def _seed(mgr, reason="Верификация норм"):
    return asyncio.run(mgr._seed_run_dir_from_latest(_Job(), reason=reason))


def test_findings_become_visible_to_standalone_stage(manager, version_dir):
    run_dir = version_dir / "03_analysis" / "runs" / "run-new"
    assert not (run_dir / "03_findings.json").exists()

    seeded = _seed(manager)

    assert seeded >= 1
    data = json.loads((run_dir / "03_findings.json").read_text(encoding="utf-8"))
    assert data["findings"][0]["id"] == "F-001"


def test_optimization_visible_too(manager, version_dir):
    """Тот же засев чинит «optimization.json не найден» у пересмотра."""
    _seed(manager, reason="Пересмотр оптимизации")
    assert (version_dir / "03_analysis/runs/run-new/optimization.json").exists()


def test_per_run_files_are_not_seeded(manager, version_dir):
    """Лог и статус прошлого прогона переносить нельзя — иначе UI покажет
    чужую ошибку как свою."""
    _seed(manager)
    run_dir = version_dir / "03_analysis" / "runs" / "run-new"
    assert not (run_dir / "pipeline_log.json").exists()
    assert not (run_dir / "audit_log.jsonl").exists()


def test_block_index_seeded_without_heavy_png(manager, version_dir):
    _seed(manager)
    blocks = version_dir / "03_analysis/runs/run-new/blocks_stage02_100"
    assert (blocks / "index.json").exists()
    assert not (blocks / "block_1.png").exists()


def test_existing_files_are_never_overwritten(manager, version_dir):
    run_dir = version_dir / "03_analysis" / "runs" / "run-new"
    (run_dir / "03_findings.json").write_text('{"findings": [{"id": "СВЕЖЕЕ"}]}', encoding="utf-8")

    _seed(manager)

    data = json.loads((run_dir / "03_findings.json").read_text(encoding="utf-8"))
    assert data["findings"][0]["id"] == "СВЕЖЕЕ"


def test_no_seed_when_run_dir_is_latest(manager, monkeypatch, version_dir):
    """В составе конвейера output_dir и есть latest — копировать нечего."""
    latest = version_dir / "03_analysis" / "latest"
    monkeypatch.setattr(
        PipelineManager, "_resolve_job_paths",
        lambda self, job: (version_dir, version_dir, latest),
        raising=False,
    )
    assert _seed(manager) == 0


def test_missing_latest_is_soft(manager, monkeypatch, tmp_path):
    """Проект без прошлых прогонов не должен ронять этап."""
    empty = tmp_path / "пусто"
    (empty / "03_analysis" / "runs" / "run-new").mkdir(parents=True)
    monkeypatch.setattr(
        PipelineManager, "_resolve_job_paths",
        lambda self, job: (empty, empty, empty / "03_analysis" / "runs" / "run-new"),
        raising=False,
    )
    assert _seed(manager) == 0
