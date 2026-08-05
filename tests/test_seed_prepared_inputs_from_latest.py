"""Полный запуск аудита переиспользует работу «Подготовки данных».

В projects_v2-primary аудит исполняется в свежей `03_analysis/runs/<job_id>`,
а подготовка пишет кропы и контекст блоков в `03_analysis/latest`. Без
переноса запуск заново качал кропы и заново собирал контекст.

Переносить можно СТРОГО артефакты подготовки: каталог кропов и каноническую
сводку контекста. Всё остальное (findings и прочие результаты прошлого аудита)
обязано остаться в latest, иначе полный прогон подхватит чужие результаты.
"""
from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from backend.app.pipeline.manager import PipelineManager
from backend.app.pipeline.stages.block_context.contract import (
    BLOCK_CONTEXT_SUMMARY_FILENAME,
)
from backend.app.pipeline.stages.gemma_enrichment.gemma_enrichment_contract import (
    STAGE02_BLOCKS_DIRNAME,
)


@pytest.fixture
def layout(tmp_path):
    version_dir = tmp_path / "versions" / "v001"
    latest = version_dir / "03_analysis" / "latest"
    run_dir = version_dir / "03_analysis" / "runs" / "job-1"
    blocks = latest / STAGE02_BLOCKS_DIRNAME
    blocks.mkdir(parents=True)
    (blocks / "index.json").write_text(json.dumps({
        "blocks": [{"block_id": "AAA", "file": "block_AAA.png", "page": 1}],
    }), encoding="utf-8")
    (blocks / "block_AAA.png").write_bytes(b"\x89PNG\r\n\x1a\n" + b"0" * 2048)
    (latest / BLOCK_CONTEXT_SUMMARY_FILENAME).write_text('{"stage": "block_context"}', encoding="utf-8")
    (latest / "03_findings.json").write_text('{"findings": ["старый аудит"]}', encoding="utf-8")
    return SimpleNamespace(version_dir=version_dir, latest=latest, run_dir=run_dir)


def _seed(monkeypatch, layout, *, v2_primary: bool = True):
    from backend.app.services.storage import storage_write_facade as swf

    monkeypatch.setattr(swf, "v2_is_primary", lambda: v2_primary)
    manager = PipelineManager.__new__(PipelineManager)
    monkeypatch.setattr(
        PipelineManager,
        "_resolve_job_paths",
        lambda self, job: (layout.version_dir, layout.version_dir, layout.run_dir),
    )
    job = SimpleNamespace(project_id="TEST/PID", version_id="v001", job_id="job-1")
    return manager._seed_prepared_inputs_from_latest(job)


def test_crops_and_context_are_reused(monkeypatch, layout):
    seeded = _seed(monkeypatch, layout)

    run_blocks = layout.run_dir / STAGE02_BLOCKS_DIRNAME
    assert seeded == {"index": 1, "png": 1, "summary": 1}
    assert (run_blocks / "index.json").is_file()
    assert (run_blocks / "block_AAA.png").is_file()
    assert (layout.run_dir / BLOCK_CONTEXT_SUMMARY_FILENAME).is_file()


def test_png_is_hardlinked_not_duplicated(monkeypatch, layout):
    _seed(monkeypatch, layout)

    src = layout.latest / STAGE02_BLOCKS_DIRNAME / "block_AAA.png"
    dst = layout.run_dir / STAGE02_BLOCKS_DIRNAME / "block_AAA.png"
    assert src.stat().st_ino == dst.stat().st_ino, "кропы должны переноситься ссылкой, а не копией"


def test_audit_results_are_not_seeded(monkeypatch, layout):
    _seed(monkeypatch, layout)

    assert not (layout.run_dir / "03_findings.json").exists(), (
        "результаты прошлого аудита не должны попадать в новый прогон"
    )


def test_existing_run_files_are_kept(monkeypatch, layout):
    run_blocks = layout.run_dir / STAGE02_BLOCKS_DIRNAME
    run_blocks.mkdir(parents=True)
    (run_blocks / "index.json").write_text('{"blocks": []}', encoding="utf-8")

    seeded = _seed(monkeypatch, layout)

    assert seeded["index"] == 0
    assert json.loads((run_blocks / "index.json").read_text(encoding="utf-8")) == {"blocks": []}


def test_legacy_mode_is_untouched(monkeypatch, layout):
    seeded = _seed(monkeypatch, layout, v2_primary=False)

    assert seeded == {"index": 0, "png": 0, "summary": 0}
    assert not layout.run_dir.exists()
