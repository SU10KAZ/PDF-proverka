"""reserc.md #9 — crop partial-success.

exit_code==2 (часть crop_url отдала HTTP 404):
- если index.json есть → продолжаем с доступными блоками (partial-success),
  пропуски попадут в coverage;
- если index.json нет (не скачалось ничего) → hard-fail.

Раньше exit_code==2 всегда был hard-fail → весь аудит падал из-за пары
404-блоков. Stage 02 crop уже вёл себя как partial; теперь Gemma base crop и
перекроп по policy — тоже (partial-success везде).
"""
from __future__ import annotations

import asyncio
import json
import types

import backend.app.pipeline.stages.crop_blocks.runner as runner


def _fake_ctx(tmp_path, exit_code):
    logs: list = []
    pipelog: list = []

    async def _log(msg, level="info"):
        logs.append((level, msg))

    def _update(stage, status, **kw):
        pipelog.append((stage, status, kw))

    async def _run_subprocess(*a, **k):
        return (exit_code, "", "stderr-text")

    ctx = types.SimpleNamespace(
        output_dir=tmp_path,
        log=_log,
        update_pipeline_log=_update,
        run_subprocess=_run_subprocess,
    )
    return ctx, logs, pipelog


def _write_index(tmp_path, dirname):
    d = tmp_path / dirname
    d.mkdir(parents=True, exist_ok=True)
    (d / "index.json").write_text(
        json.dumps({"blocks": [{"block_id": "b1"}]}), encoding="utf-8"
    )


def _statuses(pipelog):
    return [s for (_, s, _) in pipelog]


def test_exit2_with_index_is_partial_success(tmp_path):
    ctx, _logs, pipelog = _fake_ctx(tmp_path, exit_code=2)
    _write_index(tmp_path, runner.STAGE02_BLOCKS_DIRNAME)
    res = asyncio.run(runner.run_crop_blocks(ctx, project_rel_path="projects/x"))
    assert res.success is True
    assert (res.data or {}).get("partial") is True
    assert "done" in _statuses(pipelog)
    assert "error" not in _statuses(pipelog)


def test_exit2_without_index_is_hard_fail(tmp_path):
    ctx, _logs, pipelog = _fake_ctx(tmp_path, exit_code=2)
    # index.json НЕ создаём → не скачалось ничего
    res = asyncio.run(runner.run_crop_blocks(ctx, project_rel_path="projects/x"))
    assert res.success is False
    assert "error" in _statuses(pipelog)


def test_exit0_is_full_success(tmp_path):
    ctx, _logs, pipelog = _fake_ctx(tmp_path, exit_code=0)
    res = asyncio.run(runner.run_crop_blocks(ctx, project_rel_path="projects/x"))
    assert res.success is True
    assert not (res.data or {}).get("partial")
    assert "done" in _statuses(pipelog)


def test_exit1_is_hard_fail(tmp_path):
    ctx, _logs, pipelog = _fake_ctx(tmp_path, exit_code=1)
    _write_index(tmp_path, runner.STAGE02_BLOCKS_DIRNAME)  # даже с index → exit!=2 = фейл
    res = asyncio.run(runner.run_crop_blocks(ctx, project_rel_path="projects/x"))
    assert res.success is False
    assert "error" in _statuses(pipelog)


def test_recrop_exit2_with_index_is_partial(tmp_path):
    ctx, _logs, pipelog = _fake_ctx(tmp_path, exit_code=2)
    _write_index(tmp_path, runner.STAGE02_BLOCKS_DIRNAME)
    res = asyncio.run(runner.run_policy_recrop(ctx, project_rel_path="projects/x"))
    assert res.success is True
    assert (res.data or {}).get("partial") is True


def test_recrop_exit2_without_index_is_hard_fail(tmp_path):
    ctx, _logs, pipelog = _fake_ctx(tmp_path, exit_code=2)
    res = asyncio.run(runner.run_policy_recrop(ctx, project_rel_path="projects/x"))
    assert res.success is False
    assert "error" in _statuses(pipelog)
