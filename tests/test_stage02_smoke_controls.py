"""
test_stage02_smoke_controls.py
------------------------------
Регресс-тесты для post-cutover stabilization Stage 02 (2026-05-14):

Найденные проблемы (из post-cutover smoke):
1. Stage 02 при `cancel` после ~30s обработал 19/26 блоков (внутренняя
   параллельность DEFAULT_PARALLELISM=3 + быстрый GPT-5.4). Для smoke это
   слишком много блоков. Нужны env-лимиты:
   - AUDIT_STAGE02_MAX_BLOCKS: ограничивает количество блоков (blocks_filter).
   - AUDIT_STAGE02_MAX_PARALLEL_BATCHES: ограничивает параллельность.

2. При cancel Stage 02 не вызывал `record_block_analysis_usage`, поэтому
   job.cost_usd оставался $0.0, а у пользователя реально ушли деньги
   за GPT-5.4 (OpenRouter). Теперь partial cost учитывается даже при cancel.

Контракт обоих env:
- если env не задан/пустой/невалидный — production behavior без изменений.
- если задан — Stage 02 использует blocks_filter / parallelism override.

Тесты НЕ запускают реальный LLM. Используют monkeypatch
run_findings_only_for_project для проверки argv и pipeline.

Run:
    python -m pytest tests/test_stage02_smoke_controls.py -v
"""
from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))


# ─── 1. _read_stage02_smoke_env контракт ───────────────────────────────


def test_smoke_env_empty_returns_empty_dict(monkeypatch):
    """Без env-переменных → пустой dict (production default behavior)."""
    monkeypatch.delenv("AUDIT_STAGE02_MAX_BLOCKS", raising=False)
    monkeypatch.delenv("AUDIT_STAGE02_MAX_PARALLEL_BATCHES", raising=False)
    from backend.app.pipeline.stages.block_analysis.runner import _read_stage02_smoke_env
    assert _read_stage02_smoke_env() == {}


def test_smoke_env_max_blocks_int(monkeypatch):
    monkeypatch.setenv("AUDIT_STAGE02_MAX_BLOCKS", "1")
    monkeypatch.delenv("AUDIT_STAGE02_MAX_PARALLEL_BATCHES", raising=False)
    from backend.app.pipeline.stages.block_analysis.runner import _read_stage02_smoke_env
    assert _read_stage02_smoke_env() == {"max_blocks": 1}


def test_smoke_env_max_parallel_int(monkeypatch):
    monkeypatch.delenv("AUDIT_STAGE02_MAX_BLOCKS", raising=False)
    monkeypatch.setenv("AUDIT_STAGE02_MAX_PARALLEL_BATCHES", "1")
    from backend.app.pipeline.stages.block_analysis.runner import _read_stage02_smoke_env
    assert _read_stage02_smoke_env() == {"max_parallel": 1}


def test_smoke_env_both_set(monkeypatch):
    monkeypatch.setenv("AUDIT_STAGE02_MAX_BLOCKS", "2")
    monkeypatch.setenv("AUDIT_STAGE02_MAX_PARALLEL_BATCHES", "1")
    from backend.app.pipeline.stages.block_analysis.runner import _read_stage02_smoke_env
    assert _read_stage02_smoke_env() == {"max_blocks": 2, "max_parallel": 1}


def test_smoke_env_invalid_ignored(monkeypatch):
    """Невалидные значения (не-int, ≤0) игнорируются — production не ломается."""
    monkeypatch.setenv("AUDIT_STAGE02_MAX_BLOCKS", "abc")
    monkeypatch.setenv("AUDIT_STAGE02_MAX_PARALLEL_BATCHES", "0")
    from backend.app.pipeline.stages.block_analysis.runner import _read_stage02_smoke_env
    assert _read_stage02_smoke_env() == {}


def test_smoke_env_negative_ignored(monkeypatch):
    monkeypatch.setenv("AUDIT_STAGE02_MAX_BLOCKS", "-5")
    monkeypatch.setenv("AUDIT_STAGE02_MAX_PARALLEL_BATCHES", "-1")
    from backend.app.pipeline.stages.block_analysis.runner import _read_stage02_smoke_env
    assert _read_stage02_smoke_env() == {}


def test_smoke_env_empty_string_ignored(monkeypatch):
    monkeypatch.setenv("AUDIT_STAGE02_MAX_BLOCKS", "")
    monkeypatch.setenv("AUDIT_STAGE02_MAX_PARALLEL_BATCHES", "")
    from backend.app.pipeline.stages.block_analysis.runner import _read_stage02_smoke_env
    assert _read_stage02_smoke_env() == {}


# ─── 2. Stage 02 runner интеграция: env → run_findings_only_for_project ──


def _make_stage02_project(tmp_path: Path) -> Path:
    """Минимальный project_dir с stage02_blocks_index, document_graph,
    gemma_enrichment_summary — чтобы prerequisites прошли до вызова LLM."""
    project_dir = tmp_path / "M31A"
    output_dir = project_dir / "_output"
    (output_dir / "blocks_stage02_100").mkdir(parents=True)
    (output_dir / "blocks_gemma_100").mkdir(parents=True)
    # project_info
    (project_dir / "project_info.json").write_text(
        json.dumps({"project_id": "M31A", "section": "EOM", "md_file": "doc.md"}),
        encoding="utf-8",
    )
    (project_dir / "doc.md").write_text("# fake md\n\n[TEXT b1]\n", encoding="utf-8")
    # 5 blocks в stage02 index
    blocks = [{"block_id": f"BLK-{i:03d}", "page": i, "file": f"BLK-{i:03d}.png"} for i in range(1, 6)]
    (output_dir / "blocks_stage02_100" / "index.json").write_text(
        json.dumps({"blocks": blocks, "policy": {"profile": "stage02_100", "dpi": 100, "min_long_side": 800, "compact": False}}),
        encoding="utf-8",
    )
    (output_dir / "blocks_gemma_100" / "index.json").write_text(
        json.dumps({"blocks": blocks, "policy": {"profile": "gemma_100_base", "dpi": 100, "min_long_side": 800, "compact": False, "skip_small": False}}),
        encoding="utf-8",
    )
    (output_dir / "document_graph.json").write_text(
        json.dumps({"pages": [{"page": i, "sheet_no": str(i)} for i in range(1, 6)]}),
        encoding="utf-8",
    )
    (output_dir / "gemma_enrichment_summary.json").write_text(
        json.dumps({"schema_version": 2, "blocks_total": 5, "base_blocks_ok": 5}),
        encoding="utf-8",
    )
    return project_dir


def _make_ctx(project_dir: Path):
    """Минимальный PipelineStageContext с no-op callbacks + capture для usage record."""
    from backend.app.pipeline.context import PipelineStageContext

    captured = {"logs": [], "usage_summary": None, "pipeline_log": []}

    async def _log(msg, level="info"):
        captured["logs"].append((level, msg))

    async def _check_before_launch():
        return True

    async def _check_pause():
        return True

    async def _wait_for_rate_limit(reason, cli_output):
        return True

    def _record_cli_usage(*a, **k):
        pass

    def _update_pipeline_log(stage_key, status, **kwargs):
        captured["pipeline_log"].append((stage_key, status, kwargs))

    async def _run_subprocess(*a, **k):
        return (0, "", "")

    def _record_block_analysis_usage(summary):
        captured["usage_summary"] = summary

    ctx = PipelineStageContext(
        project_dir=project_dir,
        project_id="M31A",
        output_dir=project_dir / "_output",
        log=_log,
        check_before_launch=_check_before_launch,
        check_pause=_check_pause,
        wait_for_rate_limit=_wait_for_rate_limit,
        record_cli_usage=_record_cli_usage,
        update_pipeline_log=_update_pipeline_log,
        run_subprocess=_run_subprocess,
        record_block_analysis_usage=_record_block_analysis_usage,
    )
    return ctx, captured


def _patch_run_findings_only(monkeypatch, capture_dict, *, summary_overrides=None):
    """Подменяет gemma_findings_only.run_findings_only_for_project и
    check_prerequisites на mock'и, чтобы Stage 02 тесты не упирались в
    validate_gemma_summary / crop_index_matches_policy."""
    summary_overrides = summary_overrides or {}
    base_summary = {
        "model": "openai/gpt-5.4",
        "blocks_total": 5,
        "blocks_ok": 5,
        "blocks_failed": 0,
        "blocks_skipped_no_enrichment": 0,
        "wall_clock_s": 10.0,
        "cancelled": False,
        "uncovered_blocks": [],
        "totals": {
            "input_tokens": 1000,
            "output_tokens": 500,
            "reasoning_tokens": 200,
            "findings": 3,
            "estimated_cost_usd_total": 0.05,
        },
    }
    base_summary.update(summary_overrides)

    async def _fake_run(project_dir, **kwargs):
        capture_dict["call_kwargs"] = kwargs
        return {
            "output_doc": {},
            "summary": base_summary,
            "plan": [],
            "run_dir": None,
        }

    def _fake_prereq(project_dir):
        return {"ok": True, "reasons": [], "blocks_total": 5, "with_enrichment": 5, "uncovered_blocks": []}

    from backend.app.pipeline.stages.block_analysis import gemma_findings_only as gfo
    monkeypatch.setattr(gfo, "run_findings_only_for_project", _fake_run)
    monkeypatch.setattr(gfo, "check_prerequisites", _fake_prereq)


@pytest.mark.asyncio
async def test_stage02_no_env_uses_defaults(tmp_path, monkeypatch):
    """Без env: parallelism=DEFAULT_PARALLELISM, blocks_filter=None.
    Production full audit поведение НЕ изменилось."""
    monkeypatch.delenv("AUDIT_STAGE02_MAX_BLOCKS", raising=False)
    monkeypatch.delenv("AUDIT_STAGE02_MAX_PARALLEL_BATCHES", raising=False)

    project_dir = _make_stage02_project(tmp_path)
    capture = {}
    _patch_run_findings_only(monkeypatch, capture)

    from backend.app.pipeline.stages.block_analysis.runner import (
        run_block_analysis_findings_only,
    )
    from backend.app.pipeline.stages.block_analysis.gemma_findings_only import DEFAULT_PARALLELISM

    ctx, captured = _make_ctx(project_dir)
    result = await run_block_analysis_findings_only(ctx)
    assert result.success

    kw = capture["call_kwargs"]
    assert kw.get("parallelism") == DEFAULT_PARALLELISM
    assert kw.get("blocks_filter") is None


@pytest.mark.asyncio
async def test_stage02_max_blocks_limits_to_first_n(tmp_path, monkeypatch):
    """AUDIT_STAGE02_MAX_BLOCKS=1 → blocks_filter = первый block_id."""
    monkeypatch.setenv("AUDIT_STAGE02_MAX_BLOCKS", "1")
    monkeypatch.delenv("AUDIT_STAGE02_MAX_PARALLEL_BATCHES", raising=False)

    project_dir = _make_stage02_project(tmp_path)
    capture = {}
    _patch_run_findings_only(monkeypatch, capture)

    from backend.app.pipeline.stages.block_analysis.runner import (
        run_block_analysis_findings_only,
    )
    ctx, captured = _make_ctx(project_dir)
    await run_block_analysis_findings_only(ctx)

    kw = capture["call_kwargs"]
    assert kw.get("blocks_filter") == ["BLK-001"], (
        f"AUDIT_STAGE02_MAX_BLOCKS=1 не ограничил blocks_filter: {kw.get('blocks_filter')}"
    )

    # Проверим, что в audit log есть SMOKE-LIMIT warn
    smoke_logs = [
        (lvl, msg) for lvl, msg in captured["logs"]
        if "SMOKE-LIMIT" in msg and "MAX_BLOCKS" in msg
    ]
    assert smoke_logs, "Нет SMOKE-LIMIT warn записи в audit log"
    assert smoke_logs[0][0] == "warn"


@pytest.mark.asyncio
async def test_stage02_max_blocks_3_limits_to_first_3(tmp_path, monkeypatch):
    monkeypatch.setenv("AUDIT_STAGE02_MAX_BLOCKS", "3")
    project_dir = _make_stage02_project(tmp_path)
    capture = {}
    _patch_run_findings_only(monkeypatch, capture)

    from backend.app.pipeline.stages.block_analysis.runner import (
        run_block_analysis_findings_only,
    )
    ctx, _captured = _make_ctx(project_dir)
    await run_block_analysis_findings_only(ctx)

    assert capture["call_kwargs"].get("blocks_filter") == ["BLK-001", "BLK-002", "BLK-003"]


@pytest.mark.asyncio
async def test_stage02_max_parallel_overrides_default(tmp_path, monkeypatch):
    monkeypatch.delenv("AUDIT_STAGE02_MAX_BLOCKS", raising=False)
    monkeypatch.setenv("AUDIT_STAGE02_MAX_PARALLEL_BATCHES", "1")

    project_dir = _make_stage02_project(tmp_path)
    capture = {}
    _patch_run_findings_only(monkeypatch, capture)

    from backend.app.pipeline.stages.block_analysis.runner import (
        run_block_analysis_findings_only,
    )
    ctx, captured = _make_ctx(project_dir)
    await run_block_analysis_findings_only(ctx)

    assert capture["call_kwargs"].get("parallelism") == 1
    smoke_logs = [
        (lvl, msg) for lvl, msg in captured["logs"]
        if "SMOKE-LIMIT" in msg and "MAX_PARALLEL_BATCHES" in msg
    ]
    assert smoke_logs


@pytest.mark.asyncio
async def test_stage02_invalid_env_uses_defaults(tmp_path, monkeypatch):
    """Невалидный env не ломает production."""
    monkeypatch.setenv("AUDIT_STAGE02_MAX_BLOCKS", "garbage")
    monkeypatch.setenv("AUDIT_STAGE02_MAX_PARALLEL_BATCHES", "0")

    project_dir = _make_stage02_project(tmp_path)
    capture = {}
    _patch_run_findings_only(monkeypatch, capture)

    from backend.app.pipeline.stages.block_analysis.runner import (
        run_block_analysis_findings_only,
    )
    from backend.app.pipeline.stages.block_analysis.gemma_findings_only import DEFAULT_PARALLELISM
    ctx, _captured = _make_ctx(project_dir)
    await run_block_analysis_findings_only(ctx)

    assert capture["call_kwargs"].get("parallelism") == DEFAULT_PARALLELISM
    assert capture["call_kwargs"].get("blocks_filter") is None


# ─── 3. Cost-on-cancel: Stage 02 cancelled → record_block_analysis_usage ──


@pytest.mark.asyncio
async def test_stage02_cancel_records_partial_cost(tmp_path, monkeypatch):
    """При cancel Stage 02 partial cost должен попадать в usage tracker
    (раньше job.cost_usd оставался $0.0 — пользователь не видел расход)."""
    project_dir = _make_stage02_project(tmp_path)
    capture = {}
    _patch_run_findings_only(
        monkeypatch, capture,
        summary_overrides={
            "cancelled": True,
            "blocks_ok": 19,  # как в реальном smoke
            "blocks_failed": 0,
            "totals": {
                "input_tokens": 40517,
                "output_tokens": 17118,
                "reasoning_tokens": 10092,
                "findings": 30,
                "estimated_cost_usd_total": 0.32,
            },
        },
    )

    from backend.app.pipeline.stages.block_analysis.runner import (
        run_block_analysis_findings_only,
    )
    ctx, captured = _make_ctx(project_dir)
    result = await run_block_analysis_findings_only(ctx)

    assert result.cancelled
    # КЛЮЧЕВОЕ: usage_summary должен быть записан
    assert captured["usage_summary"] is not None, (
        "Cancel НЕ записал partial usage — bug повторился"
    )
    totals = captured["usage_summary"].get("totals", {})
    assert totals.get("estimated_cost_usd_total") == 0.32
    # Должна быть warn запись со списанной суммой
    cancel_logs = [
        msg for lvl, msg in captured["logs"]
        if "Stage 02 cancelled" in msg or "cancelled: обработано" in msg
    ]
    assert cancel_logs, "Нет warn-лога о partial cost при cancel"


@pytest.mark.asyncio
async def test_stage02_cancel_with_zero_blocks_skips_recording(tmp_path, monkeypatch):
    """Cancel ДО первого OK блока — usage не записывается (нет смысла)."""
    project_dir = _make_stage02_project(tmp_path)
    capture = {}
    _patch_run_findings_only(
        monkeypatch, capture,
        summary_overrides={
            "cancelled": True,
            "blocks_ok": 0,
            "totals": {
                "input_tokens": 0,
                "output_tokens": 0,
                "reasoning_tokens": 0,
                "findings": 0,
                "estimated_cost_usd_total": 0.0,
            },
        },
    )

    from backend.app.pipeline.stages.block_analysis.runner import (
        run_block_analysis_findings_only,
    )
    ctx, captured = _make_ctx(project_dir)
    result = await run_block_analysis_findings_only(ctx)

    assert result.cancelled
    # Без partial cost — recorder не вызывался
    assert captured["usage_summary"] is None


@pytest.mark.asyncio
async def test_stage02_normal_completion_records_usage_as_before(tmp_path, monkeypatch):
    """Регрессия: при нормальном завершении (не cancel) usage всё ещё записывается."""
    project_dir = _make_stage02_project(tmp_path)
    capture = {}
    _patch_run_findings_only(monkeypatch, capture)  # default cancelled=False

    from backend.app.pipeline.stages.block_analysis.runner import (
        run_block_analysis_findings_only,
    )
    ctx, captured = _make_ctx(project_dir)
    result = await run_block_analysis_findings_only(ctx)

    assert result.success
    assert captured["usage_summary"] is not None
    # totals от mocked summary
    assert captured["usage_summary"].get("totals", {}).get("estimated_cost_usd_total") == 0.05


# ─── 4. _record_findings_only_usage: cost aggregation для OpenRouter ───


def test_record_findings_only_usage_openrouter_writes_real_cost(tmp_path, monkeypatch):
    """OpenRouter (model='openai/gpt-5.4') → cost_usd реальный, не notional."""
    from backend.app.pipeline.manager import PipelineManager
    from backend.app.models.audit import AuditJob, AuditStage, JobStatus
    from backend.app.services.common import usage_service

    captured_records = []
    monkeypatch.setattr(
        usage_service.usage_tracker, "record_usage",
        lambda r: captured_records.append(r),
    )

    pm = PipelineManager()
    job = AuditJob(
        job_id="j1", project_id="M31A", version_id="v2",
        stage=AuditStage.BLOCK_ANALYSIS, status=JobStatus.RUNNING,
    )
    summary = {
        "model": "openai/gpt-5.4",
        "blocks_ok": 19,
        "wall_clock_s": 30.0,
        "totals": {
            "input_tokens": 40517,
            "output_tokens": 17118,
            "estimated_cost_usd_total": 0.3227,
        },
    }
    pm._record_findings_only_usage(job, summary)

    assert len(captured_records) == 1
    rec = captured_records[0]
    # OpenRouter — реальный платёж: cost_usd > 0, notional == 0
    assert rec.cost_usd == 0.3227
    assert rec.cost_usd_notional == 0.0
    assert rec.input_tokens == 40517
    assert rec.output_tokens == 17118
    # job aggregator тоже обновился
    assert job.cost_usd == 0.3227
    assert job.cli_calls == 19


def test_record_findings_only_usage_claude_cli_uses_notional(tmp_path, monkeypatch):
    """Claude CLI (model='claude-opus-4-7') → cost_usd=0 (subscription),
    notional=cost (для аналитики)."""
    from backend.app.pipeline.manager import PipelineManager
    from backend.app.models.audit import AuditJob, AuditStage, JobStatus
    from backend.app.services.common import usage_service

    captured_records = []
    monkeypatch.setattr(
        usage_service.usage_tracker, "record_usage",
        lambda r: captured_records.append(r),
    )

    pm = PipelineManager()
    job = AuditJob(
        job_id="j2", project_id="M31A", version_id="v1",
        stage=AuditStage.BLOCK_ANALYSIS, status=JobStatus.RUNNING,
    )
    summary = {
        "model": "claude-opus-4-7",
        "blocks_ok": 5,
        "wall_clock_s": 15.0,
        "totals": {
            "input_tokens": 5000,
            "output_tokens": 1000,
            "estimated_cost_usd_total": 0.075,
        },
    }
    pm._record_findings_only_usage(job, summary)

    rec = captured_records[0]
    assert rec.cost_usd == 0.0  # subscription
    assert rec.cost_usd_notional == 0.075
    assert job.cost_usd == 0.0  # подписка не списывается с job


def test_record_findings_only_usage_empty_summary_noop(monkeypatch):
    """Пустой summary (cancelled до первого блока) → no record."""
    from backend.app.pipeline.manager import PipelineManager
    from backend.app.models.audit import AuditJob, AuditStage, JobStatus
    from backend.app.services.common import usage_service

    captured_records = []
    monkeypatch.setattr(
        usage_service.usage_tracker, "record_usage",
        lambda r: captured_records.append(r),
    )

    pm = PipelineManager()
    job = AuditJob(
        job_id="j3", project_id="M31A", version_id="v2",
        stage=AuditStage.BLOCK_ANALYSIS, status=JobStatus.RUNNING,
    )
    pm._record_findings_only_usage(job, {"model": "openai/gpt-5.4", "totals": {}})
    assert captured_records == []
