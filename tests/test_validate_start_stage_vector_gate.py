"""Правка gemma-гейта в _validate_start_from_stage_now: вектор-проект проходит.

Контекст (16.07.2026): 13АВ-РД-ВК2.2-ПА V1 и 13АВ-РД-ДК-К1 V1 нельзя было
повторно запустить свод замечаний через add-retry. evaluate_gemma_enrichment
смотрит на legacy-индекс blocks_gemma_100/index.json, которого вектор-конвейер
не создаёт → missing_blocks на полностью готовом проекте. Рантайм-путь
(_assert_gemma_ready_for_stage, resume) уже гейтит по каноническому
block_context_summary.json; правка выравнивает pre-enqueue валидатор с ним.

Проверяем и обход (готовый вектор-проект проходит), и сохранение защиты
(реально неготовый проект по-прежнему блокируется).
"""
import tempfile
from pathlib import Path

import pytest

import backend.app.pipeline.manager as mgr
from backend.app.pipeline.manager import PipelineManager


def _run(monkeypatch, *, gemma_status, gemma_ready, block_ctx_valid,
         has_text, has_blocks, has_findings, stage):
    """Прогнать _validate_start_from_stage_now с подменёнными гейтами и входами.

    Возвращает "ok" при прохождении или текст RuntimeError при блокировке.
    """
    pm = PipelineManager()
    with tempfile.TemporaryDirectory() as td:
        out = Path(td)

        # version_service импортируется ЛОКАЛЬНО внутри метода — патчим модуль.
        import backend.app.services.common.version_service as vs
        monkeypatch.setattr(
            vs, "resolve_project_version_context",
            lambda pid, vid: {"version_dir": out, "output_dir": out},
        )
        monkeypatch.setattr(pm, "_load_project_info_for_paths", lambda *a, **k: {})
        monkeypatch.setattr(pm, "_seed_latest_gemma_artifacts_from_recent_run",
                            lambda *a, **k: None)
        monkeypatch.setattr(pm, "_assert_stage_model_config_ready", lambda: None)
        monkeypatch.setattr(pm, "_normalize_ocr_stage", lambda s: s)
        monkeypatch.setattr(pm, "_blocks_before_text_enabled", lambda: True)

        monkeypatch.setattr(
            mgr, "evaluate_gemma_enrichment",
            lambda *a, **k: {"status": gemma_status, "ready": gemma_ready},
        )
        monkeypatch.setattr(
            mgr, "gemma_gate_error",
            lambda state, tgt: f"gemma-барьер ({state.get('status')})",
        )
        # validate_block_context_summary тоже импортируется локально в методе.
        import backend.app.pipeline.stages.block_context.contract as contract
        monkeypatch.setattr(
            contract, "validate_block_context_summary",
            lambda *a, **k: {"valid": block_ctx_valid},
        )

        if has_text:
            (out / mgr.TEXT_ANALYSIS_FILENAME).write_text("{}")
        if has_blocks:
            (out / mgr.BLOCKS_ANALYSIS_FILENAME).write_text("{}")
        if has_findings:
            (out / "03_findings.json").write_text("{}")

        try:
            pm._validate_start_from_stage_now("PID", stage, version_id="v001")
            return "ok"
        except RuntimeError as e:
            return str(e)


# ─── Обход: готовый вектор-проект проходит ────────────────────────────────

def test_vector_project_passes_findings_merge(monkeypatch):
    """block_context валиден + 01/02 на месте → свод проходит, хотя gemma=missing_blocks."""
    result = _run(
        monkeypatch, gemma_status="missing_blocks", gemma_ready=False,
        block_ctx_valid=True, has_text=True, has_blocks=True, has_findings=False,
        stage="findings_merge",
    )
    assert result == "ok"


def test_vector_project_passes_norm_verify(monkeypatch):
    """strict_gemma-этап norm_verify на готовом вектор-проекте тоже проходит."""
    result = _run(
        monkeypatch, gemma_status="missing_blocks", gemma_ready=False,
        block_ctx_valid=True, has_text=True, has_blocks=True, has_findings=True,
        stage="norm_verify",
    )
    assert result == "ok"


# ─── Защита сохранена: реально неготовый проект блокируется ───────────────

def test_empty_project_still_blocked(monkeypatch):
    """Ни gemma, ни block_context → gemma-барьер срабатывает как раньше."""
    result = _run(
        monkeypatch, gemma_status="missing_blocks", gemma_ready=False,
        block_ctx_valid=False, has_text=False, has_blocks=False, has_findings=False,
        stage="findings_merge",
    )
    assert "gemma-барьер" in result


def test_missing_md_never_bypassed(monkeypatch):
    """missing_md обязателен всегда — валидный block_context его НЕ обходит."""
    result = _run(
        monkeypatch, gemma_status="missing_md", gemma_ready=False,
        block_ctx_valid=True, has_text=True, has_blocks=True, has_findings=True,
        stage="findings_merge",
    )
    assert "gemma-барьер" in result and "missing_md" in result


def test_block_context_ok_but_no_blocks_artifact_blocked(monkeypatch):
    """block_context валиден, но нет 01_blocks_analysis.json → файловый барьер держит."""
    result = _run(
        monkeypatch, gemma_status="missing_blocks", gemma_ready=False,
        block_ctx_valid=True, has_text=True, has_blocks=False, has_findings=False,
        stage="findings_merge",
    )
    assert "01_blocks_analysis.json" in result


def test_block_context_ok_but_no_text_artifact_blocked(monkeypatch):
    """block_context валиден, но нет 02_text_analysis.json → файловый барьер держит."""
    result = _run(
        monkeypatch, gemma_status="missing_blocks", gemma_ready=False,
        block_ctx_valid=True, has_text=False, has_blocks=True, has_findings=False,
        stage="findings_merge",
    )
    assert "02_text_analysis.json" in result
