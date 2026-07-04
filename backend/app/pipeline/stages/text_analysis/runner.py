"""
text_analysis/runner.py
-----------------------
Stage runner для этапа text_analysis (анализ текста MD через Claude).

Покрывает все три call-site в manager.py:
  1. _run_resumed_pipeline  — стандартный run_text_analysis, без rate-limit retry
  2. _run_ocr_pipeline      — run_text_analysis с rate-limit retry

Публичный API:
  run_text_analysis(ctx, *, stage_label, use_triage) -> StageResult
"""
from __future__ import annotations

import asyncio
import inspect
import json
import os
from pathlib import Path
from typing import TYPE_CHECKING

import backend.app.services.llm.claude_runner as claude_runner
from backend.app.pipeline.stage_result import StageResult
from backend.app.services.common.cli_utils import is_cancelled, is_rate_limited
from backend.app.pipeline.stages.text_analysis.rate_limit_retry import (
    load_rate_limit_config,
    compute_fallback_backoff,
    REASON_RATE_LIMIT_EXHAUSTED,
)

if TYPE_CHECKING:
    from backend.app.pipeline.context import PipelineStageContext

# Инъектируемая точка сна (тесты подменяют, чтобы не ждать реально).
_SLEEP = asyncio.sleep


def _error_detail(exit_code: int, output: str, max_len: int = 120) -> str:
    if not output:
        return f"Exit code {exit_code}"
    lines = output.strip().splitlines()
    useful = []
    skip_prefixes = ("╭", "╰", "│", "─", "⎿", "⏎", "\\", "  ", "Usage:", "Duration:")
    for line in reversed(lines):
        stripped = line.strip()
        if not stripped:
            continue
        if any(stripped.startswith(p) for p in skip_prefixes):
            continue
        lower = stripped.lower()
        if any(kw in lower for kw in ("error", "ошибка", "failed", "timeout", "timed out",
                                       "rate limit", "overloaded", "connection", "refused",
                                       "exception", "traceback", "permission", "not found",
                                       "invalid", "json", "unable", "cannot")):
            useful.insert(0, stripped)
            if len(useful) >= 3:
                break
        elif not useful:
            useful.append(stripped)
    if useful:
        msg = " | ".join(useful)
        return msg[:max_len - 3] + "..." if len(msg) > max_len else msg
    return f"Exit code {exit_code}"


async def run_text_analysis(
    ctx: "PipelineStageContext",
    *,
    stage_label: str = "text_analysis",
    use_triage: bool = False,
    with_rate_limit_retry: bool = True,
) -> StageResult:
    """Запуск текстового анализа или триажа страниц через claude_runner.

    Аргументы:
        stage_label: ключ для update_pipeline_log и record_cli_usage.
            Обычно "text_analysis". Используется как есть в pipeline_log.
        use_triage: если True — вызывает claude_runner.run_triage вместо
            run_text_analysis. По семантике идентично (run_triage = run_text_analysis),
            но stage_label отличается ("triage").
        with_rate_limit_retry: если True — при rate limit ждёт и повторяет.
            _run_resumed_pipeline не делал retry, _run_ocr_pipeline делал.

    Управляет:
    - update_pipeline_log("text_analysis" / stage_label, "running" → "done" / "error");
    - rate limit check + optional retry;
    - cancel check;
    - проверкой создания 01_text_analysis.json;
    - record_cli_usage.

    Не управляет:
    - job.stage / job.status (выставляет оркестратор);
    - heartbeat / cleanup (оркестратор);
    - очисткой старых файлов перед запуском (оркестратор);
    - чтением triage_data / priority_pages (оркестратор читает 01_text_analysis.json).
    """
    pid = ctx.project_id
    output_dir = ctx.output_dir
    project_info = ctx.project_info or {}

    log_stage = stage_label  # ключ в pipeline_log

    ctx.update_pipeline_log(log_stage, "running")

    # ── Pre-launch gate ──
    can_go = await ctx.check_before_launch()
    if not can_go:
        error = "Rate limit: ожидание превышено или отменено"
        ctx.update_pipeline_log(log_stage, "error", error=error)
        return StageResult.fail(error)

    # ── Запуск LLM с bounded rate-limit retry ──
    _runner = claude_runner.run_triage if use_triage else claude_runner.run_text_analysis
    _usage_label = "triage" if use_triage else "text_analysis"
    label = "Триаж" if use_triage else "Текстовый анализ"
    human = "триаже" if use_triage else "текстовом анализе"

    cfg = load_rate_limit_config()
    rl_attempt = 0  # сколько rate-limit ожиданий уже выполнено

    while True:
        version_dir = getattr(ctx, "project_dir", output_dir.parent)
        version_id = getattr(ctx, "version_id", None)
        runner_kwargs = {}
        try:
            runner_params = inspect.signature(_runner).parameters
        except (TypeError, ValueError):
            runner_params = {}
        if "output_dir" in runner_params:
            runner_kwargs["output_dir"] = output_dir
        if "version_dir" in runner_params:
            runner_kwargs["version_dir"] = version_dir
        if version_id and "version_id" in runner_params:
            runner_kwargs["version_id"] = version_id

        if runner_kwargs:
            exit_code, output, cli_result = await _runner(
                project_info, pid,
                on_output=ctx.log,
                **runner_kwargs,
            )
        else:
            scoped_env = {
                "AUDIT_OUTPUT_DIR": str(output_dir),
                "AUDIT_VERSION_DIR": str(version_dir),
                "AUDIT_PROJECT_ID": str(pid),
            }
            if version_id:
                scoped_env["AUDIT_VERSION_ID"] = str(version_id)
            previous_env = {key: os.environ.get(key) for key in scoped_env}
            os.environ.update(scoped_env)
            try:
                exit_code, output, cli_result = await _runner(
                    project_info, pid,
                    on_output=ctx.log,
                )
            finally:
                for key, value in previous_env.items():
                    if value is None:
                        os.environ.pop(key, None)
                    else:
                        os.environ[key] = value
        ctx.record_cli_usage(
            cli_result,
            _usage_label if rl_attempt == 0 else f"{_usage_label}_retry{rl_attempt}",
        )

        if is_cancelled(exit_code):
            return StageResult.cancel()

        if exit_code == 0:
            break  # успех

        rate_limited = with_rate_limit_retry and is_rate_limited(exit_code, output or "", "")
        if not rate_limited:
            # Обычная (НЕ rate-limit) ошибка — hard fail как раньше.
            error = _error_detail(exit_code, output or "")
            ctx.update_pipeline_log(log_stage, "error", error=error)
            return StageResult.fail(f"{label}: код {exit_code}")

        # ── Rate limit: bounded retry с fallback backoff ──
        rl_attempt += 1
        if rl_attempt > cfg.max_retries:
            error = (
                f"rate_limit_exhausted: лимит Claude сохраняется после "
                f"{cfg.max_retries} попыток ожидания ({label.lower()})"
            )
            await ctx.log(
                f"Rate limit при {human}: исчерпаны retry "
                f"({cfg.max_retries}) → {REASON_RATE_LIMIT_EXHAUSTED} (retry_later)",
                "error",
            )
            ctx.update_pipeline_log(
                log_stage, "error", error=error,
                detail={"reason": REASON_RATE_LIMIT_EXHAUSTED, "rate_limit_retries": cfg.max_retries},
            )
            return StageResult.fail(
                error,
                reason=REASON_RATE_LIMIT_EXHAUSTED,
                pause_on_rate_limit=cfg.pause_on_exhausted,
            )

        try:
            parsed = claude_runner.parse_rate_limit_reset(output or "")
        except Exception:
            parsed = None
        fallback = compute_fallback_backoff(rl_attempt, cfg)
        await ctx.log(
            f"Rate limit при {human}: попытка {rl_attempt}/{cfg.max_retries}; "
            + (f"reset из CLI ~{parsed}с" if parsed
               else f"reset не распарсился → fallback backoff {fallback}с"),
            "warn",
        )

        can_continue = await ctx.wait_for_rate_limit(
            f"rate limit при {human} (попытка {rl_attempt}/{cfg.max_retries})",
            output or "",
        )
        if not can_continue:
            # Отмена пользователем — выходим.
            if ctx.is_cancelled and ctx.is_cancelled():
                return StageResult.cancel()
            # Не отмена: wait не определил время сброса → bounded fallback backoff.
            await ctx.log(
                f"wait_for_rate_limit не дождался сброса → fallback backoff {fallback}с "
                f"(попытка {rl_attempt}/{cfg.max_retries})",
                "warn",
            )
            await _SLEEP(fallback)
        # Повторяем запуск LLM (следующая итерация while).

    # ── Проверка выходного файла ──
    output_path = output_dir / "01_text_analysis.json"
    if not output_path.exists():
        error = "01_text_analysis.json не создан"
        ctx.update_pipeline_log(log_stage, "error", error=error)
        return StageResult.fail(error)

    # Существование файла ещё не значит валидность: усечённый/битый ответ LLM
    # (finish_reason=length, неэкранированные кавычки) даёт ложный success на
    # одном лишь .exists(). Проверяем парсимость (с попыткой ремонта кавычек)
    # и обязательную структуру (список text_findings).
    from backend.app.pipeline.stages.block_analysis.runner import (
        validate_and_repair_json,
    )

    # to_thread: repair-ветка квадратичная, на большом JSON блокирует loop.
    is_valid, repair_msg = await asyncio.to_thread(
        validate_and_repair_json, output_path
    )
    if not is_valid:
        error = f"01_text_analysis.json невалиден (не починить): {repair_msg}"
        ctx.update_pipeline_log(log_stage, "error", error=error)
        return StageResult.fail(error)
    try:
        data = json.loads(output_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        error = f"01_text_analysis.json не парсится: {exc}"
        ctx.update_pipeline_log(log_stage, "error", error=error)
        return StageResult.fail(error)
    if not isinstance(data, dict) or not isinstance(data.get("text_findings"), list):
        error = "01_text_analysis.json без обязательного списка text_findings (вероятно усечён)"
        ctx.update_pipeline_log(log_stage, "error", error=error)
        return StageResult.fail(error)

    # ── Страж отсутствия: подтверждённо-ложные «нет данных» → «ПРОВЕРИТЬ ПО СМЕЖНЫМ» ──
    # Кандидаты (похоже на отсутствие, без absence_checked) перепроверяются по ПОЛНОМУ
    # тексту документа; понижаются только те, где данные фактически есть (present).
    # Fail-soft: любая ошибка пост-прохода не должна ронять валидный результат этапа.
    done_message = "OK"
    from backend.app.core.config import PIPELINE_ABSENCE_GUARD_ENABLED
    if PIPELINE_ABSENCE_GUARD_ENABLED:
        try:
            done_message = await asyncio.to_thread(
                _apply_absence_guard, output_path, data, project_info, pid
            )
        except Exception as exc:  # noqa: BLE001 — fail-soft
            done_message = f"OK; absence_guard пропущен: {exc}"

    ctx.update_pipeline_log(log_stage, "done", message=done_message)
    return StageResult.ok(output_path=str(output_path))


def _apply_absence_guard(output_path, data: dict, project_info: dict, pid: str) -> str:
    """Синхронный пост-проход стража отсутствия (вызывается через to_thread).

    Читает MD, прогоняет enforce_absence_guard с claude-верификатором, при понижениях
    перезаписывает 01_text_analysis.json. Возвращает сообщение для done-лога. Fail-soft.
    """
    from backend.app.pipeline.stages.text_analysis.absence_guard import (
        enforce_absence_guard, run_claude_verification,
    )
    from backend.app.pipeline.stages.prepare.task_builder import _get_md_file_path

    md_text = None
    try:
        md_path = _get_md_file_path(project_info, pid)
        if md_path and md_path != "(нет)":
            from pathlib import Path as _P
            md_text = _P(md_path).read_text(encoding="utf-8")
    except Exception:  # noqa: BLE001 — без MD страж уйдёт в безопасный режим (не понижает)
        md_text = None

    stats = enforce_absence_guard(
        data["text_findings"], md_text=md_text, verifier=run_claude_verification,
    )
    if stats["downgraded"]:
        output_path.write_text(
            json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8"
        )
    if not stats["candidates"]:
        return f"OK; absence_guard: кандидатов нет ({stats['absence_claims']} absence)"
    if not stats["verified"]:
        return (
            f"OK; absence_guard: {stats['candidates']} кандидатов НЕ проверены "
            "(нет MD/верификатора) — не понижено"
        )
    return (
        f"OK; absence_guard: понижено {stats['downgraded']}/{stats['candidates']} "
        f"проверенных (из {stats['absence_claims']} absence)"
    )
