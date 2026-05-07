"""
optimization/runner.py
----------------------
Stage runner для этапов optimization, optimization_critic, optimization_corrector.

Extracted from PipelineManager._run_optimization / _run_optimization_review.
Бизнес-логика не изменена; оркестратор (manager.py) передаёт управление
через PipelineStageContext и забирает итоговый статус.

Публичный API:
  run_optimization(ctx) -> OptimizationResult
  run_optimization_review(ctx) -> OptimizationReviewResult
"""
from __future__ import annotations

import json
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import backend.app.services.llm.claude_runner as claude_runner
from backend.app.pipeline.context import PipelineStageContext
from backend.app.pipeline.stages.block_analysis.runner import validate_and_repair_json
from backend.app.services.common.cli_utils import is_cancelled, is_rate_limited


# ─── Result types ────────────────────────────────────────────────────────────

@dataclass
class OptimizationResult:
    """Результат run_optimization."""
    success: bool
    cancelled: bool = False
    error: Optional[str] = None
    rate_limited: bool = False


@dataclass
class OptimizationReviewResult:
    """Результат run_optimization_review (critic + corrector)."""
    critic_ok: bool = False
    corrector_skipped: bool = False
    corrector_ok: bool = False
    cancelled: bool = False
    error: Optional[str] = None


# ─── Internal helpers ────────────────────────────────────────────────────────

def _extract_error_detail(exit_code: int, output: str, max_len: int = 120) -> str:
    """Extract useful error message from CLI output."""
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
        if len(msg) > max_len:
            msg = msg[:max_len - 3] + "..."
        return msg
    return f"Exit code {exit_code}"


def _read_optimization_meta(opt_file: Path) -> tuple[int, float, float]:
    """Read total_items, estimated_savings_pct, size_kb from optimization.json."""
    try:
        with open(opt_file, "r", encoding="utf-8") as f:
            data = json.load(f)
        meta = data.get("meta", {})
        return (
            int(meta.get("total_items", 0)),
            float(meta.get("estimated_savings_pct", 0)),
            round(opt_file.stat().st_size / 1024, 1),
        )
    except Exception:
        size_kb = round(opt_file.stat().st_size / 1024, 1) if opt_file.exists() else 0.0
        return (0, 0.0, size_kb)


# ─── run_optimization ────────────────────────────────────────────────────────

async def run_optimization(ctx: PipelineStageContext) -> OptimizationResult:
    """Запуск оптимизации через Claude CLI (или OpenRouter).

    Управляет:
    - rate limit pre-check и retry после паузы;
    - проверкой/логированием optimization.json после успеха;
    - обработкой cancellation.

    Не управляет:
    - cleanup active_jobs (дело оркестратора);
    - heartbeat (запускается оркестратором до вызова);
    - job.status (оркестратор читает результат и сам обновляет).
    """
    pid = ctx.project_id
    project_info = ctx.project_info or {}

    ctx.update_pipeline_log("optimization", "running")
    await ctx.log("Запуск анализа оптимизации проектных решений...")

    can_go = await ctx.check_before_launch()
    if not can_go:
        ctx.update_pipeline_log("optimization", "error", error="Rate limit: ожидание превышено")
        return OptimizationResult(
            success=False, rate_limited=True,
            error="Rate limit: ожидание превышено или отменено",
        )

    exit_code, output, cli_result = await claude_runner.run_optimization(
        project_info, pid,
        on_output=ctx.log,
    )
    ctx.record_cli_usage(cli_result, "optimization")

    if is_cancelled(exit_code):
        await ctx.log("Оптимизация отменена", "warn")
        ctx.update_pipeline_log("optimization", "error", error="Отменено")
        return OptimizationResult(success=False, cancelled=True)

    if exit_code == 0:
        opt_file = ctx.output_dir / "optimization.json"
        if opt_file.exists():
            total_items, savings, size_kb = _read_optimization_meta(opt_file)
            try:
                await ctx.log(
                    f"Оптимизация завершена: {total_items} предложений, "
                    f"~{savings}% средняя экономия ({size_kb} KB)",
                    "info",
                )
            except Exception:
                await ctx.log(f"optimization.json создан ({size_kb} KB)", "info")
        else:
            await ctx.log("optimization.json не создан — Claude не записал результат", "warn")
        ctx.update_pipeline_log("optimization", "done", message="OK")
        return OptimizationResult(success=True)

    if is_rate_limited(exit_code, output or "", ""):
        await ctx.log("Rate limit при оптимизации, ожидание...", "warn")
        can_continue = await ctx.wait_for_rate_limit(
            "rate limit при оптимизации", output or ""
        )
        if not can_continue:
            ctx.update_pipeline_log("optimization", "error",
                                    error="Rate limit: ожидание превышено")
            return OptimizationResult(
                success=False, rate_limited=True,
                error="Rate limit: ожидание превышено или отменено",
            )

        exit_code, output, cli_result = await claude_runner.run_optimization(
            project_info, pid,
            on_output=ctx.log,
        )
        ctx.record_cli_usage(cli_result, "optimization_retry")
        if exit_code == 0:
            await ctx.log("Оптимизация завершена (после паузы)", "info")
            ctx.update_pipeline_log("optimization", "done",
                                    message="OK (после rate limit паузы)")
            return OptimizationResult(success=True)
        else:
            detail = _extract_error_detail(exit_code, output)
            await ctx.log(f"Ошибка оптимизации после retry (код {exit_code})", "error")
            ctx.update_pipeline_log("optimization", "error", error=detail)
            return OptimizationResult(success=False, error=f"Exit code: {exit_code}")

    detail = _extract_error_detail(exit_code, output)
    await ctx.log(f"Ошибка оптимизации (код {exit_code})", "error")
    ctx.update_pipeline_log("optimization", "error", error=detail)
    return OptimizationResult(success=False, error=f"Exit code: {exit_code}")


# ─── run_optimization_review ─────────────────────────────────────────────────

async def run_optimization_review(ctx: PipelineStageContext) -> OptimizationReviewResult:
    """Critic + Corrector для оптимизации.

    1. Critic проверяет каждое OPT-предложение (vendor, savings, traceability).
    2. Если есть отрицательные вердикты — Corrector исправляет.
    """
    pid = ctx.project_id
    project_info = ctx.project_info or {}
    output_dir = ctx.output_dir

    # ── Critic ──
    ctx.update_pipeline_log("optimization_critic", "running")
    print(f"[{pid}] ═══ Optimization Critic (проверка оптимизации) ═══")
    await ctx.log("═══ Optimization Critic — проверка обоснованности предложений ═══")

    can_go = await ctx.check_before_launch()
    if not can_go:
        await ctx.log("Rate limit: ожидание превышено или отменено", "warn")
        return OptimizationReviewResult(cancelled=True)

    exit_code, output, cli_result = await claude_runner.run_optimization_critic(
        project_info, pid,
        on_output=ctx.log,
    )
    ctx.record_cli_usage(cli_result, "optimization_critic")

    if is_cancelled(exit_code):
        return OptimizationReviewResult(cancelled=True)

    if exit_code != 0:
        review_path_check = output_dir / "optimization_review.json"
        if review_path_check.exists():
            try:
                review_data_check = json.loads(review_path_check.read_text(encoding="utf-8"))
                reviewed = review_data_check.get("meta", {}).get("total_reviewed", 0)
                if reviewed > 0:
                    await ctx.log(
                        f"Optimization Critic: CLI код {exit_code}, но review файл валиден "
                        f"({reviewed} reviewed) — продолжаем",
                        "warn",
                    )
                    ctx.update_pipeline_log(
                        "optimization_critic", "done",
                        message=f"OK (CLI код {exit_code}, файл валиден)",
                    )
                else:
                    raise ValueError("total_reviewed == 0")
            except (json.JSONDecodeError, OSError, ValueError):
                ctx.update_pipeline_log("optimization_critic", "error",
                                        error=_extract_error_detail(exit_code, output))
                await ctx.log(f"Optimization Critic: код {exit_code}, файл невалиден", "warn")
                return OptimizationReviewResult(error=f"Critic failed: exit {exit_code}")
        else:
            ctx.update_pipeline_log("optimization_critic", "error",
                                    error=_extract_error_detail(exit_code, output))
            await ctx.log(f"Optimization Critic: код {exit_code}, файл не создан", "warn")
            return OptimizationReviewResult(error=f"Critic failed: exit {exit_code}")
    else:
        ctx.update_pipeline_log("optimization_critic", "done", message="OK")

    review_path = output_dir / "optimization_review.json"
    if not review_path.exists():
        await ctx.log("optimization_review.json не создан — пропуск Corrector", "warn")
        return OptimizationReviewResult(critic_ok=True, corrector_skipped=True)

    try:
        review_data = json.loads(review_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        await ctx.log("Ошибка чтения optimization_review.json", "warn")
        return OptimizationReviewResult(critic_ok=True,
                                        error="Ошибка чтения optimization_review.json")

    verdicts = review_data.get("meta", {}).get("verdicts", {})
    total_pass = verdicts.get("pass", 0)
    total_reviewed = review_data.get("meta", {}).get("total_reviewed", 0)
    total_issues = total_reviewed - total_pass

    await ctx.log(
        f"Optimization Critic: {total_reviewed} проверено, {total_pass} pass, {total_issues} проблем",
    )

    if total_issues == 0:
        await ctx.log("Все предложения обоснованы — Corrector не требуется")
        ctx.update_pipeline_log("optimization_corrector", "skipped",
                                message="Все предложения прошли Critic")
        return OptimizationReviewResult(critic_ok=True, corrector_skipped=True)

    # ── Corrector ──
    opt_check_path = output_dir / "optimization.json"
    if opt_check_path.exists():
        try:
            json.loads(opt_check_path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError) as e:
            await ctx.log(f"optimization.json невалиден перед Corrector: {e}", "warn")
            ctx.update_pipeline_log("optimization_corrector", "error",
                                    error="optimization.json невалиден")
            return OptimizationReviewResult(critic_ok=True,
                                            error="optimization.json невалиден перед Corrector")

    ctx.update_pipeline_log("optimization_corrector", "running")
    print(f"[{pid}] ═══ Optimization Corrector (корректировка оптимизации) ═══")
    await ctx.log(
        f"═══ Optimization Corrector — корректировка {total_issues} предложений ═══",
    )

    can_go = await ctx.check_before_launch()
    if not can_go:
        await ctx.log("Rate limit: ожидание превышено или отменено", "warn")
        return OptimizationReviewResult(critic_ok=True, cancelled=True)

    exit_code, output, cli_result = await claude_runner.run_optimization_corrector(
        project_info, pid,
        on_output=ctx.log,
    )
    ctx.record_cli_usage(cli_result, "optimization_corrector")

    if is_cancelled(exit_code):
        return OptimizationReviewResult(critic_ok=True, cancelled=True)

    opt_path = output_dir / "optimization.json"
    pre_review = output_dir / "optimization_pre_review.json"

    if exit_code != 0:
        if opt_path.exists() and pre_review.exists():
            try:
                new_data = json.loads(opt_path.read_text(encoding="utf-8"))
                old_data = json.loads(pre_review.read_text(encoding="utf-8"))
                new_count = len(new_data.get("scenarios", new_data.get("optimizations", [])))
                old_count = len(old_data.get("scenarios", old_data.get("optimizations", [])))
                if new_count > 0 and new_data != old_data:
                    await ctx.log(
                        f"Optimization Corrector: CLI код {exit_code}, но optimization.json обновлён "
                        f"({old_count} → {new_count}) — считаем успехом",
                        "warn",
                    )
                    ctx.update_pipeline_log(
                        "optimization_corrector", "done",
                        message=f"OK (CLI код {exit_code}, файл обновлён)",
                    )
                    corrector_ok = True
                else:
                    raise ValueError("Файл не изменился")
            except (json.JSONDecodeError, OSError, ValueError):
                ctx.update_pipeline_log("optimization_corrector", "error",
                                        error=_extract_error_detail(exit_code, output))
                await ctx.log(f"Optimization Corrector: код {exit_code}", "warn")
                if pre_review.exists() and opt_path.exists():
                    shutil.copy2(pre_review, opt_path)
                    await ctx.log(
                        "Восстановлен optimization_pre_review.json после ошибки Corrector", "warn"
                    )
                return OptimizationReviewResult(critic_ok=True,
                                                error=f"Corrector failed: exit {exit_code}")
        else:
            ctx.update_pipeline_log("optimization_corrector", "error",
                                    error=_extract_error_detail(exit_code, output))
            await ctx.log(f"Optimization Corrector: код {exit_code}", "warn")
            return OptimizationReviewResult(critic_ok=True,
                                            error=f"Corrector failed: exit {exit_code}")
    else:
        ctx.update_pipeline_log("optimization_corrector", "done", message="OK")
        await ctx.log("Optimization Corrector завершён — optimization.json обновлён")

    if opt_path.exists():
        is_valid, repair_msg = validate_and_repair_json(opt_path)
        if not is_valid:
            await ctx.log(
                f"ВНИМАНИЕ: optimization.json невалиден после Corrector: {repair_msg}. "
                f"Восстанавливаю pre_review версию.",
                "error",
            )
            if pre_review.exists():
                shutil.copy2(pre_review, opt_path)
                await ctx.log("Восстановлен optimization_pre_review.json", "warn")
        elif "Repaired" in repair_msg:
            await ctx.log(f"optimization.json починен автоматически: {repair_msg}", "warn")

    return OptimizationReviewResult(critic_ok=True, corrector_ok=True)
