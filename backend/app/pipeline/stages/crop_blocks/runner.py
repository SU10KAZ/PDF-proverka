"""
crop_blocks/runner.py
---------------------
Stage runner для этапа crop_blocks (скачивание и обрезка image-блоков).

Содержит:
  - Pure helper functions (build_crop_args, existing_crop_matches_policy, crop_policy_label)
    — перенесены из manager.py в предыдущих pass-ах; используются как aliases.
  - run_crop_blocks(ctx, ...) — полный crop stage (Gemma policy).
  - run_policy_recrop(ctx, ...) — форсированный перекроп при несовпадении policy.

Публичный API:
  build_crop_args(project_path, force, *, policy, output_dir_name) -> list[str]
  existing_crop_matches_policy(blocks_index_path, policy) -> bool
  crop_policy_label(policy) -> str
  run_crop_blocks(ctx, *, project_rel_path, force, policy, output_dir_name) -> StageResult
  run_policy_recrop(ctx, *, project_rel_path, policy, output_dir_name) -> StageResult
"""
from __future__ import annotations

from pathlib import Path

from backend.app.core.config import BLOCKS_SCRIPT
from backend.app.pipeline.context import PipelineStageContext
from backend.app.pipeline.stage_result import StageResult
from backend.app.pipeline.stages.gemma_enrichment.gemma_enrichment_contract import (
    GEMMA_BLOCKS_DIRNAME,
    crop_index_matches_policy,
    gemma_enrichment_crop_policy,
)


# ─── Pure helpers (re-exported from previous pass) ───────────────────────────

def build_crop_args(
    project_path: str,
    force: bool = False,
    *,
    policy: dict | None = None,
    output_dir_name: str | None = GEMMA_BLOCKS_DIRNAME,
) -> list[str]:
    """Build blocks.py crop args from an explicit crop policy.

    Gemma enrichment intentionally uses its own production crop policy and is
    not tied to the Stage 02 model choice.
    """
    policy = policy or gemma_enrichment_crop_policy()
    args = ["crop", project_path]
    if output_dir_name:
        args.extend(["--output-dir", output_dir_name])
    if policy.get("compact"):
        args.append("--compact")
    elif policy.get("dpi"):
        args.extend(["--dpi", str(int(policy["dpi"]))])
    if policy.get("skip_small") is False:
        args.append("--no-skip-small")
    if force:
        args.append("--force")
    return args


def existing_crop_matches_policy(blocks_index_path: Path, policy: dict | None = None) -> bool:
    """Check an existing crop index against an explicit crop policy."""
    return crop_index_matches_policy(blocks_index_path, policy or gemma_enrichment_crop_policy())


def crop_policy_label(policy: dict) -> str:
    """Return a human-readable label for a crop policy dict."""
    compact = "compact" if policy.get("compact") else "non-compact"
    small = "skip-small" if policy.get("skip_small", True) else "no-skip-small"
    return f"{policy.get('dpi')} DPI, {compact}, {small}"


# ─── Stage runners ────────────────────────────────────────────────────────────

async def run_crop_blocks(
    ctx: PipelineStageContext,
    *,
    project_rel_path: str,
    force: bool = False,
    policy: dict | None = None,
    output_dir_name: str = GEMMA_BLOCKS_DIRNAME,
) -> StageResult:
    """Запуск blocks.py crop для Gemma-enrichment crop policy.

    Управляет:
    - выбором force-флага (на основе несовпадения policy или stale dir);
    - update_pipeline_log("crop_blocks", "running" → "done" / "error");
    - обработкой exit_code==2 (частичная ошибка, не все блоки скачались);
    - обработкой exit_code!=0 (полная ошибка);
    - логированием через ctx.log.

    Не управляет:
    - job.stage / job.status (выставляет оркестратор);
    - heartbeat / cleanup / document_graph_v2 (дело оркестратора).
    """
    effective_policy = policy or gemma_enrichment_crop_policy()

    ctx.update_pipeline_log("crop_blocks", "running")
    await ctx.log("═══ ЭТАП 1: Кроп image-блоков из PDF ═══")

    crop_args = build_crop_args(
        project_rel_path,
        force=force,
        policy=effective_policy,
        output_dir_name=output_dir_name,
    )

    exit_code, _, stderr = await ctx.run_subprocess(
        str(BLOCKS_SCRIPT),
        crop_args,
        on_output=ctx.log,
    )

    if exit_code == 2:
        error = "Не все блоки скачались. Проверьте актуальность crop_url в result.json"
        ctx.update_pipeline_log("crop_blocks", "error", error=error)
        return StageResult.fail(
            "Кроп блоков: не все image-блоки скачались (HTTP 404). "
            "Обновите OCR-результат и повторите."
        )

    if exit_code != 0:
        error = stderr or f"Exit code: {exit_code}"
        ctx.update_pipeline_log("crop_blocks", "error", error=error)
        return StageResult.fail(f"Кроп блоков: {error}")

    label = crop_policy_label(effective_policy)
    ctx.update_pipeline_log(
        "crop_blocks", "done",
        message=f"OK (Gemma policy: {label})",
    )
    return StageResult.ok(policy_label=label)


async def run_policy_recrop(
    ctx: PipelineStageContext,
    *,
    project_rel_path: str,
    policy: dict | None = None,
    output_dir_name: str = GEMMA_BLOCKS_DIRNAME,
) -> StageResult:
    """Форсированный перекроп при несовпадении Gemma crop policy.

    Вызывается из _run_gemma_enrichment_stage когда существующий crop index
    не соответствует ожидаемой Gemma enrichment policy.

    Всегда force=True — пересоздаёт crop поверх существующего.
    """
    effective_policy = policy or gemma_enrichment_crop_policy()
    label = crop_policy_label(effective_policy)

    await ctx.log(
        f"Crop не совпадает с Gemma enrichment policy ({label}) — перекропаю перед Gemma",
        "warn",
    )
    ctx.update_pipeline_log("crop_blocks", "running")

    exit_code, _, stderr = await ctx.run_subprocess(
        str(BLOCKS_SCRIPT),
        build_crop_args(
            project_rel_path,
            force=True,
            policy=effective_policy,
            output_dir_name=output_dir_name,
        ),
        on_output=ctx.log,
    )

    if exit_code != 0:
        error = stderr or f"Exit code: {exit_code}"
        ctx.update_pipeline_log("crop_blocks", "error", error=error)
        return StageResult.fail(f"Gemma crop policy recrop failed: {error}")

    ctx.update_pipeline_log(
        "crop_blocks", "done",
        message=f"OK (Gemma policy: {label})",
    )
    return StageResult.ok(policy_label=label)
