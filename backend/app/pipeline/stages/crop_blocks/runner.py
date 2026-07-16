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
import shutil
from uuid import uuid4

from backend.app.core.config import BLOCKS_SCRIPT
from backend.app.pipeline.context import PipelineStageContext
from backend.app.pipeline.stage_result import StageResult
from backend.app.pipeline.stages.gemma_enrichment.gemma_enrichment_contract import (
    STAGE02_BLOCKS_DIRNAME,
    crop_index_matches_policy,
    stage02_crop_policy,
)


# ─── Pure helpers (re-exported from previous pass) ───────────────────────────

def build_crop_args(
    project_path: str,
    force: bool = False,
    *,
    policy: dict | None = None,
    output_dir_name: str | None = STAGE02_BLOCKS_DIRNAME,
) -> list[str]:
    """Build blocks.py crop args from an explicit crop policy.

    Stage 01 uses the canonical context crop policy.
    """
    policy = policy or stage02_crop_policy()
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
    return crop_index_matches_policy(blocks_index_path, policy or stage02_crop_policy())


def crop_policy_label(policy: dict) -> str:
    """Return a human-readable label for a crop policy dict."""
    compact = "compact" if policy.get("compact") else "non-compact"
    small = "skip-small" if policy.get("skip_small", True) else "no-skip-small"
    return f"{policy.get('dpi')} DPI, {compact}, {small}"


def sync_v2_read_canary_blocks_alias(
    project_dir: Path | str,
    output_dir: Path | str,
    source_dir_name: str = STAGE02_BLOCKS_DIRNAME,
) -> bool:
    """Mirror v2 Stage 01 crop output to the read_canary-compatible ``blocks/`` dir.

    Deploy read_canary treats ``03_analysis/{latest,runs}/blocks/index.json`` as
    the stable read contract. The audit write path keeps the richer producer
    directory name (``blocks_stage02_100``), so under projects_v2 we materialize a
    same-run ``blocks/`` alias after crop. Legacy output dirs are left alone.
    """
    project_dir = Path(project_dir)
    output_dir = Path(output_dir)
    try:
        from backend.app.services.storage.projects_v2_source_resolver import (
            is_projects_v2_version_dir,
        )

        if not is_projects_v2_version_dir(project_dir):
            return False
    except Exception:
        return False

    source_dir = output_dir / source_dir_name
    if not (source_dir / "index.json").is_file():
        return False

    dest_dir = output_dir / "blocks"
    tmp_dir = output_dir / f".blocks_alias_tmp_{uuid4().hex}"
    try:
        if tmp_dir.exists():
            shutil.rmtree(tmp_dir)
        shutil.copytree(source_dir, tmp_dir)
        if dest_dir.exists() or dest_dir.is_symlink():
            if dest_dir.is_dir() and not dest_dir.is_symlink():
                shutil.rmtree(dest_dir)
            else:
                dest_dir.unlink()
        tmp_dir.replace(dest_dir)
        return True
    finally:
        if tmp_dir.exists():
            shutil.rmtree(tmp_dir, ignore_errors=True)


def _sync_ctx_v2_read_canary_blocks_alias(
    ctx: PipelineStageContext,
    output_dir_name: str,
) -> bool:
    project_dir = getattr(ctx, "project_dir", None)
    output_dir = getattr(ctx, "output_dir", None)
    if project_dir is None or output_dir is None:
        return False
    return sync_v2_read_canary_blocks_alias(project_dir, output_dir, output_dir_name)


def _partial_crop_result(
    ctx: PipelineStageContext, output_dir_name: str, label: str
) -> StageResult | None:
    """reserc.md #9: exit_code==2 = часть crop_url отдала HTTP 404.

    Partial-success везде (как на Stage 02 crop): если хоть какие-то блоки
    скачались (index.json существует) — продолжаем с доступными, пропуски
    попадут в coverage. Если не скачалось НИЧЕГО — возвращаем None и caller
    делает hard-fail.
    """
    index_path = ctx.output_dir / output_dir_name / "index.json"
    if not index_path.exists():
        return None
    _sync_ctx_v2_read_canary_blocks_alias(ctx, output_dir_name)
    ctx.update_pipeline_log(
        "crop_blocks", "done",
        message=f"OK частично ({label}; часть блоков пропущена, см. coverage)",
    )
    return StageResult.ok(policy_label=label, partial=True)


# ─── Stage runners ────────────────────────────────────────────────────────────

async def run_crop_blocks(
    ctx: PipelineStageContext,
    *,
    project_rel_path: str,
    force: bool = False,
    policy: dict | None = None,
    output_dir_name: str = STAGE02_BLOCKS_DIRNAME,
) -> StageResult:
    """Запуск blocks.py crop для Stage 01 context policy.

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
    effective_policy = policy or stage02_crop_policy()

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
        # reserc.md #9: частичная неудача скачивания — продолжаем с доступными
        # блоками (partial-success), пропуски попадут в coverage. Hard-fail
        # только если не скачалось НИЧЕГО.
        await ctx.log(
            "Кроп блоков: часть image-блоков не скачалась (HTTP 404); "
            "продолжаю с доступными, пропуски попадут в coverage",
            "warn",
        )
        partial = _partial_crop_result(
            ctx, output_dir_name, crop_policy_label(effective_policy)
        )
        if partial is not None:
            return partial
        error = "Не скачался ни один image-блок (HTTP 404). Обновите OCR-результат и повторите."
        ctx.update_pipeline_log("crop_blocks", "error", error=error)
        return StageResult.fail(f"Кроп блоков: {error}")

    if exit_code != 0:
        error = stderr or f"Exit code: {exit_code}"
        ctx.update_pipeline_log("crop_blocks", "error", error=error)
        return StageResult.fail(f"Кроп блоков: {error}")

    label = crop_policy_label(effective_policy)
    _sync_ctx_v2_read_canary_blocks_alias(ctx, output_dir_name)
    ctx.update_pipeline_log(
        "crop_blocks", "done",
        message=f"OK (Stage 01 policy: {label})",
    )
    return StageResult.ok(policy_label=label)


async def run_policy_recrop(
    ctx: PipelineStageContext,
    *,
    project_rel_path: str,
    policy: dict | None = None,
    output_dir_name: str = STAGE02_BLOCKS_DIRNAME,
) -> StageResult:
    """Форсированный перекроп при несовпадении Stage 01 crop policy.

    Вызывается перед локальной подготовкой контекста, если crop policy устарела.

    Всегда force=True — пересоздаёт crop поверх существующего.
    """
    effective_policy = policy or stage02_crop_policy()
    label = crop_policy_label(effective_policy)

    await ctx.log(
        f"Crop не совпадает со Stage 01 policy ({label}) — выполняю перекроп",
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

    if exit_code == 2:
        # reserc.md #9: partial-success и при перекропе по policy.
        await ctx.log(
            "Перекроп: часть image-блоков не скачалась (HTTP 404); "
            "продолжаю с доступными, пропуски попадут в coverage",
            "warn",
        )
        partial = _partial_crop_result(ctx, output_dir_name, label)
        if partial is not None:
            return partial
        error = "Не скачался ни один image-блок (HTTP 404). Обновите OCR-результат и повторите."
        ctx.update_pipeline_log("crop_blocks", "error", error=error)
        return StageResult.fail(f"Gemma crop policy recrop failed: {error}")

    if exit_code != 0:
        error = stderr or f"Exit code: {exit_code}"
        ctx.update_pipeline_log("crop_blocks", "error", error=error)
        return StageResult.fail(f"Gemma crop policy recrop failed: {error}")

    _sync_ctx_v2_read_canary_blocks_alias(ctx, output_dir_name)
    ctx.update_pipeline_log(
        "crop_blocks", "done",
        message=f"OK (Gemma policy: {label})",
    )
    return StageResult.ok(policy_label=label)
