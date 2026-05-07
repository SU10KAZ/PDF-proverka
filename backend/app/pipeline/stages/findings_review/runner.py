"""
findings_review/runner.py
-------------------------
Stage runner: Critic + Corrector для замечаний (findings review).

Extracted from PipelineManager._run_findings_review.
Бизнес-логика не изменена; оркестратор (manager.py) передаёт управление
через PipelineStageContext и забирает итоговый статус.

Публичный API:
  run_findings_review(ctx) -> FindingsReviewResult
"""
from __future__ import annotations

import asyncio
import json
import shutil
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional

import backend.app.services.llm.claude_runner as claude_runner
from backend.app.core.config import (
    CRITIC_CHUNK_SIZE,
    CORRECTOR_CHUNK_SIZE,
    MAX_PARALLEL_BATCHES,
    get_stage_model,
    is_local_llm_model,
)
from backend.app.pipeline.context import PipelineStageContext
from backend.app.pipeline.stages.block_analysis.runner import validate_and_repair_json
from backend.app.services.common.cli_utils import is_cancelled


# ─── Result type ─────────────────────────────────────────────────────────────

@dataclass
class FindingsReviewResult:
    """Результат run_findings_review (critic + corrector)."""
    critic_ok: bool = False
    corrector_skipped: bool = False
    corrector_ok: bool = False
    cancelled: bool = False
    error: Optional[str] = None


# ─── Internal helpers ────────────────────────────────────────────────────────

def _extract_error_detail(exit_code: int, output: str, max_len: int = 120) -> str:
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


async def _restore_norm_quotes(output_dir: Path, ctx: PipelineStageContext) -> None:
    """Восстановить norm_quote из pre_review бэкапа.

    Corrector может перезаписать findings без этого поля.
    Берём его из бэкапа (до corrector) и подставляем обратно.
    """
    findings_path = output_dir / "03_findings.json"
    pre_review_path = output_dir / "03_findings_pre_review.json"
    if not findings_path.exists() or not pre_review_path.exists():
        return

    try:
        current = json.loads(findings_path.read_text(encoding="utf-8"))
        backup = json.loads(pre_review_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return

    backup_map = {f.get("id"): f for f in backup.get("findings", [])}
    restored = 0
    for finding in current.get("findings", []):
        fid = finding.get("id")
        if not fid or fid not in backup_map:
            continue
        orig = backup_map[fid]
        if not finding.get("norm_quote") and orig.get("norm_quote"):
            finding["norm_quote"] = orig["norm_quote"]
            restored += 1
    if restored > 0:
        findings_path.write_text(
            json.dumps(current, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )


# ─── run_findings_review ─────────────────────────────────────────────────────

async def run_findings_review(ctx: PipelineStageContext) -> FindingsReviewResult:
    """Critic + Corrector: проверка и корректировка замечаний.

    1. Critic проверяет каждое F-замечание (evidence, grounding, page/sheet)
       Если findings > CRITIC_CHUNK_SIZE — разбивает на чанки.
    2. Если есть отрицательные вердикты — Corrector исправляет.
    """
    pid = ctx.project_id
    project_info = ctx.project_info or {}
    output_dir = ctx.output_dir

    # ── Pre-Critic: Python-level grounding ──
    try:
        from backend.app.services.findings.grounding_service import run_grounding
        findings_path = output_dir / "03_findings.json"
        blocks_path = output_dir / "02_blocks_analysis.json"
        if findings_path.exists() and blocks_path.exists():
            grounding_stats = run_grounding(findings_path, blocks_path)
            await ctx.log(
                f"Grounding: {grounding_stats.get('grounding_candidates_added', 0)} "
                f"findings обогащены кандидатами "
                f"(уже привязано: {grounding_stats.get('already_grounded', 0)})",
            )
    except Exception as e:
        await ctx.log(f"Grounding пропущен: {e}", "warn")

    # ── Critic (с chunking при большом кол-ве findings) ──
    if ctx.reset_job_progress:
        ctx.reset_job_progress()

    ctx.update_pipeline_log("findings_critic", "running")
    print(f"[{pid}] ═══ ЭТАП 6.5a: Critic (проверка замечаний) ═══")
    await ctx.log("═══ ЭТАП 6.5a: Critic — проверка обоснованности замечаний ═══")

    findings_path = output_dir / "03_findings.json"
    need_chunks = False
    all_findings = []

    if findings_path.exists():
        try:
            findings_data = json.loads(findings_path.read_text(encoding="utf-8"))
            all_findings = findings_data.get("findings", findings_data.get("items", []))
            need_chunks = len(all_findings) > CRITIC_CHUNK_SIZE
        except (json.JSONDecodeError, OSError):
            pass

    if need_chunks:
        # ── Chunked Critic (ПАРАЛЛЕЛЬНЫЙ) ──
        total_findings = len(all_findings)
        chunks = [
            all_findings[i:i + CRITIC_CHUNK_SIZE]
            for i in range(0, total_findings, CRITIC_CHUNK_SIZE)
        ]
        num_chunks = len(chunks)
        await ctx.log(
            f"Chunked Critic (parallel): {total_findings} findings -> "
            f"{num_chunks} чанков по ~{CRITIC_CHUNK_SIZE}",
        )

        for chunk_idx, chunk_findings in enumerate(chunks, 1):
            suffix = f"_{chunk_idx:03d}"
            chunk_input = {
                "meta": {
                    "source": "full_review",
                    "total_findings": total_findings,
                    "chunk_count": len(chunk_findings),
                    "chunk": chunk_idx,
                    "total_chunks": num_chunks,
                },
                "findings": chunk_findings,
            }
            chunk_input_path = output_dir / f"03_findings_review_input{suffix}.json"
            chunk_input_path.write_text(
                json.dumps(chunk_input, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )

        critic_parallel = (
            1 if is_local_llm_model(get_stage_model("findings_critic"))
            else MAX_PARALLEL_BATCHES
        )
        critic_semaphore = asyncio.Semaphore(critic_parallel)
        chunk_results: list[dict | None] = [None] * num_chunks

        async def _run_critic_chunk(cidx: int) -> None:
            suffix = f"_{cidx:03d}"
            async with critic_semaphore:
                can_go = await ctx.check_before_launch()
                if not can_go:
                    await ctx.log(f"Critic чанк {cidx}: rate limit, пропуск", "warn")
                    return

                await ctx.log(
                    f"Critic чанк {cidx}/{num_chunks}: "
                    f"{len(chunks[cidx - 1])} findings...",
                )

                exit_code, output, cli_result = await claude_runner.run_findings_critic(
                    project_info, pid,
                    on_output=lambda msg: ctx.log(msg),
                    chunk_suffix=suffix,
                )
                ctx.record_cli_usage(cli_result, f"findings_critic_chunk{cidx}")

                if is_cancelled(exit_code):
                    return

                chunk_review_path = output_dir / f"03_findings_review{suffix}.json"
                if exit_code != 0 and not chunk_review_path.exists():
                    await ctx.log(
                        f"Critic чанк {cidx}/{num_chunks}: код {exit_code}, файл не создан",
                        "warn",
                    )
                    return

                if exit_code != 0:
                    await ctx.log(
                        f"Critic чанк {cidx}/{num_chunks}: CLI код {exit_code}, "
                        f"но файл создан — пробуем использовать",
                        "warn",
                    )

                if chunk_review_path.exists():
                    try:
                        chunk_review = json.loads(
                            chunk_review_path.read_text(encoding="utf-8")
                        )
                        chunk_results[cidx - 1] = chunk_review
                        chunk_meta = chunk_review.get("meta", {})
                        await ctx.log(
                            f"Critic чанк {cidx}: "
                            f"{chunk_meta.get('total_reviewed', '?')} проверено, "
                            f"{chunk_meta.get('verdicts', {}).get('pass', 0)} pass",
                        )
                    except (json.JSONDecodeError, OSError) as e:
                        await ctx.log(
                            f"Ошибка чтения результата чанка {cidx}: {e}",
                            "warn",
                        )

        tasks = [
            asyncio.create_task(_run_critic_chunk(cidx))
            for cidx in range(1, num_chunks + 1)
        ]
        gathered = await asyncio.gather(*tasks, return_exceptions=True)
        for cidx, result in enumerate(gathered, start=1):
            if isinstance(result, Exception):
                await ctx.log(
                    f"Critic чанк {cidx}: необработанное исключение task — "
                    f"{type(result).__name__}: {result}",
                    "error",
                )

        # Проверка отмены после gather
        # (нет прямого доступа к job.status, cancelled проверяется через check_before_launch)

        # Слияние результатов
        all_reviews = []
        merged_verdicts = {}
        total_reviewed_all = 0
        chunks_ok = 0

        for cr in chunk_results:
            if cr is None:
                continue
            chunks_ok += 1
            chunk_reviews = cr.get("reviews", [])
            all_reviews.extend(chunk_reviews)
            chunk_meta = cr.get("meta", {})
            total_reviewed_all += chunk_meta.get("total_reviewed", len(chunk_reviews))
            for k, v in chunk_meta.get("verdicts", {}).items():
                merged_verdicts[k] = merged_verdicts.get(k, 0) + v

        if not all_reviews and chunks_ok == 0:
            ctx.update_pipeline_log("findings_critic", "error",
                                    error="Все чанки провалились")
            await ctx.log("Critic: все чанки провалились, пропуск корректировки", "warn")
            return FindingsReviewResult(error="Все чанки провалились")

        merged_review = {
            "meta": {
                "project_id": pid,
                "review_date": datetime.now().isoformat(),
                "total_reviewed": total_reviewed_all,
                "verdicts": merged_verdicts,
                "chunks_total": num_chunks,
                "chunks_ok": chunks_ok,
            },
            "reviews": all_reviews,
        }
        review_path = output_dir / "03_findings_review.json"
        review_path.write_text(
            json.dumps(merged_review, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

        for cidx in range(1, num_chunks + 1):
            suffix = f"_{cidx:03d}"
            for pattern in [f"03_findings_review_input{suffix}.json",
                            f"03_findings_review{suffix}.json"]:
                p = output_dir / pattern
                if p.exists():
                    p.unlink()

        ctx.update_pipeline_log("findings_critic", "done",
                                message=f"Parallel: {num_chunks} chunks, {total_reviewed_all} reviewed")
        review_data = merged_review

    else:
        # ── Single-shot Critic (как раньше) ──
        can_go = await ctx.check_before_launch()
        if not can_go:
            await ctx.log("Rate limit: ожидание превышено или отменено", "warn")
            return FindingsReviewResult(error="Rate limit: ожидание превышено или отменено")

        exit_code, output, cli_result = await claude_runner.run_findings_critic(
            project_info, pid,
            on_output=lambda msg: ctx.log(msg),
        )
        ctx.record_cli_usage(cli_result, "findings_critic")

        if is_cancelled(exit_code):
            return FindingsReviewResult(cancelled=True)

        review_path = output_dir / "03_findings_review.json"
        if exit_code != 0:
            if review_path.exists():
                try:
                    review_data = json.loads(review_path.read_text(encoding="utf-8"))
                    reviewed = review_data.get("meta", {}).get("total_reviewed", 0)
                    if reviewed > 0:
                        await ctx.log(
                            f"Critic: CLI код {exit_code}, но review файл валиден "
                            f"({reviewed} reviewed) — продолжаем",
                            "warn",
                        )
                        ctx.update_pipeline_log(
                            "findings_critic", "done",
                            message=f"OK (CLI код {exit_code}, файл валиден)",
                        )
                    else:
                        raise ValueError("total_reviewed == 0")
                except (json.JSONDecodeError, OSError, ValueError):
                    ctx.update_pipeline_log("findings_critic", "error",
                                            error=_extract_error_detail(exit_code, output))
                    await ctx.log(f"Critic: код {exit_code}, файл невалиден — пропуск", "warn")
                    return FindingsReviewResult(error=f"Critic failed: exit {exit_code}")
            else:
                ctx.update_pipeline_log("findings_critic", "error",
                                        error=_extract_error_detail(exit_code, output))
                await ctx.log(f"Critic: код {exit_code}, файл не создан — пропуск", "warn")
                return FindingsReviewResult(error=f"Critic failed: exit {exit_code}")
        else:
            ctx.update_pipeline_log("findings_critic", "done", message="OK")

            if not review_path.exists():
                await ctx.log("03_findings_review.json не создан — пропуск Corrector", "warn")
                return FindingsReviewResult(critic_ok=True, corrector_skipped=True)

            try:
                review_data = json.loads(review_path.read_text(encoding="utf-8"))
            except (json.JSONDecodeError, OSError):
                await ctx.log("Ошибка чтения 03_findings_review.json", "warn")
                return FindingsReviewResult(critic_ok=True,
                                            error="Ошибка чтения 03_findings_review.json")

    # «Размышление модели»: стрим вердиктов критика
    if ctx.stream_findings_events:
        await ctx.stream_findings_events("critic")

    # ── Анализ результатов Critic ──
    verdicts = review_data.get("meta", {}).get("verdicts", {})
    total_pass = verdicts.get("pass", 0)
    total_reviewed = review_data.get("meta", {}).get("total_reviewed", 0)
    total_issues = total_reviewed - total_pass

    await ctx.log(
        f"Critic: {total_reviewed} проверено, {total_pass} pass, {total_issues} проблем",
    )

    if total_issues == 0:
        await ctx.log("Все замечания обоснованы — Corrector не требуется")
        if ctx.stream_findings_events:
            await ctx.stream_findings_events("done")
        return FindingsReviewResult(critic_ok=True, corrector_skipped=True)

    # ── Corrector (с поддержкой чанков) ──
    ctx.update_pipeline_log("findings_corrector", "running")
    print(f"[{pid}] ═══ ЭТАП 6.5b: Corrector (корректировка замечаний) ═══")

    if ctx.stream_findings_events:
        await ctx.stream_findings_events("corrector")

    issue_ids: list[str] = []
    for rev in review_data.get("reviews", []):
        if rev.get("verdict", "pass") != "pass":
            fid = rev.get("finding_id") or rev.get("id", "")
            if fid:
                issue_ids.append(fid)
    if not issue_ids:
        issue_ids = [f"F-{i:03d}" for i in range(1, total_issues + 1)]

    need_chunks = total_issues > CORRECTOR_CHUNK_SIZE
    if need_chunks:
        corrector_chunks = [
            issue_ids[i:i + CORRECTOR_CHUNK_SIZE]
            for i in range(0, len(issue_ids), CORRECTOR_CHUNK_SIZE)
        ]
        await ctx.log(
            f"═══ ЭТАП 6.5b: Corrector — {total_issues} замечаний → "
            f"{len(corrector_chunks)} чанков по ~{CORRECTOR_CHUNK_SIZE} ═══",
        )
    else:
        corrector_chunks = [issue_ids]
        await ctx.log(
            f"═══ ЭТАП 6.5b: Corrector — корректировка {total_issues} замечаний ═══",
        )

    corrector_ok = False
    for cidx, chunk_ids in enumerate(corrector_chunks):
        chunk_label = f" (чанк {cidx + 1}/{len(corrector_chunks)})" if need_chunks else ""

        review_path = output_dir / "03_findings_review.json"
        if need_chunks:
            chunk_review = dict(review_data)
            chunk_review["reviews"] = [
                r for r in review_data.get("reviews", [])
                if (r.get("finding_id") or r.get("id", "")) in chunk_ids
            ]
            chunk_meta = dict(chunk_review.get("meta", {}))
            chunk_meta["total_reviewed"] = len(chunk_review["reviews"])
            chunk_meta["chunk"] = f"{cidx + 1}/{len(corrector_chunks)}"
            chunk_review["meta"] = chunk_meta
            review_path.write_text(
                json.dumps(chunk_review, ensure_ascii=False, indent=2), encoding="utf-8",
            )
            await ctx.log(f"Corrector{chunk_label}: {', '.join(chunk_ids)}")

        can_go = await ctx.check_before_launch()
        if not can_go:
            await ctx.log("Rate limit: ожидание превышено или отменено", "warn")
            return FindingsReviewResult(critic_ok=True,
                                        error="Rate limit: ожидание превышено или отменено")

        exit_code, output, cli_result = await claude_runner.run_findings_corrector(
            project_info, pid,
            on_output=lambda msg: ctx.log(msg),
        )
        ctx.record_cli_usage(
            cli_result,
            f"findings_corrector{f'_chunk{cidx}' if need_chunks else ''}",
        )

        if is_cancelled(exit_code):
            return FindingsReviewResult(critic_ok=True, cancelled=True)

        if exit_code != 0:
            findings_path = output_dir / "03_findings.json"
            pre_review = output_dir / "03_findings_pre_review.json"
            if findings_path.exists() and pre_review.exists():
                try:
                    new_data = json.loads(findings_path.read_text(encoding="utf-8"))
                    old_data = json.loads(pre_review.read_text(encoding="utf-8"))
                    new_count = len(new_data.get("findings", []))
                    old_count = len(old_data.get("findings", []))
                    if new_count > 0 and new_data != old_data:
                        await ctx.log(
                            f"Corrector{chunk_label}: CLI код {exit_code}, но файл обновлён "
                            f"({old_count} → {new_count}) — считаем успехом",
                            "warn",
                        )
                        corrector_ok = True
                    else:
                        await ctx.log(
                            f"Corrector{chunk_label}: код {exit_code}, файл не изменился",
                            "warn",
                        )
                        if not need_chunks:
                            ctx.update_pipeline_log("findings_corrector", "error",
                                                    error=_extract_error_detail(exit_code, output))
                            return FindingsReviewResult(
                                critic_ok=True,
                                error=f"Corrector failed: exit {exit_code}",
                            )
                except (json.JSONDecodeError, OSError):
                    await ctx.log(
                        f"Corrector{chunk_label}: код {exit_code}, JSON невалиден", "warn",
                    )
                    if not need_chunks:
                        ctx.update_pipeline_log("findings_corrector", "error",
                                                error=_extract_error_detail(exit_code, output))
                        return FindingsReviewResult(
                            critic_ok=True,
                            error=f"Corrector failed: exit {exit_code}",
                        )
            else:
                await ctx.log(f"Corrector{chunk_label}: код {exit_code}", "warn")
                if not need_chunks:
                    ctx.update_pipeline_log("findings_corrector", "error",
                                            error=_extract_error_detail(exit_code, output))
                    return FindingsReviewResult(
                        critic_ok=True,
                        error=f"Corrector failed: exit {exit_code}",
                    )
        else:
            corrector_ok = True
            await ctx.log(f"Corrector{chunk_label} завершён — 03_findings.json обновлён")

    # Восстановить полный review после всех чанков
    if need_chunks:
        review_path = output_dir / "03_findings_review.json"
        review_path.write_text(
            json.dumps(review_data, ensure_ascii=False, indent=2), encoding="utf-8",
        )

    if corrector_ok:
        ctx.update_pipeline_log(
            "findings_corrector", "done",
            message=f"OK ({len(corrector_chunks)} чанков)" if need_chunks else "OK",
        )
    else:
        ctx.update_pipeline_log("findings_corrector", "error",
                                error="Все чанки провалились")

    # Валидация JSON после corrector
    findings_path = output_dir / "03_findings.json"
    if findings_path.exists():
        is_valid, repair_msg = validate_and_repair_json(findings_path)
        if not is_valid:
            await ctx.log(
                f"ВНИМАНИЕ: 03_findings.json невалиден после Corrector: {repair_msg}. "
                f"Восстанавливаю pre_review версию.",
                "error",
            )
            pre_review = output_dir / "03_findings_pre_review.json"
            if pre_review.exists():
                shutil.copy2(pre_review, findings_path)
                await ctx.log("Восстановлен 03_findings_pre_review.json", "warn")
        elif "Repaired" in repair_msg:
            await ctx.log(f"JSON починен автоматически: {repair_msg}", "warn")

    # Восстановление norm_quote из pre_review
    await _restore_norm_quotes(output_dir, ctx)

    if ctx.refresh_finding_quality:
        ctx.refresh_finding_quality()

    if ctx.stream_findings_events:
        await ctx.stream_findings_events("done")

    return FindingsReviewResult(critic_ok=True, corrector_ok=corrector_ok)
