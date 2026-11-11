"""
findings_verify/runner.py
-------------------------
Этап «Верификатор»: единый проход поверх слитого 03_findings.json.

Зачем
-----
LLM-критик (03b/03c) как фильтр статистически бесполезен (recall 17%, precision
49%, lift ≈ 0). А крупнейший класс браков (~32% отклонений эксперта) — ложные
«нет» («данные ЕСТЬ, ИИ не увидел»). Поэтому вместо LLM-критика — «Верификатор»,
который делает ДВЕ полезные вещи над финальным (дедуплицированным) списком:

1. Детерминированные структурные проверки (перенос из критика, ТОЛЬКО Python 1/2/4):
     · evidence_presence  — у замечания есть evidence/block_id;
     · block_exists       — block_id реально существуют (иначе phantom_block);
     · page_sheet_correct — page/sheet соответствуют evidence.
   Затем консервативный корректор (deterministic_corrector): чистит фантом-блоки,
   чинит page/sheet, no_evidence/contradicts → requires_human_review или мягкое
   понижение. ГЛАВНЫЙ ИНВАРИАНТ: НИЧЕГО не удаляется. Пишет 03_findings_review.json
   (downstream: UI-вердикты, экспорт, БЗ) и обновляет 03_findings.json.
   После фазы присутствия отдельные проверенные решения могут исключить запись
   из итогового списка, сохранив её в suppressed_findings и source_review_history.

2. LLM-проверка присутствия («страж отсутствия»): по ПОЛНОМУ MD подтверждает, что
   заявленный-как-отсутствующий элемент реально отсутствует; подтверждённо-ложные
   «нет» мягко понижает в «ПРОВЕРИТЬ ПО СМЕЖНЫМ». Верификатор инъектируем (claude -p
   по подписке; для больших MD — чанкинг). Безопасный инвариант: без MD/верификатора
   не понижаем ничего.

Килсвитч PIPELINE_VERIFIER_ENABLED (default True — «всегда включён»). Fail-soft:
ошибка любой внутренней фазы НЕ валит этап (замечания не теряются).

Публичный API:
  run_findings_verify(ctx) -> FindingsVerifyResult
"""
from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from backend.app.pipeline.context import PipelineStageContext

# pipeline_log-ключи: этап пишет ТЕ ЖЕ ключи, что и старый критик/корректор
# (findings_critic = структурная проверка → 03_findings_review.json; findings_corrector =
# применение вердиктов + absence → 03_findings.json). Так вся статус-машинерия (дашборд,
# read_canary, resume_detector, usage_service) и объединённый чип фронта работают БЕЗ правок.
# Ренейм в «Верификатор» — только UI-метка (Фаза B). Внутренний ключ этапа — для логов.
_CRITIC_KEY = "findings_critic"
_CORRECTOR_KEY = "findings_corrector"


@dataclass
class FindingsVerifyResult:
    """Результат этапа «Верификатор»."""
    ok: bool = False
    skipped: bool = False
    deterministic_ok: bool = False
    absence_ok: bool = False
    findings_total: int = 0
    deterministic_issues: int = 0
    phantom_cleaned: int = 0
    page_fixed: int = 0
    downgraded: int = 0
    flagged_human: int = 0
    absence_candidates: int = 0
    absence_downgraded: int = 0
    source_followups_prepared: int = 0
    source_review_candidates_ready: int = 0
    source_review_workflows_failed: int = 0
    source_review_candidates_rejected: int = 0
    source_review_not_attempted: int = 0
    source_review_cancelled: int = 0
    source_review_applied: int = 0
    source_review_not_applied: int = 0
    final_release_applied: int = 0
    final_release_held: int = 0
    final_release_not_completed: int = 0
    reviewed_groups_applied: int = 0
    reviewed_groups_not_applied: int = 0
    error: Optional[str] = None


# ─── Фаза 1: детерминированные проверки (sync, под to_thread) ─────────────────

def _run_deterministic_phase(output_dir: Path, project_id: str) -> dict:
    """Sync-обёртка: детерм. критик 1/2/4 (LLM выключен) → 03_findings_review.json;
    корректор применяет вердикты → 03_findings.json. Запускается в отдельном потоке
    (asyncio.run), чтобы синхронный json.loads больших блок-файлов не блокировал loop.
    """
    from backend.app.pipeline.stages.findings_review.deterministic_critic import (
        run_deterministic_critic,
    )
    from backend.app.pipeline.stages.findings_review.deterministic_corrector import (
        run_deterministic_corrector,
    )

    crit = asyncio.run(run_deterministic_critic(
        output_dir, project_id=project_id, llm_call=None, on_log=None, write=True,
    ))
    corr = asyncio.run(run_deterministic_corrector(
        output_dir, project_id=project_id, on_log=None, write=True,
    ))
    from .source_review_queue import build_source_review_queue, attach_pending_source_reviews
    from .source_review_graphics import attach_graphic_receipts
    from backend.app.pipeline.stages.prepare.graph_builder import resolve_document_markdown
    from backend.app.pipeline.stages.findings_review.deterministic_critic import build_index, iter_findings
    queue_error, pending = None, None
    try:
        # Never leave a previous run's review inputs available after a failed rebuild.
        (output_dir / '03_source_review_queue.json').unlink(missing_ok=True)
        current = json.loads((output_dir / '03_findings.json').read_text(encoding='utf-8'))
        findings = iter_findings(current)
        graph_path = output_dir / 'document_graph.json'
        blocks_path = output_dir / '01_blocks_analysis.json'
        graph = json.loads(graph_path.read_text(encoding='utf-8')) if graph_path.exists() else {}
        block_data = json.loads(blocks_path.read_text(encoding='utf-8')) if blocks_path.exists() else {}
        index = build_index(block_data, graph)
        known = index.image_block_ids | index.text_block_ids
        if attach_pending_source_reviews(findings, known):
            (output_dir / '03_findings.json').write_text(json.dumps(current, ensure_ascii=False, indent=2), encoding='utf-8')
        md_path = resolve_document_markdown(output_dir)
        md = md_path.read_text(encoding='utf-8') if md_path else ''
        queue = build_source_review_queue(findings, md)
        attach_graphic_receipts(queue, md, output_dir / 'blocks_stage02_100')
        (output_dir / '03_source_review_queue.json').write_text(
            json.dumps(queue, ensure_ascii=False, indent=2), encoding='utf-8')
        pending = queue['pending_count']
    except (OSError, ValueError, TypeError) as exc:
        queue_error = str(exc)
    return {
        "findings_total": crit.findings_total,
        "deterministic_issues": crit.deterministic_issues,
        "critic_error": crit.error,
        "phantom_cleaned": corr.phantom_cleaned,
        "page_fixed": corr.page_fixed,
        "downgraded": corr.downgraded,
        "flagged_human": corr.flagged_human,
        "corrector_error": corr.error,
        "source_reviews_pending": pending,
        "source_review_queue_error": queue_error,
    }


# ─── Фаза 2: LLM-проверка присутствия (sync, под to_thread) ───────────────────

def _run_absence_phase(output_dir: Path, project_info: dict, project_id: str) -> dict:
    """Sync-обёртка: «страж отсутствия» по полному MD. Понижает подтверждённо-ложные
    «нет». Верификатор — run_claude_verification_chunked (subprocess + чанкинг), поэтому
    фаза целиком в отдельном потоке. Fail-soft: без MD — безопасный режим (0 понижений).
    """
    from backend.app.pipeline.stages.text_analysis import absence_guard as ag
    from backend.app.pipeline.stages.prepare.task_builder import _get_md_file_path

    findings_path = Path(output_dir) / "03_findings.json"
    if not findings_path.is_file():
        return {"candidates": 0, "downgraded": 0, "verified": False, "no_findings": True}
    try:
        data = json.loads(findings_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return {"candidates": 0, "downgraded": 0, "verified": False, "read_error": True}

    if isinstance(data, dict):
        findings = data.get("findings") or data.get("items") or []
    elif isinstance(data, list):
        findings = data
    else:
        findings = []
    if not findings:
        return {"candidates": 0, "downgraded": 0, "verified": False}

    md_text = None
    try:
        md_path = _get_md_file_path(project_info, project_id)
        if md_path and md_path != "(нет)":
            md_text = Path(md_path).read_text(encoding="utf-8")
    except Exception:  # noqa: BLE001 — без MD страж уйдёт в безопасный режим
        md_text = None

    stats = ag.enforce_absence_guard(
        findings, md_text=md_text, verifier=ag.run_claude_verification_chunked,
    )
    if stats["downgraded"]:
        findings_path.write_text(
            json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8",
        )
    return stats


# ─── run_findings_verify ──────────────────────────────────────────────────────

async def run_findings_verify(ctx: PipelineStageContext) -> FindingsVerifyResult:
    """Этап «Верификатор»: детерм. структурные проверки + LLM-проверка присутствия."""
    from backend.app.core import config as cfg

    pid = ctx.project_id
    output_dir = ctx.output_dir
    project_info = ctx.project_info or {}

    if not getattr(cfg, "PIPELINE_VERIFIER_ENABLED", True):
        for key in (_CRITIC_KEY, _CORRECTOR_KEY):
            ctx.update_pipeline_log(key, "skipped",
                                    detail={"reason": "PIPELINE_VERIFIER_ENABLED=false"})
        await ctx.log("Верификатор отключён (PIPELINE_VERIFIER_ENABLED=false) — пропуск")
        return FindingsVerifyResult(skipped=True)

    if ctx.is_cancelled and ctx.is_cancelled():
        return FindingsVerifyResult(skipped=True)

    findings_path = Path(output_dir) / "03_findings.json"
    if not findings_path.is_file():
        for key in (_CRITIC_KEY, _CORRECTOR_KEY):
            ctx.update_pipeline_log(key, "skipped", detail={"reason": "нет 03_findings.json"})
        await ctx.log("Верификатор: 03_findings.json не найден — пропуск", "warn")
        return FindingsVerifyResult(skipped=True, error="03_findings.json не найден")

    ctx.update_pipeline_log(_CRITIC_KEY, "running")
    await ctx.log("═══ Верификатор — структурные проверки + проверка присутствия ═══")
    result = FindingsVerifyResult()

    # ── Фаза 1: детерминированные проверки (перенос из критика) ──
    try:
        det = await asyncio.to_thread(_run_deterministic_phase, Path(output_dir), pid)
        result.findings_total = det.get("findings_total", 0)
        result.deterministic_issues = det.get("deterministic_issues", 0)
        result.phantom_cleaned = det.get("phantom_cleaned", 0)
        result.page_fixed = det.get("page_fixed", 0)
        result.downgraded = det.get("downgraded", 0)
        result.flagged_human = det.get("flagged_human", 0)
        result.deterministic_ok = not (det.get("critic_error") or det.get("corrector_error"))
        await ctx.log(
            f"Верификатор (структура): {result.findings_total} замечаний, "
            f"{result.deterministic_issues} структурных проблем "
            f"(фантом-блоки: {result.phantom_cleaned}, page/sheet: {result.page_fixed}, "
            f"понижено: {result.downgraded}, на ручную проверку: {result.flagged_human})",
        )
        # findings_critic = структурная проверка (03_findings_review.json записан)
        ctx.update_pipeline_log(
            _CRITIC_KEY, "done" if result.deterministic_ok else "error",
            message=f"{result.findings_total} проверено, {result.deterministic_issues} проблем",
            error=(det.get("critic_error") or det.get("corrector_error")),
        )
    except Exception as e:  # noqa: BLE001 — fail-soft: не валим этап
        await ctx.log(f"Верификатор (структура) упал (не критично): {e}", "warn")
        result.error = f"deterministic: {e}"
        ctx.update_pipeline_log(_CRITIC_KEY, "error", error=str(e))
    ctx.update_pipeline_log(_CORRECTOR_KEY, "running")

    # ── Фаза 2: LLM-проверка присутствия («страж отсутствия») ──
    try:
        stats = await asyncio.to_thread(
            _run_absence_phase, Path(output_dir), project_info, pid,
        )
        result.absence_candidates = int(stats.get("candidates", 0) or 0)
        result.absence_downgraded = int(stats.get("downgraded", 0) or 0)
        result.absence_ok = True
        if result.absence_candidates:
            if stats.get("verified"):
                await ctx.log(
                    f"Верификатор (присутствие): понижено {result.absence_downgraded}/"
                    f"{result.absence_candidates} подтверждённо-ложных «нет»",
                )
            else:
                await ctx.log(
                    f"Верификатор (присутствие): {result.absence_candidates} кандидатов "
                    f"не проверены (нет MD/верификатора) — безопасный режим, не понижаем",
                    "warn",
                )
    except Exception as e:  # noqa: BLE001 — fail-soft
        await ctx.log(f"Верификатор (присутствие) упал (не критично): {e}", "warn")
        result.error = (result.error + f"; absence: {e}") if result.error else f"absence: {e}"

    # Explicit experimental callback only. No configured callback means no new
    # provider calls. Candidate outputs cannot authorize their own admission.
    if getattr(ctx, 'source_review_invoke', None) is not None:
        try:
            from .source_review_workflow import run_source_review_workflow_phase
            from backend.app.pipeline.stages.prepare.graph_builder import resolve_document_markdown
            workflow_md_path = resolve_document_markdown(Path(output_dir))
            if workflow_md_path is None:
                raise ValueError('source_review_md_unavailable')
            workflow = await run_source_review_workflow_phase(
                Path(output_dir), workflow_md_path.read_text(encoding='utf-8'),
                invoke=ctx.source_review_invoke,
                executor_id=getattr(ctx, 'source_review_executor_id', None),
                max_findings=getattr(ctx, 'source_review_max_findings', 0),
                auto_graphic_regions=getattr(ctx, 'source_review_auto_graphic_regions', False),
                is_cancelled=ctx.is_cancelled,
            )
            result.source_review_candidates_ready = workflow['candidate_ready']
            result.source_review_workflows_failed = workflow['not_completed']
            result.source_review_candidates_rejected = workflow['candidate_rejected']
            result.source_review_not_attempted = workflow['not_attempted']
            result.source_review_cancelled = workflow['cancelled']
            await ctx.log(f"Проверка источников: кандидатов решений {workflow['candidate_ready']}, незавершённых {workflow['not_completed']}, отклонённых кандидатов {workflow['candidate_rejected']}, не запускалось {workflow['not_attempted']}; требуется смысловая приёмка")
        except Exception as exc:
            await ctx.log(f"Последовательная проверка источников не завершена: {exc}", 'warn')
            result.error = (result.error + f"; source_workflow: {exc}") if result.error else f"source_workflow: {exc}"

    # Model-requested context is prepared locally before semantic admission.
    # Dispatch authorization remains outside the verifier.
    try:
        from .source_review_followup import run_review_response_phase
        from backend.app.pipeline.stages.prepare.graph_builder import resolve_document_markdown
        response_md_path = resolve_document_markdown(Path(output_dir))
        followups = await asyncio.to_thread(
            run_review_response_phase, Path(output_dir),
            response_md_path.read_text(encoding="utf-8") if response_md_path else "",
        )
        result.source_followups_prepared = followups["prepared"]
        if not followups["skipped"]:
            await ctx.log(f"Добор источников: подготовлено {followups['prepared']}, некорректных ответов {followups['invalid']}")
    except (OSError, ValueError, TypeError, KeyError) as exc:
        await ctx.log(f"Добор источников не подготовлен: {exc}", "warn")
        result.error = (result.error + f"; source_followup: {exc}") if result.error else f"source_followup: {exc}"

    # Optional reviewed semantic decisions run last. A raw model response is
    # insufficient; changed findings/sources invalidate the admission receipt.
    try:
        from .source_review_admission import run_review_admission_phase
        from backend.app.pipeline.stages.prepare.graph_builder import resolve_document_markdown
        md_path = resolve_document_markdown(Path(output_dir))
        admission = await asyncio.to_thread(
            run_review_admission_phase, Path(output_dir),
            md_path.read_text(encoding="utf-8") if md_path else "",
        )
        result.source_review_applied = admission["applied"]
        result.source_review_not_applied = admission["not_applied"]
        if not admission["skipped"]:
            await ctx.log(
                f"Проверка источников: применено {admission['applied']}, "
                f"не применено {admission['not_applied']}",
                "warn" if admission["not_applied"] else "info",
            )
    except (OSError, ValueError, TypeError, KeyError) as exc:
        await ctx.log(f"Решения проверки источников не применены: {exc}", "warn")
        result.error = (result.error + f"; source_review: {exc}") if result.error else f"source_review: {exc}"

    # A separately invoked final check sees source data and final wording,
    # without the prior review verdict. The original survives holds/failures.
    if getattr(ctx, 'source_final_check_invoke', None) is not None:
        try:
            from .source_review_release_workflow import run_final_release_phase
            from backend.app.pipeline.stages.prepare.graph_builder import resolve_document_markdown
            final_md_path = resolve_document_markdown(Path(output_dir))
            if final_md_path is None:
                raise ValueError('final_check_source_missing')
            release = await run_final_release_phase(
                Path(output_dir), final_md_path, invoke=ctx.source_final_check_invoke,
                executor_id=getattr(ctx, 'source_final_check_executor_id', None),
                max_findings=getattr(ctx, 'source_final_check_max_findings', 0),
                complete_citations=getattr(ctx, 'source_final_check_complete_citations', False),
                is_cancelled=ctx.is_cancelled,
            )
            result.final_release_applied = release['applied']
            result.final_release_held = release['held']
            result.final_release_not_completed = release['not_completed'] + release['not_attempted']
            await ctx.log(f"Финальная проверка: применено {release['applied']}, удержано {release['held']}, незавершено {result.final_release_not_completed}")
        except Exception as exc:
            await ctx.log(f"Финальная проверка не завершена: {exc}", 'warn')
            result.error = (result.error + f"; final_release: {exc}") if result.error else f"final_release: {exc}"

    # Explicit grouping inbox only; raw proposals never authorize a merge.
    try:
        from ..findings_merge.reviewed_grouping import run_group_admission_phase
        from backend.app.pipeline.stages.prepare.graph_builder import resolve_document_markdown
        md_path = resolve_document_markdown(Path(output_dir))
        grouping = await asyncio.to_thread(run_group_admission_phase, Path(output_dir),
            md_path.read_text(encoding="utf-8") if md_path else "")
        result.reviewed_groups_applied = grouping["applied"]
        result.reviewed_groups_not_applied = grouping["not_applied"]
        if not grouping["skipped"]:
            await ctx.log(f"Проверенная группировка: применено {grouping['applied']}, не применено {grouping['not_applied']}")
    except (OSError, ValueError, TypeError, KeyError, AttributeError) as exc:
        await ctx.log(f"Группировка не применена: {exc}", "warn")
        result.error = (result.error + f"; reviewed_grouping: {exc}") if result.error else f"reviewed_grouping: {exc}"

    # Report the final number after explicit admission, not the pre-filter count.
    try:
        final_document = json.loads(findings_path.read_text(encoding='utf-8'))
        if isinstance(final_document, dict) and isinstance(final_document.get('findings'), list):
            result.findings_total = len(final_document['findings'])
    except (OSError, ValueError):
        pass
    result.ok = result.deterministic_ok or result.absence_ok
    # findings_corrector = применение вердиктов + понижение подтверждённо-ложных «нет»
    ctx.update_pipeline_log(
        _CORRECTOR_KEY, "done" if result.ok else "error",
        message=(
            f"фантом: {result.phantom_cleaned}, page/sheet: {result.page_fixed}, "
            f"понижено: {result.downgraded + result.absence_downgraded}, "
            f"на ручную проверку: {result.flagged_human}"
        ),
        detail={
            "findings_total": result.findings_total,
            "deterministic_issues": result.deterministic_issues,
            "phantom_cleaned": result.phantom_cleaned,
            "page_fixed": result.page_fixed,
            "downgraded": result.downgraded,
            "flagged_human": result.flagged_human,
            "source_followups_prepared": result.source_followups_prepared,
            "source_review_candidates_ready": result.source_review_candidates_ready,
            "source_review_workflows_failed": result.source_review_workflows_failed,
            "source_review_candidates_rejected": result.source_review_candidates_rejected,
            "source_review_not_attempted": result.source_review_not_attempted,
            "source_review_cancelled": result.source_review_cancelled,
            "final_release_applied": result.final_release_applied,
            "final_release_held": result.final_release_held,
            "final_release_not_completed": result.final_release_not_completed,
            "source_review_applied": result.source_review_applied,
            "source_review_not_applied": result.source_review_not_applied,
            "reviewed_groups_applied": result.reviewed_groups_applied,
            "reviewed_groups_not_applied": result.reviewed_groups_not_applied,
            "absence_candidates": result.absence_candidates,
            "absence_downgraded": result.absence_downgraded,
        },
        error=result.error,
    )
    return result
