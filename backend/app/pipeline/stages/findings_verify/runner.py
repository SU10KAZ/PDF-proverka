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
    return {
        "findings_total": crit.findings_total,
        "deterministic_issues": crit.deterministic_issues,
        "critic_error": crit.error,
        "phantom_cleaned": corr.phantom_cleaned,
        "page_fixed": corr.page_fixed,
        "downgraded": corr.downgraded,
        "flagged_human": corr.flagged_human,
        "corrector_error": corr.error,
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
            "absence_candidates": result.absence_candidates,
            "absence_downgraded": result.absence_downgraded,
        },
        error=result.error,
    )
    return result
