"""Evidence validation service: read/run evidence_validation.json.

Движок — Evidence Agent v2 (EV2): восприятие⊥суждение + K прогонов + детерминированная
политика слияния (visual + norm + cross_block) с консервативным смещением (реальное
замечание не удаляется). См. backend/app/pipeline/stages/findings_review/evidence_agent_v2/.

Формат выходных decisions сохранён обратно-совместимым с прежней реализацией
(finding_id, llm_decision, human_taxonomy_reason, confidence, explanation,
verification_path, block_ids_used, evidence_checked, model_used) — фронт и стадия
пайплайна не замечают подмены движка. Дополнительные поля EV2 добавлены сверху.
"""
from __future__ import annotations

import asyncio
import datetime
import json
import os
from pathlib import Path
from typing import Optional

from backend.app.services.common import version_service
from backend.app.services.findings.kb_validation_service import get_kb_decision_map

_EVIDENCE_FILE = "evidence_validation.json"


def _output_dir(project_id: str, version_id: Optional[str] = None) -> Path:
    return version_service.resolve_version_output_dir(project_id, version_id)


def _evidence_path(project_id: str, version_id: Optional[str] = None) -> Optional[Path]:
    try:
        primary = _output_dir(project_id, version_id) / _EVIDENCE_FILE
        if primary.exists():
            return primary
    except Exception:
        primary = None
    try:
        from backend.app.services.storage.projects_v2_adapter import ProjectsV2Adapter
        adapter = ProjectsV2Adapter()
        if not adapter.is_available():
            return primary if primary and primary.exists() else None
        doc = adapter.find_document_by_project_id(project_id)
        if not doc:
            return primary if primary and primary.exists() else None
        vid = adapter.resolve_version_id(doc, version_id)
        if not vid:
            return primary if primary and primary.exists() else None
        alt = adapter.version_dir(Path(doc["doc_dir"]), vid) / "03_analysis" / "latest" / _EVIDENCE_FILE
        if alt.exists():
            return alt
    except Exception:
        pass
    return primary if primary and primary.exists() else None


def get_evidence_validation(project_id: str, version_id: Optional[str] = None) -> dict | None:
    path = _evidence_path(project_id, version_id)
    if not path:
        return None
    with path.open(encoding="utf-8") as f:
        return json.load(f)


def get_evidence_decision_map(project_id: str, version_id: Optional[str] = None) -> dict[str, dict]:
    data = get_evidence_validation(project_id, version_id)
    if not data:
        return {}
    return {d["finding_id"]: d for d in data.get("decisions", [])}


def _default_graphic_model() -> str:
    """Модель зрения по умолчанию (LM Studio через ngrok). Наследует настройку
    stage_comparison, чтобы EV2 бил в тот же локальный endpoint."""
    return (
        os.environ.get("EVIDENCE_VERIFY_GRAPHIC_MODEL")
        or os.environ.get("STAGE_COMPARISON_GRAPHIC_LLM_MODEL")
        or "qwen/qwen3.6-35b-a3b"
    )


def _fused_to_decision(fused, model: str) -> dict:
    """FusedVerdict EV2 -> запись decisions[] (обратно-совместимый формат + доп. поля)."""
    sources = list(getattr(fused, "sources_used", []) or [])
    if "visual" in sources:
        path = "graphic"
    elif sources:
        path = "text"          # норм/кросс-блок офлайн-путь (значения из известных фронту)
    else:
        path = "weak"
    return {
        "finding_id": fused.finding_id,
        "llm_decision": fused.decision,
        "human_taxonomy_reason": getattr(fused, "taxonomy", ""),
        "confidence": getattr(fused, "confidence", 0.0),
        "explanation": getattr(fused, "reason", ""),
        "verification_path": path,
        "block_ids_used": list(getattr(fused, "candidate_block_ids", []) or []),
        "evidence_checked": bool(sources),
        "model_used": model if "visual" in sources else "",
        # --- доп. поля EV2 (старый UI их игнорирует, полезны для анализа/экспорта) ---
        "source": getattr(fused, "source", ""),
        "sources_used": sources,
        "requires_human_review": bool(getattr(fused, "requires_human_review", False)),
        "evidence_quote": getattr(fused, "evidence_quote", ""),
        "norm_flags": list(getattr(fused, "norm_flags", []) or []),
        "norm_suggestions": dict(getattr(fused, "norm_suggestions", {}) or {}),
        "visual_votes": dict(getattr(fused, "visual_votes", {}) or {}),
    }


async def _run_batch_async(
    project_id: str,
    findings: list,
    *,
    section: str,
    version_id: Optional[str],
    kb_map: dict,
    model: str,
    force: bool,
    respect_kb_routing: bool,
) -> tuple[list, list, int]:
    """Прогнать EV2 по замечаниям ПОСЛЕДОВАТЕЛЬНО (concurrency=1 к LM Studio через
    ngrok — не перегружаем локальную 35B). KB-фильтр отсекает уверенные accept."""
    from backend.app.pipeline.stages.findings_review.evidence_agent_v2 import (
        should_run_evidence_verifier,
        verify_finding_multi_async,
    )

    decisions: list[dict] = []
    skipped: list[dict] = []
    errors = 0

    for finding in findings:
        fid = str(finding.get("id", ""))
        if respect_kb_routing and not force:
            run, reason = should_run_evidence_verifier(finding, kb_decision=kb_map.get(fid))
            if not run:
                skipped.append({"finding_id": fid, "reason": reason})
                continue
        try:
            fused = await verify_finding_multi_async(
                project_id, finding, section=section, version_id=version_id, model=model,
            )
            decisions.append(_fused_to_decision(fused, model))
        except Exception as exc:  # fail-soft на конкретном замечании
            errors += 1
            decisions.append({
                "finding_id": fid,
                "llm_decision": "needs_human",
                "human_taxonomy_reason": "verifier_error",
                "confidence": 0.0,
                "explanation": f"Ошибка проверки: {exc}",
                "verification_path": "weak",
                "block_ids_used": [],
                "evidence_checked": False,
                "model_used": "",
            })

    return decisions, skipped, errors


def run_evidence_validation(
    project_id: str,
    version_id: Optional[str] = None,
    section: str = "TX",
    *,
    graphic_model: Optional[str] = None,
    text_model: Optional[str] = None,
    force: bool = False,
    respect_kb_routing: bool = True,
) -> dict:
    """Синхронный вход (CLI / manager через to_thread / API через to_thread).

    Внутри драйвит async-ядро EV2 через asyncio.run — безопасно, т.к. все вызывающие
    находятся ВНЕ работающего event loop (manager и API оборачивают в to_thread).
    """
    output_dir = _output_dir(project_id, version_id)
    findings_path = output_dir / "03_findings.json"
    if not findings_path.exists():
        alt = output_dir / "03a_norms_verified.json"
        findings_path = alt if alt.exists() else findings_path
    if not findings_path.exists():
        raise FileNotFoundError(f"findings not found in {output_dir}")

    with findings_path.open(encoding="utf-8") as f:
        findings_raw = json.load(f)
    findings = findings_raw.get("findings", [])
    if not findings:
        raise ValueError("No findings to validate")

    kb_map = get_kb_decision_map(project_id, version_id) if respect_kb_routing else {}
    model = graphic_model or _default_graphic_model()

    decisions, skipped, errors = asyncio.run(_run_batch_async(
        project_id,
        findings,
        section=section,
        version_id=version_id,
        kb_map=kb_map,
        model=model,
        force=force,
        respect_kb_routing=respect_kb_routing,
    ))

    output = {
        "generated_at": datetime.datetime.now().isoformat(),
        "project_id": project_id,
        "version_id": version_id,
        "section": section,
        "engine": "ev2",
        "graphic_model": model,
        # EV2 не использует отдельную текстовую LLM (текст → norm/cross-block офлайн
        # либо needs_human); поле сохранено для обратной совместимости формата.
        "text_model": text_model or "",
        "total_findings": len(findings),
        "total_processed": len(decisions),
        "skipped_count": len(skipped),
        "errors_count": errors,
        "decisions": decisions,
    }

    out_path = output_dir / _EVIDENCE_FILE
    with out_path.open("w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    return output
