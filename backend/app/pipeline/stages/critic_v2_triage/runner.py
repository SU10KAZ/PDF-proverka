"""
Critic v2 post-processing stage runner.

Запускается над завершённым audit'ом проекта (есть 03_findings.json).
Не модифицирует никакие production artifacts. Все выходные файлы пишутся
ТОЛЬКО в <project_dir>/_output/<output_subdir>/ (default: critic_v2/).

Use cases:
    1. Backfill для существующих проектов — через scripts/backfill_critic_v2_triage.py.
    2. (Опционально, под флагом CRITIC_V2_ENABLED) — как опциональный пост-этап
       в manager pipeline. По умолчанию НЕ подключён.

Read inputs (read-only):
    _output/03_findings.json            (обязательно)
    _output/03_findings_review.json     (опционально — для legacy critic verdicts)
    _output/02_blocks_analysis.json     (опционально — для evidence_quality)
    _output/document_graph.json         (опционально — для будущих расширений)

Write outputs (только в output_subdir):
    critic_v2_triage.json               — full TriageDecision list
    critic_v2_triage_ui.json            — UI-ready export (summary/tabs/items)
    critic_v2_inline_map.json           — compact {finding_id: {score,label,...}}
    critic_v2_metrics.json              — TriageMetrics
    critic_v2_stage_summary.json        — мета: profile/timestamp/counts/inputs
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from backend.app.pipeline.stages.findings_review.critic_v2 import (
    build_triage_result,
    build_ui_export,
    compute_triage_metrics,
    run_critic_v2_offline,
    triage_decision_to_dict,
    triage_metrics_to_dict,
)
from backend.app.pipeline.stages.findings_review.critic_v2.triage import (
    PROFILE_ASSISTED_ROUND1,
    PROFILE_CONSERVATIVE,
    VALID_PROFILES,
)

logger = logging.getLogger(__name__)

# Имена выходных файлов — публичные константы, чтобы endpoint/тесты могли
# ссылаться без дублирования строк.
ARTIFACT_TRIAGE = "critic_v2_triage.json"
ARTIFACT_TRIAGE_UI = "critic_v2_triage_ui.json"
ARTIFACT_INLINE_MAP = "critic_v2_inline_map.json"
ARTIFACT_METRICS = "critic_v2_metrics.json"
ARTIFACT_STAGE_SUMMARY = "critic_v2_stage_summary.json"

# Маппинг queue → human-readable label + диапазон display-score 0–100.
# Должен соответствовать фронту (frontend/static/js/app.js CV2_DISPLAY_BUCKETS),
# чтобы inline_map.json был самодостаточным контрактом для бекенд-консьюмеров.
_QUEUE_DISPLAY = {
    "strong_keep":      {"range": (90, 100), "label": "важно проверить"},
    "main_review":      {"range": (65,  85), "label": "на проверку"},
    "borderline":       {"range": (50,  65), "label": "спорное"},
    "needs_context":    {"range": (40,  59), "label": "нужен контекст"},
    "suggested_reject": {"range": (20,  39), "label": "вероятно к отклонению"},
    "hidden_by_critic": {"range": ( 0,  19), "label": "скрыто Critic v2"},
}


@dataclass
class CriticV2TriageStageResult:
    """Результат запуска critic_v2_triage runner-а.

    success/cancelled/error — те же поля, что у StageResult, чтобы можно было
    легко обернуть в общий контракт, если этап подключат к manager.
    """
    success: bool
    cancelled: bool = False
    error: Optional[str] = None
    # Specific fields
    profile: str = ""
    findings_total: int = 0
    triage_total: int = 0
    artifacts_dir: Optional[Path] = None
    written_files: list[str] = field(default_factory=list)
    summary: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def ok(cls, **kw) -> "CriticV2TriageStageResult":
        return cls(success=True, **kw)

    @classmethod
    def fail(cls, error: str, **kw) -> "CriticV2TriageStageResult":
        return cls(success=False, error=error, **kw)


def _load_json_safe(path: Path) -> Optional[Any]:
    """Загрузить JSON, вернуть None при отсутствии/ошибке. Никаких raise."""
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        logger.warning("critic_v2_triage: failed to read %s: %s", path, exc)
        return None


def _extract_findings(findings_doc: Any) -> list[dict[str, Any]]:
    """Достать список findings из 03_findings.json.

    Schema варьируется между версиями: иногда {"findings": [...]}, иногда
    {"items": [...]}, иногда сразу список. Поддерживаем все три без падения.
    """
    if findings_doc is None:
        return []
    if isinstance(findings_doc, list):
        return [f for f in findings_doc if isinstance(f, dict)]
    if isinstance(findings_doc, dict):
        for key in ("findings", "items"):
            val = findings_doc.get(key)
            if isinstance(val, list):
                return [f for f in val if isinstance(f, dict)]
    return []


def _extract_blocks_index(blocks_doc: Any) -> Optional[set[str]]:
    """Собрать множество block_id из 02_blocks_analysis.json.

    Если файла нет или формат не распознан — возвращаем None (scorer корректно
    обрабатывает None как 'index не предоставлен').
    """
    if blocks_doc is None:
        return None
    blocks_list = None
    if isinstance(blocks_doc, dict):
        blocks_list = blocks_doc.get("blocks") or blocks_doc.get("items")
    elif isinstance(blocks_doc, list):
        blocks_list = blocks_doc
    if not isinstance(blocks_list, list):
        return None
    out: set[str] = set()
    for b in blocks_list:
        if not isinstance(b, dict):
            continue
        bid = b.get("id") or b.get("block_id")
        if isinstance(bid, str) and bid:
            out.add(bid)
    return out or None


def _compute_inline_map(triage_decisions) -> dict[str, dict[str, Any]]:
    """Скомпилировать компактный inline-map для UI.

    Каждая запись: {score, label, queue, reason, hidden_by_default,
                    evidence_quality, taxonomy_reason, source_dependency}
    score — display 0–100 (центр диапазона очереди, чтобы не зависеть от
    confidence, которое в offline-режиме часто отсутствует).
    """
    out: dict[str, dict[str, Any]] = {}
    for td in triage_decisions:
        disp = _QUEUE_DISPLAY.get(td.human_queue) or _QUEUE_DISPLAY["borderline"]
        lo, hi = disp["range"]
        score_100 = round((lo + hi) / 2)
        # Для suggested_reject / hidden_by_critic высокая внутренняя уверенность
        # критика = ниже на пользовательской шкале (контракт с фронтом).
        if td.human_queue in ("suggested_reject", "hidden_by_critic"):
            # держим в нижней половине диапазона
            score_100 = lo + (hi - lo) // 3
        out[td.finding_id] = {
            "score": int(score_100),
            "label": disp["label"],
            "queue": td.human_queue,
            "reason": td.reason or "",
            "hidden_by_default": bool(td.collapsed_by_default and td.human_queue == "hidden_by_critic"),
            "evidence_quality": td.evidence_quality or "",
            "taxonomy_reason": td.taxonomy_reason or "",
            "source_dependency": td.source_dependency or "",
        }
    return out


def _write_json(path: Path, payload: Any) -> None:
    """Атомарная запись JSON (tmp + replace)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    tmp.replace(path)


def _resolve_profile(profile: Optional[str]) -> str:
    """Нормализовать profile c fallback на conservative."""
    if not profile:
        return PROFILE_CONSERVATIVE
    p = str(profile).strip().lower()
    # assisted_round1 не входит в VALID_PROFILES? Проверим — он точно есть в triage.py
    valid = set(VALID_PROFILES) | {PROFILE_ASSISTED_ROUND1}
    if p in valid:
        return p
    logger.warning(
        "critic_v2_triage: unknown profile %r, falling back to %r",
        profile, PROFILE_CONSERVATIVE,
    )
    return PROFILE_CONSERVATIVE


def run_critic_v2_triage(
    project_dir: Path,
    *,
    output_subdir: str = "critic_v2",
    profile: Optional[str] = None,
    llm_enabled: bool = False,
    project_id: str = "",
) -> CriticV2TriageStageResult:
    """Прогнать Critic v2 triage над завершённым audit'ом проекта.

    Args:
        project_dir: путь к проекту (содержит _output/ с готовыми findings).
        output_subdir: подпапка внутри _output/ для артефактов (default "critic_v2").
        profile: triage profile (conservative|assisted|aggressive|assisted_round1|
                 assisted_round2_candidate). None → conservative.
        llm_enabled: вызывать ли LLM gate. Default False; для безопасности в этом
                     раннере LLM пока НЕ запускается даже при True — оставлено
                     как явный contract для будущего расширения.
        project_id: используется только для логов/мета. Не влияет на логику.

    Returns:
        CriticV2TriageStageResult.

    Контракт безопасности:
        * НЕ модифицирует 03_findings.json/03_findings_review.json/expert_review.json
        * НЕ пишет вне <project_dir>/_output/<output_subdir>/
        * НЕ запускает LLM (даже при llm_enabled=True в этой версии)
        * Не падает при отсутствии 02_blocks_analysis.json/document_graph.json
    """
    project_dir = Path(project_dir)
    output_dir = project_dir / "_output"
    findings_path = output_dir / "03_findings.json"

    if not findings_path.exists():
        return CriticV2TriageStageResult.fail(
            error=f"03_findings.json not found in {output_dir}",
            profile=_resolve_profile(profile),
        )

    findings_doc = _load_json_safe(findings_path)
    findings = _extract_findings(findings_doc)
    if not findings:
        return CriticV2TriageStageResult.fail(
            error="03_findings.json contains no findings",
            profile=_resolve_profile(profile),
            findings_total=0,
        )

    # Optional inputs — graceful if missing.
    blocks_doc = _load_json_safe(output_dir / "02_blocks_analysis.json")
    blocks_index = _extract_blocks_index(blocks_doc)
    has_blocks = blocks_index is not None
    has_doc_graph = (output_dir / "document_graph.json").exists()
    has_legacy_review = (output_dir / "03_findings_review.json").exists()

    resolved_profile = _resolve_profile(profile)

    # 1. Deterministic critic v2 (normalize → rule_filter → score → dedup)
    try:
        critic_result = run_critic_v2_offline(findings, blocks_index=blocks_index)
    except Exception as exc:  # noqa: BLE001 — stage не должен падать с traceback
        logger.exception("critic_v2_triage: run_critic_v2_offline failed")
        return CriticV2TriageStageResult.fail(
            error=f"run_critic_v2_offline failed: {exc}",
            profile=resolved_profile,
            findings_total=len(findings),
        )

    # 2. Triage (queue assignment). LLM явно отключён в этом раннере.
    if llm_enabled:
        # Хук на будущее: сейчас logger.warning + продолжаем offline.
        logger.warning(
            "critic_v2_triage: llm_enabled=True ignored — LLM path not "
            "implemented in this stage runner. Running offline."
        )

    try:
        triage_decisions = build_triage_result(
            findings,
            critic_result.decisions,
            llm_decisions=None,
            profile=resolved_profile,
        )
    except Exception as exc:  # noqa: BLE001
        logger.exception("critic_v2_triage: build_triage_result failed")
        return CriticV2TriageStageResult.fail(
            error=f"build_triage_result failed: {exc}",
            profile=resolved_profile,
            findings_total=len(findings),
        )

    # 3. Metrics
    try:
        metrics = compute_triage_metrics(triage_decisions, profile=resolved_profile)
    except Exception as exc:  # noqa: BLE001
        logger.exception("critic_v2_triage: compute_triage_metrics failed")
        return CriticV2TriageStageResult.fail(
            error=f"compute_triage_metrics failed: {exc}",
            profile=resolved_profile,
            findings_total=len(findings),
            triage_total=len(triage_decisions),
        )

    # 4. UI export — обогащаем records_by_id, чтобы UI items имели title/desc/section.
    # finding_id в TriageDecision = id из 03_findings.json; для project-scoped
    # endpoint никакого "ProjectName:" префикса не нужно — UI ищет по bare id.
    records_by_id: dict[str, dict[str, Any]] = {}
    project_name_for_items = project_id or project_dir.name
    for f in findings:
        fid = f.get("id") or f.get("finding_id")
        if not isinstance(fid, str):
            continue
        # Копируем оригинал и проставляем project_name — нужен endpoint'у для
        # matched_by="project_name" в scope-блоке UI export.
        rec = dict(f)
        rec.setdefault("project_name", project_name_for_items)
        records_by_id[fid] = rec

    try:
        ui_export = build_ui_export(
            triage_decisions,
            metrics,
            records_by_id=records_by_id,
            human_decisions=None,
        )
    except Exception as exc:  # noqa: BLE001
        logger.exception("critic_v2_triage: build_ui_export failed")
        return CriticV2TriageStageResult.fail(
            error=f"build_ui_export failed: {exc}",
            profile=resolved_profile,
            findings_total=len(findings),
            triage_total=len(triage_decisions),
        )

    # Дополняем UI export метаданными — фронт уже ожидает эти поля.
    generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    ui_export.setdefault("experimental", True)
    ui_export["profile"] = resolved_profile
    ui_export["generated_at"] = generated_at
    ui_export["source_project"] = {
        "project_id": project_id or project_dir.name,
        "project_name": project_name_for_items,
        "project_dir": str(project_dir),
    }
    ui_export["production_pipeline_modified"] = False

    # 5. Inline map
    inline_map = _compute_inline_map(triage_decisions)

    # 6. Persist artifacts (atomic writes)
    artifacts_dir = output_dir / output_subdir
    written: list[str] = []

    triage_payload = {
        "profile": resolved_profile,
        "generated_at": generated_at,
        "project_id": project_id or project_dir.name,
        "decisions": [triage_decision_to_dict(td) for td in triage_decisions],
        "experimental": True,
        "production_pipeline_modified": False,
    }
    _write_json(artifacts_dir / ARTIFACT_TRIAGE, triage_payload)
    written.append(ARTIFACT_TRIAGE)

    _write_json(artifacts_dir / ARTIFACT_TRIAGE_UI, ui_export)
    written.append(ARTIFACT_TRIAGE_UI)

    inline_payload = {
        "profile": resolved_profile,
        "generated_at": generated_at,
        "project_id": project_id or project_dir.name,
        "map": inline_map,
        "experimental": True,
    }
    _write_json(artifacts_dir / ARTIFACT_INLINE_MAP, inline_payload)
    written.append(ARTIFACT_INLINE_MAP)

    metrics_payload = {
        "profile": resolved_profile,
        "generated_at": generated_at,
        "metrics": triage_metrics_to_dict(metrics),
        "experimental": True,
    }
    _write_json(artifacts_dir / ARTIFACT_METRICS, metrics_payload)
    written.append(ARTIFACT_METRICS)

    summary = {
        "profile": resolved_profile,
        "generated_at": generated_at,
        "project_id": project_id or project_dir.name,
        "project_dir": str(project_dir),
        "inputs": {
            "findings_json": str(findings_path),
            "blocks_analysis_present": has_blocks,
            "document_graph_present": has_doc_graph,
            "legacy_findings_review_present": has_legacy_review,
        },
        "counts": {
            "findings_total": len(findings),
            "triage_total": len(triage_decisions),
            "primary": sum(1 for td in triage_decisions if td.human_queue in
                           ("strong_keep", "main_review", "borderline")),
            "needs_context": sum(1 for td in triage_decisions if td.human_queue == "needs_context"),
            "suggested_reject": sum(1 for td in triage_decisions if td.human_queue == "suggested_reject"),
            "hidden_by_critic": sum(1 for td in triage_decisions if td.human_queue == "hidden_by_critic"),
        },
        "llm_enabled": False,
        "llm_called": False,
        "artifacts": written,
        "artifacts_dir": str(artifacts_dir),
        "experimental": True,
        "production_pipeline_modified": False,
    }
    _write_json(artifacts_dir / ARTIFACT_STAGE_SUMMARY, summary)
    written.append(ARTIFACT_STAGE_SUMMARY)

    return CriticV2TriageStageResult.ok(
        profile=resolved_profile,
        findings_total=len(findings),
        triage_total=len(triage_decisions),
        artifacts_dir=artifacts_dir,
        written_files=written,
        summary=summary,
    )
