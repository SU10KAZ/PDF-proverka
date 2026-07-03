"""Отбор замечаний для дорогой визуальной проверки Evidence Verifier (qwen).

Раньше EV2 гоняла зрение на ВСЕ замечания → часы. Теперь — ОТБОР: визуал (qwen)
запускаем только там, где он реально нужен, остальное пропускаем (быстро, без qwen).

«Стоит смотреть» =
  • KB-спорные       — KB-агент дал borderline / reject / needs_human;
  • критические      — высокая цена ошибки, проверяем ВСЕГДА;
  • прецедент-подозрительные — похожи на ранее ОТКЛОНЁННЫЕ экспертом (офлайн-поиск в
    decisions_log): вероятный ложняк, который эксперт уже отклонял → стоит перепроверить.

Уверенный KB-accept и всё прочее (новые, не критические, без прецедента) — пропуск.
Trade-off (осознанный): НОВЫЙ ложняк без похожего прецедента визуально не проверится;
критические покрыты всегда, а force=True снимает отбор и гоняет всё.

Самодостаточно (helper _has_image_evidence инлайнен). Прецедент — офлайн, fail-soft.
"""
from __future__ import annotations

from typing import Optional


def _has_image_evidence(finding: dict) -> bool:
    for ev in finding.get("evidence") or []:
        if isinstance(ev, dict) and ev.get("type") == "image" and ev.get("block_id"):
            return True
    return False


def _is_precedent_suspect(finding: dict, section: str = "") -> bool:
    """Похоже ли замечание на ранее отклонённое экспертом (офлайн, fail-soft).

    section прокидывается снаружи: у findings часто нет поля `section`, а прецедент
    даёт +0.40 за совпадение раздела — без него сильные совпадения не набирают порог.
    """
    try:
        from .precedent import run_precedent_check
        sec = str(finding.get("section") or section or "")
        sig = run_precedent_check(finding, section=sec)
        return getattr(sig, "kind", "none") == "precedent_reject"
    except Exception:
        return False


def should_run_evidence_verifier(
    finding: dict,
    *,
    kb_decision: Optional[dict] = None,
    section: str = "",
    use_precedent: bool = True,
) -> tuple[bool, str]:
    """Вернуть (should_run, reason) — запускать ли визуальную проверку для замечания."""
    kb = (kb_decision or {}).get("llm_decision", "")
    has_image = _has_image_evidence(finding) or bool(finding.get("related_block_ids"))

    # 1) KB-спорные — проверяем
    if kb == "borderline":
        return True, "kb_borderline"
    if kb == "reject":
        return True, "kb_reject_graphic" if has_image else "kb_reject"
    if kb == "needs_human":
        return True, "kb_needs_human"
    if kb == "accept":
        return False, "kb_accept_skip"

    # 2) критические — всегда
    if "КРИТИЧ" in str(finding.get("severity") or "").upper():
        return True, "critical"

    # 3) прецедент-подозрительные (похожи на отклонённые экспертом)
    if use_precedent and _is_precedent_suspect(finding, section):
        return True, "precedent_suspect"

    # остальное — визуально не проверяем
    return False, "not_selected"
