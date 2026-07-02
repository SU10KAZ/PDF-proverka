"""KB-вердикт -> нужно ли запускать Evidence Verifier для этого замечания.

Фильтр «спорных»: перепроверяем не все замечания подряд, а только те, где есть
сомнение (KB-агент дал borderline/reject/needs_human) или где замечание графическое
и уверенно заземлённое. Уверенный accept KB — пропускаем (экономим vision).

Самодостаточно (без зависимости от удалённого пакета evidence_verifier/): helper
_has_image_evidence инлайнен здесь.
"""
from __future__ import annotations

from typing import Optional


def _has_image_evidence(finding: dict) -> bool:
    for ev in finding.get("evidence") or []:
        if isinstance(ev, dict) and ev.get("type") == "image" and ev.get("block_id"):
            return True
    return False


def should_run_evidence_verifier(
    finding: dict,
    *,
    kb_decision: Optional[dict] = None,
) -> tuple[bool, str]:
    """Вернуть (should_run, reason)."""
    kb = (kb_decision or {}).get("llm_decision", "")
    has_image = _has_image_evidence(finding) or bool(finding.get("related_block_ids"))

    if kb == "borderline":
        return True, "kb_borderline"
    if kb == "reject" and has_image:
        return True, "kb_reject_graphic"
    if kb == "reject":
        return True, "kb_reject"
    if kb == "needs_human":
        return True, "kb_needs_human"
    if has_image and finding.get("grounding_level") == "grounded_strong":
        return True, "grounded_strong_graphic"
    if kb == "accept":
        return False, "kb_accept_skip"
    return True, "default_check"
