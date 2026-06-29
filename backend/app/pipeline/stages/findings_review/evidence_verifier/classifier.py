"""Classify findings / KB entries for golden dataset and routing."""
from __future__ import annotations

from backend.app.services.findings.grounding_service import classify_grounding_level


def _has_image_evidence(finding: dict) -> bool:
    for ev in finding.get("evidence") or []:
        if isinstance(ev, dict) and ev.get("type") == "image" and ev.get("block_id"):
            return True
    return False


def _has_text_evidence_only(finding: dict) -> bool:
    has_text = bool(finding.get("evidence_text_refs"))
    for ev in finding.get("evidence") or []:
        if isinstance(ev, dict) and ev.get("type") == "text":
            has_text = True
    return has_text and not _has_image_evidence(finding)


def classify_evidence_case(finding: dict, expert_decision: str) -> str:
    """Return dataset/routing class for a finding + expert label."""
    decision = (expert_decision or "").strip().lower()
    grounding = finding.get("grounding_level") or classify_grounding_level(finding)
    has_image = _has_image_evidence(finding)

    if decision == "accepted" and has_image:
        return "graphic_confirmed"
    if decision == "rejected" and has_image:
        return "graphic_rejected"
    if decision == "rejected" and grounding == "ungrounded":
        return "ungrounded_dispute"
    if _has_text_evidence_only(finding):
        return "text_only"
    if has_image:
        return "graphic_mixed"
    if grounding == "ungrounded":
        return "ungrounded_other"
    return "text_mixed"
