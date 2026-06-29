"""KB verdict -> whether Evidence Verifier should run."""
from __future__ import annotations

from typing import Optional

from .classifier import _has_image_evidence


def should_run_evidence_verifier(
    finding: dict,
    *,
    kb_decision: Optional[dict] = None,
) -> tuple[bool, str]:
    """Return (should_run, reason)."""
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
