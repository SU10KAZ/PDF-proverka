"""Parse LLM verification responses (shared with kb_gate patterns)."""
from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from typing import Optional

VALID_DECISIONS = {"accept", "reject", "borderline", "needs_human"}
VALID_TAXONOMY = {
    "visual_or_ocr_misread", "duplicate_or_already_covered",
    "wrong_norm_context", "acceptable_design_solution",
    "not_functionally_significant", "value_already_correct",
    "already_resolved_by_project_note", "false_positive_due_to_missing_context",
    "requirement_not_mandatory", "other",
}
REJECT_CONFIDENCE_THRESHOLD = 0.75


@dataclass
class EVDecision:
    finding_id: str
    llm_decision: str
    human_taxonomy_reason: Optional[str]
    explanation: Optional[str]
    confidence: float
    verification_path: str
    block_ids_used: list = field(default_factory=list)
    evidence_checked: bool = False
    model_used: str = ""
    raw_llm: Optional[dict] = None


def _json_array_from_text(text: str) -> list:
    text = (text or "").strip()
    try:
        data = json.loads(text)
    except json.JSONDecodeError:
        data = None
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        for key in ("decisions", "items", "result", "verdict"):
            value = data.get(key)
            if isinstance(value, list):
                return value
            if isinstance(value, dict):
                return [value]
    decoder = json.JSONDecoder()
    for idx, char in enumerate(text):
        if char != "[":
            continue
        try:
            data, _ = decoder.raw_decode(text[idx:])
        except json.JSONDecodeError:
            continue
        if isinstance(data, list):
            return data
    m = re.search(r"\[.*\]", text, re.DOTALL)
    if m:
        try:
            data = json.loads(m.group(0))
        except json.JSONDecodeError:
            return []
        return data if isinstance(data, list) else []
    return []


def coerce_confidence(value) -> float:
    try:
        confidence = float(value)
    except (TypeError, ValueError):
        confidence = 0.5
    return max(0.0, min(1.0, confidence))


def parse_verification_response(
    text: str,
    *,
    expected_ids: Optional[set[str]] = None,
    verification_path: str = "unknown",
    allowed_block_ids: Optional[set[str]] = None,
) -> list[EVDecision]:
    items = _json_array_from_text(text)
    decisions: list[EVDecision] = []
    seen: set[str] = set()
    allowed_blocks = allowed_block_ids or set()
    norm_allowed = {str(x).replace("block_", "") for x in allowed_blocks} if allowed_blocks else set()

    for item in items:
        if not isinstance(item, dict):
            continue
        fid = str(item.get("finding_id", "")).strip()
        if not fid or fid in seen:
            continue
        if expected_ids is not None and fid not in expected_ids:
            continue
        seen.add(fid)

        decision = item.get("llm_decision", "borderline")
        if decision not in VALID_DECISIONS:
            decision = "borderline"
        confidence = coerce_confidence(item.get("confidence", 0.5))
        if decision == "reject" and confidence < REJECT_CONFIDENCE_THRESHOLD:
            decision = "borderline"

        reason = item.get("human_taxonomy_reason")
        if reason and reason not in VALID_TAXONOMY:
            reason = "other"

        raw_blocks = item.get("block_ids_used") or item.get("blocks_used") or []
        if isinstance(raw_blocks, list):
            blocks = [
                str(x) for x in raw_blocks
                if not norm_allowed or str(x).replace("block_", "") in norm_allowed
            ]
        else:
            blocks = []

        path = str(item.get("verification_path") or verification_path)
        decisions.append(EVDecision(
            finding_id=fid,
            llm_decision=decision,
            human_taxonomy_reason=reason,
            explanation=item.get("explanation"),
            confidence=confidence,
            verification_path=path,
            block_ids_used=blocks,
            evidence_checked=bool(item.get("evidence_checked", True)),
            raw_llm=item,
        ))
    return decisions


def missing_decision(finding: dict, *, verification_path: str = "unknown", explanation: str = "") -> EVDecision:
    return EVDecision(
        finding_id=str(finding.get("id", "?")),
        llm_decision="needs_human",
        human_taxonomy_reason=None,
        explanation=explanation or "Evidence Verifier не получил решение от модели.",
        confidence=0.0,
        verification_path=verification_path,
        block_ids_used=[],
        evidence_checked=False,
    )
