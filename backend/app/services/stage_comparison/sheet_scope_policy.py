"""Which sheet relations may feed the comparison, and which must wait.

A relation is a claim that two sheets are the same sheet.  Until that claim is
proven, everything computed on top of it is a claim about an unknown pair of
drawings — «в левом листе такой строки нет» is then a statement about the
matcher, not about the project.

The ЭОМ pair showed the cost precisely: four relations at confidence 0.29-0.34
entered the comparison next to the one relation proven at 0.70, and produced
814 «added» findings whose left side had zero structured fragments.  None of
them was a project change.

So the scope is closed by default and opens on proof:

* ``HIGH``            — the stamp matcher read the same identification line on
                        both sides (``STAMP_EXACT`` publishes ``HIGH``), or an
                        engineer answered the sheet question with YES.
* ``USER_CONFIRMED``  — an imported/legacy artifact states the same thing.
* a resolving human decision — the answer is recorded and no longer awaits
                        review.

``POSSIBLE`` and ``UNCERTAIN`` stay outside: they already produce exactly one
Stage 5 sheet question each, and that question — not hundreds of derived
findings — is what the engineer should see first.
"""
from __future__ import annotations

from typing import Any, Iterable, Mapping

#: Statuses that prove a pair on their own.
EFFECTIVE_SHEET_STATUSES = frozenset({"HIGH", "USER_CONFIRMED", "CONFIRMED"})

#: Statuses that can never enter the comparison, answered or not.
REJECTED_SHEET_STATUSES = frozenset({"NO_MATCH", "CANDIDATE_SUPERSEDED"})

#: Reason published for every relation held back from the comparison.
PENDING_REASON = "sheet_relation_unconfirmed"


def _status(relation: Mapping[str, Any]) -> str:
    return str(relation.get("status") or "UNKNOWN").upper()


def _relation_type(relation: Mapping[str, Any]) -> str:
    return str(relation.get("relation_type") or "MATCHED").upper()


def has_resolving_human_decision(relation: Mapping[str, Any]) -> bool:
    """True when an engineer answered this relation and nothing still waits."""
    decision = relation.get("human_decision")
    if not isinstance(decision, Mapping):
        return False
    if relation.get("review_required"):
        return False
    return bool(decision.get("decision_id") or decision.get("answer"))


def is_rejected(relation: Mapping[str, Any]) -> bool:
    return (
        _status(relation) in REJECTED_SHEET_STATUSES
        or _relation_type(relation) == "NO_MATCH"
    )


def is_effective(relation: Mapping[str, Any]) -> bool:
    """True when this relation may be compared without asking anyone."""
    if not isinstance(relation, Mapping) or is_rejected(relation):
        return False
    if not (relation.get("left_pages") and relation.get("right_pages")):
        return False
    return (
        _status(relation) in EFFECTIVE_SHEET_STATUSES
        or has_resolving_human_decision(relation)
    )


def is_pending_confirmation(relation: Mapping[str, Any]) -> bool:
    """True when the relation is two-sided but not yet proven."""
    if not isinstance(relation, Mapping) or is_rejected(relation):
        return False
    if not (relation.get("left_pages") and relation.get("right_pages")):
        return False
    return not is_effective(relation)


def pending_relations(
    relations: Iterable[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    """Describe the pairs the engineer must confirm before they are compared."""
    output = []
    for relation in relations or ():
        if not isinstance(relation, Mapping) or not is_pending_confirmation(relation):
            continue
        output.append({
            "relation_id": relation.get("relation_id"),
            "left_pages": sorted({int(page) for page in relation.get("left_pages") or []}),
            "right_pages": sorted({
                int(page) for page in relation.get("right_pages") or []
            }),
            "relation_type": _relation_type(relation),
            "status": _status(relation),
            "confidence": relation.get("confidence"),
            "reason_code": PENDING_REASON,
        })
    output.sort(key=lambda item: (
        item["left_pages"], item["right_pages"], str(item.get("relation_id") or "")
    ))
    return output


__all__ = [
    "EFFECTIVE_SHEET_STATUSES",
    "PENDING_REASON",
    "REJECTED_SHEET_STATUSES",
    "has_resolving_human_decision",
    "is_effective",
    "is_pending_confirmation",
    "is_rejected",
    "pending_relations",
]
