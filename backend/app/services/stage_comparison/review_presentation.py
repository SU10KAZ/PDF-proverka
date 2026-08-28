"""One shared answer to «can the engineer see this finding and decide on it?».

Stage 5 and Stage 7 used to work on the same objects: every review item became
a question *and* a decision row, so one change cost the engineer two actions —
and the question came first, phrased as an opaque id, while the decision was
refused outright until the question was answered.

The dividing line is presentability.  A review item that carries a real value
and a place on a sheet is a finding: it goes to Stage 7 and the engineer
confirms or rejects it there, even when the system could not name its
dimension.  «EI 60 → EI 90, классификация не определена» is a perfectly
reviewable row.  Only an item with nothing to show is a genuine exception, and
only that deserves a Stage 5 question.

Both stages import this module so the two views can never disagree.
"""
from __future__ import annotations

from typing import Any, Mapping


CONTRACT_VERSION = "review-presentation.v1"

_SIDES = ("LEFT", "RIGHT")
_DIRECTION_SIDES = {
    "ADDED": ("RIGHT",),
    "REMOVED": ("LEFT",),
    "ALTERED": ("LEFT", "RIGHT"),
    "REPLACED": ("LEFT", "RIGHT"),
}


def _text(value: Any) -> str | None:
    if value is None:
        return None
    text = " ".join(str(value).split())
    return text or None


def _locations(item: Mapping[str, Any]) -> dict[str, list[dict[str, Any]]]:
    provenance = item.get("provenance")
    source_atom = (
        provenance.get("source_atom") if isinstance(provenance, Mapping) else None
    )
    raw = source_atom.get("locations") if isinstance(source_atom, Mapping) else None
    if not isinstance(raw, Mapping):
        raw = provenance.get("locations") if isinstance(provenance, Mapping) else None
    output: dict[str, list[dict[str, Any]]] = {side: [] for side in _SIDES}
    if not isinstance(raw, Mapping):
        return output
    for side in _SIDES:
        values = raw.get(side)
        if not isinstance(values, (list, tuple)):
            continue
        output[side] = [dict(value) for value in values if isinstance(value, Mapping)]
    return output


def review_finding_presentation(item: Mapping[str, Any]) -> dict[str, Any]:
    """Describe what an engineer would actually see for one review item.

    ``presentable`` means there is a value to read and a page to open it on.
    Nothing here judges whether the change matters — only whether it can be
    shown honestly without a human first filling in a missing field.
    """
    before = _text(item.get("before_value"))
    after = _text(item.get("after_value"))
    locations = _locations(item)
    pages = {
        side: sorted({
            int(entry["page"])
            for entry in locations[side]
            if isinstance(entry.get("page"), int) and not isinstance(entry.get("page"), bool)
        })
        for side in _SIDES
    }
    direction = str(item.get("direction") or "").upper()
    expected_sides = _DIRECTION_SIDES.get(direction, _SIDES)
    has_value = bool(before or after)
    has_location = any(pages[side] for side in expected_sides) or any(
        pages[side] for side in _SIDES
    )
    missing: list[str] = []
    if not has_value:
        missing.append("value")
    if not has_location:
        missing.append("location")
    return {
        "contract_version": CONTRACT_VERSION,
        "presentable": has_value and has_location,
        "before_value": before,
        "after_value": after,
        "direction": direction or None,
        "left_pages": pages["LEFT"],
        "right_pages": pages["RIGHT"],
        "dimension_known": str(item.get("dimension") or "") not in {
            "", "UNKNOWN_DIMENSION",
        },
        "missing_for_presentation": missing,
    }


def is_presentable_review_item(item: Mapping[str, Any]) -> bool:
    return bool(review_finding_presentation(item)["presentable"])


__all__ = [
    "CONTRACT_VERSION",
    "is_presentable_review_item",
    "review_finding_presentation",
]
