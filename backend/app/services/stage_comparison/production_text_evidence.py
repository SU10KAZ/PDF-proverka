"""Read-only viewer projection of persisted production TEXT evidence.

The projection deliberately performs no comparison, matching, or synthesis.
It exposes exact Stage 3 deterministic pairs and exact TEXT-atom locations
only after all inputs have been proven to belong to the published generation.
"""
from __future__ import annotations

import copy
import math
from typing import Any, Mapping

from .production_artifacts import content_signature, stable_id
from .text_atom_builder import KIND as TEXT_ATOMS_KIND
from .text_atom_builder import SCHEMA_VERSION as TEXT_ATOMS_SCHEMA_VERSION
from .text_differences import KIND as TEXT_DIFFERENCES_KIND
from .text_differences import VERSION as TEXT_DIFFERENCES_VERSION
from .text_semantic_validation import stage3_content_signature


KIND = "stage_comparison_production_text_evidence"
SCHEMA_VERSION = "production-text-evidence.v1"
PUBLISHED_STATUSES = frozenset({"COMPLETED", "PARTIAL"})


class ProductionTextEvidenceConflictError(ValueError):
    """Persisted inputs do not describe one published production generation."""


def _non_negative_int(value: Any) -> int | None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        return None
    return value


def _summary(state: Mapping[str, Any]) -> dict[str, int | None]:
    """Use only counters already published in ``state.stages.text``.

    Stage 3 intentionally does not publish a total exact-match count in state,
    so ``matched_fragments`` remains unknown.  The payload separately reports
    how many persisted match pairs are available for viewer highlighting.
    """
    stages = state.get("stages")
    text = stages.get("text") if isinstance(stages, Mapping) else None
    text = text if isinstance(text, Mapping) else {}
    preparation = text.get("preparation")
    preparation = preparation if isinstance(preparation, Mapping) else {}
    deterministic = text.get("deterministic_diff")
    deterministic = deterministic if isinstance(deterministic, Mapping) else {}
    return {
        "matched_fragments": None,
        "changed_fragments": _non_negative_int(text.get("deltas")),
        "changed": _non_negative_int(deterministic.get("changed")),
        "removed": _non_negative_int(deterministic.get("removed")),
        "added": _non_negative_int(deterministic.get("added")),
        "review_required": _non_negative_int(text.get("review_required")),
        "prepared_fragments": _non_negative_int(preparation.get("fragments")),
        "text_atoms": _non_negative_int(text.get("atoms")),
    }


def _base_payload(state: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "kind": KIND,
        "schema_version": SCHEMA_VERSION,
        "version": 1,
        "available": False,
        "stale": bool(state.get("stale")),
        "run_status": str(state.get("status") or "NOT_STARTED"),
        "generation_run_id": state.get("run_id"),
        "generation_revision": _non_negative_int(state.get("revision")),
        "input_signature": state.get("input_signature"),
        "synthesis_input_signature": None,
        "summary": _summary(state),
        "available_match_pairs": 0,
        "matches": [],
        "changes": [],
        "constraints": {
            "read_only": True,
            "visualization_only": True,
            "producer_started": False,
            "matching_recomputed": False,
            "fuzzy_association_used": False,
            "match_coordinates_required": "BOTH_SIDES",
            "summary_source": "state.stages.text",
            "matched_fragments_total_available": False,
            "exact_match_coverage": "PERSISTED_DELTA_GROUPS_ONLY",
        },
    }


def evidence_is_publishable(state: Mapping[str, Any]) -> bool:
    """Return whether persisted evidence may be opened for this state."""
    if str(state.get("status") or "") not in PUBLISHED_STATUSES:
        return False
    stages = state.get("stages")
    text = stages.get("text") if isinstance(stages, Mapping) else None
    if not isinstance(text, Mapping) or text.get("status") != "COMPLETED":
        return False
    return str(text.get("source_state") or "") != "CHECK_BLOCKED"


def empty_production_text_evidence(state: Mapping[str, Any]) -> dict[str, Any]:
    """Return an honest empty response without inspecting producer artifacts."""
    return _base_payload(state)


def _normalized_bbox(value: Any) -> Any | None:
    """Accept exact persisted normalized rectangles without changing geometry."""
    numbers: tuple[float, float, float, float]
    if isinstance(value, Mapping):
        try:
            x = float(value.get("x", value.get("x0")))
            y = float(value.get("y", value.get("y0")))
            width = float(
                value["width"]
                if "width" in value
                else float(value["x1"]) - x
            )
            height = float(
                value["height"]
                if "height" in value
                else float(value["y1"]) - y
            )
        except (KeyError, TypeError, ValueError):
            return None
        numbers = (x, y, width, height)
    elif isinstance(value, (list, tuple)) and len(value) == 4:
        try:
            x0, y0, x1, y1 = (float(item) for item in value)
        except (TypeError, ValueError):
            return None
        numbers = (x0, y0, x1 - x0, y1 - y0)
    else:
        return None
    x, y, width, height = numbers
    if (
        not all(math.isfinite(item) for item in numbers)
        or width <= 0
        or height <= 0
        or x < 0
        or y < 0
        or x + width > 1
        or y + height > 1
    ):
        return None
    return copy.deepcopy(value)


def _location(value: Any, side: str) -> dict[str, Any] | None:
    if not isinstance(value, Mapping):
        return None
    page = value.get("page")
    if isinstance(page, bool) or not isinstance(page, int) or page < 1:
        return None
    raw_bboxes = value.get("bboxes") or []
    bboxes = (
        [_normalized_bbox(item) for item in raw_bboxes]
        if isinstance(raw_bboxes, list)
        else []
    )
    coordinates_available = bool(bboxes) and all(item is not None for item in bboxes)
    exact_bboxes = [item for item in bboxes if item is not None]
    fragment_id = value.get("fragment_id")
    if fragment_id is not None:
        fragment_id = str(fragment_id) or None
    return {
        "source": "TEXT",
        "document_ref": side,
        "page": page,
        "fragment_id": fragment_id,
        "block_id": None,
        "node_id": None,
        "highlight": (
            {"kind": "BBOX_SET", "bboxes": exact_bboxes}
            if coordinates_available
            else None
        ),
        "coordinate_space": (
            "NORMALIZED_PAGE_TOP_LEFT" if coordinates_available else None
        ),
        "page_size": None,
        "coordinates_available": coordinates_available,
    }


def _locations(values: Any, side: str) -> list[dict[str, Any]]:
    if values is None:
        return []
    if not isinstance(values, list):
        raise ProductionTextEvidenceConflictError(
            f"production TEXT {side} locations are malformed"
        )
    output = []
    for value in values:
        location = _location(value, side)
        if location is not None:
            output.append(location)
    output.sort(key=lambda item: (
        item["page"],
        str(item.get("fragment_id") or ""),
        content_signature(item.get("highlight")),
    ))
    return output


def _match_id(group: Mapping[str, Any], value: Mapping[str, Any]) -> str:
    return stable_id(
        "tmatch_",
        str(group.get("id") or ""),
        sorted(str(item) for item in value.get("left_fragment_ids") or []),
        sorted(str(item) for item in value.get("right_fragment_ids") or []),
        sorted(int(item) for item in value.get("left_pages") or []),
        sorted(int(item) for item in value.get("right_pages") or []),
    )


def _matches(text_differences: Mapping[str, Any]) -> list[dict[str, Any]]:
    output = []
    groups = text_differences.get("sheet_groups") or []
    if not isinstance(groups, list):
        raise ProductionTextEvidenceConflictError(
            "production Stage 3 sheet groups are malformed"
        )
    for group in groups:
        if not isinstance(group, Mapping):
            raise ProductionTextEvidenceConflictError(
                "production Stage 3 sheet group is malformed"
            )
        same = group.get("deterministic_same") or []
        if not isinstance(same, list):
            raise ProductionTextEvidenceConflictError(
                "production Stage 3 deterministic_same is malformed"
            )
        for value in same:
            if not isinstance(value, Mapping):
                raise ProductionTextEvidenceConflictError(
                    "production Stage 3 deterministic match is malformed"
                )
            left = _locations(value.get("left_anchors"), "LEFT")
            right = _locations(value.get("right_anchors"), "RIGHT")
            # This is viewer evidence, so a match must be an exact persisted
            # pair with drawable coordinates on both sides.  Page-only anchors
            # remain valid Stage 3 data but cannot prove a visual LEFT↔RIGHT
            # correspondence and therefore do not belong in this projection.
            if not any(item["coordinates_available"] for item in left) or not any(
                item["coordinates_available"] for item in right
            ):
                continue
            before = value.get("before")
            after = value.get("after")
            title = str(
                after
                if after not in (None, "")
                else before or "Совпавший текст"
            )
            output.append({
                "evidence_id": _match_id(group, value),
                "title": title,
                "before": before,
                "after": after,
                "review_required": False,
                "review_status": "CONFIRMED",
                "sides": {"LEFT": left, "RIGHT": right},
            })
    output.sort(key=lambda item: item["evidence_id"])
    if len({item["evidence_id"] for item in output}) != len(output):
        raise ProductionTextEvidenceConflictError(
            "production Stage 3 deterministic match identity is duplicated"
        )
    return output


def _target_index(synthesis: Mapping[str, Any]) -> dict[str, dict[str, str]]:
    output: dict[str, dict[str, str]] = {}

    def add(atom_id: Any, target_id: Any, target_kind: str, review_status: Any) -> None:
        atom_ref = str(atom_id or "")
        target_ref = str(target_id or "")
        if not atom_ref or not target_ref:
            return
        value = {
            "target_id": target_ref,
            "target_kind": target_kind,
            "target_review_status": str(review_status or ""),
        }
        previous = output.get(atom_ref)
        if previous is not None and previous != value:
            raise ProductionTextEvidenceConflictError(
                "published synthesis maps one TEXT atom to multiple targets"
            )
        output[atom_ref] = value

    for change in synthesis.get("changes") or []:
        if not isinstance(change, Mapping):
            continue
        for evidence in change.get("evidence_refs") or []:
            if isinstance(evidence, Mapping) and evidence.get("source") == "TEXT":
                add(
                    evidence.get("atom_id"),
                    change.get("change_id"),
                    "CHANGE",
                    change.get("review_status"),
                )
    for review in synthesis.get("review_items") or []:
        if not isinstance(review, Mapping) or review.get("source") != "TEXT":
            continue
        add(
            review.get("atom_id"),
            review.get("review_evidence_id"),
            "REVIEW_EVIDENCE",
            review.get("review_status"),
        )
    return output


def _title(before: Any, after: Any) -> str:
    if before not in (None, "") and after not in (None, ""):
        return f"{before} → {after}"
    if before not in (None, ""):
        return f"Удалено: {before}"
    if after not in (None, ""):
        return f"Добавлено: {after}"
    return "Изменение текста"


def _changes(
    text_artifact: Mapping[str, Any],
    synthesis: Mapping[str, Any],
) -> list[dict[str, Any]]:
    atoms = text_artifact.get("atoms")
    if not isinstance(atoms, list):
        raise ProductionTextEvidenceConflictError(
            "published TEXT atom artifact is malformed"
        )
    targets = _target_index(synthesis)
    output = []
    seen: set[str] = set()
    for atom in atoms:
        if not isinstance(atom, Mapping):
            raise ProductionTextEvidenceConflictError(
                "published TEXT atom is malformed"
            )
        atom_id = str(atom.get("atom_id") or "")
        if not atom_id or atom_id in seen:
            raise ProductionTextEvidenceConflictError(
                "published TEXT atom identity is missing or duplicated"
            )
        seen.add(atom_id)
        provenance = atom.get("provenance")
        locations = (
            provenance.get("locations")
            if isinstance(provenance, Mapping)
            else None
        )
        locations = locations if isinstance(locations, Mapping) else {}
        before = atom.get("before_value")
        after = atom.get("after_value")
        source_review_status = str(atom.get("review_status") or "CONFIRMED")
        target = targets.get(atom_id)
        effective_review_status = str(
            (target or {}).get("target_review_status") or source_review_status
        )
        item = {
            "evidence_id": atom_id,
            "title": _title(before, after),
            "before": before,
            "after": after,
            "review_required": effective_review_status == "REVIEW_REQUIRED",
            "review_status": effective_review_status,
            "source_review_status": source_review_status,
            "sides": {
                "LEFT": _locations(locations.get("LEFT"), "LEFT"),
                "RIGHT": _locations(locations.get("RIGHT"), "RIGHT"),
            },
        }
        if target is not None:
            item.update(target)
        output.append(item)
    output.sort(key=lambda item: item["evidence_id"])
    return output


def _validate_generation(
    state: Mapping[str, Any],
    source_snapshot: Mapping[str, Any],
    text_differences: Mapping[str, Any],
) -> Mapping[str, Any]:
    if (
        source_snapshot.get("run_id") != state.get("run_id")
        or source_snapshot.get("generation_input_signature")
        != state.get("input_signature")
    ):
        raise ProductionTextEvidenceConflictError(
            "production TEXT evidence snapshot generation does not match state"
        )
    text_source = source_snapshot.get("text")
    if not isinstance(text_source, Mapping):
        raise ProductionTextEvidenceConflictError(
            "production TEXT evidence snapshot is malformed"
        )
    text_artifact = text_source.get("artifact")
    if not isinstance(text_artifact, Mapping):
        raise ProductionTextEvidenceConflictError(
            "production TEXT evidence atom artifact is missing"
        )
    if (
        text_artifact.get("kind") != TEXT_ATOMS_KIND
        or text_artifact.get("schema_version") != TEXT_ATOMS_SCHEMA_VERSION
        or text_artifact.get("version") != 1
    ):
        raise ProductionTextEvidenceConflictError(
            "production TEXT evidence atom artifact has unsupported contract"
        )
    if text_source.get("content_digest") != content_signature(text_artifact):
        raise ProductionTextEvidenceConflictError(
            "production TEXT evidence atom snapshot digest changed"
        )
    text_stage = ((state.get("stages") or {}).get("text") or {})
    if not isinstance(text_stage, Mapping):
        raise ProductionTextEvidenceConflictError(
            "production TEXT stage is malformed"
        )
    atom_signature = text_artifact.get("input_signature")
    expected_atom_signatures = [
        text_stage.get("input_signature"),
        ((text_stage.get("text_atoms") or {}).get("input_signature"))
        if isinstance(text_stage.get("text_atoms"), Mapping)
        else None,
    ]
    if any(
        expected is not None and expected != atom_signature
        for expected in expected_atom_signatures
    ) or not any(expected is not None for expected in expected_atom_signatures):
        raise ProductionTextEvidenceConflictError(
            "production TEXT atom generation does not match state"
        )
    if (
        text_differences.get("kind") != TEXT_DIFFERENCES_KIND
        or text_differences.get("version") != TEXT_DIFFERENCES_VERSION
    ):
        raise ProductionTextEvidenceConflictError(
            "production Stage 3 text differences have unsupported contract"
        )
    deterministic = text_stage.get("deterministic_diff")
    if not isinstance(deterministic, Mapping) or (
        deterministic.get("source_signature")
        != text_differences.get("source_signature")
    ):
        raise ProductionTextEvidenceConflictError(
            "production Stage 3 generation does not match state"
        )
    provenance = text_artifact.get("provenance")
    expected_stage3 = (
        provenance.get("stage3_signature")
        if isinstance(provenance, Mapping)
        else None
    )
    try:
        actual_stage3 = stage3_content_signature(text_differences)
    except (TypeError, ValueError) as exc:
        raise ProductionTextEvidenceConflictError(
            "production Stage 3 text differences are malformed"
        ) from exc
    if not expected_stage3 or expected_stage3 != actual_stage3:
        raise ProductionTextEvidenceConflictError(
            "production Stage 3 signature does not match published TEXT atoms"
        )
    return text_artifact


def build_production_text_evidence(
    *,
    state: Mapping[str, Any],
    source_snapshot: Mapping[str, Any],
    text_differences: Mapping[str, Any],
    synthesis: Mapping[str, Any],
    synthesis_input_signature: str,
) -> dict[str, Any]:
    """Project exact viewer evidence from already-published artifacts only."""
    if not evidence_is_publishable(state):
        return empty_production_text_evidence(state)
    if not isinstance(synthesis_input_signature, str) or not (
        synthesis_input_signature.strip()
    ):
        raise ProductionTextEvidenceConflictError(
            "published synthesis signature is missing"
        )
    try:
        text_artifact = _validate_generation(
            state, source_snapshot, text_differences
        )
        matches = _matches(text_differences)
        changes = _changes(text_artifact, synthesis)
    except ProductionTextEvidenceConflictError:
        raise
    except (TypeError, ValueError) as exc:
        raise ProductionTextEvidenceConflictError(
            "production TEXT evidence is malformed"
        ) from exc
    return {
        **_base_payload(state),
        "available": True,
        "synthesis_input_signature": synthesis_input_signature,
        "available_match_pairs": len(matches),
        "matches": matches,
        "changes": changes,
    }


__all__ = [
    "KIND",
    "SCHEMA_VERSION",
    "ProductionTextEvidenceConflictError",
    "build_production_text_evidence",
    "empty_production_text_evidence",
    "evidence_is_publishable",
]
