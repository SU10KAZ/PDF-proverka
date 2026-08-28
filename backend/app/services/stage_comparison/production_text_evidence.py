"""Read-only viewer projection of persisted production TEXT evidence.

The projection deliberately performs no comparison, matching, or synthesis.
It exposes exact Stage 3 deterministic pairs and exact TEXT-atom locations
only after all inputs have been proven to belong to the published generation.

The current list of changes is read from the *effective* unified synthesis,
which is republished after every human resolution.  The frozen source
snapshot is used for provenance only — evidence refs, fragments, pages and
bounding boxes — never for current values, status or identity.
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
from .text_semantic_validation import (
    STAGE3_FULL_DIGEST_VERSION,
    stage3_content_signature,
    stage3_full_content_signature,
)


KIND = "stage_comparison_production_text_evidence"
SCHEMA_VERSION = "production-text-evidence.v2"
PUBLISHED_STATUSES = frozenset({"COMPLETED", "PARTIAL"})

TEXT_RESULT_PUBLISHED = "PUBLISHED"
TEXT_RESULT_BLOCKED = "BLOCKED"
TEXT_RESULT_NOT_PRODUCED = "NOT_PRODUCED"

MATCH_EVIDENCE_UNKNOWN = "UNKNOWN"
MATCH_EVIDENCE_VERIFIED = "VERIFIED"
MATCH_EVIDENCE_LEGACY = "UNVERIFIED_LEGACY_GENERATION"


class ProductionTextEvidenceConflictError(ValueError):
    """Persisted inputs do not describe one published production generation."""


def _non_negative_int(value: Any) -> int | None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        return None
    return value


def _text_stage(state: Mapping[str, Any]) -> Mapping[str, Any]:
    stages = state.get("stages")
    text = stages.get("text") if isinstance(stages, Mapping) else None
    return text if isinstance(text, Mapping) else {}


def text_result_state(state: Mapping[str, Any]) -> str:
    """Classify what the TEXT branch actually produced for this generation.

    ``BLOCKED`` and ``NOT_PRODUCED`` both mean *no result was built*; their
    counters are unknown rather than zero, and neither may be presented as a
    successful analysis that simply found nothing.
    """
    text = _text_stage(state)
    if "CHECK_BLOCKED" in {
        str(text.get("status") or ""),
        str(text.get("source_state") or ""),
    }:
        return TEXT_RESULT_BLOCKED
    if str(state.get("status") or "") not in PUBLISHED_STATUSES:
        return TEXT_RESULT_NOT_PRODUCED
    if text.get("status") != "COMPLETED":
        return TEXT_RESULT_NOT_PRODUCED
    return TEXT_RESULT_PUBLISHED


def _unknown_summary() -> dict[str, int | None]:
    return {
        "matched_fragments": None,
        "changed_fragments": None,
        "changed": None,
        "removed": None,
        "added": None,
        "review_required": None,
        "prepared_fragments": None,
        "text_atoms": None,
    }


def _summary(state: Mapping[str, Any]) -> dict[str, int | None]:
    """Use only counters already published in ``state.stages.text``.

    Stage 3 intentionally does not publish a total exact-match count in state,
    so ``matched_fragments`` remains unknown.  The payload separately reports
    how many persisted match pairs are available for viewer highlighting.

    When the TEXT branch produced no result at all, every counter is unknown:
    the zeros a blocked stage writes describe an aborted run, not a checked
    document with nothing in it.
    """
    if text_result_state(state) != TEXT_RESULT_PUBLISHED:
        return _unknown_summary()
    text = _text_stage(state)
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


def _blocked_reason(state: Mapping[str, Any]) -> tuple[str | None, str | None]:
    text = _text_stage(state)
    reason = text.get("reason_code")
    error = text.get("error_type")
    return (
        str(reason) if reason else None,
        str(error) if error else None,
    )


def _base_payload(state: Mapping[str, Any]) -> dict[str, Any]:
    result_state = text_result_state(state)
    reason, error = (
        _blocked_reason(state)
        if result_state == TEXT_RESULT_BLOCKED
        else (None, None)
    )
    return {
        "kind": KIND,
        "schema_version": SCHEMA_VERSION,
        "version": 1,
        "available": False,
        "stale": bool(state.get("stale")),
        "run_status": str(state.get("status") or "NOT_STARTED"),
        "text_result_state": result_state,
        "text_blocked_reason": reason,
        "text_blocked_error": error,
        "generation_run_id": state.get("run_id"),
        "generation_revision": _non_negative_int(state.get("revision")),
        "input_signature": state.get("input_signature"),
        "synthesis_input_signature": None,
        "summary": _summary(state),
        # Unknown, not zero: nothing has been counted while the projection is
        # unavailable, and a hard 0 would read as "checked, found none".
        "available_match_pairs": None,
        "match_evidence_state": MATCH_EVIDENCE_UNKNOWN,
        "change_items": None,
        "available_change_items": None,
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
            "changes_source": "effective_unified_synthesis",
            "provenance_source": "published_source_snapshot",
            "matched_fragments_total_available": False,
            "exact_match_coverage": "PERSISTED_DELTA_GROUPS_ONLY",
            "same_evidence_signature_version": STAGE3_FULL_DIGEST_VERSION,
        },
    }


def evidence_is_publishable(state: Mapping[str, Any]) -> bool:
    """Return whether persisted evidence may be opened for this state."""
    return text_result_state(state) == TEXT_RESULT_PUBLISHED


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
    seen: set[str] = set()
    for value in values:
        location = _location(value, side)
        if location is None:
            continue
        identity = content_signature(location)
        if identity in seen:
            continue
        seen.add(identity)
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


def _title(before: Any, after: Any) -> str:
    if before not in (None, "") and after not in (None, ""):
        return f"{before} → {after}"
    if before not in (None, ""):
        return f"Удалено: {before}"
    if after not in (None, ""):
        return f"Добавлено: {after}"
    return "Изменение текста"


def _snapshot_atoms(text_artifact: Mapping[str, Any]) -> dict[str, Mapping[str, Any]]:
    """Index the frozen automatic atoms; they are provenance, not truth."""
    atoms = text_artifact.get("atoms")
    if not isinstance(atoms, list):
        raise ProductionTextEvidenceConflictError(
            "published TEXT atom artifact is malformed"
        )
    output: dict[str, Mapping[str, Any]] = {}
    for atom in atoms:
        if not isinstance(atom, Mapping):
            raise ProductionTextEvidenceConflictError(
                "published TEXT atom is malformed"
            )
        atom_id = str(atom.get("atom_id") or "")
        if not atom_id or atom_id in output:
            raise ProductionTextEvidenceConflictError(
                "published TEXT atom identity is missing or duplicated"
            )
        output[atom_id] = atom
    return output


def _provenance_locations(value: Any) -> Mapping[str, Any] | None:
    if not isinstance(value, Mapping):
        return None
    locations = value.get("locations")
    return locations if isinstance(locations, Mapping) else None


def _text_atom_ids(item: Mapping[str, Any]) -> list[str]:
    output = set()
    for evidence in item.get("evidence_refs") or []:
        if isinstance(evidence, Mapping) and evidence.get("source") == "TEXT":
            atom_id = str(evidence.get("atom_id") or "")
            if atom_id:
                output.add(atom_id)
    return sorted(output)


def _sides_for(
    atom_ids: list[str],
    inline_locations: Mapping[str, Mapping[str, Any]],
    snapshot_atoms: Mapping[str, Mapping[str, Any]],
) -> dict[str, list[dict[str, Any]]]:
    """Resolve exact coordinates for one current item.

    Locations travel inside the synthesis provenance; the frozen snapshot is
    consulted only when that copy is unavailable.  Either way this is pure
    provenance — no value, status or identity is taken from it.
    """
    raw: dict[str, list[Any]] = {"LEFT": [], "RIGHT": []}
    for atom_id in atom_ids:
        locations = inline_locations.get(atom_id)
        if locations is None:
            locations = _provenance_locations(
                (snapshot_atoms.get(atom_id) or {}).get("provenance")
            )
        if locations is None:
            continue
        for side in ("LEFT", "RIGHT"):
            values = locations.get(side)
            if values is None:
                continue
            if not isinstance(values, list):
                raise ProductionTextEvidenceConflictError(
                    f"production TEXT {side} locations are malformed"
                )
            raw[side].extend(values)
    return {side: _locations(raw[side], side) for side in ("LEFT", "RIGHT")}


def _change_item(
    *,
    target_id: str,
    target_kind: str,
    item: Mapping[str, Any],
    review_status: str,
    atom_ids: list[str],
    sides: Mapping[str, list[dict[str, Any]]],
    snapshot_atoms: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    before = item.get("before_value")
    after = item.get("after_value")
    source_status = sorted({
        str((snapshot_atoms.get(atom_id) or {}).get("review_status") or "")
        for atom_id in atom_ids
    } - {""})
    return {
        # Identity is the current target itself: a viewer item can never point
        # at something the effective synthesis no longer publishes.
        "evidence_id": target_id,
        "target_id": target_id,
        "target_kind": target_kind,
        "target_review_status": review_status,
        "atom_ids": list(atom_ids),
        "title": _title(before, after),
        "before": before,
        "after": after,
        "outcome": str(item.get("outcome") or ""),
        "review_required": review_status == "REVIEW_REQUIRED",
        "review_status": review_status,
        # Provenance only: what the automatic producer said before review.
        "source_review_status": source_status[0] if len(source_status) == 1 else None,
        "coordinates_available": any(
            location["coordinates_available"]
            for side in ("LEFT", "RIGHT")
            for location in sides[side]
        ),
        "sides": {"LEFT": sides["LEFT"], "RIGHT": sides["RIGHT"]},
    }


def _changes(
    synthesis: Mapping[str, Any],
    snapshot_atoms: Mapping[str, Mapping[str, Any]],
) -> list[dict[str, Any]]:
    """Project the current effective changes, never the frozen snapshot.

    A rejected, replaced or unselected atom simply has no current target, so
    it cannot appear here at all; a corrected value is read from the target
    the human resolution actually produced.
    """
    output = []
    for change in synthesis.get("changes") or []:
        if not isinstance(change, Mapping):
            raise ProductionTextEvidenceConflictError(
                "published synthesis change is malformed"
            )
        atom_ids = _text_atom_ids(change)
        if not atom_ids:
            continue  # GRAPHIC-only change: not TEXT viewer evidence.
        change_id = str(change.get("change_id") or "")
        if not change_id:
            raise ProductionTextEvidenceConflictError(
                "published synthesis change has no identity"
            )
        provenance = change.get("provenance")
        inline: dict[str, Mapping[str, Any]] = {}
        for entry in (provenance or {}).get("source_atoms") or []:
            if not isinstance(entry, Mapping) or entry.get("source") != "TEXT":
                continue
            locations = _provenance_locations(entry.get("provenance"))
            atom_id = str(entry.get("atom_id") or "")
            if atom_id and locations is not None:
                inline[atom_id] = locations
        output.append(_change_item(
            target_id=change_id,
            target_kind="CHANGE",
            item=change,
            review_status=str(change.get("review_status") or ""),
            atom_ids=atom_ids,
            sides=_sides_for(atom_ids, inline, snapshot_atoms),
            snapshot_atoms=snapshot_atoms,
        ))
    for review in synthesis.get("review_items") or []:
        if not isinstance(review, Mapping):
            raise ProductionTextEvidenceConflictError(
                "published synthesis review item is malformed"
            )
        if review.get("source") != "TEXT":
            continue
        review_id = str(review.get("review_evidence_id") or "")
        atom_id = str(review.get("atom_id") or "")
        if not review_id or not atom_id:
            raise ProductionTextEvidenceConflictError(
                "published synthesis review item has no identity"
            )
        locations = _provenance_locations(
            (review.get("provenance") or {}).get("source_atom")
        )
        inline = {atom_id: locations} if locations is not None else {}
        output.append(_change_item(
            target_id=review_id,
            target_kind="REVIEW_EVIDENCE",
            item=review,
            review_status=str(review.get("review_status") or "REVIEW_REQUIRED"),
            atom_ids=[atom_id],
            sides=_sides_for([atom_id], inline, snapshot_atoms),
            snapshot_atoms=snapshot_atoms,
        ))
    output.sort(key=lambda item: item["evidence_id"])
    if len({item["evidence_id"] for item in output}) != len(output):
        raise ProductionTextEvidenceConflictError(
            "published synthesis target identity is duplicated"
        )
    return output


def _same_evidence_state(text_artifact: Mapping[str, Any]) -> str:
    provenance = text_artifact.get("provenance")
    provenance = provenance if isinstance(provenance, Mapping) else {}
    version = provenance.get("stage3_full_signature_version")
    signature = provenance.get("stage3_full_signature")
    if version is None and signature is None:
        # Published before ``deterministic_same`` entered any signature.  The
        # old digest is honoured for what it did cover and is never re-read as
        # if it also covered the exact same-text pairs.
        return MATCH_EVIDENCE_LEGACY
    if version != STAGE3_FULL_DIGEST_VERSION or not isinstance(signature, str):
        raise ProductionTextEvidenceConflictError(
            "production Stage 3 content signature has unsupported version"
        )
    return MATCH_EVIDENCE_VERIFIED


def _validate_generation(
    state: Mapping[str, Any],
    source_snapshot: Mapping[str, Any],
    text_differences: Mapping[str, Any],
) -> tuple[Mapping[str, Any], str]:
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
    same_state = _same_evidence_state(text_artifact)
    if same_state == MATCH_EVIDENCE_VERIFIED:
        try:
            actual_full = stage3_full_content_signature(text_differences)
        except (TypeError, ValueError) as exc:
            raise ProductionTextEvidenceConflictError(
                "production Stage 3 text differences are malformed"
            ) from exc
        if provenance.get("stage3_full_signature") != actual_full:
            raise ProductionTextEvidenceConflictError(
                "production Stage 3 exact pairs do not match published TEXT atoms"
            )
    return text_artifact, same_state


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
        text_artifact, same_state = _validate_generation(
            state, source_snapshot, text_differences
        )
        matches = (
            _matches(text_differences)
            if same_state == MATCH_EVIDENCE_VERIFIED
            else []
        )
        changes = _changes(synthesis, _snapshot_atoms(text_artifact))
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
        "match_evidence_state": same_state,
        # Unknown while the exact pairs carry no covering signature; a hard 0
        # would claim the generation was checked and holds no matches.
        "available_match_pairs": (
            len(matches) if same_state == MATCH_EVIDENCE_VERIFIED else None
        ),
        "change_items": len(changes),
        "available_change_items": sum(
            1 for item in changes if item["coordinates_available"]
        ),
        "matches": matches,
        "changes": changes,
    }


__all__ = [
    "KIND",
    "MATCH_EVIDENCE_LEGACY",
    "MATCH_EVIDENCE_UNKNOWN",
    "MATCH_EVIDENCE_VERIFIED",
    "SCHEMA_VERSION",
    "TEXT_RESULT_BLOCKED",
    "TEXT_RESULT_NOT_PRODUCED",
    "TEXT_RESULT_PUBLISHED",
    "ProductionTextEvidenceConflictError",
    "build_production_text_evidence",
    "empty_production_text_evidence",
    "evidence_is_publishable",
    "text_result_state",
]
