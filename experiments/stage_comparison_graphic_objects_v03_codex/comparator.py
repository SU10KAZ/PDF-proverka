"""Deterministic object-level comparison for already paired prepared blocks."""
from __future__ import annotations

import collections
import math
import statistics
from typing import Any, Iterable


LEDGER_SCHEMA = "graphic-change-ledger-v0.3-codex"
CHANGE_STATUSES = {
    "UNCHANGED", "ADDED", "REMOVED", "GEOMETRY_CHANGED", "STYLE_CHANGED",
    "CONNECTION_CHANGED", "POSITION_CHANGED", "UNCERTAIN",
}


def _distance(left: Iterable[float], right: Iterable[float]) -> float:
    a = list(left); b = list(right)
    return math.hypot(a[0] - b[0], a[1] - b[1])


def _center(obj: dict[str, Any]) -> tuple[float, float]:
    value = obj["center_isotropic"]
    return float(value[0]), float(value[1])


def _size(obj: dict[str, Any]) -> float:
    value = obj["size_isotropic"]
    return math.hypot(float(value[0]), float(value[1]))


def _label_texts(description: dict[str, Any], obj: dict[str, Any]) -> set[str]:
    anchors = {str(row["anchor_id"]): str(row.get("text") or "").strip() for row in description.get("input", {}).get("prepared_text_metadata", [])}
    return {anchors[value] for value in obj.get("label_anchor_ids", []) if anchors.get(value)}


def _flatten(descriptions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    result = []
    for block_index, description in enumerate(descriptions):
        for obj in description.get("objects", []):
            result.append({**obj, "scope_object_id": f"b{block_index + 1}:{obj['object_id']}", "block_index": block_index})
    return result


def _unique_signature_pairs(left: list[dict[str, Any]], right: list[dict[str, Any]], field: str) -> list[tuple[dict[str, Any], dict[str, Any], str]]:
    left_map: dict[str, list[dict[str, Any]]] = collections.defaultdict(list)
    right_map: dict[str, list[dict[str, Any]]] = collections.defaultdict(list)
    for obj in left:
        left_map[str(obj.get(field))].append(obj)
    for obj in right:
        right_map[str(obj.get(field))].append(obj)
    return [(left_map[key][0], right_map[key][0], field) for key in sorted(left_map.keys() & right_map.keys()) if key and len(left_map[key]) == len(right_map[key]) == 1]


def _alignment(left: list[dict[str, Any]], right: list[dict[str, Any]]) -> dict[str, Any]:
    anchors = _unique_signature_pairs(left, right, "geometry_signature")
    if len(anchors) < 2:
        anchors.extend(_unique_signature_pairs(left, right, "family_signature"))
    dedup: dict[tuple[str, str], tuple[dict[str, Any], dict[str, Any], str]] = {}
    for row in anchors:
        dedup[(row[0]["scope_object_id"], row[1]["scope_object_id"])] = row
    anchors = list(dedup.values())
    ratios = [_size(r) / max(_size(l), 1e-9) for l, r, _ in anchors if 0.25 <= _size(r) / max(_size(l), 1e-9) <= 4]
    scale = statistics.median(ratios) if ratios else 1.0
    translations = [(_center(r)[0] - scale * _center(l)[0], _center(r)[1] - scale * _center(l)[1]) for l, r, _ in anchors]
    translation = [statistics.median([row[0] for row in translations]), statistics.median([row[1] for row in translations])] if translations else [0.0, 0.0]
    residuals = []
    for l, r, _ in anchors:
        predicted = (scale * _center(l)[0] + translation[0], scale * _center(l)[1] + translation[1])
        residuals.append(_distance(predicted, _center(r)))
    residual = statistics.median(residuals) if residuals else None
    reliable = len(anchors) >= 2 and residual is not None and residual <= 0.035 and 0.6 <= scale <= 1.6
    return {
        "model": "translation_plus_uniform_scale", "uniform_scale": round(scale, 6),
        "translation_isotropic": [round(value, 6) for value in translation],
        "anchor_count": len(anchors), "median_residual_isotropic": None if residual is None else round(residual, 6),
        "reliable": reliable,
        "evidence": [{"left_object": l["scope_object_id"], "right_object": r["scope_object_id"], "basis": basis} for l, r, basis in anchors[:50]],
        "provenance": "unique generic object signatures; no affine warp",
    }


def _aligned_center(obj: dict[str, Any], alignment: dict[str, Any]) -> tuple[float, float]:
    center = _center(obj); scale = alignment["uniform_scale"]; dx, dy = alignment["translation_isotropic"]
    return center[0] * scale + dx, center[1] * scale + dy


def _candidate_cost(left_labels: set[str], right_labels: set[str], left: dict[str, Any], right: dict[str, Any], alignment: dict[str, Any]) -> tuple[float, list[str]] | None:
    evidence = []
    if left_labels & right_labels:
        shape = 0.0; evidence.append("same prepared label anchor")
    elif left.get("geometry_signature") == right.get("geometry_signature"):
        shape = 0.02; evidence.append("same normalized geometry signature")
    elif left.get("family_signature") == right.get("family_signature"):
        shape = 0.13; evidence.append("same generic object family")
    elif left.get("type") == right.get("type"):
        left_segments = int(left["geometry"]["segment_count"]); right_segments = int(right["geometry"]["segment_count"])
        ratio = abs(math.log((left_segments + 1) / (right_segments + 1)))
        if ratio > 0.55:
            return None
        shape = 0.32 + min(0.2, ratio * 0.2); evidence.append("same generic type and similar topology size")
    else:
        return None
    position = _distance(_aligned_center(left, alignment), _center(right))
    size_ratio = abs(math.log((_size(right) + 1e-9) / max(_size(left) * alignment["uniform_scale"], 1e-9)))
    if position > (0.18 if shape <= 0.03 else 0.09) or size_ratio > (0.65 if shape <= 0.03 else 0.35):
        return None
    return shape + min(0.35, position * 2.5) + min(0.25, size_ratio * 0.3), evidence + [f"aligned position residual={position:.5f}", f"uniform size log-ratio={size_ratio:.5f}"]


def _match(left_descriptions: list[dict[str, Any]], right_descriptions: list[dict[str, Any]], left: list[dict[str, Any]], right: list[dict[str, Any]], alignment: dict[str, Any]) -> list[dict[str, Any]]:
    candidates = []
    left_labels = {obj["scope_object_id"]: _label_texts(left_descriptions[obj["block_index"]], obj) for obj in left}
    right_labels = {obj["scope_object_id"]: _label_texts(right_descriptions[obj["block_index"]], obj) for obj in right}
    for li, left_obj in enumerate(left):
        for ri, right_obj in enumerate(right):
            if left_obj["block_index"] >= len(left_descriptions) or right_obj["block_index"] >= len(right_descriptions):
                continue
            candidate = _candidate_cost(left_labels[left_obj["scope_object_id"]], right_labels[right_obj["scope_object_id"]], left_obj, right_obj, alignment)
            if candidate is not None:
                candidates.append((candidate[0], left_obj["scope_object_id"], right_obj["scope_object_id"], li, ri, candidate[1]))
    used_left: set[int] = set(); used_right: set[int] = set(); matches = []
    for cost, _, _, li, ri, evidence in sorted(candidates):
        if li in used_left or ri in used_right:
            continue
        used_left.add(li); used_right.add(ri)
        matches.append({"left_index": li, "right_index": ri, "cost": round(cost, 6), "confidence": round(max(0.0, 1.0 - cost), 6), "evidence": evidence})
    return matches


def _style_changed(left: dict[str, Any], right: dict[str, Any], scale: float) -> tuple[bool, dict[str, Any]]:
    fields = ("stroke", "fill", "dashes", "stroke_opacity", "fill_opacity", "line_cap", "line_join")
    differences = {field: {"before": left.get("style", {}).get(field), "after": right.get("style", {}).get(field)} for field in fields if left.get("style", {}).get(field) != right.get("style", {}).get(field)}
    lw_left = left.get("style", {}).get("stroke_width"); lw_right = right.get("style", {}).get("stroke_width")
    if isinstance(lw_left, (int, float)) and isinstance(lw_right, (int, float)) and abs(math.log((lw_right + 1e-6) / max(lw_left * scale, 1e-6))) > 0.2:
        differences["stroke_width"] = {"before": lw_left, "after": lw_right, "alignment_scale": scale}
    return bool(differences), differences


def _geometry_changed(left: dict[str, Any], right: dict[str, Any]) -> tuple[bool, dict[str, Any]]:
    if left.get("geometry_signature") == right.get("geometry_signature"):
        return False, {}
    before, after = left["geometry"], right["geometry"]
    segment_ratio = abs(math.log((before["segment_count"] + 1) / (after["segment_count"] + 1)))
    length_ratio = abs(math.log((before["length_isotropic"] + 1e-6) / (after["length_isotropic"] + 1e-6)))
    topology_changed = before.get("branch_points") != after.get("branch_points") or before.get("endpoints") != after.get("endpoints")
    changed = segment_ratio > 0.18 or length_ratio > 0.12 or topology_changed
    return changed, {"before_signature": left.get("geometry_signature"), "after_signature": right.get("geometry_signature"), "segment_log_ratio": round(segment_ratio, 6), "length_log_ratio": round(length_ratio, 6), "topology_changed": topology_changed}


def _connection_sets(descriptions: list[dict[str, Any]], objects: list[dict[str, Any]]) -> dict[str, set[tuple[str, str]]]:
    result: dict[str, set[tuple[str, str]]] = collections.defaultdict(set)
    local_to_scope = {(obj["block_index"], obj["object_id"]): obj["scope_object_id"] for obj in objects}
    for block_index, description in enumerate(descriptions):
        for relation in description.get("relations", []):
            if relation.get("type") not in {"CONNECTED_TO", "PART_OF", "INTERSECTS"}:
                continue
            source = local_to_scope.get((block_index, relation.get("source_object"))); target = local_to_scope.get((block_index, relation.get("target_object")))
            if source and target:
                result[source].add((relation["type"], target)); result[target].add((relation["type"], source))
    return result


def _comparison_applicability(descriptions: list[dict[str, Any]]) -> str:
    states = [row.get("quality", {}).get("status") for row in descriptions]
    return "GRAPHIC_NOT_APPLICABLE" if states and all(value == "GRAPHIC_NOT_APPLICABLE" for value in states) else "GRAPHIC_APPLICABLE"


def compare_graphic_scopes(pair_id: str, left_descriptions: list[dict[str, Any]], right_descriptions: list[dict[str, Any]], *, block_group_id: str | None = None) -> dict[str, Any]:
    """Compare explicit scopes. Pairing is input; no block or sheet matcher runs."""
    if not left_descriptions or not right_descriptions:
        raise ValueError("comparison scope requires non-empty left_blocks[] and right_blocks[]")
    applicability = _comparison_applicability(left_descriptions + right_descriptions)
    base = {
        "schema_version": LEDGER_SCHEMA, "research_only": True, "block_pair": pair_id,
        "scope": {"block_group_id": block_group_id, "left_blocks": [row.get("input", {}).get("block_id") for row in left_descriptions], "right_blocks": [row.get("input", {}).get("block_id") for row in right_descriptions], "pairing_source": "explicit benchmark pair; no 1-to-N matcher"},
        "applicability": applicability,
    }
    if applicability == "GRAPHIC_NOT_APPLICABLE":
        return {**base, "alignment": None, "object_matches": [], "object_statuses": [], "changes": [], "validator": {"text_only_changes_removed": 0, "rule": "text-only and table-only changes cannot enter graphic ledger"}, "decision": {"graphic_verdict": "NO_GRAPHIC_CHANGE", "route": "GRAPHIC_VECTOR_OK", "route_reasons": ["prepared block type is text/table/stamp; SKIP_GRAPHIC_COMPARISON"], "correspondence_reliable": True}}
    left, right = _flatten(left_descriptions), _flatten(right_descriptions)
    alignment = _alignment(left, right); matches = _match(left_descriptions, right_descriptions, left, right, alignment)
    left_match = {row["left_index"]: row for row in matches}; right_match = {row["right_index"]: row for row in matches}
    left_connections = _connection_sets(left_descriptions, left); right_connections = _connection_sets(right_descriptions, right)
    mapped_right = {right[row["right_index"]]["scope_object_id"]: left[row["left_index"]]["scope_object_id"] for row in matches}
    statuses = []; changes = []
    def event(status: str, left_obj: dict[str, Any] | None, right_obj: dict[str, Any] | None, confidence: float, evidence: Any) -> None:
        assert status in CHANGE_STATUSES
        row = {"event_id": f"event_{len(statuses)+1:05d}", "status": status, "left_object": left_obj and left_obj["scope_object_id"], "right_object": right_obj and right_obj["scope_object_id"], "confidence": round(confidence, 6), "evidence": evidence, "basis": "graphical_object_geometry"}
        statuses.append(row)
        if status != "UNCHANGED":
            changes.append({**row, "type": status})
    for li, left_obj in enumerate(left):
        match = left_match.get(li)
        if match is None:
            event("REMOVED", left_obj, None, 0.65, ["no compatible right graphical object after deterministic alignment"]); continue
        right_obj = right[match["right_index"]]; subevents = []
        geometry, geometry_evidence = _geometry_changed(left_obj, right_obj)
        if geometry:
            subevents.append(("GEOMETRY_CHANGED", geometry_evidence))
        style, style_evidence = _style_changed(left_obj, right_obj, alignment["uniform_scale"])
        if style:
            subevents.append(("STYLE_CHANGED", style_evidence))
        left_edges = left_connections.get(left_obj["scope_object_id"], set())
        right_edges = {(kind, mapped_right.get(neighbor, f"unmatched:{neighbor}")) for kind, neighbor in right_connections.get(right_obj["scope_object_id"], set())}
        if left_edges != right_edges:
            subevents.append(("CONNECTION_CHANGED", {"before": sorted(left_edges), "after_mapped_to_left": sorted(right_edges)}))
        residual = _distance(_aligned_center(left_obj, alignment), _center(right_obj))
        if alignment["reliable"] and residual > 0.025:
            subevents.append(("POSITION_CHANGED", {"aligned_residual_isotropic": round(residual, 6)}))
        if not subevents:
            event("UNCHANGED", left_obj, right_obj, match["confidence"], match["evidence"])
        else:
            for status, evidence in subevents:
                event(status, left_obj, right_obj, min(match["confidence"], 0.82), [*match["evidence"], evidence])
    for ri, right_obj in enumerate(right):
        if ri not in right_match:
            event("ADDED", None, right_obj, 0.65, ["no compatible left graphical object after deterministic alignment"])
    # There is deliberately no TEXT_CHANGED event type.  This validator is
    # retained in the artifact so downstream integration cannot silently add it.
    before_validation = len(changes)
    changes = [row for row in changes if row.get("basis") != "text_only" and row.get("type") not in {"TEXT_CHANGED", "TABLE_CHANGED"}]
    matched_fraction = len(matches) / max(len(left), len(right), 1)
    dangerous = any(row.get("quality", {}).get("dangerous_cap") for row in left_descriptions + right_descriptions)
    insufficient = any(row.get("quality", {}).get("status") == "VECTOR_DATA_INSUFFICIENT" for row in left_descriptions + right_descriptions)
    uncertain_share = sum(row["confidence"] < 0.55 for row in changes) / max(len(changes), 1)
    correspondence_reliable = matched_fraction >= 0.72 and alignment["reliable"] and uncertain_share <= 0.1
    if insufficient or not left or not right or matched_fraction < 0.1:
        route = "GRAPHIC_VISION_ONLY"
    elif dangerous or not correspondence_reliable or changes:
        route = "GRAPHIC_HYBRID"
    else:
        route = "GRAPHIC_VECTOR_OK"
    reasons = []
    if insufficient or not left or not right: reasons.append("vector layer fundamentally insufficient")
    if dangerous: reasons.append("dangerous spatial/relation cap")
    if not alignment["reliable"]: reasons.append("translation+uniform-scale alignment not reliable")
    if matched_fraction < .72: reasons.append(f"object matched fraction {matched_fraction:.3f} < 0.72")
    if changes: reasons.append("object-level change requires fused raster interpretation")
    if not reasons: reasons.append("reliable extraction, alignment and object correspondence; no graphical change")
    return {
        **base, "alignment": alignment,
        "object_matches": [{**row, "left_object": left[row["left_index"]]["scope_object_id"], "right_object": right[row["right_index"]]["scope_object_id"]} for row in matches],
        "object_statuses": statuses, "changes": changes,
        "validator": {"text_only_changes_removed": before_validation - len(changes), "table_only_changes_removed": 0, "rule": "text-only and table-only changes cannot enter graphic ledger"},
        "decision": {"graphic_verdict": "GRAPHIC_CHANGE" if changes else "NO_GRAPHIC_CHANGE", "route": route, "route_reasons": reasons, "correspondence_reliable": correspondence_reliable, "matched_fraction": round(matched_fraction, 6), "left_object_count": len(left), "right_object_count": len(right)},
    }


__all__ = ["compare_graphic_scopes", "LEDGER_SCHEMA", "CHANGE_STATUSES"]
