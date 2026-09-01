"""Complete engineering-unresolved inventory for the v2 experiment."""
from __future__ import annotations

from collections import Counter
from typing import Any, Mapping

from ..ai import routing as legacy_routing
from ..production_artifacts import content_signature, stable_id, utc_now
from . import schemas

KIND = "stage_comparison_ai_v2_unresolved_inventory"
SCHEMA_VERSION = "stage-comparison-ai-v2-inventory.v1"

ELIGIBLE = "AI_ELIGIBLE"
NO_EVIDENCE = "AI_INELIGIBLE_NO_EVIDENCE"
POLICY = "AI_INELIGIBLE_POLICY"
HUMAN_AUTHORITY = "AI_INELIGIBLE_HUMAN_AUTHORITY"
DECISIONS = (ELIGIBLE, NO_EVIDENCE, POLICY, HUMAN_AUTHORITY)

ROUTED = "ROUTED"
NOT_ROUTED = "NOT_ROUTED"

_LEGACY_DECISIONS = {
    legacy_routing.ELIGIBLE: ELIGIBLE,
    legacy_routing.INELIGIBLE_EVIDENCE: NO_EVIDENCE,
    legacy_routing.INELIGIBLE_POLICY: POLICY,
}

_TASK_TYPES = {
    legacy_routing.KIND_TEXT_REVIEW: schemas.CHANGE_INTERPRETATION,
    legacy_routing.KIND_TABLE_UNPROVEN: schemas.TABLE_ROW_IDENTITY,
    legacy_routing.KIND_TABLE_BLOCKED: schemas.TABLE_ROW_IDENTITY,
    legacy_routing.KIND_CONSISTENCY_REVIEW:
        schemas.DOCUMENT_INCONSISTENCY_REVIEW,
    legacy_routing.KIND_CHANGE_REVIEW: schemas.CHANGE_INTERPRETATION,
}


def _legacy_item(value: Mapping[str, Any]) -> dict[str, Any]:
    decision = _LEGACY_DECISIONS.get(str(value.get("decision") or ""), POLICY)
    source_kind = str(value.get("kind") or "")
    task_type = _TASK_TYPES.get(source_kind, schemas.UNRESOLVABLE)
    if source_kind == legacy_routing.KIND_CONSISTENCY_REVIEW:
        summary = str(value.get("summary") or "").lower()
        if any(word in summary for word in ("подпис", "обознач", "label")):
            task_type = schemas.LABEL_CONFLICT
    if source_kind == legacy_routing.KIND_TABLE_BLOCKED and (
        value.get("reason_code") == legacy_routing.REASON_MODE_MISMATCH
    ):
        task_type = schemas.MODE_RELATION
    unresolved = bool(value.get("unresolved", True))
    routed = unresolved and decision == ELIGIBLE
    return {
        "task_id": str(value.get("item_id") or ""),
        "task_type": task_type,
        "source_kind": source_kind,
        "decision": decision,
        "route_status": ROUTED if routed else NOT_ROUTED,
        "routed_to_ai": routed,
        "unresolved": unresolved,
        "reason_code": str(value.get("reason_code") or ""),
        "reason": str(value.get("reason") or ""),
        "available_evidence": list(value.get("available_evidence") or ()),
        "missing_evidence": list(value.get("missing_evidence") or ()),
        "subject": value.get("subject"),
        "side": value.get("side"),
        "summary": str(value.get("summary") or ""),
        "routing_payload": dict(value.get("routing_payload") or {}),
    }


def _apply_human_review_route(
    item: dict[str, Any],
    human_review_plan: Mapping[str, Any] | None,
) -> dict[str, Any]:
    route = ((human_review_plan or {}).get("ai_routing") or {}).get(item["task_id"])
    if not isinstance(route, Mapping) or route.get("routed_to_ai") is not False:
        return item
    classification = str(route.get("classification") or "")
    decision = NO_EVIDENCE if classification == "MISSING_EVIDENCE" else POLICY
    return {
        **item,
        "decision": decision,
        "route_status": NOT_ROUTED,
        "routed_to_ai": False,
        "reason_code": f"HUMAN_REVIEW_CLASSIFIED_{classification or 'NON_ACTIONABLE'}",
        "reason": "human-review orchestrator classified the item before AI routing",
        "pre_ai_classification": classification,
    }


def _graph_ambiguities(direct_page: Mapping[str, Any] | None) -> list[dict[str, Any]]:
    matching = ((direct_page or {}).get("comparison_result") or {}).get("matching")
    output: list[dict[str, Any]] = []
    for item in (matching or {}).get("ambiguous") or ():
        if not isinstance(item, Mapping):
            continue
        left_id = str(item.get("left_id") or "")
        candidates = [
            str(value.get("right_id") or "")
            for value in item.get("right_candidates") or ()
            if isinstance(value, Mapping) and value.get("right_id")
        ]
        if not left_id:
            continue
        decision = ELIGIBLE if candidates else NO_EVIDENCE
        output.append({
            "task_id": stable_id("aiv2_graph", left_id, *candidates),
            "task_type": schemas.FUNCTIONAL_IDENTITY,
            "source_kind": "GRAPH_ENTITY_AMBIGUITY",
            "decision": decision,
            "route_status": ROUTED if decision == ELIGIBLE else NOT_ROUTED,
            "routed_to_ai": decision == ELIGIBLE,
            "unresolved": True,
            "reason_code": (
                "GRAPH_CANDIDATES_AVAILABLE" if candidates
                else "NO_GRAPH_CANDIDATES"
            ),
            "reason": (
                "кандидаты существуют, но детерминированный запас уверенности "
                "недостаточен"
                if candidates else "на другой стороне нет кандидата"
            ),
            "available_evidence": [
                "узел слева, кандидаты справа и их графовые связи"
            ] if candidates else ["узел слева"],
            "missing_evidence": [
                "проверенное функциональное тождество"
            ] if candidates else ["кандидат правой стороны"],
            "subject": left_id,
            "side": "LEFT",
            "summary": f"Неоднозначное соответствие графового узла {left_id}.",
            "routing_payload": {
                "left_node_id": left_id,
                "right_node_ids": candidates,
            },
        })
    return output


def build_inventory(
    *,
    legacy_inventory: Mapping[str, Any] | None,
    direct_page: Mapping[str, Any] | None,
    human_review_plan: Mapping[str, Any] | None = None,
    pair_id: str = "",
    generated_at: str | None = None,
) -> dict[str, Any]:
    """Build an inventory where every unresolved item has an explicit route.

    V2 intentionally routes every evidence-bearing legacy class, including
    consistency reviews and incomplete graphic changes.  It additionally
    expands the single aggregate matcher warning into individual graph
    identity tasks so those cases cannot disappear behind one meta finding.
    """
    items = [
        _apply_human_review_route(_legacy_item(value), human_review_plan)
        for value in (legacy_inventory or {}).get("items") or ()
        if isinstance(value, Mapping)
    ]
    items.extend(_graph_ambiguities(direct_page))
    # Legacy mode-blocked rows use one match_id for two independently
    # reviewable facets (power and current).  A dict keyed by that ID silently
    # dropped the second facet.  V2 preserves the original ID when unique and
    # deterministically mints a task ID only for collisions.
    seen: dict[str, int] = {}
    for item in items:
        original = str(item.get("task_id") or "")
        ordinal = seen.get(original, 0)
        seen[original] = ordinal + 1
        if ordinal:
            item["source_item_id"] = original
            item["task_id"] = stable_id(
                "aiv2_task", original, str(item.get("summary") or ""), ordinal
            )
    items.sort(key=lambda value: (value["task_type"], value["task_id"]))

    unresolved = [value for value in items if value["unresolved"]]
    counts: dict[str, int] = {
        "total_records": len(items),
        "total_engineering_unresolved": len(unresolved),
        "routed": sum(bool(value["routed_to_ai"]) for value in unresolved),
        "not_routed": sum(not value["routed_to_ai"] for value in unresolved),
    }
    for decision in DECISIONS:
        counts[decision] = sum(
            value["decision"] == decision for value in unresolved
        )
    by_type = {
        key: value for key, value in sorted(Counter(
            item["task_type"] for item in unresolved
        ).items())
    }
    not_routed_reasons = {
        key: value for key, value in sorted(Counter(
            item["reason_code"] for item in unresolved
            if not item["routed_to_ai"]
        ).items())
    }
    if counts["routed"] + counts["not_routed"] != len(unresolved):
        raise AssertionError("unresolved inventory contains silently dropped items")
    if counts[ELIGIBLE] != counts["routed"]:
        raise AssertionError("eligible inventory items are not all routed")

    payload = {
        "kind": KIND,
        "schema_version": SCHEMA_VERSION,
        "version": 1,
        "pair_id": pair_id,
        "generated_at": generated_at or utc_now(),
        "items": items,
        "counts": counts,
        "by_task_type": by_type,
        "not_routed_reasons": not_routed_reasons,
        "decisions": list(DECISIONS),
        "constraints": {
            "uses_model": False,
            "complete_route_accounting": True,
            "human_decisions_are_read_only": True,
            "human_review_classified_before_routing": bool(human_review_plan),
        },
    }
    payload["input_signature"] = content_signature({
        "schema": SCHEMA_VERSION,
        "pair_id": pair_id,
        "items": items,
    })
    return payload


def eligible_items(value: Mapping[str, Any]) -> list[dict[str, Any]]:
    return [
        dict(item) for item in value.get("items") or ()
        if isinstance(item, Mapping)
        and item.get("unresolved", True)
        and item.get("decision") == ELIGIBLE
        and item.get("routed_to_ai") is True
    ]


__all__ = [
    "DECISIONS",
    "ELIGIBLE",
    "HUMAN_AUTHORITY",
    "KIND",
    "NO_EVIDENCE",
    "POLICY",
    "SCHEMA_VERSION",
    "build_inventory",
    "eligible_items",
]
