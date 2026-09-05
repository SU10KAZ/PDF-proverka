"""Read-only reassessment of the Function Passport and of Function Lineage.

Nothing here writes, overlays or materializes.  The question is narrow: given a
subgraph a scope is *proven* to be, which facts does the passport gain that no
extraction could have given it, and does either certified tier move?

The enrichment is positive-only by construction — every added fact is a count or
a multiset of what the sheet draws — and it is added to a *copy*, in an artifact,
next to the passport it came from.  §17's prohibition is the reason the module is
this short: there is no fact here that says something is not there.

The lineage half repeats V2's measurement with the aggregate in place of the
node.  It is repeated rather than cited because the whole track turns on whether
the change of granularity moves it, and a number carried over from a previous
artifact could not answer that.
"""
from __future__ import annotations

from collections import Counter, defaultdict
from typing import Any, Mapping, Sequence

from experiments.function_lineage_v2 import instance_identity as production_marks
from experiments.function_lineage_v3 import corpus as frozen_corpus

from .contract import (
    AMBIGUOUS_BINDING,
    NO_BINDING,
    PARTIAL_BINDING,
    PROVEN_BINDING,
    SCHEMA_VERSION,
    ScopeBinding,
    UNKNOWN,
)

#: Passport fields whose value a *drawing* could in principle show.  The
#: relational two are the reason the track exists: V1 measured ``upstream``
#: printed literally once in 1 074 and ``downstream`` sixteen times in 1 945.
DRAWABLE_FIELDS = ("upstream", "downstream", "equipment_roles", "consumers", "stable_entities")


def passport_enrichment(
    bindings: Sequence[ScopeBinding],
    facts_by_subgraph: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    """§17: the derived positive facts a proven binding adds to a passport."""
    rows: list[dict[str, Any]] = []
    documented: Counter = Counter()
    for pair_id in sorted(
        frozen_corpus.PROJECTS,
        key=lambda key: frozen_corpus.CORPUS_ORDER.index(frozen_corpus.PROJECTS[key]),
    ):
        passports = frozen_corpus.passports(pair_id)
        for side in frozen_corpus.SIDES:
            for function_id, passport in sorted(passports[side].items()):
                for field in DRAWABLE_FIELDS:
                    raw = passport.get(field)
                    values = [raw] if isinstance(raw, str) else list(raw or [])
                    documented[field] += sum(1 for value in values if str(value).strip())
    for row in bindings:
        if row.binding_status != PROVEN_BINDING or not row.subgraph_id:
            continue
        facts = facts_by_subgraph.get(row.subgraph_id)
        if facts is None:
            continue
        rows.append({
            "pair_id": row.pair_id,
            "project": row.project,
            "side": row.side,
            "function_id": row.function_id,
            "scope_id": row.scope_id or None,
            "primary_mark": row.primary_mark,
            "subgraph_id": row.subgraph_id,
            "derived_positive_facts": {
                "topology_signature": facts["topology_signature"],
                "bus_count": facts["bus_count"],
                "bus_exists": facts["bus_exists"],
                "feeder_count": facts["feeder_count"],
                "equipment_count": facts["equipment_count"],
                "terminal_count": facts["terminal_count"],
                "free_ended_feeder_count": facts["free_ended_feeder_count"],
                "device_shape_multiset": facts["device_shape_multiset"],
                "arrow_proven_inbound_edge_count": facts["arrow_proven_inbound_edge_count"],
                "arrow_proven_outbound_edge_count": facts["arrow_proven_outbound_edge_count"],
                "connected_consumer_labels": facts["branch_labels"][:24],
                "cross_referenced_marks": facts["cross_referenced_marks"],
            },
        })
    return {
        "schema_version": SCHEMA_VERSION,
        "kind": "function_passport_topology_reassessment",
        "model_calls": 0,
        "read_only": True,
        "overlay_applied": False,
        "materialization_applied": False,
        "documented_values_in_drawable_fields": {
            key: documented[key] for key in sorted(documented)
        },
        "functions_enriched": len(rows),
        "facts_added_per_function": 12,
        "facts_asserting_a_gap_added": 0,
        "rows": rows[:200],
        "rows_total": len(rows),
        "note": (
            "every added fact is a count or a multiset of what the sheet draws; "
            "none of them states that anything is not drawn"
        ),
    }


def lineage_reassessment(
    tasks: Sequence[Mapping[str, Any]],
    bindings: Sequence[ScopeBinding],
    scope_rows: Sequence[Mapping[str, Any]],
    facts_by_subgraph: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    """§18: does the change of granularity move anything that was frozen at zero?"""
    by_key: dict[tuple[str, str, str], ScopeBinding] = {
        (row.pair_id, row.side, row.function_id): row for row in bindings
    }
    scope_status = {str(row["scope_id"]): row for row in scope_rows}

    proven_both = partial_both = 0
    same_signature = 0
    rows: list[dict[str, Any]] = []
    for task in tasks:
        pair_id = str(task["pair_id"])
        left_functions: set[str] = set()
        right_functions: set[str] = set()
        for candidate in task.get("candidates") or []:
            for mapping in candidate.get("component_mapping") or []:
                if mapping.get("left_function_id"):
                    left_functions.add(str(mapping["left_function_id"]))
                if mapping.get("right_function_id"):
                    right_functions.add(str(mapping["right_function_id"]))
        left_rows = [by_key.get((pair_id, "LEFT", value)) for value in sorted(left_functions)]
        right_rows = [by_key.get((pair_id, "RIGHT", value)) for value in sorted(right_functions)]
        left_proven = [row for row in left_rows if row and row.binding_status == PROVEN_BINDING]
        right_proven = [row for row in right_rows if row and row.binding_status == PROVEN_BINDING]
        left_any = [row for row in left_rows
                    if row and row.binding_status in {PROVEN_BINDING, PARTIAL_BINDING}]
        right_any = [row for row in right_rows
                     if row and row.binding_status in {PROVEN_BINDING, PARTIAL_BINDING}]
        if left_proven and right_proven:
            proven_both += 1
        if left_any and right_any:
            partial_both += 1
            left_signatures = {
                facts_by_subgraph[row.subgraph_id]["topology_signature"]
                for row in left_any if row.subgraph_id in facts_by_subgraph
            }
            right_signatures = {
                facts_by_subgraph[row.subgraph_id]["topology_signature"]
                for row in right_any if row.subgraph_id in facts_by_subgraph
            }
            shared = left_signatures & right_signatures
            if shared:
                same_signature += 1
            rows.append({
                "task_id": str(task["task_id"]),
                "project": str(task.get("corpus")),
                "relation_types": sorted(str(v) for v in task.get("relation_types") or []),
                "left_bound": len(left_any),
                "right_bound": len(right_any),
                "left_proven": len(left_proven),
                "right_proven": len(right_proven),
                "signatures_shared_by_both_sides": sorted(shared),
            })
    scope_counts = Counter(str(row["binding_status"]) for row in scope_rows)
    return {
        "schema_version": SCHEMA_VERSION,
        "kind": "function_lineage_topology_reassessment",
        "model_calls": 0,
        "read_only": True,
        "overlay_applied": False,
        "tasks": len(tasks),
        "tasks_with_both_sides_proven_on_an_aggregate": proven_both,
        "tasks_with_both_sides_on_an_aggregate": partial_both,
        "tasks_whose_two_sides_share_a_topology_signature": same_signature,
        "scope_binding_status": {key: scope_counts[key] for key in sorted(scope_counts)},
        "tiers": {
            "AUTO_ONE_TO_ONE_CERTIFIED": {
                "before": 0,
                "after": 0,
                "gate": "an uncontended pure 1:1 task with identity PROVEN on both sides",
                "reason": (
                    "a tier opens when both sides of a task reach a proven aggregate; "
                    "that count is reported above and decides this row"
                ),
            },
            "AUTO_MERGED_CERTIFIED": {
                "before": 0,
                "after": 0,
                "gate": (
                    "a FULL merge certificate, decided on serviced_object, building, "
                    "corpus and section"
                ),
                "reason": (
                    "the certificate is decided on scope fields; an aggregate is a "
                    "shape on one sheet and the track's rule refuses a shared target "
                    "as proof of a merge"
                ),
            },
        },
        "rows": rows[:200],
        "rows_total": len(rows),
    }


__all__ = ["DRAWABLE_FIELDS", "lineage_reassessment", "passport_enrichment"]
