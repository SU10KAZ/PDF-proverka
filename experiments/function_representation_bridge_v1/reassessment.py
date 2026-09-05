"""Read-only reassessment of Function Lineage, and the §18 question about the other two corpora.

Nothing here writes, overlays or materializes.  The candidate generator is not
touched and no candidate is added, removed or reordered: assembly facts are laid
alongside the frozen artifacts as positive evidence, and the question is only
whether anything that was frozen at zero moves.

The tier question is asked honestly and is allowed to answer no.  Both certified
tiers rest on identity, and identity is ``PROVEN`` only through a primary mark
bound to a drawn thing; the merge certificate is decided on scope fields —
``serviced_object``, ``building``, ``corpus``, ``section`` — which an assembly
does not carry and which this track has no right to supply.

§18 has its own section because it is the design's real test.  ``IOS2.1`` and
``IOS3.1`` have almost no proven topology, so if an assembly can only be a
schematic, the whole idea is a schematic-only idea wearing a neutral name.  The
answer is measured per document: how many assemblies each corpus produces, on
which channel, and how many functions each channel reaches.
"""
from __future__ import annotations

from collections import Counter, defaultdict
from typing import Any, Mapping, Sequence

from experiments.function_lineage_v3 import corpus as frozen_corpus

from .contract import (
    AMBIGUOUS,
    AssemblyMembership,
    DRAWN_STROKE_GROUP,
    DRAWN_TABLE_LATTICE,
    FunctionalAssembly,
    PARTIAL,
    PROVEN,
    PROVEN_CONNECTED_COMPONENT,
    SCHEMA_VERSION,
    UNKNOWN,
)

#: Passport fields whose value a drawing or a table could in principle show.
DRAWABLE_FIELDS = ("upstream", "downstream", "equipment_roles", "consumers", "stable_entities")


def passport_enrichment(
    memberships: Sequence[AssemblyMembership],
    facts_by_assembly: Mapping[str, Mapping[str, Any]],
    assemblies_by_id: Mapping[str, FunctionalAssembly],
) -> dict[str, Any]:
    """The derived positive facts a joined membership adds to a passport."""
    documented: Counter = Counter()
    for pair_id in sorted(
        frozen_corpus.PROJECTS,
        key=lambda key: frozen_corpus.CORPUS_ORDER.index(frozen_corpus.PROJECTS[key]),
    ):
        passports = frozen_corpus.passports(pair_id)
        for side in frozen_corpus.SIDES:
            for _function_id, passport in sorted(passports[side].items()):
                for field in DRAWABLE_FIELDS:
                    raw = passport.get(field)
                    values = [raw] if isinstance(raw, str) else list(raw or [])
                    documented[field] += sum(1 for value in values if str(value).strip())

    rows: list[dict[str, Any]] = []
    per_representation: Counter = Counter()
    for row in memberships:
        if row.membership_status not in {PROVEN, PARTIAL} or not row.assembly_id:
            continue
        facts = facts_by_assembly.get(row.assembly_id)
        assembly = assemblies_by_id.get(row.assembly_id)
        if facts is None or assembly is None:
            continue
        per_representation[assembly.representation_type] += 1
        rows.append({
            "pair_id": row.pair_id,
            "project": row.project,
            "side": row.side,
            "function_id": row.function_id,
            "scope_id": row.scope_id,
            "membership_status": row.membership_status,
            "membership_channel": row.membership_channel,
            "assembly_id": row.assembly_id,
            "representation_type": assembly.representation_type,
            "assembly_kind": assembly.assembly_kind,
            "derived_positive_facts": {
                key: facts[key] for key in sorted(facts)
                if key not in {"folded_strings"}
            },
        })
    return {
        "schema_version": SCHEMA_VERSION,
        "kind": "function_passport_assembly_reassessment",
        "model_calls": 0,
        "read_only": True,
        "overlay_applied": False,
        "materialization_applied": False,
        "documented_values_in_drawable_fields": {
            key: documented[key] for key in sorted(documented)
        },
        "functions_enriched": len(rows),
        "functions_enriched_by_representation": {
            key: per_representation[key] for key in sorted(per_representation)
        },
        "facts_asserting_a_gap_added": 0,
        "rows": rows[:200],
        "rows_total": len(rows),
        "note": (
            "every added fact is a count, a designation or a printed quantity of what "
            "the sheet shows; none of them states that anything is not shown"
        ),
    }


def lineage_reassessment(
    tasks: Sequence[Mapping[str, Any]],
    memberships: Sequence[AssemblyMembership],
    scope_rows: Sequence[Mapping[str, Any]],
    signature_of_assembly: Mapping[str, Mapping[str, str]],
    coverage: Mapping[str, Any],
) -> dict[str, Any]:
    """§19: does representation-neutral evidence move anything frozen at zero?"""
    by_key: dict[tuple[str, str, str], AssemblyMembership] = {
        (row.pair_id, row.side, row.function_id): row for row in memberships
    }
    both = 0
    sharing_signature = 0
    rows: list[dict[str, Any]] = []
    for task in tasks:
        pair_id = str(task["pair_id"])
        left: set[str] = set()
        right: set[str] = set()
        for candidate in task.get("candidates") or []:
            for mapping in candidate.get("component_mapping") or []:
                if mapping.get("left_function_id"):
                    left.add(str(mapping["left_function_id"]))
                if mapping.get("right_function_id"):
                    right.add(str(mapping["right_function_id"]))
        left_rows = [
            by_key.get((pair_id, "LEFT", value)) for value in sorted(left)
        ]
        right_rows = [
            by_key.get((pair_id, "RIGHT", value)) for value in sorted(right)
        ]
        joined_left = [
            row for row in left_rows
            if row and row.membership_status in {PROVEN, PARTIAL} and row.assembly_id
        ]
        joined_right = [
            row for row in right_rows
            if row and row.membership_status in {PROVEN, PARTIAL} and row.assembly_id
        ]
        if not (joined_left and joined_right):
            continue
        both += 1
        left_signatures = {
            signature_of_assembly.get(row.assembly_id, {}).get("NAMES_ONLY")
            for row in joined_left
        }
        right_signatures = {
            signature_of_assembly.get(row.assembly_id, {}).get("NAMES_ONLY")
            for row in joined_right
        }
        shared = {value for value in left_signatures & right_signatures if value}
        if shared:
            sharing_signature += 1
        rows.append({
            "task_id": str(task["task_id"]),
            "project": str(task.get("corpus")),
            "relation_types": sorted(str(value) for value in task.get("relation_types") or []),
            "left_joined": len(joined_left),
            "right_joined": len(joined_right),
            "left_proven": sum(1 for row in joined_left if row.membership_status == PROVEN),
            "right_proven": sum(1 for row in joined_right if row.membership_status == PROVEN),
            "signatures_shared_by_both_sides": sorted(shared),
        })
    scope_counts = Counter(str(row["membership_status"]) for row in scope_rows)
    proven_both = sum(
        1 for row in rows if row["left_proven"] and row["right_proven"]
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "kind": "function_lineage_representation_bridge_reassessment",
        "model_calls": 0,
        "read_only": True,
        "overlay_applied": False,
        "candidate_generator_changed": False,
        "candidates_changed": 0,
        "tasks": len(tasks),
        "tasks_with_assembly_facts_on_both_sides": both,
        "tasks_with_a_proven_membership_on_both_sides": proven_both,
        "tasks_whose_two_sides_share_an_assembly_signature": sharing_signature,
        "coverage_before_this_track": {
            "BOTH_SIDES_ON_TOPOLOGY": 0,
            "source": "function_topology_v1 cross_representation_audit",
        },
        "coverage_after_this_track": dict(coverage.get("by_coverage_class", {})),
        "scope_membership_status": {key: scope_counts[key] for key in sorted(scope_counts)},
        "tiers": {
            "AUTO_ONE_TO_ONE_CERTIFIED": {
                "before": 0,
                "after": 0,
                "gate": "an uncontended pure 1:1 task with identity PROVEN on both sides",
                "reason": (
                    "identity is PROVEN only through a primary mark bound to a drawn "
                    "member; the count of tasks proven on both sides is reported above "
                    "and decides this row"
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
                    "the certificate is decided on scope fields an assembly does not "
                    "carry; a shared assembly is not a merge and this track does not "
                    "soften that rule"
                ),
            },
        },
        "rows": rows[:200],
        "rows_total": len(rows),
    }


def corpora_without_topology(
    assemblies: Sequence[FunctionalAssembly],
    memberships: Sequence[AssemblyMembership],
) -> dict[str, Any]:
    """§18: can an assembly be built where almost no topology exists?"""
    by_document: dict[str, Counter] = defaultdict(Counter)
    for assembly in assemblies:
        counter = by_document[assembly.document]
        counter["assemblies"] += 1
        counter[assembly.assembly_channel] += 1
        counter[f"representation_{assembly.representation_type}"] += 1
    for row in memberships:
        document = f"{row.project}/{row.side}"
        if row.membership_status in {PROVEN, PARTIAL}:
            by_document[document]["functions_joined"] += 1
        by_document[document]["functions"] += 1
    rows = [
        {"document": document, **{key: int(value) for key, value in sorted(counter.items())}}
        for document, counter in sorted(by_document.items())
    ]
    schematic_free = [
        row for row in rows
        if not row.get(PROVEN_CONNECTED_COMPONENT) and row.get("assemblies")
    ]
    return {
        "documents": rows,
        "documents_whose_assemblies_are_all_non_schematic": [
            row["document"] for row in schematic_free
        ],
        "reading": (
            "an assembly built from a ruled table or a stroke group where no conductor "
            "was ever proven is the direct answer to §18; a document that produces "
            "none is reported as the data gap it is"
        ),
    }


__all__ = [
    "DRAWABLE_FIELDS",
    "corpora_without_topology",
    "lineage_reassessment",
    "passport_enrichment",
]
