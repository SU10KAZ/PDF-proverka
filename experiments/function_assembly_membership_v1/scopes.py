"""Lifting function certificates to FunctionScopes, and reading assemblies back.

§1B of the track: an assembly may carry several scopes, and nothing here
splits an assembly to make a scope look alone.  A scope certifies when every
component it requires certifies; whether those components landed on one drawn
container or on several is recorded as a *cause*, because the answer is a
finding about how the passport cut the sheet, not a defect of either side.
"""
from __future__ import annotations

from collections import Counter, defaultdict
from typing import Any, Mapping, Sequence

from .contract import (
    A_COMPONENT_IS_AMBIGUOUS,
    A_COMPONENT_IS_CONTRADICTORY,
    AMBIGUOUS,
    CERTIFIED,
    COMPONENTS_CERTIFIED_TO_ONE_CONTAINER,
    COMPONENTS_CERTIFIED_TO_SEVERAL_CONTAINERS,
    CONTRADICTORY,
    MULTI_SCOPE,
    MembershipCertificate,
    NO_CERTIFIED_SCOPE,
    NO_COMPONENT_JOINED,
    ONE_SCOPE,
    PARTIAL,
    SOME_COMPONENTS_UNCERTIFIED,
    UNKNOWN,
)


def lift_to_scopes(
    rows: Sequence[MembershipCertificate],
    components_of_scope: Mapping[str, Sequence[str]],
    component_of_function: Mapping[tuple[str, str], str],
) -> list[dict[str, Any]]:
    by_component: dict[str, list[MembershipCertificate]] = defaultdict(list)
    for row in rows:
        component = component_of_function.get((row.pair_id, row.function_id))
        if component:
            by_component[component].append(row)
    out: list[dict[str, Any]] = []
    for scope_id in sorted(components_of_scope):
        required = sorted(components_of_scope[scope_id])
        members = [row for component in required for row in by_component.get(component, [])]
        statuses = Counter(row.status for row in members)
        certified = [row for row in members if row.status == CERTIFIED]
        containers = sorted({item for row in certified for item in row.certified_assembly_ids})
        channels = sorted({row.channel for row in certified if row.channel})
        if not members:
            status, cause = UNKNOWN, NO_COMPONENT_JOINED
        elif statuses[CONTRADICTORY]:
            status, cause = CONTRADICTORY, A_COMPONENT_IS_CONTRADICTORY
        elif statuses[AMBIGUOUS]:
            status, cause = AMBIGUOUS, A_COMPONENT_IS_AMBIGUOUS
        elif statuses[CERTIFIED] == len(members):
            status = CERTIFIED
            cause = (
                COMPONENTS_CERTIFIED_TO_ONE_CONTAINER if len(containers) == 1
                else COMPONENTS_CERTIFIED_TO_SEVERAL_CONTAINERS
            )
        elif statuses[CERTIFIED] or statuses[PARTIAL]:
            status, cause = PARTIAL, SOME_COMPONENTS_UNCERTIFIED
        else:
            status, cause = UNKNOWN, NO_COMPONENT_JOINED
        out.append({
            "scope_id": scope_id,
            "required_component_ids": required,
            "component_status": {key: statuses[key] for key in sorted(statuses)},
            "status": status,
            "cause": cause,
            "assembly_ids": containers if status == CERTIFIED else [],
            "candidate_assembly_ids": sorted({
                item for row in members
                for item in (row.candidate_assembly_ids or (row.assembly_id,) if row.assembly_id else row.candidate_assembly_ids)
                if item
            }) if status != CERTIFIED else [],
            "container_count": len(containers) if status == CERTIFIED else 0,
            "channels": channels,
            "function_ids": sorted({row.function_id for row in members}),
            "sides": sorted({row.side for row in members}),
        })
    return out


def scope_census(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    statuses = Counter(str(row["status"]) for row in rows)
    causes = Counter(str(row["cause"]) for row in rows)
    containers = Counter(int(row["container_count"]) for row in rows if row["status"] == CERTIFIED)
    return {
        "scopes": len(rows),
        "by_status": {key: statuses[key] for key in sorted(statuses)},
        "by_cause": {key: causes[key] for key in sorted(causes)},
        "certified_scopes_by_container_count": {str(key): containers[key] for key in sorted(containers)},
    }


def assembly_composition(
    rows: Sequence[MembershipCertificate],
    assemblies: Sequence[Any],
) -> dict[str, Any]:
    """Assembly ← {Scope A, Scope B, …}: read from certified rows and never forced."""
    scopes_of: dict[str, set[str]] = defaultdict(set)
    functions_of: dict[str, set[str]] = defaultdict(set)
    classes_of: dict[str, set[str]] = defaultdict(set)
    for row in rows:
        if row.status != CERTIFIED:
            continue
        for assembly_id in row.certified_assembly_ids:
            functions_of[assembly_id].add(row.function_id)
            if row.scope_id:
                scopes_of[assembly_id].add(row.scope_id)
    composition: Counter = Counter()
    histogram: Counter = Counter()
    details: list[dict[str, Any]] = []
    by_id = {item.assembly_id: item for item in assemblies}
    for assembly_id in sorted(functions_of):
        functions = functions_of[assembly_id]
        scopes = scopes_of[assembly_id]
        kind = ONE_SCOPE if len(functions) == 1 else MULTI_SCOPE
        composition[kind] += 1
        histogram[len(functions)] += 1
        assembly = by_id.get(assembly_id)
        details.append({
            "assembly_id": assembly_id,
            "document": assembly.document if assembly else None,
            "physical_page": assembly.physical_page if assembly else None,
            "assembly_kind": assembly.assembly_kind if assembly else None,
            "representation_type": assembly.representation_type if assembly else None,
            "owner_designation": assembly.owner_designation if assembly else None,
            "composition": kind,
            "certified_function_ids": sorted(functions),
            "certified_scope_ids": sorted(scopes),
        })
    composition[NO_CERTIFIED_SCOPE] = max(len(assemblies) - len(functions_of), 0)
    return {
        "assemblies": len(assemblies),
        "assemblies_with_a_certified_function": len(functions_of),
        "by_composition": {key: composition[key] for key in sorted(composition)},
        "certified_functions_per_assembly": {str(key): histogram[key] for key in sorted(histogram)},
        "rows": details,
        "rule": (
            "several certified scopes on one drawn container are the passport's cut of one "
            "sheet; the container is never split to make a scope look alone"
        ),
    }


__all__ = ["assembly_composition", "lift_to_scopes", "scope_census"]
