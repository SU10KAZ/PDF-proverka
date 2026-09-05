"""Joining a FunctionScope to an assembly — and refusing to when nothing drawn joins them.

Three channels, tried in order of what they prove, and each with a ceiling the
contract enforces rather than this module's care.

``PROVEN_TOPOLOGY_OWNERSHIP``
    The frozen channel of ``function_topology_v1``: the scope's own printed mark
    runs along a member conductor.  It may prove.  It also reaches only nine
    functions of 313, and the reason is that track's finding #2 — a board's name
    is printed in the title and the stamp, not along a wire.

``DOCUMENTED_VALUE_IN_ONE_ASSEMBLY``
    A documented value of the passport is printed, literally, inside exactly one
    drawn container of the function's own page.  This is the channel that exists
    because of the *other* finding: 233 of 313 functions carry no primary mark at
    all, so a layer that can only follow marks is blind to two thirds of the
    corpus.  A value has to **distinguish** to be allowed to vote — the lesson
    the standard-mode retrieval already paid for, when ``Проверил`` from a title
    block declared a surname found.  Two conditions, and both are structural:
    the value must be long enough to be more than a word, and it must land in
    exactly one container.  A value printed in two containers votes for neither.
    The length is reported as a sensitivity curve rather than a tuned truth.

``SHEET_MARK_WITH_ONE_ASSEMBLY``
    The mark is printed on the sheet and the sheet draws exactly one assembly
    with a proven extent.  Uniqueness, not proximity, and capped at PARTIAL.

An unjoined scope asserts nothing.  It is a statement about this producer and
never about the installation, so every unjoined row carries a *mechanism*
instead of a verdict, and §7 forbids reading any of them as a disagreement with
the other version.
"""
from __future__ import annotations

from collections import Counter, defaultdict
from typing import Any, Iterable, Mapping, Sequence

from experiments.function_lineage_v2 import instance_identity as production_marks
from experiments.function_lineage_v3 import corpus as frozen_corpus
from experiments.pdf_evidence_v1.reassessment import BOUND_FIELDS
from experiments.pdf_evidence_v1.textnorm import MIN_COMPARABLE, normalize

from .contract import (
    AMBIGUOUS,
    AssemblyMembership,
    DOCUMENTED_VALUE_IN_ONE_ASSEMBLY,
    FunctionalAssembly,
    MARK_NOT_ON_A_CONDUCTOR,
    NO_ASSEMBLY_ON_THE_SHEET,
    NO_DOCUMENTED_VALUE_IS_PRINTED,
    NO_VECTOR_LAYER,
    PARTIAL,
    PRINTED_VALUES_LIE_OUTSIDE_EVERY_CONTAINER,
    PROVEN,
    PROVEN_TOPOLOGY_OWNERSHIP,
    SCOPE_HAS_NO_PRINTED_MARK,
    SEVERAL_ASSEMBLIES_CARRY_THE_EVIDENCE,
    SHEET_MARK_WITH_ONE_ASSEMBLY,
    UNKNOWN,
    stable_id,
)
from .representation import PageRepresentation

#: A documented value shorter than this may match a container by accident, so it
#: may not vote for one on its own.  Reported with a sensitivity curve, never
#: presented as a tuned single truth.
MIN_DISCRIMINATING_CHARS = 8
#: The lengths the audit walks when it publishes that curve.
SENSITIVITY_LENGTHS = (4, 6, 8, 10, 12, 16)


def documented_values(passport: Mapping[str, Any]) -> list[tuple[str, str]]:
    """The passport's own documented values, in the frozen field order of V1."""
    out: list[tuple[str, str]] = []
    for field in BOUND_FIELDS:
        raw = passport.get(field)
        if raw is None:
            continue
        values = [raw] if isinstance(raw, str) else list(raw)
        for value in values:
            text = str(value).strip()
            if text:
                out.append((field, text))
    return out


def primary_mark_of(passport: Mapping[str, Any]) -> str | None:
    facts = production_marks.function_instance_identity(passport)
    value = facts["identity_facts"].get("primary_mark")
    return str(value) if value else None


def _assembly_of_label(assemblies: Sequence[FunctionalAssembly]) -> dict[str, str]:
    out: dict[str, str] = {}
    for assembly in assemblies:
        for label_id in assembly.member_label_ids:
            out[label_id] = assembly.assembly_id
    return out


def value_votes(
    passport: Mapping[str, Any],
    page: PageRepresentation,
    assembly_of_label: Mapping[str, str],
    *,
    minimum_chars: int = MIN_DISCRIMINATING_CHARS,
) -> tuple[dict[str, set[str]], Counter]:
    """Which assembly each documented value points at, and why the rest do not."""
    folded = page.folded_labels()
    votes: dict[str, set[str]] = defaultdict(set)
    outcome: Counter = Counter()
    for _field, value in documented_values(passport):
        needle = normalize(value)
        if len(needle) < MIN_COMPARABLE:
            outcome["too_short_to_compare"] += 1
            continue
        containers: set[str] = set()
        printed = False
        for text, label_id in folded:
            if needle == text or (len(needle) >= minimum_chars and needle and needle in text):
                printed = True
                owner = assembly_of_label.get(label_id)
                if owner:
                    containers.add(owner)
        if not printed:
            outcome["not_printed_on_the_sheet"] += 1
        elif not containers:
            outcome["printed_outside_every_container"] += 1
        elif len(containers) > 1:
            outcome["printed_in_several_containers"] += 1
        else:
            outcome["votes_for_one_assembly"] += 1
            votes[next(iter(containers))].add(value)
    return votes, outcome


def bind_function(
    *,
    pair_id: str,
    project: str,
    side: str,
    function_id: str,
    scope_id: str | None,
    fragment_id: str | None,
    passport: Mapping[str, Any],
    page: PageRepresentation | None,
    assemblies: Sequence[FunctionalAssembly],
    minimum_chars: int = MIN_DISCRIMINATING_CHARS,
) -> AssemblyMembership:
    """One function against one page's assemblies."""
    physical_page = int(passport["source_sheet"]["physical_page"])
    primary = primary_mark_of(passport)
    base = dict(
        membership_id=stable_id("fmem", {
            "pair_id": pair_id, "side": side, "function_id": function_id,
            "scope_id": scope_id, "fragment_id": fragment_id,
        }),
        pair_id=pair_id, project=project, side=side, function_id=function_id,
        scope_id=scope_id, fragment_id=fragment_id, physical_page=physical_page,
        primary_mark=primary,
    )
    if page is None:
        return AssemblyMembership(**base, membership_status=UNKNOWN, cause=NO_VECTOR_LAYER)
    if not assemblies:
        return AssemblyMembership(
            **base, membership_status=UNKNOWN, cause=NO_ASSEMBLY_ON_THE_SHEET)

    # 1. the frozen topology channel — the only one that may prove
    if primary:
        carrying = sorted(
            assembly.assembly_id for assembly in assemblies
            if assembly.topology_subgraph_ids and primary in set(assembly.named_designations)
            and _mark_bound_to_members(primary, assembly, page)
        )
        if len(carrying) == 1:
            assembly = next(item for item in assemblies if item.assembly_id == carrying[0])
            nodes = [
                node_id for node_id in page.aggregation.nodes_of_mark.get(primary, ())
                if node_id in set(assembly.member_node_ids)
            ] if page.aggregation else []
            return AssemblyMembership(
                **base, membership_status=PROVEN,
                membership_channel=PROVEN_TOPOLOGY_OWNERSHIP, cause=UNKNOWN,
                assembly_id=assembly.assembly_id, candidate_assembly_ids=tuple(carrying),
                evidence_refs=tuple(f"node:{node_id}" for node_id in nodes[:24]),
                notes=(f"member_nodes_named={len(nodes)}",),
            )
        if len(carrying) > 1:
            return AssemblyMembership(
                **base, membership_status=AMBIGUOUS,
                membership_channel=PROVEN_TOPOLOGY_OWNERSHIP,
                cause=SEVERAL_ASSEMBLIES_CARRY_THE_EVIDENCE,
                candidate_assembly_ids=tuple(carrying),
            )

    # 2. a documented value printed inside exactly one drawn container
    votes, outcome = value_votes(
        passport, page, _assembly_of_label(assemblies), minimum_chars=minimum_chars)
    if len(votes) == 1:
        assembly_id = next(iter(votes))
        return AssemblyMembership(
            **base, membership_status=PARTIAL,
            membership_channel=DOCUMENTED_VALUE_IN_ONE_ASSEMBLY,
            cause=UNKNOWN if primary else SCOPE_HAS_NO_PRINTED_MARK,
            assembly_id=assembly_id, candidate_assembly_ids=(assembly_id,),
            evidence_refs=tuple(f"value:{value}" for value in sorted(votes[assembly_id])[:24]),
            notes=(f"documented_values_pointing_here={len(votes[assembly_id])}",),
        )
    if len(votes) > 1:
        return AssemblyMembership(
            **base, membership_status=AMBIGUOUS,
            membership_channel=DOCUMENTED_VALUE_IN_ONE_ASSEMBLY,
            cause=SEVERAL_ASSEMBLIES_CARRY_THE_EVIDENCE,
            candidate_assembly_ids=tuple(sorted(votes)),
        )

    # 3. the sheet-scoped mark, with exactly one proven assembly on the sheet
    proven = sorted(
        assembly.assembly_id for assembly in assemblies
        if assembly.membership_status == PROVEN
    )
    if primary and primary in set(page.sheet_marks):
        if len(proven) == 1:
            return AssemblyMembership(
                **base, membership_status=PARTIAL,
                membership_channel=SHEET_MARK_WITH_ONE_ASSEMBLY,
                cause=MARK_NOT_ON_A_CONDUCTOR, assembly_id=proven[0],
                candidate_assembly_ids=tuple(proven),
                evidence_refs=(f"sheet_mark:{primary}",),
                notes=("the mark is printed on the sheet and the sheet draws exactly one "
                       "assembly with a proven extent",),
            )
        if len(proven) > 1:
            return AssemblyMembership(
                **base, membership_status=AMBIGUOUS,
                membership_channel=SHEET_MARK_WITH_ONE_ASSEMBLY,
                cause=SEVERAL_ASSEMBLIES_CARRY_THE_EVIDENCE,
                candidate_assembly_ids=tuple(proven),
            )
    # The first question that stopped this function, in the order they were asked.
    if not primary:
        cause = SCOPE_HAS_NO_PRINTED_MARK
    elif outcome["printed_outside_every_container"]:
        cause = PRINTED_VALUES_LIE_OUTSIDE_EVERY_CONTAINER
    elif primary in set(page.sheet_marks):
        cause = NO_ASSEMBLY_ON_THE_SHEET
    else:
        cause = NO_DOCUMENTED_VALUE_IS_PRINTED
    return AssemblyMembership(
        **base, membership_status=UNKNOWN, cause=cause,
        notes=tuple(f"{key}={value}" for key, value in sorted(outcome.items())),
    )


def _mark_bound_to_members(
    mark: str, assembly: FunctionalAssembly, page: PageRepresentation
) -> bool:
    if page.aggregation is None:
        return False
    nodes = set(page.aggregation.nodes_of_mark.get(mark, ()))
    return bool(nodes & set(assembly.member_node_ids))


def bind_corpus(
    pages: Mapping[tuple[str, str], dict[int, PageRepresentation]],
    assemblies: Mapping[tuple[str, str], dict[int, list[FunctionalAssembly]]],
    scope_of_function: Mapping[tuple[str, str], str],
    fragment_of_function: Mapping[tuple[str, str], str],
    *,
    minimum_chars: int = MIN_DISCRIMINATING_CHARS,
) -> list[AssemblyMembership]:
    """Every function of every frozen document, joined or reasoned about."""
    rows: list[AssemblyMembership] = []
    for pair_id in sorted(
        frozen_corpus.PROJECTS,
        key=lambda key: frozen_corpus.CORPUS_ORDER.index(frozen_corpus.PROJECTS[key]),
    ):
        project = frozen_corpus.PROJECTS[pair_id]
        passports = frozen_corpus.passports(pair_id)
        for side in frozen_corpus.SIDES:
            page_map = pages[(pair_id, side)]
            assembly_map = assemblies[(pair_id, side)]
            for function_id, passport in sorted(passports[side].items()):
                physical_page = int(passport["source_sheet"]["physical_page"])
                rows.append(bind_function(
                    pair_id=pair_id, project=project, side=side,
                    function_id=str(function_id),
                    scope_id=scope_of_function.get((pair_id, str(function_id))),
                    fragment_id=fragment_of_function.get((pair_id, str(function_id))),
                    passport=passport,
                    page=page_map.get(physical_page),
                    assemblies=assembly_map.get(physical_page, []),
                    minimum_chars=minimum_chars,
                ))
    return rows


def census(rows: Sequence[AssemblyMembership]) -> dict[str, Any]:
    statuses = Counter(row.membership_status for row in rows)
    channels = Counter(row.membership_channel for row in rows if row.membership_channel)
    causes = Counter(row.cause for row in rows if row.membership_status not in {PROVEN, PARTIAL})
    per_document: dict[str, Counter] = defaultdict(Counter)
    for row in rows:
        per_document[f"{row.project}/{row.side}"][row.membership_status] += 1
    return {
        "functions": len(rows),
        "by_status": {key: statuses[key] for key in sorted(statuses)},
        "by_channel": {key: channels[key] for key in sorted(channels)},
        "by_cause": {key: causes[key] for key in sorted(causes)},
        "by_document": {
            key: {status: value[status] for status in sorted(value)}
            for key, value in sorted(per_document.items())
        },
        "joined": statuses[PROVEN] + statuses[PARTIAL],
        "reading": (
            "a function this layer did not join is a function it says nothing about; "
            "§7 forbids reading it as a disagreement with the other version"
        ),
    }


def sensitivity(
    pages: Mapping[tuple[str, str], dict[int, PageRepresentation]],
    assemblies: Mapping[tuple[str, str], dict[int, list[FunctionalAssembly]]],
    scope_of_function: Mapping[tuple[str, str], str],
    fragment_of_function: Mapping[tuple[str, str], str],
    lengths: Sequence[int] = SENSITIVITY_LENGTHS,
) -> dict[str, Any]:
    """The discriminating-length curve, published instead of a tuned number."""
    rows: list[dict[str, Any]] = []
    for length in lengths:
        bound = bind_corpus(
            pages, assemblies, scope_of_function, fragment_of_function,
            minimum_chars=length,
        )
        statuses = Counter(row.membership_status for row in bound)
        rows.append({
            "minimum_discriminating_chars": int(length),
            "PROVEN": statuses[PROVEN],
            "PARTIAL": statuses[PARTIAL],
            "AMBIGUOUS": statuses[AMBIGUOUS],
            "UNKNOWN": statuses[UNKNOWN],
        })
    return {
        "operating_point": MIN_DISCRIMINATING_CHARS,
        "curve": rows,
        "rule": (
            "a value must distinguish before it may vote; the length is reported "
            "as a curve because a single tuned number would look like a fact"
        ),
    }


def lift_to_scopes(
    rows: Sequence[AssemblyMembership],
    components_of_scope: Mapping[str, Sequence[str]],
    component_of_function: Mapping[tuple[str, str], str],
) -> list[dict[str, Any]]:
    """Lift function memberships to the FunctionScope the lineage layer uses.

    A scope joins when every component it requires joins, and all of them land on
    the same assembly.  A scope spread across two assemblies is not one assembly,
    and saying so is the point.
    """
    by_component: dict[str, list[AssemblyMembership]] = defaultdict(list)
    for row in rows:
        component = component_of_function.get((row.pair_id, row.function_id))
        if component:
            by_component[component].append(row)
    out: list[dict[str, Any]] = []
    for scope_id in sorted(components_of_scope):
        required = sorted(components_of_scope[scope_id])
        members = [row for component in required for row in by_component.get(component, [])]
        chosen = sorted({row.assembly_id for row in members if row.assembly_id})
        statuses = Counter(row.membership_status for row in members)
        causes = Counter(row.cause for row in members if row.membership_status != PROVEN)
        if members and statuses[PROVEN] == len(members) and len(chosen) == 1:
            status, channel, assembly = PROVEN, PROVEN_TOPOLOGY_OWNERSHIP, chosen[0]
        elif len(chosen) > 1:
            status, channel, assembly = AMBIGUOUS, None, None
        elif len(chosen) == 1:
            status, channel, assembly = PARTIAL, DOCUMENTED_VALUE_IN_ONE_ASSEMBLY, chosen[0]
        else:
            status, channel, assembly = UNKNOWN, None, None
        out.append({
            "scope_id": scope_id,
            "required_component_ids": required,
            "component_membership_status": {key: statuses[key] for key in sorted(statuses)},
            "membership_status": status,
            "membership_channel": channel,
            "assembly_id": assembly,
            "candidate_assembly_ids": chosen if assembly is None else [],
            "cause": _dominant(causes) if status != PROVEN else UNKNOWN,
            "function_ids": sorted({row.function_id for row in members}),
            "sides": sorted({row.side for row in members}),
        })
    return out


def _dominant(causes: Counter) -> str:
    if not causes:
        return UNKNOWN
    top = max(causes.values())
    return sorted(key for key, value in causes.items() if value == top)[0]


__all__ = [
    "MIN_DISCRIMINATING_CHARS",
    "SENSITIVITY_LENGTHS",
    "bind_corpus",
    "bind_function",
    "census",
    "documented_values",
    "lift_to_scopes",
    "primary_mark_of",
    "sensitivity",
    "value_votes",
]
