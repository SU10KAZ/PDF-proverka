"""Binding a FunctionScope to a drawn aggregate — and refusing to when it cannot.

The measurement of V2 that this module answers: on the control sheet, the board's
own name ``ГРЩ1`` runs along thirty feeders, and V2 bound it to thirty *nodes*.
Thirty nodes are not thirty functions; they are one board.  The subgraph is that
board, so the same evidence that produced a useless one-to-thirty join produces a
one-to-one join here — and it does so without a single new rule about labels.

The measurement that shapes the second channel is less comfortable.  Across
IOS1.1 every function whose page carries topology has its primary mark **printed
on that page** — and on the left side, none of those marks runs along a
conductor.  The board's name is printed in the *title* and in the *stamp*: it
names the whole drawing, not a wire.  That is a real, usable fact and it is not a
proof of ownership, so it gets its own channel and its own ceiling:

* ``MARK_BOUND_TO_MEMBER_NODE``            → may reach ``PROVEN_BINDING``
* ``SHEET_MARK_WITH_ONE_PROVEN_SUBGRAPH``  → may never exceed ``PARTIAL_BINDING``

The ceiling is enforced by the contract, not by care.  And an unbound scope
asserts nothing: §12 forbids reading it as a contradiction, so every unbound row
carries a *mechanism* instead of a verdict.
"""
from __future__ import annotations

from collections import Counter, defaultdict
from typing import Any, Iterable, Mapping, Sequence

from experiments.function_lineage_v2 import instance_identity as production_marks
from experiments.function_lineage_v3 import corpus as frozen_corpus
from experiments.pdf_evidence_v2.contract import LABEL_ANCHOR

from .aggregation import PageAggregation
from .contract import (
    AMBIGUOUS_BINDING,
    FUNCTION_GRANULARITY_MISMATCH,
    MARK_BOUND_TO_MEMBER_NODE,
    MARK_NOT_ON_A_CONDUCTOR,
    NO_BINDING,
    NO_SCHEMA_PAGE,
    NO_VECTOR_LAYER,
    PARTIAL_BINDING,
    PROVEN,
    PROVEN_BINDING,
    SCOPE_HAS_NO_PRINTED_MARK,
    SEVERAL_SUBGRAPHS_CARRY_THE_MARK,
    SHEET_MARK_WITH_ONE_PROVEN_SUBGRAPH,
    NO_PROVEN_AGGREGATE_ON_THE_SHEET,
    ScopeBinding,
    TEXT_REPRESENTATION_ONLY,
    UNKNOWN,
    stable_id,
)


def primary_mark_of(passport: Mapping[str, Any]) -> str | None:
    """The passport's own primary mark, through the production extractor."""
    facts = production_marks.function_instance_identity(passport)
    value = facts["identity_facts"].get("primary_mark")
    return str(value) if value else None


def bind_function(
    *,
    pair_id: str,
    project: str,
    side: str,
    function_id: str,
    fragment_id: str | None,
    scope_id: str | None,
    passport: Mapping[str, Any],
    page: PageAggregation | None,
    page_has_strokes: bool,
) -> ScopeBinding:
    """One function against one page's aggregates."""
    physical_page = int(passport["source_sheet"]["physical_page"])
    primary = primary_mark_of(passport)
    identity = {
        "pair_id": pair_id, "side": side, "function_id": function_id,
        "fragment_id": fragment_id, "scope_id": scope_id,
    }
    binding_id = stable_id("ftbind", identity)
    base = dict(
        binding_id=binding_id, pair_id=pair_id, project=project, side=side,
        scope_id=scope_id or "", function_id=function_id, fragment_id=fragment_id,
        physical_page=physical_page, primary_mark=primary,
    )

    if primary is None:
        return ScopeBinding(
            **base, binding_status=UNKNOWN, cause=SCOPE_HAS_NO_PRINTED_MARK,
            notes=("the passport carries no primary mark to look for",),
        )
    if not page_has_strokes:
        return ScopeBinding(**base, binding_status=NO_BINDING, cause=NO_VECTOR_LAYER)
    if page is None or not page.subgraphs:
        return ScopeBinding(**base, binding_status=NO_BINDING, cause=NO_SCHEMA_PAGE)

    carrying = sorted(
        subgraph.subgraph_id for subgraph in page.subgraphs
        if primary in set(subgraph.function_marks)
    )
    if len(carrying) == 1:
        subgraph = next(s for s in page.subgraphs if s.subgraph_id == carrying[0])
        evidence = tuple(
            node_id for node_id in page.nodes_of_mark.get(primary, ())
            if node_id in set(subgraph.member_node_ids)
        )
        return ScopeBinding(
            **base, binding_status=PROVEN_BINDING,
            binding_channel=MARK_BOUND_TO_MEMBER_NODE, cause=UNKNOWN,
            subgraph_id=subgraph.subgraph_id, candidate_subgraph_ids=tuple(carrying),
            evidence_refs=tuple(f"node:{node_id}" for node_id in evidence[:24]),
            notes=(f"member_nodes_named={len(evidence)}",),
        )
    if len(carrying) > 1:
        return ScopeBinding(
            **base, binding_status=AMBIGUOUS_BINDING,
            binding_channel=MARK_BOUND_TO_MEMBER_NODE,
            cause=SEVERAL_SUBGRAPHS_CARRY_THE_MARK,
            candidate_subgraph_ids=tuple(carrying),
        )

    proven = sorted(
        subgraph.subgraph_id for subgraph in page.subgraphs
        if subgraph.boundary_status == PROVEN
    )
    if primary in page.sheet_marks:
        if len(proven) == 1:
            return ScopeBinding(
                **base, binding_status=PARTIAL_BINDING,
                binding_channel=SHEET_MARK_WITH_ONE_PROVEN_SUBGRAPH,
                cause=MARK_NOT_ON_A_CONDUCTOR, subgraph_id=proven[0],
                candidate_subgraph_ids=tuple(proven),
                evidence_refs=(f"sheet_mark:{primary}",),
                notes=(
                    "the mark is printed on the sheet and the sheet draws exactly "
                    "one aggregate with a proven extent",
                ),
            )
        if len(proven) > 1:
            return ScopeBinding(
                **base, binding_status=AMBIGUOUS_BINDING,
                binding_channel=SHEET_MARK_WITH_ONE_PROVEN_SUBGRAPH,
                cause=SEVERAL_SUBGRAPHS_CARRY_THE_MARK,
                candidate_subgraph_ids=tuple(proven),
            )
        return ScopeBinding(
            **base, binding_status=NO_BINDING, cause=NO_PROVEN_AGGREGATE_ON_THE_SHEET,
            notes=("the mark is printed, and no aggregate of the sheet has a proven extent",),
        )
    return ScopeBinding(
        **base, binding_status=NO_BINDING, cause=TEXT_REPRESENTATION_ONLY,
        notes=("the mark is documented and is not among the strings this sheet prints",),
    )


def bind_corpus(
    results: Mapping[tuple[str, str], list[Any]],
    aggregations: Mapping[tuple[str, str], dict[int, PageAggregation]],
    scope_of_function: Mapping[tuple[str, str], str],
    fragment_of_function: Mapping[tuple[str, str], str],
) -> list[ScopeBinding]:
    """Every function of every frozen document, bound or reasoned about."""
    rows: list[ScopeBinding] = []
    for pair_id in sorted(
        frozen_corpus.PROJECTS,
        key=lambda key: frozen_corpus.CORPUS_ORDER.index(frozen_corpus.PROJECTS[key]),
    ):
        project = frozen_corpus.PROJECTS[pair_id]
        passports = frozen_corpus.passports(pair_id)
        for side in frozen_corpus.SIDES:
            pages = {result.page: result for result in results[(pair_id, side)]}
            aggregated = aggregations[(pair_id, side)]
            for function_id, passport in sorted(passports[side].items()):
                physical_page = int(passport["source_sheet"]["physical_page"])
                result = pages.get(physical_page)
                rows.append(bind_function(
                    pair_id=pair_id, project=project, side=side,
                    function_id=str(function_id),
                    fragment_id=fragment_of_function.get((pair_id, str(function_id))),
                    scope_id=scope_of_function.get((pair_id, str(function_id))),
                    passport=passport,
                    page=aggregated.get(physical_page),
                    page_has_strokes=bool(result is not None and len(result.data.strokes.edges)),
                ))
    return rows


def granularity_notes(rows: Sequence[ScopeBinding]) -> dict[str, Any]:
    """§8, measured: how many passport functions land on one drawn aggregate.

    This is not a lineage merge and the artifact says so in its own vocabulary.
    It is the internal structure of one function on one sheet — N branches to one
    board — and the histogram is the direct answer to the track's central task.
    """
    functions_of_subgraph: dict[str, set[str]] = defaultdict(set)
    for row in rows:
        if row.subgraph_id and row.binding_status in {PROVEN_BINDING, PARTIAL_BINDING}:
            functions_of_subgraph[row.subgraph_id].add(f"{row.pair_id}:{row.side}:{row.function_id}")
    histogram = Counter(len(value) for value in functions_of_subgraph.values())
    return {
        "bound_subgraphs": len(functions_of_subgraph),
        "functions_per_bound_subgraph": {
            str(key): histogram[key] for key in sorted(histogram)
        },
        "subgraphs_carrying_several_functions": sum(
            1 for value in functions_of_subgraph.values() if len(value) > 1),
        "distinction": (
            "several passport functions on one drawn aggregate is the internal "
            "structure of one sheet; a cross-version merge is a claim about two "
            "documents and is decided elsewhere"
        ),
    }


def aggregate_to_scopes(
    rows: Sequence[ScopeBinding],
    components_of_scope: Mapping[str, Sequence[str]],
    component_of_function: Mapping[tuple[str, str], str],
) -> list[dict[str, Any]]:
    """Lift function bindings to the FunctionScope the lineage layer actually uses.

    A scope requires a set of components.  It binds when *every* component it
    requires binds, and all of them land on the same drawn aggregate: a scope that
    spreads across two boards is not one board, and saying so is the point.
    """
    by_component: dict[str, list[ScopeBinding]] = defaultdict(list)
    for row in rows:
        component = component_of_function.get((row.pair_id, row.function_id))
        if component:
            by_component[component].append(row)
    out: list[dict[str, Any]] = []
    for scope_id in sorted(components_of_scope):
        required = sorted(components_of_scope[scope_id])
        members = [row for component in required for row in by_component.get(component, [])]
        subgraphs = sorted({row.subgraph_id for row in members if row.subgraph_id})
        statuses = Counter(row.binding_status for row in members)
        causes = Counter(row.cause for row in members if row.binding_status != PROVEN_BINDING)
        if members and statuses[PROVEN_BINDING] == len(members) and len(subgraphs) == 1:
            status, channel = PROVEN_BINDING, MARK_BOUND_TO_MEMBER_NODE
            chosen = subgraphs[0]
        elif len(subgraphs) > 1:
            status, channel, chosen = AMBIGUOUS_BINDING, None, None
        elif len(subgraphs) == 1 and members and statuses[PROVEN_BINDING] + statuses[PARTIAL_BINDING] == len(members):
            status, channel, chosen = PARTIAL_BINDING, SHEET_MARK_WITH_ONE_PROVEN_SUBGRAPH, subgraphs[0]
        elif len(subgraphs) == 1:
            status, channel, chosen = PARTIAL_BINDING, SHEET_MARK_WITH_ONE_PROVEN_SUBGRAPH, subgraphs[0]
        elif members and statuses[UNKNOWN] == len(members):
            status, channel, chosen = UNKNOWN, None, None
        else:
            status, channel, chosen = NO_BINDING, None, None
        out.append({
            "scope_id": scope_id,
            "required_component_ids": required,
            "component_binding_status": {key: statuses[key] for key in sorted(statuses)},
            "binding_status": status,
            "binding_channel": channel,
            "subgraph_id": chosen,
            "candidate_subgraph_ids": subgraphs if chosen is None else [],
            "cause": (_dominant_cause(causes) if status != PROVEN_BINDING else UNKNOWN),
            "function_ids": sorted({row.function_id for row in members}),
            "sides": sorted({row.side for row in members}),
        })
    return out


def _dominant_cause(causes: Counter) -> str:
    """The mechanism that stopped the most of a scope's components, ties by name."""
    if not causes:
        return UNKNOWN
    top = max(causes.values())
    return sorted(key for key, value in causes.items() if value == top)[0]


__all__ = [
    "aggregate_to_scopes", "bind_corpus", "bind_function", "granularity_notes",
    "primary_mark_of",
]
