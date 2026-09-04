"""Read-only re-evaluation of the Function Passport and Function Lineage.

Nothing here writes, overlays or materializes anything.  The question is only:
*if* Function Lineage could read a proven topology, which of its facts would
find a home, and would either certified tier open.

Three regimes, side by side, because the honest comparison is not against V1's
published table but against V1's *rule* run on the same read of the same page:

* ``V1_REGION`` — V1's ownership channels, unchanged, applied to the same
  printed strings this package extracted.  A string is fragment-local when a
  drawn lattice, a closed box or a stroke along it says so.
* ``V2_TOPOLOGY`` — the string is bound to a node of the proven graph.  This is
  strictly harder: the owner must be a conductor the layer has proven, not any
  group of strokes.
* ``V2_WITH_ALIGNMENT`` — additionally counts what the co-extensive label
  column would reach.  It is reported to size the headroom and is never
  claimed: every edge behind this column carries ``NO_CLAIM``.

The relational fields are the reason the track exists.  ``upstream`` and
``downstream`` are printed literally once in 1 074 and sixteen times in 1 945;
no extraction rescues them because they are not printed.  A graph does not need
them printed — it needs the wire drawn.  So they are measured differently here:
not "is the documented value printed inside a region" but "does the node this
function is bound to have a proven neighbour at all".
"""
from __future__ import annotations

from collections import Counter, defaultdict
from typing import Any, Iterable, Mapping, Sequence

from experiments.function_lineage_v2 import instance_identity
from experiments.function_lineage_v3 import corpus as frozen_corpus
from experiments.pdf_evidence_v1.reassessment import (
    BOUND_FIELDS,
    RELATION_FIELDS,
    SCOPE_FIELDS,
    _certified_tier_entrants,
)
from experiments.pdf_evidence_v1.textnorm import comparable, normalize

from . import identity as identity_module
from .contract import BUS, EQUIPMENT, FEEDER, SCHEMA_VERSION, TERMINAL
from .pipeline import PageResult

V1_REGION = "V1_REGION"
V2_TOPOLOGY = "V2_TOPOLOGY"
V2_WITH_ALIGNMENT = "V2_WITH_ALIGNMENT"
REGIMES = (V1_REGION, V2_TOPOLOGY, V2_WITH_ALIGNMENT)

FRAGMENT_LOCAL = "FRAGMENT_LOCAL"
SHEET_SHARED = "SHEET_SHARED"
UNKNOWN_SCOPE = "UNKNOWN_SCOPE"
NOT_PLACED = "NOT_PLACED_BY_THIS_REGIME"
PLACEMENTS = (FRAGMENT_LOCAL, SHEET_SHARED, UNKNOWN_SCOPE, NOT_PLACED)


def _folded(result: PageResult) -> dict[str, list[tuple[str, str]]]:
    """Per regime, the folded text of every string the regime may count."""
    bound = {
        record.label_id: record.node_id
        for record in result.bindings if record.status == "BOUND" and record.node_id
    }
    aligned: dict[str, str] = {}
    nodes = result.topology.node_by_id()
    for edge in result.topology.edges:
        if edge.connection_claim != "NO_CLAIM":
            continue
        anchor = nodes.get(edge.from_node_id)
        if anchor is None:
            continue
        for reference in anchor.evidence_refs:
            if reference.startswith("label:"):
                aligned[reference.split(":", 1)[1]] = edge.to_node_id
    rows: dict[str, list[tuple[str, str]]] = {regime: [] for regime in REGIMES}
    for label in result.data.labels:
        label_id = str(label["label_id"])
        folded = normalize(str(label["text"]))
        ownership = result.v1_ownership.get(label_id) or {}
        if ownership.get("applicability") == FRAGMENT_LOCAL:
            rows[V1_REGION].append((folded, str(ownership.get("region_id") or "")))
        if label_id in bound:
            rows[V2_TOPOLOGY].append((folded, bound[label_id]))
            rows[V2_WITH_ALIGNMENT].append((folded, bound[label_id]))
        elif label_id in aligned:
            rows[V2_WITH_ALIGNMENT].append((folded, aligned[label_id]))
    return rows


def _values(passport: Mapping[str, Any], field: str) -> list[str]:
    raw = passport.get(field)
    values = [raw] if isinstance(raw, str) else list(raw or [])
    return [str(value) for value in values if str(value).strip()]


def field_placement(
    results: Mapping[tuple[str, str], list[PageResult]]
) -> dict[str, Any]:
    """Every documented passport value, placed under all three regimes."""
    per_regime: dict[str, dict[str, Counter]] = {
        regime: defaultdict(Counter) for regime in REGIMES
    }
    owners: dict[str, dict[str, set[str]]] = {
        regime: defaultdict(set) for regime in REGIMES
    }
    totals: Counter = Counter()
    for pair_id in sorted(frozen_corpus.PROJECTS, key=lambda key: frozen_corpus.CORPUS_ORDER.index(frozen_corpus.PROJECTS[key])):
        passports = frozen_corpus.passports(pair_id)
        for side in frozen_corpus.SIDES:
            pages = {result.page: result for result in results[(pair_id, side)]}
            folded_cache: dict[int, dict[str, list[tuple[str, str]]]] = {}
            for function_id, passport in sorted(passports[side].items()):
                page_number = int(passport["source_sheet"]["physical_page"])
                result = pages.get(page_number)
                if result is None:
                    continue
                if page_number not in folded_cache:
                    folded_cache[page_number] = _folded(result)
                rows = folded_cache[page_number]
                for field in BOUND_FIELDS:
                    for value in _values(passport, field):
                        totals[field] += 1
                        needle = comparable(value)
                        if not needle:
                            for regime in REGIMES:
                                per_regime[regime][field][UNKNOWN_SCOPE] += 1
                            continue
                        for regime in REGIMES:
                            hits = [owner for text, owner in rows[regime] if needle in text]
                            if not hits:
                                per_regime[regime][field][NOT_PLACED] += 1
                                continue
                            distinct = sorted(set(hits))
                            if len(distinct) == 1:
                                per_regime[regime][field][FRAGMENT_LOCAL] += 1
                                owners[regime][field].add(distinct[0])
                            else:
                                per_regime[regime][field][UNKNOWN_SCOPE] += 1
    return {
        "schema_version": SCHEMA_VERSION,
        "kind": "function_passport_reassessment",
        "model_calls": 0,
        "read_only": True,
        "regimes": [
            {
                "regime": regime,
                "fields": {
                    field: dict(sorted(per_regime[regime][field].items()))
                    for field in BOUND_FIELDS
                },
                "fragment_local_total": sum(
                    per_regime[regime][field].get(FRAGMENT_LOCAL, 0) for field in BOUND_FIELDS
                ),
                "distinct_owners": {
                    field: len(owners[regime][field]) for field in BOUND_FIELDS
                    if owners[regime][field]
                },
            }
            for regime in REGIMES
        ],
        "values_documented": dict(sorted(totals.items())),
    }


def node_marks(topology) -> dict[str, set[str]]:
    """Marks the sheet prints on each node, folded the way identity folds them.

    The passport's mark and the sheet's ink are normalized differently — the CAD
    font hands back ``ГPЩ1`` with a Latin ``P`` — so both sides go through the
    production extractor before they are compared.  Comparing the raw strings
    matches nothing at all, which is what the first run of this measurement
    reported.
    """
    out: dict[str, set[str]] = {}
    for node_id, texts in identity_module.bound_marks(topology).items():
        found: set[str] = set()
        for text in texts:
            for row in instance_identity.extract_marks(str(text)):
                found.add(str(row["mark"]))
        if found:
            out[node_id] = found
    return out


def relational_facts(
    results: Mapping[tuple[str, str], list[PageResult]]
) -> dict[str, Any]:
    """What the graph can say that no extraction could: who is wired to whom.

    The join is not one to one and pretending otherwise produced a zero.  A
    passport function is a *switchboard*; a node is one of its wires, and the
    board's mark is printed on every one of them.  So a function is joined to
    the **set** of nodes whose bound strings carry its own mark, and the facts
    are read off the set: which buses it sits on, which devices are in series
    with it, and which nodes outside the set it touches.

    A function whose mark is printed nowhere on a bound string gets nothing.
    """
    functions = 0
    joined_single = 0
    joined_set = 0
    rows: list[dict[str, Any]] = []
    size_histogram: Counter = Counter()
    for pair_id in sorted(frozen_corpus.PROJECTS, key=lambda key: frozen_corpus.CORPUS_ORDER.index(frozen_corpus.PROJECTS[key])):
        passports = frozen_corpus.passports(pair_id)
        for side in frozen_corpus.SIDES:
            pages = {result.page: result for result in results[(pair_id, side)]}
            cache: dict[int, tuple[dict[str, set[str]], dict[str, set[str]], dict[str, int], dict[str, str]]] = {}
            for function_id, passport in sorted(passports[side].items()):
                functions += 1
                page_number = int(passport["source_sheet"]["physical_page"])
                result = pages.get(page_number)
                if result is None:
                    continue
                if page_number not in cache:
                    adjacency = identity_module.electrical_adjacency(result.topology)
                    cache[page_number] = (
                        node_marks(result.topology),
                        adjacency,
                        identity_module.hops_to_bus(result.topology, adjacency),
                        {node.node_id: node.node_kind for node in result.topology.nodes},
                    )
                marks, adjacency, distance, kinds = cache[page_number]
                facts = instance_identity.function_instance_identity(passport)
                primary = facts["identity_facts"].get("primary_mark")
                if not primary:
                    continue
                hosts = sorted(
                    node_id for node_id, found in marks.items() if str(primary) in found
                )
                if not hosts:
                    continue
                size_histogram[len(hosts)] += 1
                if len(hosts) == 1:
                    joined_single += 1
                else:
                    joined_set += 1
                inside = set(hosts)
                outside = sorted({
                    neighbour for host in hosts
                    for neighbour in adjacency.get(host, ())
                    if neighbour not in inside
                })
                reachable = [distance.get(host) for host in hosts if distance.get(host) is not None]
                rows.append({
                    "corpus": frozen_corpus.PROJECTS[pair_id],
                    "side": side,
                    "function_id": str(function_id),
                    "physical_page": page_number,
                    "primary_mark": str(primary),
                    "nodes": hosts[:24],
                    "node_count": len(hosts),
                    "node_kinds": dict(sorted(Counter(kinds.get(host) for host in hosts).items())),
                    "connected_to_outside_the_function": [
                        {"node_id": node_id, "node_kind": kinds.get(node_id)}
                        for node_id in outside[:24]
                    ],
                    "outside_neighbour_count": len(outside),
                    "equipment_in_series": sum(
                        1 for node_id in outside if kinds.get(node_id) == EQUIPMENT),
                    "buses_touched": sorted({
                        node_id for node_id in outside if kinds.get(node_id) == BUS
                    })[:8],
                    "is_itself_a_bus": any(kinds.get(host) == BUS for host in hosts),
                    "shortest_hops_to_a_bus": min(reachable) if reachable else None,
                })
    return {
        "schema_version": SCHEMA_VERSION,
        "kind": "topology_relational_facts",
        "model_calls": 0,
        "read_only": True,
        "functions": functions,
        "functions_joined_to_a_node_set_by_their_own_printed_mark": joined_single + joined_set,
        "joined_to_exactly_one_node": joined_single,
        "joined_to_several_nodes": joined_set,
        "node_set_size_histogram": {
            str(key): value for key, value in sorted(size_histogram.items())
        },
        "functions_with_at_least_one_proven_neighbour": sum(
            1 for row in rows if row["connected_to_outside_the_function"]),
        "functions_reaching_a_bus": sum(
            1 for row in rows if row["shortest_hops_to_a_bus"] is not None),
        "functions_whose_own_wires_include_a_bus": sum(
            1 for row in rows if row["is_itself_a_bus"]),
        "functions_touching_a_bus_directly": sum(1 for row in rows if row["buses_touched"]),
        "functions_with_a_device_in_series": sum(
            1 for row in rows if row["equipment_in_series"]),
        "rule": (
            "a passport function is a switchboard and a node is one of its "
            "wires; the join is a set, and the board's own mark is what makes it"
        ),
        "rows": rows[:200],
        "rows_total": len(rows),
    }


def tier_reassessment(
    results: Mapping[tuple[str, str], list[PageResult]]
) -> dict[str, Any]:
    """Do either certified tier's entrants move?  Asked, not assumed."""
    feasibility = instance_identity.certified_tier_feasibility()
    functions = 0
    with_any_bound_mark = 0
    with_matching_primary_mark = 0
    proven_identity: dict[tuple[str, str], bool] = {}
    for pair_id in sorted(frozen_corpus.PROJECTS):
        passports = frozen_corpus.passports(pair_id)
        for side in frozen_corpus.SIDES:
            pages = {result.page: result for result in results[(pair_id, side)]}
            marks_by_page: dict[int, set[str]] = {}
            for function_id, passport in sorted(passports[side].items()):
                functions += 1
                page_number = int(passport["source_sheet"]["physical_page"])
                result = pages.get(page_number)
                if result is None:
                    continue
                if page_number not in marks_by_page:
                    found: set[str] = set()
                    for texts in identity_module.bound_marks(result.topology).values():
                        for text in texts:
                            for row in instance_identity.extract_marks(text):
                                found.add(str(row["mark"]))
                    marks_by_page[page_number] = found
                marks = marks_by_page[page_number]
                if marks:
                    with_any_bound_mark += 1
                facts = instance_identity.function_instance_identity(passport)
                primary = facts["identity_facts"].get("primary_mark")
                if primary and primary in marks:
                    with_matching_primary_mark += 1
                    proven_identity[(pair_id, str(function_id))] = True
    entrants = _certified_tier_entrants(proven_identity)
    return {
        "schema_version": SCHEMA_VERSION,
        "kind": "certified_tier_reassessment",
        "model_calls": 0,
        "read_only": True,
        "overlay_applied": False,
        "functions": functions,
        "functions_whose_page_binds_any_mark_to_a_node": with_any_bound_mark,
        "functions_whose_primary_mark_is_bound_to_a_node": with_matching_primary_mark,
        "feasibility": {
            key: (len(value) if isinstance(value, (list, dict)) else value)
            for key, value in feasibility.items()
        },
        "tiers": {
            "AUTO_ONE_TO_ONE_CERTIFIED": {
                "before": 0,
                "after": entrants["entrants"],
                "gate": "an uncontended pure 1:1 task with identity PROVEN on both sides",
                "detail": entrants,
            },
            "AUTO_MERGED_CERTIFIED": {
                "before": 0,
                "after": 0,
                "gate": (
                    "a FULL merge certificate, decided on serviced_object, "
                    "building, corpus and section"
                ),
                "detail": {
                    "reason": (
                        "the certificate is decided on the scope fields; a "
                        "convergence in the drawing is a shape and the track's "
                        "own rule refuses a shared target as proof of a merge"
                    ),
                },
            },
        },
    }


# ---------------------------------------------------------------------------
# merge, split, distribution
# ---------------------------------------------------------------------------

MERGED = "MERGED_N_TO_1"
SPLIT = "SPLIT_1_TO_N"
DISTRIBUTED = "FUNCTION_DISTRIBUTED"
CONTINUED = "CONTINUED_1_TO_1"


def _holdout_tasks() -> list[Mapping[str, Any]]:
    import json

    path = (
        frozen_corpus.COMPARISON_ROOT
        / "20260904_function_lineage_v2_6_holdout_evaluation"
        / "holdout_population.json"
    )
    if not path.is_file():
        return []
    return list(json.loads(path.read_text(encoding="utf-8"))["tasks"])


def _function_nodes(
    results: Mapping[tuple[str, str], list[PageResult]]
) -> dict[tuple[str, str], dict[str, Any]]:
    """Every function, joined to the set of nodes its own mark names."""
    out: dict[tuple[str, str], dict[str, Any]] = {}
    for pair_id in sorted(frozen_corpus.PROJECTS):
        passports = frozen_corpus.passports(pair_id)
        for side in frozen_corpus.SIDES:
            pages = {result.page: result for result in results[(pair_id, side)]}
            cache: dict[int, tuple[dict[str, set[str]], dict[str, set[str]]]] = {}
            for function_id, passport in sorted(passports[side].items()):
                page_number = int(passport["source_sheet"]["physical_page"])
                result = pages.get(page_number)
                if result is None:
                    continue
                if page_number not in cache:
                    cache[page_number] = (
                        node_marks(result.topology),
                        identity_module.electrical_adjacency(result.topology),
                    )
                marks, adjacency = cache[page_number]
                facts = instance_identity.function_instance_identity(passport)
                primary = facts["identity_facts"].get("primary_mark")
                if not primary:
                    continue
                hosts = sorted(
                    node_id for node_id, found in marks.items() if str(primary) in found
                )
                if not hosts:
                    continue
                inside = set(hosts)
                outside = {
                    neighbour for host in hosts
                    for neighbour in adjacency.get(host, ())
                    if neighbour not in inside
                }
                out[(pair_id, str(function_id))] = {
                    "side": side,
                    "nodes": hosts,
                    "physical_page": page_number,
                    "degree": len(outside),
                }
    return out


def merge_and_split(
    results: Mapping[tuple[str, str], list[PageResult]]
) -> dict[str, Any]:
    """Can a drawn convergence certify a merge?  Asked on the frozen tasks.

    A merge is a claim about two documents; a convergence is a shape on one
    sheet.  The two can only meet where both sides of a task are joined to a
    node by their own printed marks, so that is what is counted first.  A
    shared target still proves nothing — the check is whether the right-hand
    node's own branch actually fans in as many ways as the task has left-hand
    functions.
    """
    from experiments.pdf_evidence_v1.reassessment import _task_sides

    tasks = _holdout_tasks()
    nodes = _function_nodes(results)
    pairs = {project: pair_id for pair_id, project in frozen_corpus.PROJECTS.items()}
    buckets: dict[str, Counter] = defaultdict(Counter)
    examples: list[dict[str, Any]] = []
    for task in tasks:
        if task.get("sentinel"):
            continue
        pair_id = pairs.get(str(task["corpus"]))
        if pair_id is None:
            continue
        kind = "+".join(sorted(task["relation_types"]))
        buckets[kind]["tasks"] += 1
        left, right = _task_sides(task, pair_id)
        if not left or not right:
            continue
        left_nodes = [nodes.get((pair_id, function_id)) for function_id in left]
        right_nodes = [nodes.get((pair_id, function_id)) for function_id in right]
        buckets[kind]["left_functions"] += len(left)
        buckets[kind]["right_functions"] += len(right)
        buckets[kind]["left_functions_on_a_node"] += sum(1 for row in left_nodes if row)
        buckets[kind]["right_functions_on_a_node"] += sum(1 for row in right_nodes if row)
        if not all(left_nodes) or not all(right_nodes):
            continue
        buckets[kind]["tasks_with_every_side_on_a_node"] += 1
        if MERGED in task["relation_types"] and len(right_nodes) == 1:
            fan = int(right_nodes[0]["degree"])
            if fan >= len(left_nodes):
                buckets[kind]["right_branch_fans_in_at_least_as_far"] += 1
            if len(examples) < 8:
                examples.append({
                    "corpus": str(task["corpus"]),
                    "task_id": str(task["task_id"]),
                    "left_functions": len(left_nodes),
                    "right_node_degree": fan,
                })
    return {
        "schema_version": SCHEMA_VERSION,
        "kind": "topology_merge_and_split",
        "model_calls": 0,
        "read_only": True,
        "tasks_read": len(tasks),
        "by_relation": {kind: dict(sorted(row.items())) for kind, row in sorted(buckets.items())},
        "examples": examples,
        "rule": (
            "a shared target does not prove a merge; a convergence is a shape on "
            "one sheet and a merge is a claim about two"
        ),
    }


__all__ = [
    "CONTINUED", "DISTRIBUTED", "MERGED", "PLACEMENTS", "REGIMES", "SPLIT",
    "V1_REGION", "V2_TOPOLOGY", "V2_WITH_ALIGNMENT",
    "field_placement", "merge_and_split", "node_marks", "relational_facts",
    "tier_reassessment",
]
