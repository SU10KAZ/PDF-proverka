"""Identity of an aggregate, from structure and never from where it was printed.

A signature has one job here: survive a re-drawing.  §10 of the track lists what
it must survive — new coordinates, a different feeder order, a new page layout,
the whole function moved to another sheet — so nothing geometric and nothing
positional may enter it.  The physical page does not enter it.  Raw node
identifiers do not enter it: they are addresses minted from a position on a
page, so a signature built on them would be a page identity wearing a different
name.

The signature is built in four nested tiers, because the track asks two
different questions and one number cannot answer both.  §10 asks whether an
identity survives a re-layout: the more text a tier carries, the more fragile it
is to a revision that edits a cable size.  §16 asks whether an identity
separates two functions of the same class: the less text a tier carries, the
more identical twins it produces.  Both are measured, per tier, and the tension
between them is the finding rather than a footnote.

* ``SHAPE_ONLY``           — counts by kind, the degree histogram, the bus degrees
                             and how deep the branches run.  Pure graph.
* ``SHAPE_AND_DEVICES``    — plus the multiset of member device shapes.
* ``SHAPE_AND_NAMES``      — plus the marks that own the aggregate.
* ``SHAPE_AND_CONSUMERS``  — plus the folded strings bound to its members.
"""
from __future__ import annotations

import hashlib
import json
from collections import Counter, defaultdict, deque
from typing import Any, Iterable, Mapping, Sequence

from experiments.pdf_evidence_v1.textnorm import normalize
from experiments.pdf_evidence_v2.contract import BUS, EQUIPMENT, FEEDER, LABEL_ANCHOR

from .aggregation import PageAggregation
from .contract import (
    FunctionTopologySubgraph,
    SHAPE_AND_CONSUMERS,
    SHAPE_AND_DEVICES,
    SHAPE_AND_NAMES,
    SHAPE_ONLY,
    SIGNATURE_TIERS,
)

#: How deep the branch-depth histogram looks before it stops mattering.
DEPTH_LIMIT = 12


def _digest(payload: Any) -> str:
    blob = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return "fsig_" + hashlib.sha256(blob.encode("utf-8")).hexdigest()[:20]


def _histogram(values: Iterable[int]) -> dict[str, int]:
    counter = Counter(int(value) for value in values)
    return {str(key): counter[key] for key in sorted(counter)}


def shape_payload(
    subgraph: FunctionTopologySubgraph,
    adjacency: Mapping[str, set[str]],
    kinds: Mapping[str, str],
) -> dict[str, Any]:
    """The graph-structural part, with no text and no coordinate in it."""
    members = set(subgraph.member_node_ids)
    degree = {node_id: len(set(adjacency.get(node_id, ())) & members) for node_id in members}
    buses = [node_id for node_id in members if kinds.get(node_id) == BUS]
    depth: dict[str, int] = {node_id: 0 for node_id in buses}
    queue: deque[tuple[str, int]] = deque((node_id, 0) for node_id in buses)
    while queue:
        current, level = queue.popleft()
        if level >= DEPTH_LIMIT:
            continue
        for neighbour in sorted(set(adjacency.get(current, ())) & members):
            if neighbour in depth:
                continue
            depth[neighbour] = level + 1
            queue.append((neighbour, level + 1))
    free_feeders = [
        node_id for node_id in members
        if kinds.get(node_id) == FEEDER and degree.get(node_id, 0) <= 1
    ]
    return {
        "member_counts_by_kind": {
            key: value for key, value in sorted(
                Counter(kinds.get(node_id, "UNKNOWN") for node_id in members).items())
        },
        "bus_count": len(buses),
        "bus_degrees": sorted(degree.get(node_id, 0) for node_id in buses),
        "degree_histogram": _histogram(degree.values()),
        "branch_depth_histogram": _histogram(depth.values()),
        "members_unreachable_from_a_bus": len(members) - len(depth) if buses else None,
        "free_ended_feeder_count": len(free_feeders),
        "proven_edge_count": len(subgraph.member_edge_ids),
    }


def device_payload(
    subgraph: FunctionTopologySubgraph, symbol_of_node: Mapping[str, str | None]
) -> dict[str, Any]:
    counter = Counter(
        symbol_of_node.get(node_id) or "-" for node_id in subgraph.equipment_node_ids
    )
    return {"device_shape_multiset": {key: counter[key] for key in sorted(counter)}}


def name_payload(subgraph: FunctionTopologySubgraph) -> dict[str, Any]:
    return {"owner_marks": sorted(subgraph.function_marks)}


def consumer_payload(subgraph: FunctionTopologySubgraph) -> dict[str, Any]:
    folded = Counter(normalize(text) for text in subgraph.consumer_labels)
    return {"consumer_label_multiset": {key: folded[key] for key in sorted(folded)}}


def signatures_of(
    subgraph: FunctionTopologySubgraph,
    adjacency: Mapping[str, set[str]],
    kinds: Mapping[str, str],
    symbol_of_node: Mapping[str, str | None],
) -> dict[str, Any]:
    """All four tiers of one aggregate, and the payload each was built from."""
    shape = shape_payload(subgraph, adjacency, kinds)
    devices = device_payload(subgraph, symbol_of_node)
    names = name_payload(subgraph)
    consumers = consumer_payload(subgraph)
    payloads = {
        SHAPE_ONLY: dict(shape),
        SHAPE_AND_DEVICES: {**shape, **devices},
        SHAPE_AND_NAMES: {**shape, **devices, **names},
        SHAPE_AND_CONSUMERS: {**shape, **devices, **names, **consumers},
    }
    return {
        "signatures": {tier: _digest(payloads[tier]) for tier in SIGNATURE_TIERS},
        "shape": shape,
        "devices": devices["device_shape_multiset"],
        "owner_marks": names["owner_marks"],
        "ingredients_excluded": [
            "physical_page", "node_id", "bbox", "anchor", "coordinate", "island_id",
        ],
    }


def annotate(page: PageAggregation, result) -> list[dict[str, Any]]:
    """Attach the canonical signature to every aggregate of a page."""
    topology = result.topology
    kinds = {node.node_id: node.node_kind for node in topology.nodes}
    symbol_of_node = {node.node_id: node.symbol_signature for node in topology.nodes}
    adjacency: dict[str, set[str]] = defaultdict(set)
    from .aggregation import _adjacency, _electrical_edges

    adjacency.update(_adjacency(_electrical_edges(topology)))
    rows: list[dict[str, Any]] = []
    for subgraph in page.subgraphs:
        computed = signatures_of(subgraph, adjacency, kinds, symbol_of_node)
        subgraph.topology_signature = computed["signatures"][SHAPE_AND_DEVICES]
        rows.append({
            "subgraph_id": subgraph.subgraph_id,
            "document": subgraph.document,
            "physical_page": subgraph.physical_page,
            "boundary_status": subgraph.boundary_status,
            **computed,
        })
    return rows


def assert_layout_independent(rows: Sequence[Mapping[str, Any]]) -> None:
    """A signature that carries a page or a node identifier is not an identity."""
    for row in rows:
        blob = json.dumps(
            {key: row[key] for key in ("signatures", "shape", "devices", "owner_marks")
             if key in row},
            ensure_ascii=False, sort_keys=True,
        )
        for forbidden in ("n:", "i:", "e:", "\"bbox\"", "\"anchor\""):
            if forbidden in blob:
                raise AssertionError(f"signature payload carries {forbidden!r}")


def distinguishing_power(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    """How much each tier separates aggregates that are genuinely different."""
    out: dict[str, Any] = {}
    for tier in SIGNATURE_TIERS:
        counter = Counter(row["signatures"][tier] for row in rows)
        out[tier] = {
            "subgraphs": len(rows),
            "distinct_signatures": len(counter),
            "largest_group": max(counter.values()) if counter else 0,
            "singletons": sum(1 for value in counter.values() if value == 1),
        }
    return out


def same_class_separation(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    """§16: can structure tell two functions of the same class apart?

    The class here is the *owner mark series* — ``ВРУ``, ``ЩО``, ``ГРЩ`` — which is
    what "same class" means on these sheets.  Within a series the ordinals differ
    (``ВРУ1``, ``ВРУ2``), so a tier that separates them is telling two instances of
    one class apart without ever looking at a page.
    """
    by_series: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for row in rows:
        for mark in row["owner_marks"]:
            series = "".join(character for character in mark if not character.isdigit())
            if series:
                by_series[series].append(row)
    groups: list[dict[str, Any]] = []
    for series in sorted(by_series):
        members = by_series[series]
        if len(members) < 2:
            continue
        groups.append({
            "owner_series": series,
            "subgraphs": len(members),
            "distinct_by_tier": {
                tier: len({row["signatures"][tier] for row in members})
                for tier in SIGNATURE_TIERS
            },
            "indistinguishable_by_shape_only": len(members) - len(
                {row[
                    "signatures"][SHAPE_ONLY] for row in members}),
        })
    return {
        "series_with_two_or_more_subgraphs": len(groups),
        "groups": groups,
        "rule": (
            "the class is the owner mark series; separating two ordinals of one "
            "series is separating two instances of one class"
        ),
    }


__all__ = [
    "DEPTH_LIMIT", "annotate", "assert_layout_independent", "consumer_payload",
    "device_payload", "distinguishing_power", "name_payload", "same_class_separation",
    "shape_payload", "signatures_of",
]
