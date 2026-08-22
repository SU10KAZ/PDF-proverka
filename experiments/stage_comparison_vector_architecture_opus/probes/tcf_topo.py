#!/usr/bin/env python3
"""TCF — instrumented re-implementation of extractor._topology.

This is a FORK of `experiments/stage_comparison_vector_blocks/extractor.py::_topology`
(Track A, commit 1619fc3f).  Track A files are never modified.  The fork keeps the
Track A algorithm bit-for-bit and only *adds* instrumentation:

  * per-crossing parameters (t, u, distance to nearest endpoint of either segment,
    same-component flag, "already joined by a T-junction" flag),
  * count of segments collapsed to a self-loop by node snapping,
  * the retained/dropped split when the topology cap fires,
  * an optional `crossing_limit` so the 5000-truncation can be lifted.

`selftest()` proves the fork reproduces Track A numbers exactly at the default
tolerance 0.0025 / cap 8000.
"""
from __future__ import annotations

import collections
import math
from typing import Any, Sequence

from experiments.stage_comparison_vector_blocks.extractor import (
    _all_segments,
    _bbox,
    _distance,
    _point_segment_distance,
    _round,
    _segment_intersection,
)

COMPARATOR_KEYS = (
    "node_count",
    "edge_count",
    "connected_components",
    "endpoints",
    "branch_points",
    "t_junctions",
    "x_crossings_unconnected",
    "closed_contours",
    "nested_contours",
)


def topology(
    primitives: Sequence[dict[str, Any]],
    tolerance: float,
    topology_cap: int,
    *,
    crossing_limit: int = 5_000,
    keep_crossings: bool = False,
    nesting_limit: int = 1000,
) -> dict[str, Any]:
    all_segments = _all_segments(primitives)
    segments = all_segments
    capped = len(segments) > topology_cap
    dropped_lengths: list[float] = []
    if capped:
        ordered = sorted(segments, key=lambda item: item["length"], reverse=True)
        segments = ordered[:topology_cap]
        dropped_lengths = [item["length"] for item in ordered[topology_cap:]]

    node_cells: dict[tuple[int, int], list[int]] = collections.defaultdict(list)
    nodes: list[list[float]] = []

    def cell(point: Sequence[float], size: float = tolerance) -> tuple[int, int]:
        return round(float(point[0]) / size), round(float(point[1]) / size)

    def node_id(point: Sequence[float]) -> int:
        cx, cy = cell(point)
        for gx in range(cx - 1, cx + 2):
            for gy in range(cy - 1, cy + 2):
                for candidate in node_cells.get((gx, gy), []):
                    if _distance(nodes[candidate], point) <= tolerance:
                        return candidate
        index = len(nodes)
        nodes.append([_round(point[0]), _round(point[1])])
        node_cells[(cx, cy)].append(index)
        return index

    edges = []
    adjacency: dict[int, set[int]] = collections.defaultdict(set)
    parent: list[int] = []
    collapsed = 0
    segment_nodes: list[tuple[int, int]] = []

    def ensure(index: int) -> None:
        while len(parent) <= index:
            parent.append(len(parent))

    def find(index: int) -> int:
        while parent[index] != index:
            parent[index] = parent[parent[index]]
            index = parent[index]
        return index

    def union(left: int, right: int) -> None:
        left, right = find(left), find(right)
        if left != right:
            parent[right] = left

    for segment_index, segment in enumerate(segments):
        left, right = node_id(segment["p1"]), node_id(segment["p2"])
        ensure(max(left, right))
        segment_nodes.append((left, right))
        if left == right:
            collapsed += 1
            continue
        adjacency[left].add(right)
        adjacency[right].add(left)
        union(left, right)
        edges.append({"segment_index": segment_index, "left": left, "right": right})

    edge_by_segment = {edge["segment_index"]: edge for edge in edges}

    index_cell = max(tolerance * 4, 0.005)
    segment_cells: dict[tuple[int, int], list[int]] = collections.defaultdict(list)
    skipped_from_index = 0
    for index, segment in enumerate(segments):
        bbox = _bbox([segment["p1"], segment["p2"]])
        x0, y0 = math.floor(bbox[0] / index_cell), math.floor(bbox[1] / index_cell)
        x1, y1 = math.floor(bbox[2] / index_cell), math.floor(bbox[3] / index_cell)
        if (x1 - x0 + 1) * (y1 - y0 + 1) > 600:
            skipped_from_index += 1
            continue
        for gx in range(x0, x1 + 1):
            for gy in range(y0, y1 + 1):
                segment_cells[(gx, gy)].append(index)

    t_junctions = []
    t_pairs: set[tuple[int, int]] = set()
    for current_node, point in enumerate(nodes):
        gx, gy = math.floor(point[0] / index_cell), math.floor(point[1] / index_cell)
        candidates = set()
        for x in range(gx - 1, gx + 2):
            for y in range(gy - 1, gy + 2):
                candidates.update(segment_cells.get((x, y), []))
        for segment_index in candidates:
            edge = edge_by_segment.get(segment_index)
            if edge is None or current_node in {edge["left"], edge["right"]}:
                continue
            segment = segments[segment_index]
            if _point_segment_distance(point, [segment["p1"], segment["p2"]]) <= tolerance:
                adjacency[current_node].update((edge["left"], edge["right"]))
                adjacency[edge["left"]].add(current_node)
                adjacency[edge["right"]].add(current_node)
                union(current_node, edge["left"])
                t_junctions.append({"node": current_node, "segment_index": segment_index})
                t_pairs.add((current_node, segment_index))

    node_owner_segments: dict[int, set[int]] = collections.defaultdict(set)
    for seg_index, (a, b) in enumerate(segment_nodes):
        node_owner_segments[a].add(seg_index)
        node_owner_segments[b].add(seg_index)

    crossings: list[dict[str, Any]] = []
    crossing_records: list[dict[str, Any]] = []
    compared = 0
    skipped_dense_cells = 0
    compared_pairs = set()
    stop = False
    for members in segment_cells.values():
        if stop:
            break
        if len(members) > 150:
            skipped_dense_cells += 1
            continue
        for offset, first_index in enumerate(members):
            for second_index in members[offset + 1:]:
                pair = (min(first_index, second_index), max(first_index, second_index))
                if pair in compared_pairs:
                    continue
                compared_pairs.add(pair)
                compared += 1
                if segments[first_index]["primitive_id"] == segments[second_index]["primitive_id"]:
                    continue
                hit = _segment_intersection(segments[first_index], segments[second_index])
                if hit:
                    crossings.append({"point": [_round(hit[0]), _round(hit[1])], "segments": list(pair)})
                    if keep_crossings:
                        crossing_records.append(
                            _crossing_record(hit, pair, segments, segment_nodes, t_pairs, find, tolerance)
                        )
                    if len(crossings) >= crossing_limit:
                        stop = True
                        break
            if stop:
                break

    by_root: dict[int, set[int]] = collections.defaultdict(set)
    for node in range(len(nodes)):
        by_root[find(node)].add(node)
    edge_by_component: dict[int, list[int]] = collections.defaultdict(list)
    for edge in edges:
        edge_by_component[find(edge["left"])].append(edge["segment_index"])
    component_rows = []
    for root, members in by_root.items():
        segment_ids = edge_by_component.get(root, [])
        component_rows.append({"node_count": len(members), "segment_count": len(segment_ids)})

    degree_histogram = collections.Counter(len(adjacency[node]) for node in range(len(nodes)))
    closed_primitives = [item for item in primitives if item["closed"]]
    nested = 0
    for inner in closed_primitives[:nesting_limit]:
        a = inner["normalized"]["bbox"]
        for outer in closed_primitives[:nesting_limit]:
            if inner is outer:
                continue
            b = outer["normalized"]["bbox"]
            if b[0] < a[0] and b[1] < a[1] and b[2] > a[2] and b[3] > a[3]:
                nested += 1
                break

    result = {
        "tolerance_norm": tolerance,
        "segments_total": len(all_segments),
        "segments_used": len(segments),
        "segments_capped": capped,
        "node_count": len(nodes),
        "edge_count": len(edges),
        "connected_components": len(component_rows),
        "endpoints": degree_histogram.get(1, 0),
        "branch_points": sum(v for d, v in degree_histogram.items() if d >= 3),
        "degree_histogram": {str(k): v for k, v in sorted(degree_histogram.items())},
        "t_junctions": len(t_junctions),
        "x_crossings_unconnected": len(crossings),
        "crossings_truncated": len(crossings) >= crossing_limit,
        "closed_contours": len(closed_primitives),
        "nested_contours": nested,
        # instrumentation (not in Track A)
        "_collapsed_segments": collapsed,
        "_dropped_segment_lengths": dropped_lengths,
        "_retained_min_length": min((s["length"] for s in segments), default=0.0),
        "_segments_skipped_from_index": skipped_from_index,
        "_dense_cells_skipped": skipped_dense_cells,
        "_pairs_compared": compared,
        "_crossing_limit": crossing_limit,
    }
    if keep_crossings:
        result["_crossings"] = crossing_records
    return result


def _crossing_record(hit, pair, segments, segment_nodes, t_pairs, find, tolerance) -> dict[str, Any]:
    x, y, t, u = hit
    first, second = segments[pair[0]], segments[pair[1]]
    ends = [first["p1"], first["p2"], second["p1"], second["p2"]]
    near = min(_distance((x, y), e) for e in ends)
    a1, a2 = segment_nodes[pair[0]]
    b1, b2 = segment_nodes[pair[1]]
    same_component = find(a1) == find(b1)
    joined_by_t = any(
        (node, seg) in t_pairs
        for node, seg in ((a1, pair[1]), (a2, pair[1]), (b1, pair[0]), (b2, pair[0]))
    )
    return {
        "point": [_round(x), _round(y)],
        "segments": list(pair),
        "t": round(float(t), 6),
        "u": round(float(u), 6),
        "min_param": round(min(t, 1 - t, u, 1 - u), 6),
        "nearest_endpoint_distance": round(near, 6),
        "endpoint_within_tolerance": bool(near <= tolerance),
        "same_component": bool(same_component),
        "joined_by_t_junction": bool(joined_by_t),
        "len1": first["length"],
        "len2": second["length"],
        "primitive_ids": [first["primitive_id"], second["primitive_id"]],
    }


def selftest(description: dict[str, Any]) -> dict[str, Any]:
    """Compare the fork against the stored Track A topology at defaults."""
    stored = description["topology"]
    mine = topology(description["geometry"]["primitives"], stored["tolerance_norm"], 8_000)
    diff = {
        key: (stored.get(key), mine.get(key))
        for key in (*COMPARATOR_KEYS, "segments_total", "segments_used", "degree_histogram")
        if stored.get(key) != mine.get(key)
    }
    return diff
