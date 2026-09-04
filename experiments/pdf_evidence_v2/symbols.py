"""Symbols: the ink between two conductors, and what it is worth knowing about.

A schematic does not draw a breaker *next to* a wire.  It cuts the wire and
draws the breaker into the gap.  Any topology that treats a conductor as a
maximal stroke therefore reports a sheet full of disconnected stubs: on the
control page of this corpus the bus and the feeder above the first breaker are
twenty-eight points apart and belong, electrically, to the same branch.

So the ink that is not a conductor is not noise to be dropped.  It is clustered
by drawn contact, and a cluster that touches two proven conductors is the
device between them.  Nothing here needs to know what the device *is*: a
breaker, a meter, a transformer and a pump all behave identically for the
purpose of continuity, and naming them is the label layer's job, not geometry's.

Two properties are computed per cluster and both are honest about their limits:

* **ports** — the places a proven conductor ends on the cluster.  A port is a
  drawn contact within the snap tolerance, never the nearest stroke.
* **a signature** — the cluster's own shape, quantized.  It says that two
  clusters were drawn from the same block, and it says nothing about what the
  block means.  Two occurrences of one signature are always two nodes: control
  G of this track exists because a graph that merges them merges two feeders.

A cluster larger than a symbol is not a symbol.  It gets a node, it gets no
ports, and it bridges nothing — the alternative is a single sprawling blob that
silently welds half a sheet into one device.
"""
from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from typing import Any, Sequence

import numpy as np

#: Longest axis edge that may be a symbol's own stroke, in points.  Above it a
#: stroke is a run of wire, not a piece of a device.
SYMBOL_EDGE_MAX = 60.0
#: A cluster wider than this across its diagonal is not a device.
SYMBOL_MAX_SIZE = 140.0
#: …and neither is one with more strokes than this.
SYMBOL_MAX_STROKES = 240
#: Drawn contact tolerance, shared with the junction rules.
TOUCH_TOL = 1.0
#: Signature grid.  Coarse on purpose: the same block placed twice differs in
#: the last decimal, and a signature that notices is a signature that never
#: matches.
SIGNATURE_GRID = 12


@dataclass
class SymbolCluster:
    """One connected piece of non-conductor ink."""

    index: int
    bbox: tuple[float, float, float, float]
    strokes: int
    axis_members: tuple[int, ...]
    slanted_members: tuple[int, ...]
    signature: str
    oversize: bool
    ports: tuple[tuple[int, float, float], ...] = ()

    @property
    def centre(self) -> tuple[float, float]:
        return ((self.bbox[0] + self.bbox[2]) / 2.0, (self.bbox[1] + self.bbox[3]) / 2.0)

    @property
    def diagonal(self) -> float:
        return float(np.hypot(self.bbox[2] - self.bbox[0], self.bbox[3] - self.bbox[1]))

    def to_dict(self) -> dict[str, Any]:
        return {
            "cluster_index": self.index,
            "bbox": [round(float(value), 2) for value in self.bbox],
            "strokes": self.strokes,
            "signature": self.signature,
            "oversize": self.oversize,
            "ports": [[int(edge), round(float(x), 2), round(float(y), 2)] for edge, x, y in self.ports],
        }


def _signature(points: np.ndarray, bbox: Sequence[float], strokes: int) -> str:
    """A shape hash of the cluster, invariant to where it sits on the sheet.

    Scale is normalized away deliberately: the same block is inserted at
    different scales on the same sheet.  Rotation and mirroring are *not*
    normalized, because a mirrored device is drawn mirrored on purpose and
    folding the two together would hide it.
    """
    width = max(bbox[2] - bbox[0], 1e-6)
    height = max(bbox[3] - bbox[1], 1e-6)
    scaled = np.column_stack([
        np.round((points[:, 0] - bbox[0]) / width * SIGNATURE_GRID).astype(int),
        np.round((points[:, 1] - bbox[1]) / height * SIGNATURE_GRID).astype(int),
        np.round((points[:, 2] - bbox[0]) / width * SIGNATURE_GRID).astype(int),
        np.round((points[:, 3] - bbox[1]) / height * SIGNATURE_GRID).astype(int),
    ])
    normalized = np.where(
        (scaled[:, 0] > scaled[:, 2])
        | ((scaled[:, 0] == scaled[:, 2]) & (scaled[:, 1] > scaled[:, 3])),
        scaled[:, [2, 3, 0, 1]].T, scaled.T,
    ).T
    rows = sorted(tuple(int(value) for value in row) for row in normalized)
    payload = f"{strokes}|" + ";".join(",".join(str(value) for value in row) for row in rows)
    return "sym_" + hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


def build_clusters(
    edges: np.ndarray,
    edge_eligible: np.ndarray,
    edge_length: np.ndarray,
    slanted: np.ndarray,
) -> list[SymbolCluster]:
    """Cluster the non-conductor ink of one page by drawn contact."""
    from scipy.sparse import coo_matrix
    from scipy.sparse.csgraph import connected_components
    from scipy.spatial import cKDTree

    axis_pool = np.nonzero(edge_eligible & (edge_length <= SYMBOL_EDGE_MAX))[0]
    members = []
    if len(axis_pool):
        members.append(edges[axis_pool])
    if len(slanted):
        members.append(slanted)
    if not members:
        return []
    stack = np.vstack(members)
    origin = np.concatenate([
        np.full(len(axis_pool), 0, dtype=np.int8),
        np.full(len(slanted), 1, dtype=np.int8),
    ])
    source = np.concatenate([axis_pool, np.arange(len(slanted))])
    endpoints = np.vstack([stack[:, :2], stack[:, 2:]])
    owner = np.concatenate([np.arange(len(stack)), np.arange(len(stack))])
    tree = cKDTree(endpoints)
    pairs = tree.query_pairs(r=TOUCH_TOL, output_type="ndarray")
    if len(pairs):
        left = owner[pairs[:, 0]]
        right = owner[pairs[:, 1]]
    else:
        left = right = np.zeros(0, dtype=int)
    graph = coo_matrix((np.ones(len(left)), (left, right)), shape=(len(stack), len(stack)))
    count, labels = connected_components(graph, directed=False)
    clusters: list[SymbolCluster] = []
    for component in range(count):
        mask = labels == component
        rows = stack[mask]
        bbox = (
            float(min(rows[:, 0].min(), rows[:, 2].min())),
            float(min(rows[:, 1].min(), rows[:, 3].min())),
            float(max(rows[:, 0].max(), rows[:, 2].max())),
            float(max(rows[:, 1].max(), rows[:, 3].max())),
        )
        strokes = int(mask.sum())
        diagonal = float(np.hypot(bbox[2] - bbox[0], bbox[3] - bbox[1]))
        clusters.append(SymbolCluster(
            index=component,
            bbox=bbox,
            strokes=strokes,
            axis_members=tuple(int(value) for value in source[mask & (origin == 0)]),
            slanted_members=tuple(int(value) for value in source[mask & (origin == 1)]),
            signature=_signature(rows, bbox, strokes),
            oversize=diagonal > SYMBOL_MAX_SIZE or strokes > SYMBOL_MAX_STROKES,
        ))
    clusters.sort(key=lambda cluster: (round(cluster.bbox[1], 2), round(cluster.bbox[0], 2)))
    for position, cluster in enumerate(clusters):
        cluster.index = position
    return clusters


def attach_ports(
    clusters: Sequence[SymbolCluster],
    edges: np.ndarray,
    horizontal: np.ndarray,
    conductor: np.ndarray,
) -> dict[int, list[tuple[int, float, float]]]:
    """Where a proven conductor ends on a cluster.

    A conductor's *endpoint* must land on the cluster's ink.  A conductor
    passing through a cluster's bounding box is not a port: a bus drawn across
    a busy area would otherwise acquire a port on every symbol it passes.
    """
    from scipy.spatial import cKDTree

    if not len(clusters):
        return {}
    ports: dict[int, list[tuple[int, float, float]]] = {}
    conductor_indices = np.nonzero(conductor)[0]
    if not len(conductor_indices):
        return {}
    ends = np.vstack([
        edges[conductor_indices][:, :2],
        edges[conductor_indices][:, 2:],
    ])
    end_owner = np.concatenate([conductor_indices, conductor_indices])
    tree = cKDTree(ends)
    for cluster in clusters:
        if cluster.oversize:
            continue
        stack: list[np.ndarray] = []
        if cluster.axis_members:
            stack.append(edges[list(cluster.axis_members)])
        if not stack and not cluster.slanted_members:
            continue
        found: dict[int, tuple[float, float]] = {}
        candidates = tree.query_ball_point(
            [cluster.centre[0], cluster.centre[1]], r=cluster.diagonal / 2.0 + TOUCH_TOL
        )
        for position in candidates:
            point = ends[position]
            if not (
                cluster.bbox[0] - TOUCH_TOL <= point[0] <= cluster.bbox[2] + TOUCH_TOL
                and cluster.bbox[1] - TOUCH_TOL <= point[1] <= cluster.bbox[3] + TOUCH_TOL
            ):
                continue
            found.setdefault(int(end_owner[position]), (float(point[0]), float(point[1])))
        if found:
            ports[cluster.index] = sorted(
                (edge, point[0], point[1]) for edge, point in found.items()
            )
    return ports


__all__ = [
    "SIGNATURE_GRID", "SYMBOL_EDGE_MAX", "SYMBOL_MAX_SIZE", "SYMBOL_MAX_STROKES",
    "TOUCH_TOL", "SymbolCluster", "attach_ports", "build_clusters",
]
