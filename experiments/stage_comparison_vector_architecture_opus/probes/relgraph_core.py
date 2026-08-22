#!/usr/bin/env python3
"""relgraph_core -- Track-B probe: generic RELATION GRAPH over vector block primitives.

Research-only proof of concept. Reads a Track-A `vector_block.json`
(VectorBlockDescription v0.1) and derives:

  entities  : crude graphical objects = endpoint-connected clusters of segments,
              plus text spans
  relations : contains / adjacent / connected(endpoint or T) / crosses /
              parallel / labelled_by_text / member_of_group / repeats_along /
              between

Everything is discipline-free and derived only from normalized geometry
already present in the v0.1 description. Nothing here touches production code.

Run from repo root:
    python -m experiments.stage_comparison_vector_architecture_opus.probes.relgraph_core <vector_block.json>
"""
from __future__ import annotations

import collections
import json
import math
import sys
import time
from pathlib import Path
from typing import Any, Iterable, Sequence

# ---------------------------------------------------------------- parameters

CLUSTER_TOL = 0.0025      # endpoint coincidence -> same object (extractor topology tol)
CONNECT_EPS = 0.004       # endpoint of A touching interior of a segment of B
ADJACENT_EPS = 0.02       # bbox gap that still counts as "adjacent-near"
LABEL_EPS = 0.03          # text -> nearest object
PARALLEL_DEG = 5.0
GRID = 0.01               # spatial grid cell (normalized units)
MAX_CELLS_PER_SEGMENT = 400
PAIR_CHECK_CAP = 4_000_000
CLUSTER_CAP = 20_000
REL_PER_TYPE_CAP = 200_000


# ---------------------------------------------------------------- geometry

def _seg_len(s: Sequence[float]) -> float:
    return math.hypot(s[2] - s[0], s[3] - s[1])


def _angle(s: Sequence[float]) -> float:
    return math.degrees(math.atan2(s[3] - s[1], s[2] - s[0])) % 180.0


def _angle_dist(a: float, b: float) -> float:
    d = abs(a - b) % 180.0
    return min(d, 180.0 - d)


def _point_seg_dist(px: float, py: float, s: Sequence[float]) -> float:
    x0, y0, x1, y1 = s[0], s[1], s[2], s[3]
    dx, dy = x1 - x0, y1 - y0
    den = dx * dx + dy * dy
    if den <= 1e-18:
        return math.hypot(px - x0, py - y0)
    t = ((px - x0) * dx + (py - y0) * dy) / den
    t = max(0.0, min(1.0, t))
    return math.hypot(px - (x0 + t * dx), py - (y0 + t * dy))


def _proper_intersect(a: Sequence[float], b: Sequence[float]) -> bool:
    """True when a and b cross strictly inside both segments."""
    ax0, ay0, ax1, ay1 = a[0], a[1], a[2], a[3]
    bx0, by0, bx1, by1 = b[0], b[1], b[2], b[3]
    rx, ry = ax1 - ax0, ay1 - ay0
    sx, sy = bx1 - bx0, by1 - by0
    den = rx * sy - ry * sx
    if abs(den) < 1e-14:
        return False
    t = ((bx0 - ax0) * sy - (by0 - ay0) * sx) / den
    u = ((bx0 - ax0) * ry - (by0 - ay0) * rx) / den
    return 0.02 < t < 0.98 and 0.02 < u < 0.98


def _bbox_gap(a: Sequence[float], b: Sequence[float]) -> float:
    dx = max(a[0] - b[2], b[0] - a[2], 0.0)
    dy = max(a[1] - b[3], b[1] - a[3], 0.0)
    return math.hypot(dx, dy)


def _contains(outer: Sequence[float], inner: Sequence[float], slack: float = 0.001) -> bool:
    return (
        outer[0] - slack <= inner[0]
        and outer[1] - slack <= inner[1]
        and outer[2] + slack >= inner[2]
        and outer[3] + slack >= inner[3]
    )


def _area(b: Sequence[float]) -> float:
    return max(b[2] - b[0], 0.0) * max(b[3] - b[1], 0.0)


# ---------------------------------------------------------------- loading

def segments_of(desc: dict[str, Any]) -> list[tuple]:
    """(x0,y0,x1,y1, primitive_index) in block-normalized coordinates."""
    out: list[tuple] = []
    for pi, prim in enumerate(desc["geometry"]["primitives"]):
        for start, end in prim["normalized"]["segments"]:
            if abs(start[0] - end[0]) < 1e-9 and abs(start[1] - end[1]) < 1e-9:
                continue
            out.append((float(start[0]), float(start[1]), float(end[0]), float(end[1]), pi))
    return out


# ---------------------------------------------------------------- clustering

class _UF:
    def __init__(self, n: int) -> None:
        self.p = list(range(n))

    def find(self, x: int) -> int:
        p = self.p
        while p[x] != x:
            p[x] = p[p[x]]
            x = p[x]
        return x

    def union(self, a: int, b: int) -> None:
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            self.p[rb] = ra


def build_clusters(segs: Sequence[tuple], tol: float = CLUSTER_TOL) -> list[list[int]]:
    """Endpoint-coincidence connected components over segment indexes."""
    uf = _UF(len(segs))
    cell = max(tol, 1e-6)
    nodes: dict[tuple[int, int], list[tuple[float, float, int]]] = collections.defaultdict(list)

    def node_for(x: float, y: float, sidx: int) -> None:
        gx, gy = int(math.floor(x / cell)), int(math.floor(y / cell))
        for ax in (gx - 1, gx, gx + 1):
            for ay in (gy - 1, gy, gy + 1):
                for (nx, ny, owner) in nodes.get((ax, ay), ()):  # type: ignore[arg-type]
                    if (nx - x) ** 2 + (ny - y) ** 2 <= tol * tol:
                        uf.union(owner, sidx)
                        return
        nodes[(gx, gy)].append((x, y, sidx))

    for i, s in enumerate(segs):
        node_for(s[0], s[1], i)
        node_for(s[2], s[3], i)

    groups: dict[int, list[int]] = collections.defaultdict(list)
    for i in range(len(segs)):
        groups[uf.find(i)].append(i)
    return list(groups.values())


# ---------------------------------------------------------------- features

def _size_bucket(diag: float) -> int:
    if diag <= 1e-6:
        return -14
    return max(-14, min(1, int(round(math.log2(diag)))))


def _aspect_bucket(w: float, h: float) -> int:
    if w <= 1e-9 or h <= 1e-9:
        return 9
    r = math.log2(w / h)
    return max(-4, min(4, int(round(r))))


def _shape_class(n: int, closed_ratio: float, rectness: float, roundness: float) -> str:
    if n == 1:
        return "seg"
    if n <= 6 and rectness > 0.9:
        return "rect"
    if n >= 12 and roundness > 0.8:
        return "round"
    if n <= 6:
        return "poly_s"
    if n <= 24:
        return "poly_m"
    if n <= 120:
        return "poly_l"
    return "poly_xl"


def _shape_fp(segs: Sequence[tuple], bbox: Sequence[float], quant: int = 12) -> str:
    """Scale-invariant fingerprint: cluster normalized to its own unit box."""
    w = max(bbox[2] - bbox[0], 1e-9)
    h = max(bbox[3] - bbox[1], 1e-9)
    toks = []
    for s in segs:
        ax = int(round((s[0] - bbox[0]) / w * quant))
        ay = int(round((s[1] - bbox[1]) / h * quant))
        bx = int(round((s[2] - bbox[0]) / w * quant))
        by = int(round((s[3] - bbox[1]) / h * quant))
        if (bx, by) < (ax, ay):
            ax, ay, bx, by = bx, by, ax, ay
        toks.append((ax, ay, bx, by))
    toks.sort()
    import hashlib

    payload = repr(toks).encode()
    return hashlib.sha1(payload).hexdigest()[:10]


def cluster_feature(idx: int, members: Sequence[int], segs: Sequence[tuple]) -> dict[str, Any]:
    xs0 = min(min(segs[i][0], segs[i][2]) for i in members)
    ys0 = min(min(segs[i][1], segs[i][3]) for i in members)
    xs1 = max(max(segs[i][0], segs[i][2]) for i in members)
    ys1 = max(max(segs[i][1], segs[i][3]) for i in members)
    bbox = (xs0, ys0, xs1, ys1)
    w, h = xs1 - xs0, ys1 - ys0
    diag = math.hypot(w, h)
    ms = [segs[i] for i in members]
    total_len = sum(_seg_len(s) for s in ms)
    # rectness: fraction of length in axis-aligned segments
    axis_len = sum(_seg_len(s) for s in ms if _angle_dist(_angle(s), 0.0) < 2.0 or _angle_dist(_angle(s), 90.0) < 2.0)
    rectness = axis_len / total_len if total_len > 0 else 0.0
    # roundness: perimeter vs bbox ellipse-ish, plus radial variance
    cx, cy = (xs0 + xs1) / 2, (ys0 + ys1) / 2
    rr = [math.hypot(s[0] - cx, s[1] - cy) for s in ms]
    mean_r = sum(rr) / len(rr) if rr else 0.0
    if mean_r > 1e-9 and len(rr) > 3:
        var = sum((r - mean_r) ** 2 for r in rr) / len(rr)
        roundness = max(0.0, 1.0 - math.sqrt(var) / mean_r)
    else:
        roundness = 0.0
    n = len(members)
    closed_ratio = 1.0 if _is_closed_loop(ms) else 0.0
    shape = _shape_class(n, closed_ratio, rectness, roundness)
    sb = _size_bucket(diag)
    ab = _aspect_bucket(w, h)
    cls = f"{shape}|s{sb}|a{ab}"
    return {
        "id": idx,
        "bbox": bbox,
        "center": (cx, cy),
        "n_segments": n,
        "length": total_len,
        "diag": diag,
        "shape": shape,
        "size_bucket": sb,
        "aspect_bucket": ab,
        "cls": cls,
        "fp": _shape_fp(ms, bbox),
        "dominant_angle": _dominant_angle(ms),
        "members": list(members),
    }


def _is_closed_loop(ms: Sequence[tuple]) -> bool:
    deg: dict[tuple[int, int], int] = collections.Counter()
    q = 1.0 / CLUSTER_TOL
    for s in ms:
        deg[(int(s[0] * q), int(s[1] * q))] += 1
        deg[(int(s[2] * q), int(s[3] * q))] += 1
    return all(v % 2 == 0 for v in deg.values())


def _dominant_angle(ms: Sequence[tuple]) -> float | None:
    acc: dict[int, float] = collections.defaultdict(float)
    for s in ms:
        acc[int(_angle(s) // 5) * 5] += _seg_len(s)
    if not acc:
        return None
    total = sum(acc.values())
    best, val = max(acc.items(), key=lambda kv: kv[1])
    return float(best) if val / total > 0.6 else None


def _len_bucket(n: int) -> int:
    if n <= 2:
        return 1
    if n <= 5:
        return 2
    if n <= 12:
        return 3
    return 4


def text_feature(idx: int, t: dict[str, Any]) -> dict[str, Any]:
    b = t["bbox_norm"]
    txt = (t.get("text") or "").strip()
    return {
        "id": idx,
        "bbox": (float(b[0]), float(b[1]), float(b[2]), float(b[3])),
        "center": (float(t["x_norm"]), float(t["y_norm"])),
        "text": txt,
        "category": t.get("category", "label"),
        "cls": f"txt|{t.get('category','label')}|L{_len_bucket(len(txt))}",
        "cls_with_text": f"txt|{txt.lower()}",
    }


# ---------------------------------------------------------------- spatial index

def _grid_cells(bbox: Sequence[float], cell: float = GRID) -> Iterable[tuple[int, int]]:
    gx0, gy0 = int(math.floor(bbox[0] / cell)), int(math.floor(bbox[1] / cell))
    gx1, gy1 = int(math.floor(bbox[2] / cell)), int(math.floor(bbox[3] / cell))
    n = (gx1 - gx0 + 1) * (gy1 - gy0 + 1)
    if n > MAX_CELLS_PER_SEGMENT:
        step_x = max(1, (gx1 - gx0 + 1) // 20)
        step_y = max(1, (gy1 - gy0 + 1) // 20)
        for gx in range(gx0, gx1 + 1, step_x):
            for gy in range(gy0, gy1 + 1, step_y):
                yield (gx, gy)
        return
    for gx in range(gx0, gx1 + 1):
        for gy in range(gy0, gy1 + 1):
            yield (gx, gy)


# ---------------------------------------------------------------- relations

def build_relation_graph(desc: dict[str, Any], *, verbose: bool = False) -> dict[str, Any]:
    t_start = time.time()
    segs = segments_of(desc)
    groups = build_clusters(segs)
    groups.sort(key=len, reverse=True)
    cluster_capped = len(groups) > CLUSTER_CAP
    groups = groups[:CLUSTER_CAP]
    clusters = [cluster_feature(i, m, segs) for i, m in enumerate(groups)]
    texts = [text_feature(i, t) for i, t in enumerate(desc.get("texts", []))]
    t_cluster = time.time()

    rel: collections.Counter[tuple] = collections.Counter()
    raw: dict[str, list] = collections.defaultdict(list)
    stats: dict[str, Any] = {}

    # --- spatial index of cluster bboxes
    cell_map: dict[tuple[int, int], list[int]] = collections.defaultdict(list)
    for c in clusters:
        for g in _grid_cells(c["bbox"]):
            cell_map[g].append(c["id"])
    # inflated map -> candidate pairs for adjacency (gap up to ADJACENT_EPS)
    infl_map: dict[tuple[int, int], list[int]] = collections.defaultdict(list)
    for c in clusters:
        b = c["bbox"]
        for g in _grid_cells((b[0] - ADJACENT_EPS, b[1] - ADJACENT_EPS,
                              b[2] + ADJACENT_EPS, b[3] + ADJACENT_EPS)):
            infl_map[g].append(c["id"])

    # candidate cluster pairs sharing a grid cell
    cand: set[tuple[int, int]] = set()
    checks = 0
    pair_capped = False
    for ids in infl_map.values():
        k = len(ids)
        if checks + k * (k - 1) // 2 > PAIR_CHECK_CAP:
            pair_capped = True
            continue
        checks += k * (k - 1) // 2
        for i in range(k):
            for j in range(i + 1, k):
                a, b = ids[i], ids[j]
                cand.add((a, b) if a < b else (b, a))
    stats["candidate_cluster_pairs"] = len(cand)
    stats["pair_cap_hit"] = pair_capped

    # --- contains: tightest enclosing cluster
    by_area = sorted(clusters, key=lambda c: _area(c["bbox"]))
    tightest: dict[int, int] = {}
    for a, b in cand:
        ca, cb = clusters[a], clusters[b]
        if _contains(ca["bbox"], cb["bbox"]) and _area(ca["bbox"]) > _area(cb["bbox"]) * 1.05:
            outer, inner = ca, cb
        elif _contains(cb["bbox"], ca["bbox"]) and _area(cb["bbox"]) > _area(ca["bbox"]) * 1.05:
            outer, inner = cb, ca
        else:
            continue
        cur = tightest.get(inner["id"])
        if cur is None or _area(clusters[cur]["bbox"]) > _area(outer["bbox"]):
            tightest[inner["id"]] = outer["id"]
    for inner_id, outer_id in tightest.items():
        rel[("contains", clusters[outer_id]["cls"], clusters[inner_id]["cls"])] += 1
        if len(raw["contains"]) < 200000:
            raw["contains"].append((outer_id, inner_id))

    # --- adjacent / parallel
    for a, b in cand:
        ca, cb = clusters[a], clusters[b]
        if tightest.get(a) == b or tightest.get(b) == a:
            continue
        gap = _bbox_gap(ca["bbox"], cb["bbox"])
        scale = min(max(ca["diag"], 1e-6), max(cb["diag"], 1e-6))
        if gap <= min(ADJACENT_EPS, max(scale, 0.002)):
            key = tuple(sorted((ca["cls"], cb["cls"])))
            rel[("adjacent", key[0], key[1])] += 1
            if len(raw["adjacent"]) < 200000:
                raw["adjacent"].append((a, b))
            da, db = ca["dominant_angle"], cb["dominant_angle"]
            if da is not None and db is not None and _angle_dist(da, db) <= PARALLEL_DEG:
                rel[("parallel", key[0], key[1])] += 1

    # --- segment index for connected / crosses
    seg_cells: dict[tuple[int, int], list[int]] = collections.defaultdict(list)
    owner = [-1] * len(segs)
    for c in clusters:
        for m in c["members"]:
            owner[m] = c["id"]
    for i, s in enumerate(segs):
        if owner[i] < 0:
            continue
        bb = (min(s[0], s[2]), min(s[1], s[3]), max(s[0], s[2]), max(s[1], s[3]))
        for g in _grid_cells(bb):
            seg_cells[g].append(i)

    connected: set[tuple[int, int]] = set()
    conn_checks = 0
    for i, s in enumerate(segs):
        if owner[i] < 0:
            continue
        for (px, py) in ((s[0], s[1]), (s[2], s[3])):
            gx, gy = int(math.floor(px / GRID)), int(math.floor(py / GRID))
            for ax in (gx - 1, gx, gx + 1):
                for ay in (gy - 1, gy, gy + 1):
                    for j in seg_cells.get((ax, ay), ()):  # type: ignore[arg-type]
                        if owner[j] == owner[i] or owner[j] < 0:
                            continue
                        conn_checks += 1
                        if conn_checks > PAIR_CHECK_CAP:
                            break
                        key = (owner[i], owner[j]) if owner[i] < owner[j] else (owner[j], owner[i])
                        if key in connected:
                            continue
                        if _point_seg_dist(px, py, segs[j]) <= CONNECT_EPS:
                            connected.add(key)
    for a, b in connected:
        key = tuple(sorted((clusters[a]["cls"], clusters[b]["cls"])))
        rel[("connected", key[0], key[1])] += 1
        if len(raw["connected"]) < 200000:
            raw["connected"].append((a, b))
    stats["connected_checks"] = conn_checks

    # --- crosses (proper intersection, not connected)
    crossed: set[tuple[int, int]] = set()
    x_checks = 0
    for cellids in seg_cells.values():
        k = len(cellids)
        if k < 2 or k > 200:
            continue
        for ii in range(k):
            for jj in range(ii + 1, k):
                i, j = cellids[ii], cellids[jj]
                if owner[i] == owner[j]:
                    continue
                key = (owner[i], owner[j]) if owner[i] < owner[j] else (owner[j], owner[i])
                if key in connected or key in crossed:
                    continue
                x_checks += 1
                if x_checks > PAIR_CHECK_CAP:
                    break
                if _proper_intersect(segs[i], segs[j]):
                    crossed.add(key)
    for a, b in crossed:
        key = tuple(sorted((clusters[a]["cls"], clusters[b]["cls"])))
        rel[("crosses", key[0], key[1])] += 1
        if len(raw["crosses"]) < 200000:
            raw["crosses"].append((a, b))
    stats["cross_checks"] = x_checks

    # --- labelled_by_text : text -> nearest cluster
    label_pairs: list[tuple[int, int]] = []
    for t in texts:
        best, bestd = None, LABEL_EPS
        gx0, gy0 = int(math.floor(t["bbox"][0] / GRID)), int(math.floor(t["bbox"][1] / GRID))
        gx1, gy1 = int(math.floor(t["bbox"][2] / GRID)), int(math.floor(t["bbox"][3] / GRID))
        seen: set[int] = set()
        span = int(math.ceil(LABEL_EPS / GRID))
        for gx in range(gx0 - span, gx1 + span + 1):
            for gy in range(gy0 - span, gy1 + span + 1):
                for cid in cell_map.get((gx, gy), ()):  # type: ignore[arg-type]
                    if cid in seen:
                        continue
                    seen.add(cid)
                    d = _bbox_gap(t["bbox"], clusters[cid]["bbox"])
                    if d < bestd:
                        bestd, best = d, cid
        if best is None:
            rel[("text_unanchored", t["cls"], "-")] += 1
        else:
            rel[("labelled_by", clusters[best]["cls"], t["cls"])] += 1
            label_pairs.append((best, t["id"]))
    raw["labelled_by"] = label_pairs[:20000]

    # --- member_of_group + repeats_along
    by_fp: dict[str, list[int]] = collections.defaultdict(list)
    for c in clusters:
        if c["n_segments"] < 3:
            continue  # a 1-2 segment cluster has a degenerate self-normalized fingerprint
        by_fp[c["fp"]].append(c["id"])
    groups_out = []
    for fp, ids in by_fp.items():
        if len(ids) < 2:
            continue
        cls = clusters[ids[0]]["cls"]
        for _ in ids:
            rel[("member_of_group", cls, fp[:4])] += 1
        entry = {"fp": fp, "cls": cls, "count": len(ids), "rows": [], "cols": []}
        # repeats_along
        for axis, key, other in (("x", 0, 1), ("y", 1, 0)):
            lanes: dict[int, list[int]] = collections.defaultdict(list)
            for cid in ids:
                lanes[int(round(clusters[cid]["center"][other] / 0.01))].append(cid)
            for lane, lane_ids in lanes.items():
                if len(lane_ids) < 3:
                    continue
                pos = sorted(clusters[c]["center"][key] for c in lane_ids)
                gaps = [pos[i + 1] - pos[i] for i in range(len(pos) - 1)]
                mean = sum(gaps) / len(gaps)
                if mean <= 1e-6:
                    continue
                cv = (sum((g - mean) ** 2 for g in gaps) / len(gaps)) ** 0.5 / mean
                if cv < 0.35:
                    rel[("repeats_along", axis, cls)] += 1
                    entry["rows" if axis == "x" else "cols"].append(len(lane_ids))
        groups_out.append(entry)
        # --- between: text inside the span of two consecutive group members
    groups_out.sort(key=lambda g: -g["count"])

    # --- between (limited): text whose center lies between two connected clusters
    btw = 0
    for a, b in list(connected)[:20000]:
        ca, cb = clusters[a], clusters[b]
        lo = (min(ca["center"][0], cb["center"][0]), min(ca["center"][1], cb["center"][1]))
        hi = (max(ca["center"][0], cb["center"][0]), max(ca["center"][1], cb["center"][1]))
        if (hi[0] - lo[0]) + (hi[1] - lo[1]) < 0.005:
            continue
        for cid, tid in label_pairs:
            if cid not in (a, b):
                continue
            t = texts[tid]
            if lo[0] <= t["center"][0] <= hi[0] and lo[1] <= t["center"][1] <= hi[1]:
                key = tuple(sorted((ca["cls"], cb["cls"])))
                rel[("between", t["cls"], key[0])] += 1
                btw += 1
                break
        if btw > 20000:
            break

    entity: collections.Counter[str] = collections.Counter(c["cls"] for c in clusters)
    entity.update(t["cls"] for t in texts)

    t_end = time.time()
    return {
        "block_id": desc.get("block_id"),
        "clusters": clusters,
        "texts": texts,
        "relations": rel,
        "entities": entity,
        "groups": groups_out,
        "raw": dict(raw),
        "stats": {
            **stats,
            "n_segments": len(segs),
            "n_clusters": len(clusters),
            "cluster_capped": cluster_capped,
            "n_texts": len(texts),
            "cluster_size_hist": dict(collections.Counter(
                min(c["n_segments"], 100) if c["n_segments"] < 10 else (10 if c["n_segments"] < 100 else 100)
                for c in clusters).most_common()),
            "n_relation_tokens": len(rel),
            "n_relation_instances": sum(rel.values()),
            "seconds_cluster": round(t_cluster - t_start, 2),
            "seconds_total": round(t_end - t_start, 2),
        },
    }


# ---------------------------------------------------------------- similarity

def weighted_jaccard(a: collections.Counter, b: collections.Counter) -> float:
    if not a and not b:
        return 1.0
    keys = set(a) | set(b)
    inter = sum(min(a.get(k, 0), b.get(k, 0)) for k in keys)
    union = sum(max(a.get(k, 0), b.get(k, 0)) for k in keys)
    return inter / union if union else 1.0


def cosine(a: collections.Counter, b: collections.Counter) -> float:
    keys = set(a) | set(b)
    dot = sum(a.get(k, 0) * b.get(k, 0) for k in keys)
    na = math.sqrt(sum(v * v for v in a.values()))
    nb = math.sqrt(sum(v * v for v in b.values()))
    return dot / (na * nb) if na and nb else (1.0 if not a and not b else 0.0)


def relation_json(graph: dict[str, Any]) -> dict[str, Any]:
    """Serializable, LLM-facing relation-graph payload (no per-entity coordinates)."""
    rel = graph["relations"]
    return {
        "entities": dict(graph["entities"].most_common()),
        "relations": [
            {"rel": k[0], "a": k[1], "b": k[2], "n": v}
            for k, v in sorted(rel.items(), key=lambda kv: -kv[1])
        ],
        "groups": [
            {"cls": g["cls"], "count": g["count"], "rows": g["rows"], "cols": g["cols"]}
            for g in graph["groups"][:60]
        ],
        "stats": graph["stats"],
    }


def main() -> None:
    path = Path(sys.argv[1])
    desc = json.loads(path.read_text())
    g = build_relation_graph(desc, verbose=True)
    print(json.dumps(g["stats"], ensure_ascii=False, indent=2))
    for k, v in g["relations"].most_common(25):
        print(f"{v:7d}  {k}")


if __name__ == "__main__":
    main()
