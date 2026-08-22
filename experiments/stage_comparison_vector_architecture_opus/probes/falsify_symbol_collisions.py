"""falsify_ probe, attack A: descriptor collisions between REAL symbol instances.

Mines connected components of vector geometry out of real drawing pages, computes
the same class of descriptors the v0.1 backbone carries at L2/L3
(primitive-type counts, segment count, node/endpoint/branch counts, cycle count,
degree histogram, aspect ratio) and asks:

    how often do two components that are DIFFERENT SHAPES share an
    identical generic-topology descriptor?

Because the v0.1 `structural_signature.level_3_structural_topology` hash is built
from exactly (primitive type counter, closed count, degree histogram,
component segment counts, text categories), a collision here is a collision of
the layer that the LLM is handed at L3.

Shape identity is decided on the block-normalized segment set (translate to the
component bbox, scale by the longer side, quantize) -- i.e. exactly the
normalization v0.1 uses (`normalization_removes: page position, uniform scale`).

Run:
  python -m experiments.stage_comparison_vector_architecture_opus.probes.falsify_symbol_collisions \
      --pdf <path> --page <i> [--clip x0 y0 x1 y1] [--out name]
"""
from __future__ import annotations

import argparse
import collections
import json
import math
from pathlib import Path

import fitz

ROOT = Path(__file__).resolve().parents[3]
ART = Path(__file__).resolve().parents[1] / "artifacts"

NODE_Q = 0.5      # pt grid used to weld endpoints into topology nodes
SHAPE_Q = 24      # normalized shape grid (component-normalized, per longer side)


def _pt(v):
    """cdrawings returns plain tuples; get_drawings returns Point/Rect objects."""
    if hasattr(v, "x"):
        return (float(v.x), float(v.y))
    return (float(v[0]), float(v[1]))


def _rect4(v):
    if hasattr(v, "x0"):
        return (float(v.x0), float(v.y0), float(v.x1), float(v.y1))
    return (float(v[0]), float(v[1]), float(v[2]), float(v[3]))


def _quad_pts(v):
    if hasattr(v, "ul"):
        return [_pt(v.ul), _pt(v.ur), _pt(v.lr), _pt(v.ll)]
    if len(v) == 4 and not isinstance(v[0], (int, float)):
        return [_pt(p) for p in v]
    return [
        (float(v[0]), float(v[1])),
        (float(v[2]), float(v[3])),
        (float(v[4]), float(v[5])),
        (float(v[6]), float(v[7])),
    ]


def _segments(page, clip=None):
    out = []
    for path in page.get_cdrawings():
        style = (
            round(float(path.get("width") or 0), 2),
            tuple(path.get("color") or ()) if path.get("color") else None,
            tuple(path.get("fill") or ()) if path.get("fill") else None,
        )
        for item in path.get("items", ()):
            kind = item[0]
            edges = []
            try:
                if kind == "l":
                    edges = [(_pt(item[1]), _pt(item[2]))]
                elif kind == "c":
                    edges = [(_pt(item[1]), _pt(item[4]))]
                elif kind == "re":
                    x0, y0, x1, y1 = _rect4(item[1])
                    cs = [(x0, y0), (x1, y0), (x1, y1), (x0, y1)]
                    edges = [(cs[i], cs[(i + 1) % 4]) for i in range(4)]
                elif kind == "qu":
                    cs = _quad_pts(item[1])
                    edges = [(cs[i], cs[(i + 1) % 4]) for i in range(4)]
            except Exception:
                continue
            for a, b in edges:
                if clip is not None:
                    if not (clip[0] <= a[0] <= clip[2] and clip[1] <= a[1] <= clip[3]):
                        continue
                    if not (clip[0] <= b[0] <= clip[2] and clip[1] <= b[1] <= clip[3]):
                        continue
                if abs(a[0] - b[0]) < 1e-9 and abs(a[1] - b[1]) < 1e-9:
                    continue
                out.append((a, b, kind, style))
    return out


class UF:
    def __init__(self):
        self.p = {}

    def find(self, x):
        self.p.setdefault(x, x)
        while self.p[x] != x:
            self.p[x] = self.p[self.p[x]]
            x = self.p[x]
        return x

    def union(self, a, b):
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            self.p[ra] = rb


def components(segs):
    uf = UF()

    def node(p):
        return (int(round(p[0] / NODE_Q)), int(round(p[1] / NODE_Q)))

    for a, b, _k, _s in segs:
        uf.union(node(a), node(b))
    groups = collections.defaultdict(list)
    for seg in segs:
        groups[uf.find(node(seg[0]))].append(seg)
    return list(groups.values())


def descriptor(comp):
    xs = [p[0] for s in comp for p in (s[0], s[1])]
    ys = [p[1] for s in comp for p in (s[0], s[1])]
    x0, x1, y0, y1 = min(xs), max(xs), min(ys), max(ys)
    w, h = max(x1 - x0, 1e-6), max(y1 - y0, 1e-6)
    scale = max(w, h)
    deg = collections.Counter()
    nodes = set()
    edges = set()
    for a, b, _k, _s in comp:
        na = (int(round(a[0] / NODE_Q)), int(round(a[1] / NODE_Q)))
        nb = (int(round(b[0] / NODE_Q)), int(round(b[1] / NODE_Q)))
        nodes.add(na)
        nodes.add(nb)
        edges.add((na, nb) if na < nb else (nb, na))
    for na, nb in edges:
        deg[na] += 1
        deg[nb] += 1
    hist = collections.Counter(deg.values())
    kinds = collections.Counter(s[2] for s in comp)
    cycles = len(edges) - len(nodes) + 1

    def shape_pts(mirror=False):
        pts = []
        for a, b, _k, _s in comp:
            pair = []
            for p in (a, b):
                nx = (p[0] - x0) / scale
                ny = (p[1] - y0) / scale
                if mirror:
                    nx = (w / scale) - nx
                pair.append((int(round(nx * SHAPE_Q)), int(round(ny * SHAPE_Q))))
            pair.sort()
            if pair[0] != pair[1]:
                pts.append((pair[0][0], pair[0][1], pair[1][0], pair[1][1]))
        return tuple(sorted(set(pts)))

    def norm_segments(mirror=False):
        out = []
        for a, b, _k, _s in comp:
            pair = []
            for p in (a, b):
                nx = (p[0] - x0) / scale
                ny = (p[1] - y0) / scale
                if mirror:
                    nx = (w / scale) - nx
                pair.append((nx, ny))
            out.append(tuple(sorted(pair)))
        return out

    def rotated(segs, k):
        """Rotate the block-normalized segment set by k*90 degrees, then
        re-normalize to its own bbox (v0.1 normalizes to the block bbox)."""
        out = []
        for (ax, ay), (bx, by) in segs:
            for _ in range(k):
                ax, ay = ay, 1.0 - ax
                bx, by = by, 1.0 - bx
            out.append(((ax, ay), (bx, by)))
        xs = [c for s2 in out for c in (s2[0][0], s2[1][0])]
        ys = [c for s2 in out for c in (s2[0][1], s2[1][1])]
        ox, oy = min(xs), min(ys)
        sc = max(max(xs) - ox, max(ys) - oy, 1e-9)
        return [
            tuple(sorted((((p[0] - ox) / sc, (p[1] - oy) / sc) for p in s2)))
            for s2 in out
        ]

    base_norm = norm_segments()
    return {
        "norm_segments": base_norm,
        "norm_segments_mirror": norm_segments(mirror=True),
        "norm_segments_rot90": rotated(base_norm, 1),
        "norm_segments_rot180": rotated(base_norm, 2),
        "norm_segments_rot270": rotated(base_norm, 3),
        "bbox": [round(x0, 2), round(y0, 2), round(x1, 2), round(y1, 2)],
        "w": round(w, 2),
        "h": round(h, 2),
        "n_segments": len(comp),
        "n_nodes": len(nodes),
        "n_edges": len(edges),
        "endpoints": hist.get(1, 0),
        "branch_points": sum(c for d, c in hist.items() if d >= 3),
        "cycles": cycles,
        "degree_histogram": dict(sorted(hist.items())),
        "kinds": dict(sorted(kinds.items())),
        # this tuple mirrors what extractor._signatures hashes into level_3
        "l3_key": json.dumps(
            [
                dict(sorted(kinds.items())),
                cycles,
                dict(sorted(hist.items())),
                len(comp),
                round(w / h, 1),
            ],
            sort_keys=True,
        ),
        "shape_key": str(shape_pts()),
        "shape_key_mirror_x": str(shape_pts(mirror=True)),
    }


def _seg_feature(seg):
    (ax, ay), (bx, by) = seg
    return ((ax + bx) / 2, (ay + by) / 2, math.hypot(bx - ax, by - ay),
            math.atan2(by - ay, bx - ax) % math.pi)


def shape_similarity(a_segs, b_segs, tol=0.03) -> float:
    """Directional, order-independent coverage between two block-normalized
    segment sets -- the same idea comparator._directional_segment_coverage uses."""
    fa = [_seg_feature(s) for s in a_segs]
    fb = [_seg_feature(s) for s in b_segs]
    if not fa or not fb:
        return 0.0

    def cov(src, dst):
        hit = 0
        for mx, my, ln, ang in src:
            for nx, ny, ln2, ang2 in dst:
                if abs(mx - nx) <= tol and abs(my - ny) <= tol and abs(ln - ln2) <= tol * 2:
                    da = abs(ang - ang2)
                    if min(da, math.pi - da) <= 0.25:
                        hit += 1
                        break
        return hit / len(src)

    return min(cov(fa, fb), cov(fb, fa))


def cluster_shapes(group, threshold=0.9, cap=40):
    """Greedy clustering of components by measured shape similarity."""
    clusters = []  # list of (representative, members)
    for comp in group[:cap]:
        placed = False
        for rep, members in clusters:
            if shape_similarity(comp["norm_segments"], rep["norm_segments"]) >= threshold:
                members.append(comp)
                placed = True
                break
        if not placed:
            clusters.append((comp, [comp]))
    return clusters


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--pdf", required=True)
    ap.add_argument("--page", type=int, required=True)
    ap.add_argument("--clip", type=float, nargs=4, default=None)
    ap.add_argument("--min-seg", type=int, default=4)
    ap.add_argument("--max-seg", type=int, default=400)
    ap.add_argument("--max-size", type=float, default=200.0, help="max component bbox side in pt")
    ap.add_argument("--shape-threshold", type=float, default=0.9)
    ap.add_argument("--min-cycles", type=int, default=0, help="require at least this many closed loops")
    ap.add_argument("--min-branch", type=int, default=0, help="require at least this many degree>=3 nodes")
    ap.add_argument("--min-size", type=float, default=2.0, help="min component bbox side in pt")
    ap.add_argument("--aspect", type=float, default=None, help="require 1/aspect <= w/h <= aspect")
    ap.add_argument("--out", default="falsify_symbol_collisions.json")
    args = ap.parse_args()

    path = ROOT / args.pdf if not Path(args.pdf).is_absolute() else Path(args.pdf)
    doc = fitz.open(path)
    page = doc[args.page]
    segs = _segments(page, args.clip)
    comps = components(segs)
    words = page.get_text("words")
    kept = []
    for comp in comps:
        d = descriptor(comp)
        if not (args.min_seg <= d["n_segments"] <= args.max_seg):
            continue
        if max(d["w"], d["h"]) > args.max_size or max(d["w"], d["h"]) < args.min_size:
            continue
        if args.aspect is not None:
            ratio = d["w"] / d["h"]
            if not (1 / args.aspect <= ratio <= args.aspect):
                continue
        if d["cycles"] < args.min_cycles or d["branch_points"] < args.min_branch:
            continue
        cx = (d["bbox"][0] + d["bbox"][2]) / 2
        cy = (d["bbox"][1] + d["bbox"][3]) / 2
        near = sorted(
            words,
            key=lambda w: math.hypot((w[0] + w[2]) / 2 - cx, (w[1] + w[3]) / 2 - cy),
        )[:3]
        d["near_text"] = [w[4] for w in near]
        kept.append(d)

    by_l3 = collections.defaultdict(list)
    for d in kept:
        by_l3[d["l3_key"]].append(d)
    collisions = []
    colliding_components = 0
    for key, group in by_l3.items():
        clusters = cluster_shapes(group, threshold=args.shape_threshold)
        if len(clusters) > 1:
            reps = [rep for rep, _m in clusters]
            worst = 1.0
            pair_sims = []
            for i in range(len(reps)):
                for j in range(i + 1, len(reps)):
                    s = shape_similarity(reps[i]["norm_segments"], reps[j]["norm_segments"])
                    pair_sims.append(round(s, 4))
                    worst = min(worst, s)
            colliding_components += len(group)
            collisions.append(
                {
                    "l3_key": key,
                    "n_instances": len(group),
                    "n_distinct_shapes": len(clusters),
                    "cluster_sizes": [len(m) for _r, m in clusters],
                    "min_pair_shape_similarity": round(worst, 4),
                    "pair_shape_similarities": pair_sims[:20],
                    "representatives": [
                        dict(
                            {k: v for k, v in rep.items() if not k.startswith("norm_segments")},
                            segments_norm=[
                                [round(c, 4) for p in seg for c in p]
                                for seg in rep["norm_segments"][:400]
                            ],
                        )
                        for rep in reps[:6]
                    ],
                }
            )
    # mirror pairs: x-mirror of A matches B's shape, but A does not match B directly
    mirror_pairs = []
    for i, d in enumerate(kept):
        if d["n_segments"] < 4:
            continue
        for other in kept[i + 1 : i + 400]:
            if other["l3_key"] != d["l3_key"]:
                continue
            direct = shape_similarity(d["norm_segments"], other["norm_segments"])
            if direct >= 0.9:
                continue
            mirrored = shape_similarity(d["norm_segments_mirror"], other["norm_segments"])
            if mirrored >= 0.95:
                mirror_pairs.append(
                    {
                        "a": dict(
                            {k: v for k, v in d.items() if not k.startswith("norm_segments")},
                            segments_norm=[[round(c, 4) for pp in seg for c in pp] for seg in d["norm_segments"][:400]],
                        ),
                        "b": dict(
                            {k: v for k, v in other.items() if not k.startswith("norm_segments")},
                            segments_norm=[[round(c, 4) for pp in seg for c in pp] for seg in other["norm_segments"][:400]],
                        ),
                        "direct_shape_similarity": round(direct, 4),
                        "mirrored_shape_similarity": round(mirrored, 4),
                    }
                )
                break
        if len(mirror_pairs) >= 30:
            break
    # rotation twins: same object placed at 90/180/270 degrees.
    # v0.1 normalization removes page position and uniform scale but NOT rotation.
    rotation_twins = []
    for i, d in enumerate(kept):
        if d["n_segments"] < 6:
            continue
        for other in kept[i + 1 : i + 600]:
            direct = shape_similarity(d["norm_segments"], other["norm_segments"])
            if direct >= 0.9:
                continue
            for k, field in ((90, "norm_segments_rot90"), (180, "norm_segments_rot180"), (270, "norm_segments_rot270")):
                rot = shape_similarity(d[field], other["norm_segments"])
                if rot >= 0.95:
                    rotation_twins.append(
                        {
                            "degrees": k,
                            "direct_shape_similarity": round(direct, 4),
                            "rotated_shape_similarity": round(rot, 4),
                            "same_l3_key": d["l3_key"] == other["l3_key"],
                            "a": dict(
                                {kk: vv for kk, vv in d.items() if not kk.startswith("norm_segments")},
                                segments_norm=[[round(c, 4) for pp in seg for c in pp] for seg in d["norm_segments"][:400]],
                            ),
                            "b": dict(
                                {kk: vv for kk, vv in other.items() if not kk.startswith("norm_segments")},
                                segments_norm=[[round(c, 4) for pp in seg for c in pp] for seg in other["norm_segments"][:400]],
                            ),
                        }
                    )
                    break
            if rotation_twins and rotation_twins[-1]["a"]["bbox"] == d["bbox"]:
                break
        if len(rotation_twins) >= 20:
            break

    collisions.sort(key=lambda c: (c["min_pair_shape_similarity"], -c["n_instances"]))
    payload = {
        "pdf": str(path.relative_to(ROOT)) if str(path).startswith(str(ROOT)) else str(path),
        "page_index": args.page,
        "clip": args.clip,
        "node_weld_pt": NODE_Q,
        "shape_grid": SHAPE_Q,
        "total_components": len(comps),
        "components_in_symbol_range": len(kept),
        "filters": {"min_seg": args.min_seg, "max_seg": args.max_seg,
                    "max_size_pt": args.max_size, "min_cycles": args.min_cycles,
                    "min_branch": args.min_branch},
        "distinct_l3_keys": len(by_l3),
        "l3_keys_with_multiple_shapes": len(collisions),
        "components_in_colliding_keys": colliding_components,
        "shape_cluster_threshold": args.shape_threshold,
        "mirror_pair_instances": len(mirror_pairs),
        "rotation_twin_instances": len(rotation_twins),
        "collisions": collisions[:40],
        "mirror_pairs": mirror_pairs[:20],
        "rotation_twins": rotation_twins[:20],
    }
    ART.mkdir(parents=True, exist_ok=True)
    (ART / args.out).write_text(json.dumps(payload, ensure_ascii=False, indent=1), encoding="utf-8")
    print(json.dumps({k: v for k, v in payload.items() if k not in ("collisions", "mirror_pairs")}, ensure_ascii=False, indent=1))
    print("wrote", ART / args.out)


if __name__ == "__main__":
    main()
