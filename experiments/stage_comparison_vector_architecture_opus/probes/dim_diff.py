#!/usr/bin/env python3
"""dim_* probe: diff two blocks at the level of DIMENSION OBJECTS.

Each dimension is (value, measured span between two extension-line feet).  The
diff matches dimensions across versions by the LOCATION OF THE MEASURED SPAN,
not by the position of the number, so that a value edit is reported as
    Размер 2500 -> 2700  (at <location>)
instead of an unrelated delete + insert.

Run from repo root:
    python -m experiments.stage_comparison_vector_architecture_opus.probes.dim_diff \
        --left <res.json> --right <res.json> --pair <id> --out <json>
"""
from __future__ import annotations

import argparse
import json
import math
import statistics
from pathlib import Path
from typing import Any

MATCH_TOL = 0.012          # normalised block units for the span midpoint
SPAN_TOL = 0.20            # relative span difference still considered "same place"
DIR_TOL_DEG = 5.0
OFFSET_TOL = 12.0          # how far the whole chain may have shifted sideways


def norm(pt, rect):
    w = max(rect[2] - rect[0], 1e-9)
    h = max(rect[3] - rect[1], 1e-9)
    return ((pt[0] - rect[0]) / w, (pt[1] - rect[1]) / h)


def prep(res: dict[str, Any], only_corroborated: bool, frame: str = "bbox_norm") -> list[dict[str, Any]]:
    rect = res["source"]["block_rect"]
    if frame == "absolute":
        rect = [0.0, 0.0, 1.0, 1.0]      # keep raw PDF points
    out = []
    for d in res["dimensions"]:
        if not d.get("detected"):
            continue
        if only_corroborated and not d.get("scale_ok"):
            continue
        a = norm(d["foot_a"], rect)
        b = norm(d["foot_b"], rect)
        mid = ((a[0] + b[0]) / 2, (a[1] + b[1]) / 2)
        ang = math.degrees(math.atan2(b[1] - a[1], b[0] - a[0])) % 180.0
        out.append({
            "text_id": d["text_id"], "value": d["value"], "text": d["text"],
            "span_pt": d["span_pt"], "a": a, "b": b, "mid": mid, "ang": ang,
            "len_norm": math.dist(a, b),
            "predicted_mm": d.get("predicted_mm"),
        })
    return out


def adiff(x, y):
    d = abs(x - y) % 180.0
    return min(d, 180.0 - d)


def match(left, right, shift=(0.0, 0.0)):
    used = set()
    pairs = []
    for i, l in enumerate(left):
        lm = (l["mid"][0] + shift[0], l["mid"][1] + shift[1])
        best, bestk = None, None
        for j, r in enumerate(right):
            if j in used:
                continue
            if adiff(l["ang"], r["ang"]) > DIR_TOL_DEG:
                continue
            d = math.dist(lm, r["mid"])
            if d > MATCH_TOL:
                continue
            dl = abs(l["len_norm"] - r["len_norm"]) / max(l["len_norm"], 1e-9)
            if dl > SPAN_TOL:
                continue
            k = (round(d, 5), round(dl, 5))
            if best is None or k < bestk:
                best, bestk = j, k
        if best is not None:
            used.add(best)
            pairs.append((i, best, bestk[0]))
    return pairs, used


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--left", required=True)
    ap.add_argument("--right", required=True)
    ap.add_argument("--pair", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--frame", default="bbox_norm", choices=["bbox_norm", "absolute"],
                    help="coordinate frame the two sides are compared in")
    ap.add_argument("--tol", type=float, default=None,
                    help="match tolerance (normalised units, or PDF points for --frame absolute)")
    ap.add_argument("--all-bindings", action="store_true",
                    help="include bindings the scale check rejected (ablation)")
    a = ap.parse_args()
    global MATCH_TOL
    MATCH_TOL = a.tol if a.tol is not None else (6.0 if a.frame == "absolute" else 0.012)
    L = json.loads(Path(a.left).read_text(encoding="utf-8"))
    R = json.loads(Path(a.right).read_text(encoding="utf-8"))
    left = prep(L, not a.all_bindings, a.frame)
    right = prep(R, not a.all_bindings, a.frame)

    pairs, used = match(left, right)
    # global drift: re-match after removing the median offset of the first pass
    if pairs:
        dx = statistics.median(right[j]["mid"][0] - left[i]["mid"][0] for i, j, _ in pairs)
        dy = statistics.median(right[j]["mid"][1] - left[i]["mid"][1] for i, j, _ in pairs)
    else:
        dx = dy = 0.0
    pairs, used = match(left, right, shift=(dx, dy))
    if pairs:
        dx += statistics.median(right[j]["mid"][0] - (left[i]["mid"][0] + dx) for i, j, _ in pairs)
        dy += statistics.median(right[j]["mid"][1] - (left[i]["mid"][1] + dy) for i, j, _ in pairs)
        pairs, used = match(left, right, shift=(dx, dy))

    changed, same = [], 0
    for i, j, d in pairs:
        l, r = left[i], right[j]
        if abs(l["value"] - r["value"]) < 1e-9:
            same += 1
            continue
        span_ratio = r["span_pt"] / l["span_pt"] if l["span_pt"] else 0
        val_ratio = r["value"] / l["value"] if l["value"] else 0
        changed.append({
            "left_value": l["value"], "right_value": r["value"],
            "at_norm": [round(v, 4) for v in l["mid"]],
            "match_distance_norm": round(d, 5),
            "left_span_pt": l["span_pt"], "right_span_pt": r["span_pt"],
            "geometry_moved_with_value": abs(span_ratio - val_ratio) < 0.05,
            "sentence": f"Размер {l['text']} → {r['text']}",
        })
    # ---- pass 1b (RELATION layer): a chain interval may have been re-cut.
    # Before claiming "value X -> Y", test whether the left interval is tiled by
    # several right intervals whose values sum to the left value (split), or the
    # mirror case (merge).  This needs the CHAIN relation, not the dimensions alone.
    matched_left = {i for i, _, _ in pairs}
    retiled = []
    consumed_right: set[int] = set()
    for i, l in enumerate(left):
        if i in matched_left:
            continue
        la = (l["a"][0] + dx, l["a"][1] + dy)
        lb = (l["b"][0] + dx, l["b"][1] + dy)
        if la > lb:
            la, lb = lb, la
        tol_c = min(MATCH_TOL, 3.0)
        L = math.dist(la, lb)
        ux, uy = ((lb[0] - la[0]) / L, (lb[1] - la[1]) / L) if L else (1.0, 0.0)

        def tpos(p):
            return (p[0] - la[0]) * ux + (p[1] - la[1]) * uy

        cand = []
        for j, r in enumerate(right):
            if j in used or j in consumed_right:
                continue
            if adiff(l["ang"], r["ang"]) > DIR_TOL_DEG:
                continue
            ta, tb = sorted((tpos(r["a"]), tpos(r["b"])))
            # must lie on the same line, not merely nearby
            off = abs((r["mid"][0] - la[0]) * -uy + (r["mid"][1] - la[1]) * ux)
            if off > OFFSET_TOL:
                continue
            if ta < -tol_c or tb > L + tol_c:
                continue
            cand.append((ta, tb, j))
        cand.sort()
        # greedy contiguous tiling of [0, L]
        parts, cursor = [], 0.0
        for ta, tb, j in cand:
            if abs(ta - cursor) <= tol_c and tb > cursor:
                parts.append(j)
                cursor = tb
                if cursor >= L - tol_c:
                    break
        if len(parts) < 2 or abs(cursor - L) > tol_c:
            continue
        total = sum(right[j]["value"] for j in parts)
        cover = sum(right[j]["span_pt"] for j in parts)
        if abs(total - l["value"]) / max(l["value"], 1e-9) <= 0.02 and \
           abs(cover - l["span_pt"]) / max(l["span_pt"], 1e-9) <= 0.05:
            for j in parts:
                used.add(j)
                consumed_right.add(j)
            matched_left.add(i)
            vals = " + ".join(right[j]["text"] for j in parts)
            retiled.append({
                "left_value": l["value"],
                "right_values": [right[j]["value"] for j in parts],
                "at": [round(v, 4) for v in l["mid"]],
                "sentence": f"Размерная цепочка: {l['text']} разбит на {vals}",
            })

    # ---- second pass: a dimension whose VALUE changed also MOVES one foot.
    # Anchor on the shared extension-line foot instead of the span midpoint.
    foot_pairs = []
    for i, l in enumerate(left):
        if i in matched_left:
            continue
        la = (l["a"][0] + dx, l["a"][1] + dy)
        lb = (l["b"][0] + dx, l["b"][1] + dy)
        best, bestd = None, None
        for j, r in enumerate(right):
            if j in used:
                continue
            if adiff(l["ang"], r["ang"]) > DIR_TOL_DEG:
                continue
            d = min(math.dist(la, r["a"]), math.dist(la, r["b"]),
                    math.dist(lb, r["a"]), math.dist(lb, r["b"]))
            other = math.dist(((la[0] + lb[0]) / 2, (la[1] + lb[1]) / 2), r["mid"])
            if d > MATCH_TOL or other > 6 * MATCH_TOL:
                continue
            if best is None or d < bestd:
                best, bestd = j, d
        if best is not None:
            used.add(best)
            matched_left.add(i)
            foot_pairs.append((i, best, bestd))
    for i, j, d in foot_pairs:
        l, r = left[i], right[j]
        span_ratio = r["span_pt"] / l["span_pt"] if l["span_pt"] else 0
        val_ratio = r["value"] / l["value"] if l["value"] else 0
        changed.append({
            "left_value": l["value"], "right_value": r["value"],
            "at_norm": [round(v, 4) for v in l["mid"]],
            "match_distance_norm": round(d, 5),
            "matched_by": "shared_extension_foot",
            "left_span_pt": l["span_pt"], "right_span_pt": r["span_pt"],
            "geometry_moved_with_value": abs(span_ratio - val_ratio) < 0.05,
            "sentence": f"Размер {l['text']} → {r['text']}",
        })
    pairs = pairs + foot_pairs
    removed = [{"value": l["value"], "at_norm": [round(v, 4) for v in l["mid"]],
                "sentence": f"Размер {l['text']} удалён"}
               for i, l in enumerate(left) if i not in matched_left]
    added = [{"value": r["value"], "at_norm": [round(v, 4) for v in r["mid"]],
              "sentence": f"Добавлен размер {r['text']}"}
             for j, r in enumerate(right) if j not in used]

    out = {
        "pair": a.pair,
        "frame": a.frame,
        "match_tolerance": MATCH_TOL,
        "only_corroborated_bindings": not a.all_bindings,
        "left_dimensions": len(left), "right_dimensions": len(right),
        "matched": len(pairs), "unchanged": same,
        "global_drift": [round(dx, 5), round(dy, 5)],
        "chain_retiled": retiled,
        "value_changed": changed,
        "removed": removed,
        "added": added,
    }
    Path(a.out).parent.mkdir(parents=True, exist_ok=True)
    Path(a.out).write_text(json.dumps(out, ensure_ascii=False, indent=1), encoding="utf-8")
    print(f"{a.pair}: L={len(left)} R={len(right)} matched={len(pairs)} unchanged={same} "
          f"changed={len(changed)} retiled={len(retiled)} removed={len(removed)} added={len(added)} drift={out['global_drift']}")
    for c in retiled:
        print("   ", c["sentence"], "at", c["at"])
    for c in changed[:12]:
        print("   ", c["sentence"], "at", c["at_norm"],
              "spans", c["left_span_pt"], "->", c["right_span_pt"],
              "geom_consistent" if c["geometry_moved_with_value"] else "TEXT_ONLY_EDIT")


if __name__ == "__main__":
    main()
