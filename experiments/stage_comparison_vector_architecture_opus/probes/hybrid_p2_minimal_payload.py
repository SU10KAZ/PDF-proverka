#!/usr/bin/env python3
"""Probe HYBRID-2: build a MINIMAL AI payload from Track A artifacts and measure it.

The payload carries only: what changed, which objects are affected, before/after
values, structural context, uncertainty. Everything else (hashes, tolerance
tables, pattern ids, full text dumps, per-pair caveat boilerplate) is dropped.

Run from repo root with a python that has tiktoken:
    <venv>/bin/python -m experiments.stage_comparison_vector_architecture_opus.probes.hybrid_p2_minimal_payload
"""
from __future__ import annotations

import collections
import json
import math
from pathlib import Path

import tiktoken

from experiments.stage_comparison_vector_blocks import comparator as CMP

ENC = tiktoken.get_encoding("o200k_base")
ROOT = Path(__file__).resolve().parents[3]
TRACK_A = ROOT / "experiments" / "stage_comparison_vector_blocks" / "artifacts"
OUT = Path(__file__).resolve().parents[1] / "artifacts"
SEP = (",", ":")
DENSE_SEGMENT_LIMIT = 6000  # above this we skip unmatched-geometry localisation

# The same five pairs Track A put through its AI experiment.
PAIRS = ("ss_scheme_text_changed", "ss_table_graphic", "ar_plan", "vk_nodes", "eom_singleline_changed")
VARIANTS = {"span": ("span", False), "span_pagectx": ("span", True), "object": ("object", False)}


def dumps(obj) -> str:
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=SEP)


def ntok(obj) -> int:
    return len(ENC.encode(dumps(obj)))


def load(*parts) -> dict:
    return json.loads((TRACK_A.joinpath(*parts)).read_text(encoding="utf-8"))


# ---------------------------------------------------------------- geo helpers
def region_name(x: float, y: float) -> str:
    col = "слева" if x < 1 / 3 else ("центр" if x < 2 / 3 else "справа")
    row = "верх" if y < 1 / 3 else ("середина" if y < 2 / 3 else "низ")
    return f"{row}-{col}"


def text_positions(desc: dict) -> dict[str, list[tuple[float, float]]]:
    pos: dict[str, list[tuple[float, float]]] = collections.defaultdict(list)
    for t in desc["texts"]:
        pos[t["text"]].append((t["x_norm"], t["y_norm"]))
    return pos


def nearest_context(desc: dict, x: float, y: float, exclude: set[str], k: int = 3) -> list[str]:
    """The k nearest text spans that did NOT change — the 'object' the change sits on."""
    cands = []
    for t in desc["texts"]:
        if t["text"] in exclude:
            continue
        if len(t["text"].strip()) < 2:
            continue
        d = math.hypot(t["x_norm"] - x, t["y_norm"] - y)
        cands.append((d, t["text"]))
    cands.sort()
    out, seen = [], set()
    for d, s in cands:
        if s in seen:
            continue
        seen.add(s)
        out.append(s)
        if len(out) >= k:
            break
    return out


def cluster(points: list[tuple[float, float, dict]], eps: float = 0.06) -> list[list[dict]]:
    """Single-linkage spatial clustering of change events."""
    n = len(points)
    parent = list(range(n))

    def find(a):
        while parent[a] != a:
            parent[a] = parent[parent[a]]
            a = parent[a]
        return a

    def union(a, b):
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[rb] = ra

    for i in range(n):
        for j in range(i + 1, n):
            if math.hypot(points[i][0] - points[j][0], points[i][1] - points[j][1]) <= eps:
                union(i, j)
    groups: dict[int, list[dict]] = collections.defaultdict(list)
    for i, p in enumerate(points):
        groups[find(i)].append(p[2])
    return list(groups.values())


def unmatched_centers(src_desc: dict, dst_desc: dict, tolerance: float):
    """Localised uncovered segments of src against dst (my own wrapper; comparator
    only returns primitive ids, which cannot be turned into a crop)."""
    src, src_total = CMP._segment_features(src_desc["geometry"]["primitives"])
    dst, dst_total = CMP._segment_features(dst_desc["geometry"]["primitives"])
    if src_total > DENSE_SEGMENT_LIMIT or dst_total > DENSE_SEGMENT_LIMIT:
        return None, src_total, dst_total
    cell = max(tolerance * 2.0, 0.001)
    buckets = collections.defaultdict(list)
    for f in dst:
        buckets[(round(f["center"][0] / cell), round(f["center"][1] / cell))].append(f)
    misses = []
    for first in src:
        gx, gy = round(first["center"][0] / cell), round(first["center"][1] / cell)
        found = False
        for x in range(gx - 2, gx + 3):
            for y in range(gy - 2, gy + 3):
                for second in buckets.get((x, y), []):
                    if CMP._distance(first["center"], second["center"]) > tolerance * 3.0:
                        continue
                    if abs(first["length"] - second["length"]) > tolerance * 8.0:
                        continue
                    if CMP._angle_distance(first["angle"], second["angle"]) > max(1.0, tolerance * 500):
                        continue
                    found = True
                    break
                if found:
                    break
            if found:
                break
        if not found:
            misses.append(first)
    return misses, src_total, dst_total


def miss_regions(misses, min_size: int = 8):
    pts = [(m["center"][0], m["center"][1], m) for m in misses]
    out = []
    for grp in cluster(pts, eps=0.05):
        if len(grp) < min_size:
            continue
        xs = [g["center"][0] for g in grp]
        ys = [g["center"][1] for g in grp]
        out.append(
            {
                "n_segments": len(grp),
                "bbox": [round(min(xs), 3), round(min(ys), 3), round(max(xs), 3), round(max(ys), 3)],
                "where": region_name(sum(xs) / len(xs), sum(ys) / len(ys)),
                "total_length": round(sum(g["length"] for g in grp), 4),
            }
        )
    out.sort(key=lambda r: -r["n_segments"])
    return out[:5]


# ---------------------------------------------------------------- the payload
def object_items(desc: dict) -> list[dict]:
    """Text LINES: the cheapest possible object layer (probe HYBRID-3)."""
    from experiments.stage_comparison_vector_architecture_opus.probes import (
        hybrid_p3_object_layer_gain as p3,
    )
    return [
        {"text": ln["text"], "x_norm": ln["x"], "y_norm": ln["y"], "category": "line"}
        for ln in p3.to_lines(desc)
    ]


def diff_items(litems: list[dict], ritems: list[dict]):
    """Same rule the comparator uses: multiset diff, then nearest-position pairing."""
    lc, rc = collections.Counter(i["text"] for i in litems), collections.Counter(i["text"] for i in ritems)
    removed_c, added_c = lc - rc, rc - lc
    lun = [i for i in litems if removed_c[i["text"]] > 0]
    run = [i for i in ritems if added_c[i["text"]] > 0]
    used, values = set(), []
    for first in lun:
        cands = [(k, s) for k, s in enumerate(run) if k not in used]
        if not cands:
            break
        k, second = min(cands, key=lambda row: math.hypot(first["x_norm"] - row[1]["x_norm"],
                                                          first["y_norm"] - row[1]["y_norm"]))
        d = math.hypot(first["x_norm"] - second["x_norm"], first["y_norm"] - second["y_norm"])
        if d <= 0.04:
            used.add(k)
            values.append({"left": first["text"], "right": second["text"],
                           "x": first["x_norm"], "y": first["y_norm"]})
    paired_l = {v["left"] for v in values}
    paired_r = {v["right"] for v in values}
    removed = [i for i in lun if i["text"] not in paired_l]
    added = [i for i in run if i["text"] not in paired_r]
    return values, added, removed


def crop_attributed(pair_id: str) -> set[str]:
    """Strings the page-context predicate (probe HYBRID-8) proves are crop-window
    artefacts: present on the other document's page, inside this crop rect,
    outside the other crop rect, and nowhere inside the other crop rect."""
    path = OUT / "hybrid_crop_attribution.json"
    if not path.is_file():
        return set()
    row = json.loads(path.read_text("utf-8")).get(pair_id, {})
    return set(row.get("attributable_strings_right_only", [])) | set(
        row.get("attributable_strings_left_only", [])
    )


def build(pair_id: str, level: str = "span", use_page_context: bool = False) -> dict:
    left = load("descriptions", pair_id, "left", "vector_block.json")
    right = load("descriptions", pair_id, "right", "vector_block.json")
    cmp_ = load("comparisons", pair_id, "comparison.json")

    lq = cmp_["text"]["left_layer_quality"]["status"]
    rq = cmp_["text"]["right_layer_quality"]["status"]
    text_reliable = bool(cmp_["text"]["reliable"])
    geom = cmp_["geometry"]
    lpos, rpos = text_positions(left), text_positions(right)

    changed_strings = set()
    events: list[tuple[float, float, dict]] = []

    # RULE: text the extractor itself flagged undecodable is never shipped to a model.
    # The block is handed to a targeted Vision task instead (see hybrid task contract).
    text_undecodable = (not text_reliable) or ("UNDECODABLE" in (lq, rq))
    crop_strings = crop_attributed(pair_id) if use_page_context else set()

    if text_undecodable:
        pass
    elif level == "object":
        values, added, removed = diff_items(object_items(left), object_items(right))
        for vc in values:
            changed_strings.update((vc["left"], vc["right"]))
            events.append((vc["x"], vc["y"], {"kind": "value", "before": vc["left"], "after": vc["right"]}))
        for i in added:
            changed_strings.add(i["text"])
            events.append((i["x_norm"], i["y_norm"], {"kind": "added", "after": i["text"]}))
        for i in removed:
            changed_strings.add(i["text"])
            events.append((i["x_norm"], i["y_norm"], {"kind": "removed", "before": i["text"]}))
    else:
        for vc in cmp_["text"]["value_changes"]:
            changed_strings.update((vc["left"], vc["right"]))
            p = lpos.get(vc["left"]) or rpos.get(vc["right"])
            if not p:
                continue
            x, y = p[0]
            events.append((x, y, {"kind": "value", "before": vc["left"], "after": vc["right"]}))
        for s in cmp_["text"]["added"]:
            changed_strings.add(s)
            p = rpos.get(s)
            if not p:
                continue
            x, y = p[0]
            events.append((x, y, {"kind": "added", "after": s}))
        for s in cmp_["text"]["removed"]:
            changed_strings.add(s)
            p = lpos.get(s)
            if not p:
                continue
            x, y = p[0]
            events.append((x, y, {"kind": "removed", "before": s}))

    n_before = len(events)
    if crop_strings:
        events = [
            e for e in events
            if not (e[2].get("after") in crop_strings or e[2].get("before") in crop_strings)
        ]
    n_crop_suppressed = n_before - len(events)

    # Text clipped by the crop edge: a removed string and an added string at the same
    # place where one is a strict prefix of the other. Not a value change.
    n_truncated = 0
    if use_page_context:
        rem = [e for e in events if e[2]["kind"] in ("removed", "value")]
        add = [e for e in events if e[2]["kind"] in ("added", "value")]
        drop = set()
        for i, e in enumerate(events):
            if id(e) in drop:
                continue
            a = e[2].get("before") or e[2].get("after")
            if e[2]["kind"] == "value":
                x, y = e[2]["before"], e[2]["after"]
                if x and y and x != y and (x.startswith(y) or y.startswith(x)):
                    drop.add(id(e))
                    n_truncated += 1
                continue
            for f in events:
                if f is e or id(f) in drop:
                    continue
                if e[2]["kind"] == f[2]["kind"]:
                    continue
                b = f[2].get("before") or f[2].get("after")
                if not a or not b or a == b:
                    continue
                if (a.startswith(b) or b.startswith(a)) and math.hypot(e[0] - f[0], e[1] - f[1]) <= 0.06:
                    drop.add(id(e))
                    drop.add(id(f))
                    n_truncated += 1
                    break
        events = [e for e in events if id(e) not in drop]

    changes = []
    for idx, grp in enumerate(cluster(events, eps=0.06), start=1):
        xs, ys = [], []
        for x, y, ev in events:
            if ev in grp:
                xs.append(x)
                ys.append(y)
        cx, cy = (sum(xs) / len(xs), sum(ys) / len(ys)) if xs else (0.0, 0.0)
        vals = [[e["before"], e["after"]] for e in grp if e["kind"] == "value"]
        seen = {v for pair_ in vals for v in pair_}
        added = [e["after"] for e in grp if e["kind"] == "added" and e["after"] not in seen]
        removed = [e["before"] for e in grp if e["kind"] == "removed" and e["before"] not in seen]
        changes.append(
            {
                "id": f"C{idx}",
                "at": [round(cx, 3), round(cy, 3)],
                "where": region_name(cx, cy),
                "context": nearest_context(right if added else left, cx, cy, changed_strings),
                **({"values": vals} if vals else {}),
                **({"added": added} if added else {}),
                **({"removed": removed} if removed else {}),
            }
        )
    changes.sort(key=lambda c: (c["at"][1], c["at"][0]))
    for i, c in enumerate(changes, start=1):
        c["id"] = f"C{i}"

    tol = geom["selected_tolerance"]
    l_miss, l_tot, r_tot = unmatched_centers(left, right, tol)
    r_miss, _, _ = unmatched_centers(right, left, tol)
    only_right = miss_regions(r_miss) if r_miss is not None else None
    only_left = miss_regions(l_miss) if l_miss is not None else None

    topo = cmp_["topology"]["counts"]
    payload = {
        "pair": pair_id,
        "level": level,
        "context": {
            "geometry_match": round(geom["similarity"], 3),
            "text_match": round(cmp_["text"]["effective_similarity"], 3),
            "components": [topo["connected_components"]["left"], topo["connected_components"]["right"]],
            "branches": [topo["branch_points"]["left"], topo["branch_points"]["right"]],
            "closed_contours": [topo["closed_contours"]["left"], topo["closed_contours"]["right"]],
            "text_items": [cmp_["text"]["left_total"], cmp_["text"]["right_total"]],
        },
        "changes": changes,
        "crop_window": {
            "suppressed_events": n_crop_suppressed,
            "truncated_at_crop_edge": n_truncated,
            "reason": "текст найден на странице другой версии вне сравниваемой рамки блока — это разная граница кадрирования, не изменение проекта",
        } if crop_strings else None,
        "geometry_only_in_right": only_right,
        "geometry_only_in_left": only_left,
        "uncertainty": [],
    }
    if not text_reliable or "UNDECODABLE" in (lq, rq):
        payload["uncertainty"].append(
            {"code": "TEXT_LAYER_UNDECODABLE", "sides": [lq, rq],
             "means": "значения подписей не читаются из вектора; сравнение значений не выполнено"}
        )
    if geom.get("encoding_rewrite_suspected"):
        payload["uncertainty"].append(
            {"code": "ENCODING_REWRITE", "means": "изменилось только упаковывание путей PDF; счётчики примитивов и топологии не являются признаком изменения"}
        )
    if l_miss is None:
        payload["uncertainty"].append(
            {"code": "GEOMETRY_TOO_DENSE_TO_LOCALISE", "segments": [l_tot, r_tot],
             "means": "несовпавшая геометрия не локализована"}
        )
    if only_right and not only_left:
        payload["uncertainty"].append(
            {"code": "EXTRA_CONTOUR_ONLY_IN_RIGHT",
             "means": "контур есть только справа: либо новый элемент, либо разная граница кадра"}
        )
    return payload


# ---------------------------------------------------------------- measurement
def track_a_pair_tokens(pair_id: str) -> int:
    from experiments.stage_comparison_vector_architecture_opus.probes import (
        hybrid_p1_prompt_composition as p1,
    )
    for payload in p1.build_payloads():
        if payload["pair_id"] == pair_id:
            return p1.tok(payload)
    raise KeyError(pair_id)


def main() -> None:
    p1 = __import__(
        "experiments.stage_comparison_vector_architecture_opus.probes.hybrid_p1_prompt_composition",
        fromlist=["x"],
    )
    ta = {pl["pair_id"]: p1.tok(pl) for pl in p1.build_payloads()}
    res = {"per_pair": {}, "totals": {}}
    dump: dict = {}
    tot = {"track_a": 0, **{k: 0 for k in VARIANTS}}
    for pair_id in PAIRS:
        tot["track_a"] += ta[pair_id]
        row = {"track_a_l3_plus_diff_tokens": ta[pair_id]}
        for name, (level, page_ctx) in VARIANTS.items():
            mp = build(pair_id, level, page_ctx)
            n = ntok(mp)
            tot[name] += n
            dump.setdefault(pair_id, {})[name] = mp
            row[name] = {
                "tokens": n,
                "bytes_utf8": len(dumps(mp).encode("utf-8")),
                "clusters": len(mp["changes"]),
                "uncertainty": [u["code"] for u in mp["uncertainty"]],
                "crop_window": mp.get("crop_window"),
                "reduction_vs_track_a": round(ta[pair_id] / max(n, 1), 2),
            }
        res["per_pair"][pair_id] = row
    res["totals"] = {
        "track_a_tokens": tot["track_a"],
        "minimal_span_tokens": tot["span"],
        "reduction_span": round(tot["track_a"] / tot["span"], 2),
        "minimal_span_pagectx_tokens": tot["span_pagectx"],
        "reduction_span_pagectx": round(tot["track_a"] / tot["span_pagectx"], 2),
        "minimal_object_tokens": tot["object"],
        "reduction_object": round(tot["track_a"] / tot["object"], 2),
    }
    OUT.mkdir(parents=True, exist_ok=True)
    (OUT / "hybrid_minimal_payloads.json").write_text(
        json.dumps(dump, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    (OUT / "hybrid_minimal_payload_sizes.json").write_text(
        json.dumps(res, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    print(json.dumps(res["totals"], ensure_ascii=False, indent=2))
    for pid, r in res["per_pair"].items():
        print(f"{pid:24s} trackA {r['track_a_l3_plus_diff_tokens']:>6}  "
              f"span {r['span']['tokens']:>5} ({r['span']['reduction_vs_track_a']}x, {r['span']['clusters']} cl)  "
              f"pagectx {r['span_pagectx']['tokens']:>5}  object {r['object']['tokens']:>5}")


if __name__ == "__main__":
    main()
