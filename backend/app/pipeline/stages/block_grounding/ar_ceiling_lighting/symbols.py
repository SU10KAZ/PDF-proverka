"""Классификация символов плана по эталонам легенды самого листа.

Кластеризация цветных элементов (union-find по примыканию bbox через
SpatialIndex) → структурная сигнатура кластера → сравнение с шаблонами
легенды. Несовпадение не «дотягивается» до ближайшего типа: кластер
честно уходит в unresolved_symbol с причиной.
"""
from __future__ import annotations

import collections
import re

from .legend import ELEV_RE
from .spatial import SpatialIndex, bbox_gap, seg_angle_deg

# насколько размеры элементов кластера могут отличаться от эталона легенды
SIZE_TOL = 0.28
MAX_SYMBOL_ELEMENT = 42.0  # pt; крупнее — не элемент условного обозначения


def _bbox_of_segment(s):
    return (min(s["p1"][0], s["p2"][0]), min(s["p1"][1], s["p2"][1]),
            max(s["p1"][0], s["p2"][0]), max(s["p1"][1], s["p2"][1]))


def _inside(bb, zone):
    return bb[0] >= zone[0] and bb[1] >= zone[1] and bb[2] <= zone[2] and bb[3] <= zone[3]


def collect_symbol_elements(inv: dict, scope_of, legend_zone) -> list[dict]:
    """Цветные (red/green) элементы block_scope — кандидаты в символы."""
    elements = []

    def push(kind, bbox, color, ref):
        if bbox[2] - bbox[0] > MAX_SYMBOL_ELEMENT or bbox[3] - bbox[1] > MAX_SYMBOL_ELEMENT:
            return
        if scope_of(bbox) != "block":
            return
        if legend_zone and _inside(bbox, legend_zone):
            return
        elements.append({"eid": len(elements), "kind": kind, "bbox": bbox,
                         "color": color, "ref": ref})

    for c in inv["circles"]:
        if c["color_family"] not in ("red", "green"):
            continue
        r = c["d"] / 2
        push("circle", (c["center"][0] - r, c["center"][1] - r, c["center"][0] + r, c["center"][1] + r),
             c["color_family"], c)
    for q in inv["quads"]:
        if q["color_family"] in ("red", "green"):
            push("rect", q["bbox"], q["color_family"], q)
    for s in inv["segments"]:
        if s["kind"] != "l" or s["color_family"] not in ("red", "green"):
            continue
        push("line", _bbox_of_segment(s), s["color_family"], s)
    return elements


def cluster_elements(elements: list[dict], *, join_gap: float = 1.2) -> list[list[dict]]:
    index = SpatialIndex(cell=8.0)
    for el in elements:
        index.insert(el["eid"], el["bbox"])
    parent = list(range(len(elements)))

    def find(a):
        while parent[a] != a:
            parent[a] = parent[parent[a]]
            a = parent[a]
        return a

    for el in elements:
        for oid in index.query(el["bbox"], pad=join_gap):
            if oid == el["eid"]:
                continue
            if bbox_gap(el["bbox"], elements[oid]["bbox"]) <= join_gap:
                ra, rb = find(el["eid"]), find(oid)
                if ra != rb:
                    parent[max(ra, rb)] = min(ra, rb)
    groups: dict[int, list[dict]] = collections.defaultdict(list)
    for el in elements:
        groups[find(el["eid"])].append(el)
    return [groups[k] for k in sorted(groups)]


def cluster_signature(cluster: list[dict], texts_index) -> dict:
    xs0 = min(el["bbox"][0] for el in cluster)
    ys0 = min(el["bbox"][1] for el in cluster)
    xs1 = max(el["bbox"][2] for el in cluster)
    ys1 = max(el["bbox"][3] for el in cluster)
    bbox = (round(xs0, 2), round(ys0, 2), round(xs1, 2), round(ys1, 2))
    circles = sorted(round(el["ref"]["d"], 1) for el in cluster if el["kind"] == "circle")
    rects = sorted((round(min(el["ref"]["w"], el["ref"]["h"]), 1),
                    round(max(el["ref"]["w"], el["ref"]["h"]), 1))
                   for el in cluster if el["kind"] == "rect")
    n_axis = n_diag = 0
    for el in cluster:
        if el["kind"] != "line":
            continue
        ang = seg_angle_deg(el["ref"]["p1"], el["ref"]["p2"])
        if ang < 12 or ang > 168 or 78 < ang < 102:
            n_axis += 1
        else:
            n_diag += 1
    colors = sorted({el["color"] for el in cluster})
    letters = []
    for t in texts_index(bbox, pad=1.5):
        val = re.sub(r"\s+", "", t["text"])
        if len(val) <= 2 and val.isalpha():
            letters.append(val.upper().replace("M", "М"))
    return {"bbox": bbox, "circles": circles, "rects": rects,
            "n_axis_lines": n_axis, "n_diag_lines": n_diag,
            "colors": colors, "inner_letters": sorted(set(letters))}


def _dims_match(a: list, b: list) -> bool:
    if len(a) != len(b):
        return False
    for va, vb in zip(a, b):
        if isinstance(va, tuple):
            if any(abs(x - y) > SIZE_TOL * max(y, 1.0) for x, y in zip(va, vb)):
                return False
        elif abs(va - vb) > SIZE_TOL * max(vb, 1.0):
            return False
    return True


def match_template(sig: dict, templates: list[dict]) -> tuple[dict | None, list[str]]:
    """Единственный строго совпавший шаблон или (None, причины)."""
    reasons = []
    matched = []
    for tpl in templates:
        ts = tpl["signature"]
        if tpl["kind"] in ("group_label", "ceiling_type_tag", "ceiling_elevation_tag",
                           "unresolved_legend_row"):
            continue  # текстовые/потолочные эталоны матчатся отдельными путями
        if not _dims_match(sig["circles"], ts["circles"]):
            reasons.append(f"{tpl['kind']}: окружности {sig['circles']} != {ts['circles']}")
            continue
        if not _dims_match(sig["rects"], ts["rects"]):
            reasons.append(f"{tpl['kind']}: рамки {sig['rects']} != {ts['rects']}")
            continue
        if sig["n_diag_lines"] != ts["n_diag_lines"]:
            reasons.append(f"{tpl['kind']}: диагонали {sig['n_diag_lines']} != {ts['n_diag_lines']}")
            continue
        if abs(sig["n_axis_lines"] - ts["n_axis_lines"]) > 0:
            reasons.append(f"{tpl['kind']}: осевые {sig['n_axis_lines']} != {ts['n_axis_lines']}")
            continue
        tpl_colors = sorted(k for k in ts["colors"] if k in ("red", "green"))
        if tpl_colors and sig["colors"] != tpl_colors:
            reasons.append(f"{tpl['kind']}: цвет {sig['colors']} != {tpl_colors}")
            continue
        if ts["inner_letters"] and sig["inner_letters"] != ts["inner_letters"]:
            reasons.append(f"{tpl['kind']}: буквы {sig['inner_letters']} != {ts['inner_letters']}")
            continue
        if not ts["inner_letters"] and "М" in sig["inner_letters"]:
            reasons.append(f"{tpl['kind']}: лишняя буква М в кластере")
            continue
        matched.append(tpl)
    if len(matched) == 1:
        return matched[0], []
    if len(matched) > 1:
        return None, [f"неоднозначно: {[t['kind'] for t in matched]}"]
    return None, reasons


def classify_clusters(clusters: list[list[dict]], templates: list[dict], texts_index) -> list[dict]:
    """Классификация кластеров; составные кластеры делятся по рамкам."""
    out = []
    for cluster in clusters:
        sig = cluster_signature(cluster, texts_index)
        tpl, reasons = match_template(sig, templates)
        if tpl is not None:
            out.append(_symbol(cluster, sig, tpl))
            continue
        rects = [el for el in cluster if el["kind"] == "rect"]
        if len(rects) >= 2:
            parts = _split_by_rects(cluster, rects)
            part_syms = []
            for part in parts:
                psig = cluster_signature(part, texts_index)
                ptpl, _ = match_template(psig, templates)
                part_syms.append((part, psig, ptpl))
            if all(ptpl is not None for _, _, ptpl in part_syms):
                for part, psig, ptpl in part_syms:
                    out.append(_symbol(part, psig, ptpl, split_from_multi=True))
                continue
            out.append({"kind": "unresolved_symbol", "signature": sig, "bbox": sig["bbox"],
                        "reason": "template_residual_above_threshold",
                        "detail": "составной кластер: часть секций не совпала с эталонами",
                        "residuals": reasons[:6]})
            continue
        out.append({"kind": "unresolved_symbol", "signature": sig, "bbox": sig["bbox"],
                    "reason": "template_residual_above_threshold", "residuals": reasons[:6]})
    for i, sym in enumerate(out):
        sym["symbol_id"] = f"sym-{i + 1}"
        b = sym["bbox"]
        sym["center"] = (round((b[0] + b[2]) / 2, 2), round((b[1] + b[3]) / 2, 2))
    return out


def _symbol(cluster, sig, tpl, *, split_from_multi: bool = False) -> dict:
    return {"kind": tpl["kind"], "kind_label": tpl["label"], "bbox": sig["bbox"],
            "signature": sig, "split_from_multi": split_from_multi,
            "element_refs": sorted(el["eid"] for el in cluster)}


def _split_by_rects(cluster, rects) -> list[list[dict]]:
    parts = [[r] for r in rects]
    centers = [((r["bbox"][0] + r["bbox"][2]) / 2, (r["bbox"][1] + r["bbox"][3]) / 2) for r in rects]
    for el in cluster:
        if el["kind"] == "rect":
            continue
        c = ((el["bbox"][0] + el["bbox"][2]) / 2, (el["bbox"][1] + el["bbox"][3]) / 2)
        best = min(range(len(rects)),
                   key=lambda i: (c[0] - centers[i][0]) ** 2 + (c[1] - centers[i][1]) ** 2)
        parts[best].append(el)
    return parts


# ---------------------------------------------------------------- потолки

def detect_ceiling_markers(inv: dict, scope_of, legend_zone) -> tuple[list[dict], list[dict]]:
    """Составные потолочные маркеры: рамка типа + рамка отметки.

    Обе рамки синие; поле с текстом вида «+2.850» — отметка, короткое
    поле — тип. Составной маркер подтверждается геометрией: рамки
    выровнены по X и почти касаются по вертикали.
    """
    frames = []
    for q in inv["quads"]:
        if q["color_family"] != "blue":
            continue
        if scope_of(q["bbox"]) != "block":
            continue
        if legend_zone and _inside(q["bbox"], legend_zone):
            continue
        frames.append(dict(q))
    chars = []
    for t in inv["texts"]:
        for ch in t["chars"]:
            chars.append({"c": ch["c"], "bbox": ch["bbox"], "layer": t["layer"]})
    char_index = SpatialIndex(cell=12.0)
    for i, ch in enumerate(chars):
        char_index.insert(i, ch["bbox"])

    for f in frames:
        b = f["bbox"]
        inner = []
        for i in char_index.query(b):
            ch = chars[i]
            cx = (ch["bbox"][0] + ch["bbox"][2]) / 2
            cy = (ch["bbox"][1] + ch["bbox"][3]) / 2
            if b[0] <= cx <= b[2] and b[1] <= cy <= b[3]:
                inner.append(ch)
        inner.sort(key=lambda ch: ch["bbox"][0])
        f["text"] = "".join(ch["c"] for ch in inner).strip()
        f["is_elevation"] = bool(ELEV_RE.match(f["text"]))

    markers = []
    unpaired = []
    used: set[int] = set()
    frames.sort(key=lambda f: (f["bbox"][1], f["bbox"][0]))
    for i, f in enumerate(frames):
        if i in used or f["is_elevation"]:
            continue
        best = None
        for j, g in enumerate(frames):
            if j in used or j == i or not g["is_elevation"]:
                continue
            dx = abs((f["bbox"][0] + f["bbox"][2]) / 2 - (g["bbox"][0] + g["bbox"][2]) / 2)
            dy = g["bbox"][1] - f["bbox"][3]  # отметка ниже типа
            if dx <= 4.0 and -1.0 <= dy <= 8.0:
                if best is None or dy < best[0]:
                    best = (dy, j)
        if best is not None:
            used.update((i, best[1]))
            g = frames[best[1]]
            bb = (min(f["bbox"][0], g["bbox"][0]), f["bbox"][1], max(f["bbox"][2], g["bbox"][2]), g["bbox"][3])
            markers.append({
                "marker_id": f"ceil-{len(markers) + 1}",
                "ceiling_type": f["text"] or None,
                "elevation": g["text"].replace(",", ".") or None,
                "bbox": tuple(round(v, 2) for v in bb),
                "center": (round((bb[0] + bb[2]) / 2, 2), round((bb[1] + bb[3]) / 2, 2)),
                "type_bbox": f["bbox"],
                "elevation_bbox": g["bbox"],
                "evidence": {"type_quad_did": f["did"], "elevation_quad_did": g["did"]},
            })
    for i, f in enumerate(frames):
        if i not in used:
            unpaired.append({"bbox": f["bbox"], "text": f.get("text") or "",
                             "is_elevation": f["is_elevation"], "did": f["did"]})
    return markers, unpaired


# ------------------------------------------------------------ цифры групп

def split_number_labels(inv: dict, scope_of, legend_zone) -> list[dict]:
    """Красные числовые подписи block_scope с раскроем «слипшихся» спанов.

    Спан делится на числа по геометрии СИМВОЛОВ: разрыв по x больше
    0.45 высоты знака или сдвиг базовой линии больше 0.3 высоты — граница
    чисел («34» из двух подписей групп 3 и 4 не станет группой 34).
    """
    labels = []
    for t in inv["texts"]:
        if t["color_family"] not in ("red", "black"):
            continue
        if scope_of(t["bbox"]) != "block":
            continue
        if legend_zone and _inside(t["bbox"], legend_zone):
            continue
        digits = [ch for ch in t["chars"] if ch["c"].isdigit()]
        if not digits or any(not (ch["c"].isdigit() or ch["c"].isspace()) for ch in t["chars"]):
            continue
        # Знаки идут в логическом порядке письма; вертикальные размеры
        # повёрнуты на 90° (ход по y). Порядок НЕ пересортировываем —
        # раскраиваем по геометрическим разрывам вдоль доминирующей оси.
        axis = 0
        if len(digits) >= 2:
            dx = abs(digits[-1]["bbox"][0] - digits[0]["bbox"][0])
            dy_span = abs(digits[-1]["bbox"][1] - digits[0]["bbox"][1])
            axis = 1 if dy_span > dx else 0
        runs: list[list[dict]] = [[digits[0]]]
        for prev, cur in zip(digits, digits[1:]):
            size = max(prev["bbox"][3] - prev["bbox"][1], prev["bbox"][2] - prev["bbox"][0], 1.0)
            step = abs((cur["bbox"][axis] + cur["bbox"][axis + 2]) / 2
                       - (prev["bbox"][axis] + prev["bbox"][axis + 2]) / 2)
            char_w = prev["bbox"][axis + 2] - prev["bbox"][axis]
            cross = abs(cur["bbox"][3 - axis] - prev["bbox"][3 - axis])
            if step > char_w + 0.45 * size or cross > 0.3 * size:
                runs.append([])
            runs[-1].append(cur)
        for run in runs:
            bb = (min(ch["bbox"][0] for ch in run), min(ch["bbox"][1] for ch in run),
                  max(ch["bbox"][2] for ch in run), max(ch["bbox"][3] for ch in run))
            labels.append({
                "label_id": f"num-{len(labels) + 1}",
                "value": "".join(ch["c"] for ch in run),
                "color_family": t["color_family"],
                "bbox": tuple(round(v, 2) for v in bb),
                "center": (round((bb[0] + bb[2]) / 2, 2), round((bb[1] + bb[3]) / 2, 2)),
                "tid": t["tid"],
                "layer": t["layer"],
                "split_from_span": len(runs) > 1,
            })
    return labels
