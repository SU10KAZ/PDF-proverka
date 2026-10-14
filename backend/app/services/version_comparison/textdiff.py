"""Текст «было → стало» основной области (план §5 шаг 5).

На входе — разбор слов из ``engine._compare_words``: слова только в A, только
в B и сдвинутые (B уже в координатах A). На выходе — список изменений:

* ``changed`` — на одном месте слова A заменены словами B («Рр=323.55кВт →
  Рр=324.26кВт»); соседние слова строки склеиваются во фразу;
* ``added`` / ``removed`` — слова только в B / только в A;
* ``moved`` — тот же текст на другом месте («сдвиг надписи», с вектором).

Нормализация не скрывает отличия, а классифицирует их (``norm``): пробелы
(не считаются отличием, но учитываются), замена латиница ↔ кириллица того же
начертания, формат числа «,» ↔ «.». Числа разбираются и сравниваются по
порядку — изменённые выводятся парами.
"""
from __future__ import annotations

import re
from typing import Any

# Латинские буквы, совпадающие по начертанию с кириллическими.
_HOMOGLYPHS = str.maketrans("ABCEHKMOPTXaceopxyі", "АВСЕНКМОРТХасеорхуі")
_NUMBER = re.compile(r"\d+(?:[.,]\d+)?")


def _overlap(a: list[float], b: list[float]) -> tuple[float, float]:
    """Перекрытие по x и по y (pt, может быть отрицательным — зазор)."""
    return min(a[2], b[2]) - max(a[0], b[0]), min(a[3], b[3]) - max(a[1], b[1])


def _iou(a: list[float], b: list[float]) -> float:
    ox, oy = _overlap(a, b)
    if ox <= 0 or oy <= 0:
        return 0.0
    inter = ox * oy
    union = (a[2] - a[0]) * (a[3] - a[1]) + (b[2] - b[0]) * (b[3] - b[1]) - inter
    return inter / union if union > 0 else 0.0


def _vertical(w: dict) -> bool:
    """Надпись повёрнута на 90°: по направлению строки из PDF, иначе по форме рамки."""
    if w.get("dir"):
        return w["dir"] in ("up", "down")
    r = w["pt"]
    return len(w["text"]) > 1 and (r[3] - r[1]) > 1.5 * (r[2] - r[0])


def _same_place(a: dict, b: dict) -> bool:
    """Слово B стоит на месте слова A: перекрываются или это одна строка с нахлёстом."""
    if _iou(a["pt"], b["pt"]) > 0.2:
        return True
    ra, rb = a["pt"], b["pt"]
    ox, oy = _overlap(ra, rb)
    ha, hb = ra[3] - ra[1], rb[3] - rb[1]
    wa, wb = ra[2] - ra[0], rb[2] - rb[0]
    if _vertical(a) or _vertical(b):
        return oy > 0 and ox >= 0.5 * min(wa, wb)
    return ox > 0 and oy >= 0.5 * min(ha, hb)


def _line_neighbors(a: dict, b: dict) -> bool:
    """Соседние слова одной строки (для склейки во фразу)."""
    ra, rb = a["pt"], b["pt"]
    h = max(1e-6, min(ra[3] - ra[1], rb[3] - rb[1]))
    ox, oy = _overlap(ra, rb)
    if _vertical(a) and _vertical(b):
        w = max(1e-6, min(ra[2] - ra[0], rb[2] - rb[0]))
        return ox >= 0.5 * w and oy > -1.2 * w
    return oy >= 0.5 * h and ox > -1.2 * h


def _reading_order(words: list[dict]) -> list[dict]:
    if not words:
        return []
    up = sum(1 for w in words if w.get("dir") == "up")
    down = sum(1 for w in words if w.get("dir") == "down")
    if max(up, down) * 2 > len(words):
        # Повёрнутая надпись: строки — по x, слова в строке — вдоль y.
        wd = sorted(w["pt"][2] - w["pt"][0] for w in words)[len(words) // 2] or 1.0
        sign = -1 if up >= down else 1
        return sorted(words, key=lambda w: (round(((w["pt"][0] + w["pt"][2]) / 2) / (0.6 * wd)),
                                            sign * (w["pt"][1] + w["pt"][3]) / 2))
    h = sorted(w["pt"][3] - w["pt"][1] for w in words)[len(words) // 2] or 1.0
    return sorted(words, key=lambda w: (round(((w["pt"][1] + w["pt"][3]) / 2) / (0.6 * h)), w["pt"][0]))


def _union(rects: list[list[float]]) -> list[float] | None:
    if not rects:
        return None
    return [round(min(r[0] for r in rects), 2), round(min(r[1] for r in rects), 2),
            round(max(r[2] for r in rects), 2), round(max(r[3] for r in rects), 2)]


def _components(n: int, edges: list[tuple[int, int]]) -> list[list[int]]:
    parent = list(range(n))

    def find(i: int) -> int:
        while parent[i] != i:
            parent[i] = parent[parent[i]]
            i = parent[i]
        return i
    for i, j in edges:
        parent[find(i)] = find(j)
    groups: dict[int, list[int]] = {}
    for i in range(n):
        groups.setdefault(find(i), []).append(i)
    return list(groups.values())


def classify(before: str, after: str) -> str | None:
    """Класс нормализации: None — настоящее изменение текста."""
    b, a = before.replace(" ", ""), after.replace(" ", "")
    if b == a:
        return "spaces"
    if b.translate(_HOMOGLYPHS) == a.translate(_HOMOGLYPHS):
        return "symbol"
    if b.replace(",", ".") == a.replace(",", "."):
        return "number_format"
    if b.translate(_HOMOGLYPHS).replace(",", ".") == a.translate(_HOMOGLYPHS).replace(",", "."):
        return "symbol"
    return None


def numbers(before: str, after: str) -> list[dict[str, str]]:
    """Изменённые числа по порядку: [{"before": "323.55", "after": "324.26"}, ...]."""
    nb, na = _NUMBER.findall(before), _NUMBER.findall(after)
    out = []
    if len(nb) == len(na):
        for x, y in zip(nb, na):
            if x.replace(",", ".") != y.replace(",", "."):
                out.append({"before": x, "after": y})
        return out
    # Разное число чисел: отличающиеся как мультимножества, без пар.
    rest_a = list(na)
    gone = []
    for x in nb:
        if x in rest_a:
            rest_a.remove(x)
        else:
            gone.append(x)
    return [{"before": x, "after": ""} for x in gone] + [{"before": "", "after": y} for y in rest_a]


def build(only_a: list[dict], only_b: list[dict], moved: list[dict],
          to_b_page: Any = None) -> list[dict[str, Any]]:
    """Изменения текста. ``to_b_page(rect_in_a) -> rect`` переводит в координаты страницы B."""
    words = [("a", w) for w in only_a] + [("b", w) for w in only_b]
    edges = []
    for i, (si, wi) in enumerate(words):
        for j in range(i + 1, len(words)):
            sj, wj = words[j]
            if si != sj and _same_place(wi, wj):
                edges.append((i, j))
    # Склейка во фразу: изменённые слова одной строки, стоящие вплотную (зазор
    # не больше ~1 высоты строки). Надписи, разнесённые по строке, — раздельно.
    groups = _components(len(words), edges)
    group_of = {}
    for g, members in enumerate(groups):
        for k in members:
            group_of[k] = g
    merge = []
    for i, (_si, wi) in enumerate(words):
        for j in range(i + 1, len(words)):
            if group_of[i] != group_of[j] and _line_neighbors(wi, words[j][1]):
                merge.append((group_of[i], group_of[j]))
    merged = _components(len(groups), merge)

    changes: list[dict[str, Any]] = []
    for gids in merged:
        members = [k for g in gids for k in groups[g]]
        wa = _reading_order([words[k][1] for k in members if words[k][0] == "a"])
        wb = _reading_order([words[k][1] for k in members if words[k][0] == "b"])
        before = " ".join(w["text"] for w in wa)
        after = " ".join(w["text"] for w in wb)
        kind = "changed" if wa and wb else ("removed" if wa else "added")
        b_in_a = _union([w["pt"] for w in wb])
        item = {"kind": kind, "before": before, "after": after,
                "a_bbox": _union([w["pt"] for w in wa]), "b_in_a_bbox": b_in_a,
                "b_bbox": _union([to_b_page(b_in_a)]) if (to_b_page and b_in_a) else b_in_a,
                "norm": classify(before, after) if kind == "changed" else None,
                "numbers": numbers(before, after)}
        changes.append(item)

    # Сдвиги надписей: слова с общим вектором сдвига в одной строке — одна фраза.
    mv_groups = _components(len(moved), [
        (i, j) for i in range(len(moved)) for j in range(i + 1, len(moved))
        if abs(_vec(moved[i])[0] - _vec(moved[j])[0]) <= 0.5 and abs(_vec(moved[i])[1] - _vec(moved[j])[1]) <= 0.5
        and _line_neighbors({"pt": moved[i]["a_pt"], "text": moved[i]["text"], "dir": moved[i].get("dir")},
                            {"pt": moved[j]["a_pt"], "text": moved[j]["text"], "dir": moved[j].get("dir")})])
    for members in mv_groups:
        ws = _reading_order([{"pt": moved[k]["a_pt"], "text": moved[k]["text"], "dir": moved[k].get("dir")}
                             for k in members])
        text = " ".join(w["text"] for w in ws)
        b_in_a = _union([moved[k]["b_in_a_pt"] for k in members])
        dx, dy = _vec(moved[members[0]])
        changes.append({"kind": "moved", "before": text, "after": text,
                        "a_bbox": _union([moved[k]["a_pt"] for k in members]), "b_in_a_bbox": b_in_a,
                        "b_bbox": _union([to_b_page(b_in_a)]) if to_b_page else b_in_a,
                        "shift_pt": [round(dx, 2), round(dy, 2)], "norm": None, "numbers": []})

    return finalize(changes)


def finalize(changes: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Порядок чтения (по A) и идентификаторы t1, t2, …"""
    def order_key(c: dict) -> tuple[float, float]:
        r = c["a_bbox"] or c["b_in_a_bbox"]
        return (round(r[1] / 8.0), r[0])
    changes.sort(key=order_key)
    for k, c in enumerate(changes, 1):
        c["id"] = f"t{k}"
    return changes


def shifted(words: list[dict], dx: float, dy: float) -> list[dict]:
    """Копии слов, сдвинутые на (dx, dy) — для сравнения внутри перемещённого блока."""
    return [w | {"pt": [w["pt"][0] + dx, w["pt"][1] + dy, w["pt"][2] + dx, w["pt"][3] + dy]} for w in words]


def _vec(mv: dict) -> tuple[float, float]:
    a, b = mv["a_pt"], mv["b_in_a_pt"]
    return (b[0] + b[2]) / 2 - (a[0] + a[2]) / 2, (b[1] + b[3]) / 2 - (a[1] + a[3]) / 2


def significant(changes: list[dict]) -> int:
    """Число отличий текста для канала: без сдвигов и без изменений только пробелов."""
    return sum(1 for c in changes if c["kind"] != "moved" and c.get("norm") != "spaces")
