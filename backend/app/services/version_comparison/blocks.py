"""Перемещённые блоки (план §5 шаг 3.4, этап 2).

После общего совмещения часть листа может быть сдвинута целиком (таблицу
перенесли на другое место). Без этого шага она выглядит как «удалено +
добавлено». Здесь:

1. по одинаковым словам «только в A» / «только в B» (и сдвинутым) ищутся
   группы не меньше ``BLOCK_MIN_WORDS`` слов с общим вектором (±``BLOCK_TOL_PT``),
   длина вектора — не меньше ``BLOCK_MIN_SHIFT_PT``;
2. группа подтверждается растром: пиксели «только A» в области блока,
   сдвинутые на вектор, совпадают с пикселями «только B» (доля ≥ ``BLOCK_MIN_IOU``);
3. объяснённые сдвигом пиксели убираются из «только A/B» в слой «перемещено»,
   остаток внутри блока — обычные отличия («изменения внутри блока»).

Основное наложение при этом не деформируется: сдвиг учитывается только для
отнесения пикселей к классу.
"""
from __future__ import annotations

from collections import Counter, defaultdict
from typing import Any

import cv2
import numpy as np

from . import settings


def _center(r: list[float]) -> tuple[float, float]:
    return (r[0] + r[2]) / 2, (r[1] + r[3]) / 2


def find(words_cmp: dict[str, list[dict]]) -> list[dict[str, Any]]:
    """Кандидаты в перемещённые блоки по словам: [{vector, a_words, b_words}]."""
    pairs: list[tuple[float, float, dict, dict]] = []
    by_text_a: dict[str, list[dict]] = defaultdict(list)
    by_text_b: dict[str, list[dict]] = defaultdict(list)
    for w in words_cmp["only_a"]:
        by_text_a[w["text"]].append(w)
    for w in words_cmp["only_b"]:
        by_text_b[w["text"]].append(w)
    for text, la in by_text_a.items():
        lb = by_text_b.get(text, [])
        if not lb or len(la) * len(lb) > 16:
            continue
        for wa in la:
            for wb in lb:
                (ax, ay), (bx, by) = _center(wa["pt"]), _center(wb["pt"])
                pairs.append((bx - ax, by - ay, wa, wb))
    for mv in words_cmp["moved"]:
        (ax, ay), (bx, by) = _center(mv["a_pt"]), _center(mv["b_in_a_pt"])
        pairs.append((bx - ax, by - ay, {"text": mv["text"], "pt": mv["a_pt"], "dir": mv.get("dir")},
                      {"text": mv["text"], "pt": mv["b_in_a_pt"], "dir": mv.get("dir")}))
    tol = settings.BLOCK_TOL_PT
    blocks = []
    used: set[int] = set()
    while True:
        votes = Counter((round(dx / tol), round(dy / tol)) for i, (dx, dy, _a, _b) in enumerate(pairs) if i not in used)
        if not votes:
            break
        (kx, ky), n = votes.most_common(1)[0]
        if n < settings.BLOCK_MIN_WORDS:
            break
        cx, cy = kx * tol, ky * tol
        members = [i for i, (dx, dy, _a, _b) in enumerate(pairs)
                   if i not in used and abs(dx - cx) <= tol and abs(dy - cy) <= tol]
        used.update(members)
        dx = float(np.median([pairs[i][0] for i in members]))
        dy = float(np.median([pairs[i][1] for i in members]))
        if (dx * dx + dy * dy) ** 0.5 < settings.BLOCK_MIN_SHIFT_PT:
            continue
        # Одно слово A — в одной паре.
        seen_a, seen_b, mem = set(), set(), []
        for i in members:
            a, b = pairs[i][2], pairs[i][3]
            if id(a) in seen_a or id(b) in seen_b:
                continue
            seen_a.add(id(a))
            seen_b.add(id(b))
            mem.append(i)
        if len(mem) >= settings.BLOCK_MIN_WORDS:
            blocks.append({"vector": [dx, dy], "a_words": [pairs[i][2] for i in mem], "b_words": [pairs[i][3] for i in mem]})
    return blocks


def _union(rects: list[list[float]]) -> list[float]:
    return [min(r[0] for r in rects), min(r[1] for r in rects), max(r[2] for r in rects), max(r[3] for r in rects)]


def _shift(mask: np.ndarray, dx: int, dy: int) -> np.ndarray:
    m = np.float32([[1, 0, dx], [0, 1, dy]])
    return cv2.warpAffine(mask, m, (mask.shape[1], mask.shape[0]), flags=cv2.INTER_NEAREST, borderValue=0)


def confirm(blocks: list[dict[str, Any]], a_only: np.ndarray, b_only: np.ndarray, canvas: Any,
            mm_px: float) -> list[dict[str, Any]]:
    """Растровая проверка и перенос объяснённых пикселей в слой «перемещено».

    ``a_only``/``b_only`` изменяются на месте. Возвращает подтверждённые блоки
    с ``bbox_a_pt`` (место в A), ``bbox_b_in_a_pt`` (место в B) и масками.
    """
    out = []
    h, w = a_only.shape
    kernel = np.ones((5, 5), np.uint8)
    for blk in blocks:
        ra = _union([x["pt"] for x in blk["a_words"]])
        pad = 3 * mm_px / canvas.s           # мм → pt
        ra = [ra[0] - pad, ra[1] - pad, ra[2] + pad, ra[3] + pad]
        x0, y0, x1, y1 = canvas.px(ra)
        x0, y0, x1, y1 = max(0, x0), max(0, y0), min(w, x1), min(h, y1)
        if x1 <= x0 or y1 <= y0:
            continue
        dxp, dyp = int(round(blk["vector"][0] * canvas.s)), int(round(blk["vector"][1] * canvas.s))
        region = np.zeros_like(a_only)
        region[y0:y1, x0:x1] = 1
        a_blk = a_only & region
        b_blk = b_only & _shift(region, dxp, dyp)
        a_to_b = _shift(a_blk, dxp, dyp)
        expl_b = b_blk & cv2.dilate(a_to_b, kernel)
        expl_a = a_blk & _shift(cv2.dilate(b_blk, kernel), -dxp, -dyp)
        total = int(a_blk.sum() + b_blk.sum())
        share = (int(expl_a.sum()) + int(expl_b.sum())) / total if total else 0.0
        if share < settings.BLOCK_MIN_IOU:
            continue
        a_only[expl_a > 0] = 0
        b_only[expl_b > 0] = 0
        rb = _union([x["pt"] for x in blk["b_words"]])
        rb = [rb[0] - pad, rb[1] - pad, rb[2] + pad, rb[3] + pad]
        out.append(blk | {"bbox_a_pt": [round(v, 2) for v in ra], "bbox_b_in_a_pt": [round(v, 2) for v in rb],
                          "share": round(share, 3), "moved_a": expl_a, "moved_b": expl_b})
    return out
