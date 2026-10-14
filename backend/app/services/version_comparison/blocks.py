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


def _touching_box(mask: np.ndarray, rect: tuple[int, int, int, int]) -> list[int] | None:
    """Габарит компонент ``mask``, задевающих прямоугольник (поиск в окне вокруг него)."""
    h, w = mask.shape
    x0, y0, x1, y1 = rect
    bw, bh = x1 - x0, y1 - y0
    X0, Y0, X1, Y1 = max(0, x0 - bw), max(0, y0 - bh), min(w, x1 + bw), min(h, y1 + bh)
    if X1 <= X0 or Y1 <= Y0:
        return None
    n, lab, stats, _ = cv2.connectedComponentsWithStats(np.ascontiguousarray(mask[Y0:Y1, X0:X1]), connectivity=8)
    inner = lab[max(0, y0 - Y0):max(0, y1 - Y0), max(0, x0 - X0):max(0, x1 - X0)]
    ids = [int(v) for v in np.unique(inner) if v > 0]
    if not ids:
        return None
    return [X0 + min(int(stats[i][0]) for i in ids), Y0 + min(int(stats[i][1]) for i in ids),
            X0 + max(int(stats[i][0] + stats[i][2]) for i in ids), Y0 + max(int(stats[i][1] + stats[i][3]) for i in ids)]


def _shift(mask: np.ndarray, dx: int, dy: int) -> np.ndarray:
    m = np.float32([[1, 0, dx], [0, 1, dy]])
    return cv2.warpAffine(mask, m, (mask.shape[1], mask.shape[0]), flags=cv2.INTER_NEAREST, borderValue=0)


def confirm(blocks: list[dict[str, Any]], a_only: np.ndarray, b_only: np.ndarray, canvas: Any,
            mm_px: float, dark_a: np.ndarray | None = None, dark_b: np.ndarray | None = None) -> list[dict[str, Any]]:
    """Растровая проверка и перенос объяснённых пикселей в слой «перемещено».

    ``a_only``/``b_only`` изменяются на месте. Возвращает подтверждённые блоки
    с ``bbox_a_pt`` (место в A), ``bbox_b_in_a_pt`` (место в B) и масками.

    Пиксель «только A» объяснён, если на его новом месте в B есть линия
    (``dark_b``), а не только «только B»: при сдвиге меньше размера блока
    старое и новое места перекрываются, и часть линий B там общая с A.

    Слово A (и слово B) переезжает не больше одного раза: блоки идут в порядке
    ``find`` (больше слов — раньше), и слова, уже занятые подтверждённым блоком,
    следующему не достаются. Иначе одна строка A («Огнезащитный герметик»)
    «переезжает» и на своё место, и на новую строку B с тем же первым словом
    («Огнезащитный состав»), а новая строка B пропадает из добавленного.
    """
    if dark_a is None:
        dark_a = a_only
    if dark_b is None:
        dark_b = b_only
    out = []
    h, w = a_only.shape
    kernel = np.ones((5, 5), np.uint8)
    claimed_a: set[int] = set()
    claimed_b: set[int] = set()
    for blk in blocks:
        keep = [i for i, (wa, wb) in enumerate(zip(blk["a_words"], blk["b_words"]))
                if id(wa) not in claimed_a and id(wb) not in claimed_b]
        if len(keep) < settings.BLOCK_MIN_WORDS:
            continue
        if len(keep) < len(blk["a_words"]):
            blk = blk | {"a_words": [blk["a_words"][i] for i in keep], "b_words": [blk["b_words"][i] for i in keep]}
        ra = _union([x["pt"] for x in blk["a_words"]])
        pad = 3 * mm_px / canvas.s           # мм → pt
        ra = [ra[0] - pad, ra[1] - pad, ra[2] + pad, ra[3] + pad]
        x0, y0, x1, y1 = canvas.px(ra)
        x0, y0, x1, y1 = max(0, x0), max(0, y0), min(w, x1), min(h, y1)
        if x1 <= x0 or y1 <= y0:
            continue
        dxp, dyp = int(round(blk["vector"][0] * canvas.s)), int(round(blk["vector"][1] * canvas.s))
        # Рамка и линии таблицы выходят за слова: область расширяется на
        # соединённые с ней линии «только A» (и «только B» на новом месте).
        ext = [_touching_box(a_only, (x0, y0, x1, y1)),
               _touching_box(b_only, (x0 + dxp, y0 + dyp, x1 + dxp, y1 + dyp))]
        bx = [x0, y0, x1, y1]
        for k, eb in enumerate(ext):
            if eb is not None:
                sx, sy = (0, 0) if k == 0 else (dxp, dyp)
                bx = [min(bx[0], eb[0] - sx), min(bx[1], eb[1] - sy), max(bx[2], eb[2] - sx), max(bx[3], eb[3] - sy)]
        if (bx[2] - bx[0]) * (bx[3] - bx[1]) <= 4 * (x1 - x0) * (y1 - y0):
            x0, y0, x1, y1 = max(0, bx[0]), max(0, bx[1]), min(w, bx[2]), min(h, bx[3])
            ra = canvas.pt([x0, y0, x1, y1])
        region = np.zeros_like(a_only)
        region[y0:y1, x0:x1] = 1
        a_blk = a_only & region
        region_b = _shift(region, dxp, dyp)
        b_blk = b_only & region_b
        expl_b = b_blk & cv2.dilate(_shift(dark_a & region, dxp, dyp), kernel)
        expl_a = a_blk & _shift(cv2.dilate(dark_b & region_b, kernel), -dxp, -dyp)
        total = int(a_blk.sum() + b_blk.sum())
        share = (int(expl_a.sum()) + int(expl_b.sum())) / total if total else 0.0
        if share < settings.BLOCK_MIN_IOU:
            continue
        a_only[expl_a > 0] = 0
        b_only[expl_b > 0] = 0
        vx, vy = blk["vector"]
        rb = [ra[0] + vx, ra[1] + vy, ra[2] + vx, ra[3] + vy]
        claimed_a.update(id(x) for x in blk["a_words"])
        claimed_b.update(id(x) for x in blk["b_words"])
        out.append(blk | {"bbox_a_pt": [round(v, 2) for v in ra], "bbox_b_in_a_pt": [round(v, 2) for v in rb],
                          "share": round(share, 3), "moved_a": expl_a, "moved_b": expl_b})
    return out
