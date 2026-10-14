"""Сравнение пары листов A → B (план §5 шаги 3–6, §5.1, §6).

Функция ``compute_sheet_pair`` самодостаточна и принимает только
сериализуемые аргументы: она выполняется в общем пуле процессов бэкенда.
Результат пишется в папку пары листов: ``result.json``, ``masks.npz``
(слои наложения, упакованные биты), ``preview.png``.
"""
from __future__ import annotations

import gzip
import hashlib
import json
import math
import time
from collections import defaultdict
from pathlib import Path
from typing import Any

import cv2
import fitz
import numpy as np

from . import blocks, pdfio, registration, separation, service, settings, status as st, textdiff
from .settings import ALGO_VERSION

LAYERS = ("common", "a_only", "b_only", "light_a", "light_b", "color", "style", "moved", "within_tol", "noise",
          "zones", "ink_any")
# Классы регионов-сдвигов: показываются, но изменением не считаются.
SHIFT_CLASSES = ("text_moved", "block_moved", "shifted")
COLORS = {  # RGB
    "common": (150, 156, 163),
    "ink_any": (222, 225, 229),
    "a_only": (212, 48, 58),
    "b_only": (20, 108, 201),
    "light_a": (236, 160, 166),
    "light_b": (150, 190, 236),
    "color": (144, 69, 170),
    "style": (0, 150, 136),
    "moved": (40, 140, 60),
    "within_tol": (196, 178, 60),
    "noise": (60, 170, 90),
}
PREVIEW_MAX = 2400


# ------------------------------------------------------------------ геометрия

def _fitz_matrix(m: np.ndarray) -> fitz.Matrix:
    return fitz.Matrix(m[0][0], m[1][0], m[0][1], m[1][1], m[0][2], m[1][2])


def _apply(m: np.ndarray, x: float, y: float) -> tuple[float, float]:
    return m[0][0] * x + m[0][1] * y + m[0][2], m[1][0] * x + m[1][1] * y + m[1][2]


def _invert(m: np.ndarray) -> np.ndarray:
    return cv2.invertAffineTransform(np.array(m, dtype=np.float64))


def _rect_b_to_a(m: np.ndarray, r: list[float]) -> list[float]:
    pts = [_apply(m, x, y) for x, y in ((r[0], r[1]), (r[2], r[1]), (r[0], r[3]), (r[2], r[3]))]
    xs, ys = [p[0] for p in pts], [p[1] for p in pts]
    return [min(xs), min(ys), max(xs), max(ys)]


class Canvas:
    """Холст наложения: объединение листа A и перенесённого листа B, px."""

    def __init__(self, rect_a: list[float], rect_b_in_a: list[float], dpi: int):
        self.dpi = dpi
        self.s = dpi / 72.0
        self.ox = min(rect_a[0], rect_b_in_a[0])
        self.oy = min(rect_a[1], rect_b_in_a[1])
        x1 = max(rect_a[2], rect_b_in_a[2])
        y1 = max(rect_a[3], rect_b_in_a[3])
        self.w = int(math.ceil((x1 - self.ox) * self.s))
        self.h = int(math.ceil((y1 - self.oy) * self.s))

    def px(self, r: list[float]) -> list[int]:
        return [int(math.floor((r[0] - self.ox) * self.s)), int(math.floor((r[1] - self.oy) * self.s)),
                int(math.ceil((r[2] - self.ox) * self.s)), int(math.ceil((r[3] - self.oy) * self.s))]

    def pt(self, r: list[int]) -> list[float]:
        return [r[0] / self.s + self.ox, r[1] / self.s + self.oy, r[2] / self.s + self.ox, r[3] / self.s + self.oy]

    def matrix(self, pre: fitz.Matrix | None = None) -> fitz.Matrix:
        base = fitz.Matrix(1, 0, 0, 1, -self.ox, -self.oy) * fitz.Matrix(self.s, self.s)
        return (pre * base) if pre is not None else base

    def describe(self) -> dict[str, Any]:
        return {"dpi": self.dpi, "origin_pt": [self.ox, self.oy], "width_px": self.w, "height_px": self.h}


def _render_into(page: fitz.Page, canvas: Canvas, pre: fitz.Matrix | None = None) -> np.ndarray:
    arr, x0, y0 = pdfio.render(page, canvas.matrix(pre))
    out = np.full((canvas.h, canvas.w, 3), 255, np.uint8)
    sx0, sy0 = max(0, -x0), max(0, -y0)
    dx0, dy0 = max(0, x0), max(0, y0)
    w = min(arr.shape[1] - sx0, canvas.w - dx0)
    h = min(arr.shape[0] - sy0, canvas.h - dy0)
    if w > 0 and h > 0:
        out[dy0:dy0 + h, dx0:dx0 + w] = arr[sy0:sy0 + h, sx0:sx0 + w]
    return out


def _disk(r: int) -> np.ndarray:
    return cv2.getStructuringElement(cv2.MORPH_ELLIPSE, (2 * r + 1, 2 * r + 1))


def _rect_mask(shape: tuple[int, int], rects_px: list[list[int]]) -> np.ndarray:
    m = np.zeros(shape, np.uint8)
    for x0, y0, x1, y1 in rects_px:
        x0, y0 = max(0, x0), max(0, y0)
        x1, y1 = min(shape[1], x1), min(shape[0], y1)
        if x1 > x0 and y1 > y0:
            m[y0:y1, x0:x1] = 1
    return m


# ---------------------------------------------------------------------- текст

def _words_in_canvas(words: list[list[Any]], canvas: Canvas, m: np.ndarray | None) -> list[dict[str, Any]]:
    out = []
    for w in words:
        rect = [w[0], w[1], w[2], w[3]]
        if m is not None:
            rect = _rect_b_to_a(m, rect)
        out.append({"text": str(w[4]), "pt": rect, "src": w[5] if len(w) > 5 else "text",
                    "dir": w[6] if len(w) > 6 else None})
    return out


def _reading_order_words(words: list[dict]) -> list[dict]:
    """Слова зоны в порядке чтения (строки сверху вниз, слева направо)."""
    return sorted(words, key=lambda w: (round(_center(w["pt"])[1] / 3.0), w["pt"][0]))


def _center(r: list[float]) -> tuple[float, float]:
    return (r[0] + r[2]) / 2, (r[1] + r[3]) / 2


def _compare_words(wa: list[dict], wb: list[dict], same_pt: float) -> dict[str, list[dict]]:
    """Слова основной области A и B (B уже в координатах A).

    * совпадает — тот же текст не дальше ``same_pt`` (допуск сравнения);
    * сдвиг надписи — тот же текст дальше допуска, но не дальше WORD_MOVE_PT;
    * только в A / только в B — остальное.
    Ближайшие пары подбираются жадно по расстоянию внутри каждого текста.
    """
    index_b: dict[str, list[int]] = defaultdict(list)
    for i, w in enumerate(wb):
        index_b[w["text"]].append(i)
    candidates = []
    for ia, w in enumerate(wa):
        cx, cy = _center(w["pt"])
        for jb in index_b.get(w["text"], []):
            bx, by = _center(wb[jb]["pt"])
            d = math.hypot(bx - cx, by - cy)
            if d <= settings.WORD_MOVE_PT:
                candidates.append((d, ia, jb))
    candidates.sort()
    used_a: set[int] = set()
    used_b: set[int] = set()
    moved = []
    same = 0
    for d, ia, jb in candidates:
        if ia in used_a or jb in used_b:
            continue
        used_a.add(ia)
        used_b.add(jb)
        if d <= same_pt:
            same += 1
        else:
            moved.append({"text": wa[ia]["text"], "a_pt": wa[ia]["pt"], "b_in_a_pt": wb[jb]["pt"], "shift_pt": round(d, 2),
                          "dir": wa[ia].get("dir")})
    only_a = [w for i, w in enumerate(wa) if i not in used_a]
    only_b = [w for i, w in enumerate(wb) if i not in used_b]
    return {"only_a": only_a, "only_b": only_b, "moved": moved, "same": same}


def _in_rects(r: list[float], rects: list[list[float]]) -> bool:
    cx, cy = _center(r)
    return any(z[0] <= cx <= z[2] and z[1] <= cy <= z[3] for z in rects)


def _is_stroke(component: np.ndarray) -> bool:
    """Компонента — один прямой штрих (отрезок любой толщины до четверти длины)."""
    (_cx, _cy), (rw, rh), _ang = cv2.minAreaRect(cv2.findNonZero(component))
    return min(rw, rh) <= max(2.5, settings.GLYPH_STROKE_RATIO * max(rw, rh))


def _glyph_line(mask: np.ndarray, mm_px: float, common: np.ndarray | None = None) -> bool:
    """В области похоже на строку надписи: ≥3 знака одной высоты в ряд.

    Промежуток между знаками надписи пуст. Если в нём общая для A и B линия —
    это не надпись, а одна фигура (облако изменений, контур), разрезанная
    пересекающими её линиями на похожие куски. Знаком не считаются дуга шире
    двух высот (облако), залитое пятно; строка из одних прямых штрихов —
    штриховка или параллельные линии.
    """
    n, lab, stats, cents = cv2.connectedComponentsWithStats(mask, connectivity=8)
    glyphs = []
    for i in range(1, n):
        x, y, w, h, area = (int(v) for v in stats[i])
        if not (1.0 * mm_px <= h <= 6 * mm_px and w <= 6 * mm_px):
            continue
        if w > settings.GLYPH_MAX_ASPECT * h or area > settings.GLYPH_MAX_FILL * w * h:
            continue
        stroke = _is_stroke((lab[y:y + h, x:x + w] == i).astype(np.uint8))
        glyphs.append((stats[i], cents[i], stroke))
    glyphs.sort(key=lambda g: g[1][0])
    for i in range(len(glyphs)):
        chain = [glyphs[i]]
        for g in glyphs[i + 1:]:
            last = chain[-1]
            same_row = abs(g[1][1] - last[1][1]) <= 1.0 * mm_px
            similar = 0.6 <= g[0][3] / max(1, last[0][3]) <= 1.6
            gap_x0, gap_x1 = last[0][0] + last[0][2], g[0][0]
            # Знаки идут друг за другом: допускается лишь небольшой нахлёст
            # (кернинг), куски одной фигуры перекрываются сильнее.
            close = -0.25 * min(g[0][3], last[0][3]) <= gap_x1 - gap_x0 <= 3 * mm_px
            if close and common is not None and gap_x1 > gap_x0:
                gy0 = max(last[0][1], g[0][1])
                gy1 = min(last[0][1] + last[0][3], g[0][1] + g[0][3])
                if gy1 > gy0 and common[gy0:gy1, gap_x0:gap_x1].any():
                    close = False
            if same_row and similar and close:
                chain.append(g)
        if len(chain) >= 3 and not all(g[2] for g in chain):
            return True
    return False


def _shift_explains(vectors: list[tuple[float, float]], diff: np.ndarray, any_a: np.ndarray, any_b: np.ndarray,
                    box_px: list[int], canvas: Any) -> bool:
    """Регион целиком объясняется сдвигами соседних надписей или блока (знак переехал вместе с подписью).

    Каждая связная часть отличия (знак, линия) ищет свой сдвиг в пределах
    ±``SHIFT_REFINE_PT`` от сдвига соседней надписи: строки легенды при
    перевёрстке расходятся на разные расстояния, знак и подпись — на чуть
    разные. Часть объяснена, если на сдвинутом месте другая версия рисует ту
    же фигуру (почти каждый её пиксель ложится на линию). Новая линия или знак
    рядом так не объясняются: такой же фигуры поблизости в другой версии нет.
    """
    h, w = diff.shape
    x0, y0, x1, y1 = box_px
    sel = diff[y0:y1, x0:x1]
    total = int(sel.sum())
    if not total:
        return False
    R = int(round(settings.SHIFT_REFINE_PT * canvas.s))
    k = np.ones((3, 3), np.uint8)
    n, lab, stats, _ = cv2.connectedComponentsWithStats(sel, connectivity=8)
    explained = 0
    for i in range(1, n):
        cx, cy, cw, ch, area = (int(v) for v in stats[i])
        comp = (lab[cy:cy + ch, cx:cx + cw] == i).astype(np.uint8)
        gx, gy = x0 + cx, y0 + cy
        ink_a = any_a[gy:gy + ch, gx:gx + cw] & comp
        ink_b = any_b[gy:gy + ch, gx:gx + cw] & comp
        for src, dst, sign in ((ink_a, any_b, 1), (ink_b, any_a, -1)):
            if not src.any():
                continue
            best = 0
            for vx, vy in vectors:
                dx, dy = int(round(sign * vx * canvas.s)), int(round(sign * vy * canvas.s))
                X0, Y0 = gx + dx - R, gy + dy - R
                X1, Y1 = X0 + cw + 2 * R, Y0 + ch + 2 * R
                if X0 < 0 or Y0 < 0 or X1 > w or Y1 > h:
                    continue
                win = cv2.dilate(dst[Y0:Y1, X0:X1], k).astype(np.float32)
                score = cv2.matchTemplate(win, src.astype(np.float32), cv2.TM_CCORR)
                best = max(best, int(round(float(score.max()))))
            explained += min(int(src.sum()), best)
    return explained >= settings.SHIFT_EXPLAINED_SHARE * total


def _overdraw(rgb_a: np.ndarray, rgb_b: np.ndarray, color: np.ndarray, dark_a: np.ndarray, dark_b: np.ndarray,
              box_px: list[int], pad: int) -> bool:
    """Цвет сменился из-за порядка наложения линий, а не перекраски.

    На пересечении A показывает цвет одной линии, B — другой (с краевым
    смешением сглаживания): цвет почти каждой такой точки A есть рядом в B
    (линия продолжается), и наоборот. У перекрашенной линии прежнего цвета
    рядом в другой версии нет.
    """
    x0, y0, x1, y1 = box_px
    h, w = color.shape
    X0, Y0, X1, Y1 = max(0, x0 - pad), max(0, y0 - pad), min(w, x1 + pad), min(h, y1 + pad)
    win = (slice(Y0, Y1), slice(X0, X1))
    sel = np.zeros((Y1 - Y0, X1 - X0), bool)
    sel[y0 - Y0:y1 - Y0, x0 - X0:x1 - X0] = color[y0:y1, x0:x1].astype(bool)
    if not sel.any():
        return False
    lab_a = cv2.cvtColor(np.ascontiguousarray(rgb_a[win]), cv2.COLOR_RGB2Lab).astype(np.float32)
    lab_b = cv2.cvtColor(np.ascontiguousarray(rgb_b[win]), cv2.COLOR_RGB2Lab).astype(np.float32)

    def explained(points: np.ndarray, lab: np.ndarray, dark: np.ndarray) -> float:
        palette = np.unique(lab[dark[win].astype(bool)].astype(np.int16), axis=0).astype(np.float32)
        if not len(palette):
            return 0.0
        pts = points[:: max(1, len(points) // 200)]
        dist = np.linalg.norm(pts[:, None, :] - palette[None, :, :], axis=2).min(axis=1)
        return float((dist < settings.OVERDRAW_DELTA).mean())

    share = settings.OVERDRAW_MIN_SHARE
    return explained(lab_a[sel], lab_b, dark_b) >= share and explained(lab_b[sel], lab_a, dark_a) >= share


def _warning_lines(words: list[dict]) -> list[dict[str, Any]]:
    """Предупреждения, вшитые в основной поток (не отделены): строки с «НЕ СОГЛАСОВАНО» и т.п.

    Предупреждение — отдельная надпись: строка из одной фразы (до 4 слов) или
    набранная заметно крупнее основного текста. Слово «копия» в названии листа
    из ведомости («…сети копия 1…») предупреждением не считается.
    """
    heights = sorted(w["pt"][3] - w["pt"][1] for w in words)
    median_h = heights[len(heights) // 2] if heights else 0.0
    rows: list[list[dict]] = []
    for w in sorted(words, key=lambda w: (_center(w["pt"])[1], w["pt"][0])):
        if rows and abs(_center(rows[-1][0]["pt"])[1] - _center(w["pt"])[1]) <= 3:
            rows[-1].append(w)
        else:
            rows.append([w])
    out = []
    for row in rows:
        text = " ".join(x["text"] for x in sorted(row, key=lambda x: x["pt"][0]))
        row_h = max(x["pt"][3] - x["pt"][1] for x in row)
        prominent = len(row) <= 4 or (median_h > 0 and row_h >= 1.5 * median_h)
        if prominent and any(p.search(text) for p in separation.WARNING_PATTERNS):
            out.append({"text": text[:120], "bbox_pt": [min(x["pt"][0] for x in row), min(x["pt"][1] for x in row),
                                                       max(x["pt"][2] for x in row), max(x["pt"][3] for x in row)]})
    return out


# --------------------------------------------------------------- служебное

def _image_digests(doc: fitz.Document, pno: int) -> set[str]:
    out = set()
    for img in doc[pno].get_images(full=True):
        try:
            out.add(hashlib.sha256(doc.xref_stream_raw(img[0]) or b"").hexdigest()[:16])
        except Exception:
            pass
    return out


def _compare_annotations(aa: list[dict], bb: list[dict], m: np.ndarray) -> dict[str, Any]:
    def key(a):
        return (a["type"], a["subject"], a["author"])

    def iou(r1, r2):
        x0, y0 = max(r1[0], r2[0]), max(r1[1], r2[1])
        x1, y1 = min(r1[2], r2[2]), min(r1[3], r2[3])
        inter = max(0, x1 - x0) * max(0, y1 - y0)
        union = (r1[2] - r1[0]) * (r1[3] - r1[1]) + (r2[2] - r2[0]) * (r2[3] - r2[1]) - inter
        return inter / union if union > 0 else 0.0

    bb = [dict(b, rect_in_a=_rect_b_to_a(m, b["rect"])) for b in bb]
    used = set()
    same, changed, removed = [], [], []
    for a in aa:
        match = None
        for j, b in enumerate(bb):
            if j in used or key(a) != key(b):
                continue
            if iou(a["rect"], b["rect_in_a"]) >= 0.5:
                match = j
                break
        if match is None:
            removed.append(a)
            continue
        used.add(match)
        if (a["content"] or "") != (bb[match]["content"] or ""):
            changed.append({"before": a, "after": bb[match]})
        else:
            same.append(a)
    added = [b for j, b in enumerate(bb) if j not in used]
    return {"same": len(same), "added": added, "removed": removed, "changed": changed}


def _overlay_text(rec: dict[str, Any]) -> str:
    return " ".join((rec.get("text") or "").split())


def _compare_overlays(sep_a: list[dict], sep_b: list[dict]) -> list[dict[str, Any]]:
    """Отделённые наложенные объекты A и B по видам: появился / исчез / изменился."""
    out = []
    kinds = sorted({s["kind"] for s in sep_a} | {s["kind"] for s in sep_b})
    for kind in kinds:
        la = [s for s in sep_a if s["kind"] == kind]
        lb = [s for s in sep_b if s["kind"] == kind]
        if la and not lb:
            out.append({"kind": kind, "change": "removed", "before": [_overlay_text(s) for s in la]})
        elif lb and not la:
            out.append({"kind": kind, "change": "added", "after": [_overlay_text(s) for s in lb]})
        else:
            ta = [_overlay_text(s) for s in la]
            tb = [_overlay_text(s) for s in lb]
            out.append({"kind": kind, "change": "changed" if ta != tb else "same_text", "before": ta, "after": tb})
    return out


# ------------------------------------------------------------- маски и превью

def _pack(mask: np.ndarray) -> np.ndarray:
    return np.packbits(mask.astype(bool), axis=1)


def compose(layers: dict[str, np.ndarray], enabled: tuple[str, ...]) -> np.ndarray:
    h, w = next(iter(layers.values())).shape
    out = np.full((h, w, 3), 255, np.uint8)
    order = ("ink_any", "common", "light_a", "light_b", "within_tol", "noise", "style", "moved", "color", "a_only",
             "b_only")
    for name in order:
        if name in enabled and name in layers:
            out[layers[name].astype(bool)] = COLORS[name]
    if "zones" in enabled and "zones" in layers:
        z = layers["zones"].astype(bool)
        hatch = ((np.add.outer(np.arange(h), np.arange(w)) // 6) % 2 == 0)
        out[z & hatch] = (out[z & hatch] * 0.82 + np.array([255, 230, 160]) * 0.18).astype(np.uint8)
    return out


def _preview(layers: dict[str, np.ndarray]) -> np.ndarray:
    """Уменьшенное наложение: отличия не теряются (максимум по блоку)."""
    h, w = layers["common"].shape
    f = max(1, int(math.ceil(max(w, h) / PREVIEW_MAX)))
    small = {}
    for name, m in layers.items():
        if f > 1:
            m = cv2.dilate(m, np.ones((f, f), np.uint8)) if name not in ("ink_any", "common", "zones") else m
            m = cv2.resize(m, (max(1, w // f), max(1, h // f)), interpolation=cv2.INTER_NEAREST if name not in (
                "ink_any", "common") else cv2.INTER_AREA)
        small[name] = m
    return compose(small, ("ink_any", "common", "light_a", "light_b", "style", "moved", "color", "a_only", "b_only",
                           "zones"))


# ---------------------------------------------------------------------- расчёт

def _load_words(profile_dir: str, page: int) -> list[list[Any]]:
    with gzip.open(Path(profile_dir) / "words" / f"p{page:04d}.json.gz", "rt", encoding="utf-8") as fh:
        return json.load(fh)


def _load_mask(profile_dir: str, page: int) -> np.ndarray | None:
    img = cv2.imread(str(Path(profile_dir) / "mask36" / f"p{page:04d}.png"), cv2.IMREAD_GRAYSCALE)
    return None if img is None else (img > 0).astype(np.uint8)


def _zone_rects(rec: dict[str, Any], separated_kinds: set[str]) -> list[dict[str, Any]]:
    out = []
    for z in rec.get("zones", []):
        if z["type"] == "release_stamp" and "release_stamp" in separated_kinds:
            continue  # отметка отделена как объект — область под ней сравнивается
        out.append(z)
    return out


def _part_area(mask36: np.ndarray | None, zones_b: list[list[float]], m: np.ndarray) -> list[float] | None:
    """Габарит содержимого листа B (вне служебных зон) в системе A, pt, с запасом 3 мм."""
    if mask36 is None:
        return None
    k = 36 / 72.0
    mk = mask36.copy()
    for z in zones_b:
        mk[max(0, int(z[1] * k)):int(z[3] * k) + 1, max(0, int(z[0] * k)):int(z[2] * k) + 1] = 0
    ys, xs = np.nonzero(mk)
    if not len(xs):
        return None
    pad = 3 * 72 / 25.4
    rb = [xs.min() / k - pad, ys.min() / k - pad, (xs.max() + 1) / k + pad, (ys.max() + 1) / k + pad]
    return _rect_b_to_a(m, rb)


def compute_sheet_pair(task: dict[str, Any]) -> dict[str, Any]:
    """Сравнить пару листов. Возвращает краткую сводку; полный результат — в out_dir."""
    started = time.time()
    out_dir = Path(task["out_dir"])
    out_dir.mkdir(parents=True, exist_ok=True)
    a, b = task["a"], task["b"]
    rec_a, rec_b = a["profile"], b["profile"]
    dpi = int(task["params"]["dpi"])
    tol_pt = float(task["params"]["tol_pt"])
    reasons: list[str] = list(task.get("reasons") or [])
    channels: dict[str, dict[str, Any]] = {}
    base = {
        "schema": "version_comparison.sheet_result/v1", "algo": ALGO_VERSION,
        "sheet_pair_id": task["sheet_pair_id"], "row_id": task.get("row_id"),
        "a": {k: a[k] for k in ("version_id", "sha", "page")} | {"sheet_no": rec_a["stamp"].get("sheet_no"),
                                                                    "title": rec_a["stamp"].get("title"),
                                                                    "scan": bool(rec_a.get("scan"))},
        "b": {k: b[k] for k in ("version_id", "sha", "page")} | {"sheet_no": rec_b["stamp"].get("sheet_no"),
                                                                    "title": rec_b["stamp"].get("title"),
                                                                    "scan": bool(rec_b.get("scan"))},
        "params": {"dpi": dpi, "tol_pt": tol_pt, "dark_threshold": settings.DARK_THRESHOLD,
                   "light_threshold": settings.LIGHT_THRESHOLD, "color_delta": settings.COLOR_DELTA},
        "match": task.get("match") or {},
    }
    is_spec = "spec" in (rec_a.get("kind"), rec_b.get("kind"))
    annots_a = [x for x in rec_a.get("annots", []) if x["role"] == "markup"]
    annots_b = [x for x in rec_b.get("annots", []) if x["role"] == "markup"]

    # Быстрый путь — только побайтовое тождество содержимого (план §8).
    if rec_a.get("content_hash") and rec_a.get("content_hash") == rec_b.get("content_hash"):
        ident = registration.identity()
        ann = _compare_annotations(annots_a, annots_b, ident)
        ann_diffs = len(ann["added"]) + len(ann["removed"]) + len(ann["changed"])
        for name in ("geometry", "color", "text", "text_coverage", "zones", "warnings"):
            channels[name] = st.checked(0, basis="тождество содержимого страницы")
        channels["annotations"] = (st.checked(ann_diffs) if (annots_a or annots_b)
                                   else st.not_applicable("аннотаций нет ни в A, ни в B"))
        channels["spec_positions"] = (st.checked(0, basis="тождество содержимого") if is_spec
                                      else st.not_applicable("не спецификация"))
        status, reasons = st.decide(channels, reasons)
        result = base | {
            "identical_content": True, "status": status, "reasons": reasons, "channels": channels,
            "transform": {"method": "identity", "matrix": ident.tolist(), "identity": True, "reliable": True},
            "canvas": Canvas([0, 0, rec_a["width"], rec_a["height"]], [0, 0, rec_b["width"], rec_b["height"]], dpi).describe(),
            "regions": [], "text": {"changes": [], "only_a": [], "only_b": [], "moved": []},
            "within_tolerance": {"components": 0, "px": 0},
            "zones": [], "overlays": [], "service": [], "annotations": ann, "separated": {"a": [], "b": []},
            "elapsed_s": round(time.time() - started, 3),
        }
        (out_dir / "result.json").write_text(json.dumps(result, ensure_ascii=False), encoding="utf-8")
        return {"status": status, "regions": 0, "identical": True}

    data_a = pdfio.read_bytes(Path(a["pdf"]))
    data_b = pdfio.read_bytes(Path(b["pdf"]))
    pa, pb = a["page"] - 1, b["page"] - 1
    plain_a = pdfio.open_memory(data_a)
    plain_b = pdfio.open_memory(data_b)
    sep_a = separation.separate(data_a, pa, other_image_digests=_image_digests(plain_b, pb))
    sep_b = separation.separate(data_b, pb, other_image_digests=_image_digests(plain_a, pa))
    kinds_a = {s["kind"] for s in sep_a.separated}
    kinds_b = {s["kind"] for s in sep_b.separated}
    zones_a = _zone_rects(rec_a, kinds_a)
    zones_b = _zone_rects(rec_b, kinds_b)

    # Слова основной области — из копии без отделённых объектов и без аннотаций.
    for sep in (sep_a, sep_b):
        pdfio.strip_annotations(sep.doc)
    words_a = [w[:5] + ["text", w[5]] for w in pdfio.page_words_dir(sep_a.doc[pa])]
    words_b = [w[:5] + ["text", w[5]] for w in pdfio.page_words_dir(sep_b.doc[pb])]
    shx_a = [w for w in _load_words(a["profile_dir"], a["page"]) if len(w) > 5 and w[5] == "shx"]
    shx_b = [w for w in _load_words(b["profile_dir"], b["page"]) if len(w) > 5 and w[5] == "shx"]
    words_a += shx_a
    words_b += shx_b

    reg = registration.register(words_a, words_b, [z["bbox"] for z in zones_a], [z["bbox"] for z in zones_b],
                                _load_mask(a["profile_dir"], a["page"]), _load_mask(b["profile_dir"], b["page"]), 36)
    m = np.array(reg["matrix"], dtype=np.float64)
    if not reg["reliable"]:
        reasons.append("ALIGNMENT_UNRELIABLE")
    elif reg.get("dice36") is not None and reg["dice36"] < settings.ALIGN_MIN_DICE:
        # Опорные слова совпали, но рисунки листов в целом не накладываются:
        # совмещена только часть (лист разделён/перекомпонован или переделан).
        reasons.append("ALIGNMENT_PARTIAL")
    page_a, page_b = sep_a.doc[pa], sep_b.doc[pb]
    rect_a = [0.0, 0.0, float(page_a.rect.width), float(page_a.rect.height)]
    rect_b_in_a = _rect_b_to_a(m, [0.0, 0.0, float(page_b.rect.width), float(page_b.rect.height)])
    canvas = Canvas(rect_a, rect_b_in_a, dpi)
    try:
        rgb_a = _render_into(page_a, canvas)
        rgb_b = _render_into(page_b, canvas, _fitz_matrix(m))
    except Exception as exc:  # pragma: no cover - защитный путь
        reasons.append("RENDER_FAILED")
        for name in st.CHANNELS:
            channels.setdefault(name, st.unchecked(f"рендер не выполнен: {exc}"))
        status, reasons = st.decide(channels, reasons)
        result = base | {"status": status, "reasons": reasons, "channels": channels, "error": str(exc)}
        (out_dir / "result.json").write_text(json.dumps(result, ensure_ascii=False), encoding="utf-8")
        return {"status": status, "regions": 0, "identical": False}
    gray_a = cv2.cvtColor(rgb_a, cv2.COLOR_RGB2GRAY)
    gray_b = cv2.cvtColor(rgb_b, cv2.COLOR_RGB2GRAY)
    dark_a = (gray_a < settings.DARK_THRESHOLD).astype(np.uint8)
    dark_b = (gray_b < settings.DARK_THRESHOLD).astype(np.uint8)
    any_a = (gray_a < settings.LIGHT_THRESHOLD).astype(np.uint8)
    any_b = (gray_b < settings.LIGHT_THRESHOLD).astype(np.uint8)
    shape = gray_a.shape

    # Служебные зоны (штамп, поля, неотделённая отметка) — вне основного наложения.
    zone_px_a = [canvas.px(z["bbox"]) for z in zones_a]
    zone_px_b = [canvas.px(_rect_b_to_a(m, z["bbox"])) for z in zones_b]
    zones_mask = _rect_mask(shape, zone_px_a + zone_px_b)
    main = 1 - zones_mask
    # Часть листа (лист A разделён на несколько листов B): сравнивается только
    # область, которую занимает содержимое B, перенесённое в систему A.
    part_info = None
    part_rect_a = None
    if task.get("part") == "split":
        part_rect_a = _part_area(_load_mask(b["profile_dir"], b["page"]), [z["bbox"] for z in zones_b], m)
        if part_rect_a is not None:
            pm_ = _rect_mask(shape, [canvas.px(part_rect_a)])
            ink_a = dark_a & main
            total = int(ink_a.sum())
            part_info = {"kind": "split", "area_a_pt": [round(v, 2) for v in part_rect_a],
                         "coverage_a": round(float((ink_a & pm_).sum()) / total, 4) if total else None}
            main = main & pm_
    elif task.get("part") == "floors":
        part_info = {"kind": "floors", "note": "лист B — один из этажей типового листа A; сравнивается весь лист"}

    r = max(1, int(round(tol_pt * canvas.s)))
    kernel = _disk(r)
    dil_dark_a, dil_dark_b = cv2.dilate(dark_a, kernel), cv2.dilate(dark_b, kernel)
    dil_any_a, dil_any_b = cv2.dilate(any_a, kernel), cv2.dilate(any_b, kernel)
    unc_a = dark_a & (1 - dil_dark_b) & main
    unc_b = dark_b & (1 - dil_dark_a) & main
    # Оформление линии: тёмной линии одной версии в другой рядом соответствует
    # только светлая/тонкая линия — линия та же, изменились толщина или тон
    # (например, убран дубль подложки, нарисованный толстым пером). Это отличие,
    # но не удаление: отдельный класс. Светлая ЗАЛИВКА линией не считается —
    # тёмная линия поверх неё остаётся удалённой/добавленной.
    wide = np.ones((settings.STYLE_FILL_PX, settings.STYLE_FILL_PX), np.uint8)
    fill_a = cv2.dilate(cv2.morphologyEx(any_a & (1 - dark_a), cv2.MORPH_OPEN, wide), kernel)
    fill_b = cv2.dilate(cv2.morphologyEx(any_b & (1 - dark_b), cv2.MORPH_OPEN, wide), kernel)
    style_a = unc_a & dil_any_b & (1 - fill_b)
    style_b = unc_b & dil_any_a & (1 - fill_a)
    a_only = unc_a & (1 - style_a)
    b_only = unc_b & (1 - style_b)
    # Шум — узко (план §5 шаг 4): крошечная компонента (≤ NOISE_MAX_PX), которая
    # примыкает к общей линии, — сглаживание у конца или края линии. Изолированная
    # точка на пустом месте шумом не считается никогда.
    common_near = cv2.dilate((dark_a & dark_b).astype(np.uint8), np.ones((3, 3), np.uint8))
    noise = np.zeros(shape, np.uint8)
    for layer in (a_only, b_only, style_a, style_b):
        n_c, lab_c, st_c, _ = cv2.connectedComponentsWithStats(layer, connectivity=8)
        small = [i for i in range(1, n_c) if st_c[i][4] <= settings.NOISE_MAX_PX]
        if small:
            touch = np.bincount(lab_c[common_near > 0], minlength=n_c)
            ids = np.array([i for i in small if touch[i] > 0], dtype=np.int32)
            if len(ids):
                sel = np.isin(lab_c, ids)
                noise[sel] = 1
                layer[sel] = 0
    style = style_a | style_b
    ring = _disk(r + 1)
    # Светлое «только в A/B» — заметно темнее самого тёмного пикселя другой
    # версии рядом: дрожание сглаживания у порога 250 отличием не считается.
    min_a, min_b = cv2.erode(gray_a, kernel).astype(np.int16), cv2.erode(gray_b, kernel).astype(np.int16)
    light_a = (any_a & (1 - dil_any_b) & (1 - cv2.dilate(a_only | style_a, ring)) & main
               & ((min_b - gray_a) > settings.LIGHT_DELTA))
    light_b = (any_b & (1 - dil_any_a) & (1 - cv2.dilate(b_only | style_b, ring)) & main
               & ((min_a - gray_b) > settings.LIGHT_DELTA))
    light_a, light_b = light_a.astype(np.uint8), light_b.astype(np.uint8)
    del min_a, min_b, fill_a, fill_b, dil_any_a, dil_any_b
    # Тот же узкий шум для светлого: 1–2 пикселя края более толстой линии,
    # примыкающие к линии, которая есть в A или B.
    ink_near = cv2.dilate((dark_a | dark_b).astype(np.uint8), np.ones((3, 3), np.uint8))
    for layer in (light_a, light_b):
        n_c, lab_c, st_c, _ = cv2.connectedComponentsWithStats(layer, connectivity=8)
        small = [i for i in range(1, n_c) if st_c[i][4] <= settings.NOISE_MAX_PX]
        if small:
            touch = np.bincount(lab_c[ink_near > 0], minlength=n_c)
            ids = np.array([i for i in small if touch[i] > 0], dtype=np.int32)
            if len(ids):
                sel = np.isin(lab_c, ids)
                noise[sel] = 1
                layer[sel] = 0
    del ink_near
    strict = ((dark_a ^ dark_b) & main)
    within_tol = strict & (1 - a_only) & (1 - b_only) & (1 - style)
    common_dark = dark_a & dark_b & main
    # Канал цвета: цветность (a*b* в Lab) на общих линиях — у серого и чёрного
    # она ≈ 0, поэтому сглаживание краёв её не меняет; яркость — только во
    # внутренних пикселях линий (без краёв), где сглаживания нет.
    lab_a = cv2.cvtColor(rgb_a, cv2.COLOR_RGB2Lab).astype(np.int16)
    lab_b = cv2.cvtColor(rgb_b, cv2.COLOR_RGB2Lab).astype(np.int16)
    chroma = np.hypot(lab_a[:, :, 1] - lab_b[:, :, 1], lab_a[:, :, 2] - lab_b[:, :, 2])
    core = cv2.erode(common_dark, np.ones((3, 3), np.uint8))
    lum = np.abs(lab_a[:, :, 0] - lab_b[:, :, 0])
    color = ((common_dark & (chroma > settings.COLOR_DELTA)) | (core & (lum > settings.LUMA_DELTA))).astype(np.uint8)
    del lab_a, lab_b, chroma, lum, core
    # Цвет 1–2 пикселей на общей линии — смешение сглаживания на краю после
    # пересчёта масштаба/поворота, а не перекраска (у неё сотни пикселей).
    n_c, lab_c, st_c, _ = cv2.connectedComponentsWithStats(color, connectivity=8)
    ids = np.array([i for i in range(1, n_c) if st_c[i][4] <= settings.NOISE_MAX_PX], dtype=np.int32)
    if len(ids):
        sel = np.isin(lab_c, ids)
        noise[sel] = 1
        color[sel] = 0
    del lab_c

    mm_px = dpi / 25.4
    # Текст основной области.
    zone_rects_a = [z["bbox"] for z in zones_a] + [_rect_b_to_a(m, z["bbox"]) for z in zones_b]
    # Все слова текстового слоя (и в служебных зонах): по ним решается, есть ли
    # у отличия текст. Надпись, центр которой попал в зону штампа другой
    # версии, всё равно читается — это не «надпись без текстового слоя».
    words_all = _words_in_canvas(words_a, canvas, None) + _words_in_canvas(words_b, canvas, m)
    wa = [w for w in _words_in_canvas(words_a, canvas, None) if not _in_rects(w["pt"], zone_rects_a)]
    wb = [w for w in _words_in_canvas(words_b, canvas, m) if not _in_rects(w["pt"], zone_rects_a)]
    if part_rect_a is not None:
        wa = [w for w in wa if _in_rects(w["pt"], [part_rect_a])]
        wb = [w for w in wb if _in_rects(w["pt"], [part_rect_a])]
    words_cmp = _compare_words(wa, wb, max(tol_pt, 0.25))

    # Перемещённые блоки (план §5 шаг 3.4): сдвиг части листа целиком —
    # объяснённые сдвигом пиксели уходят в слой «перемещено», остаток —
    # изменения внутри блока. Слова блока становятся «сдвигом надписи».
    moves = blocks.confirm(blocks.find(words_cmp), a_only, b_only, canvas, mm_px,
                           dark_a=dark_a.astype(np.uint8), dark_b=dark_b.astype(np.uint8))
    moved_px = np.zeros(shape, np.uint8)
    for k, mv in enumerate(moves, 1):
        moved_px |= mv.pop("moved_a") | mv.pop("moved_b")
        gone_a, gone_b = set(), set()
        for wa_, wb_ in zip(mv["a_words"], mv["b_words"]):
            if any(wa_ is w for w in words_cmp["only_a"]) and any(wb_ is w for w in words_cmp["only_b"]):
                gone_a.add(id(wa_))
                gone_b.add(id(wb_))
                d = float(np.hypot(*mv["vector"]))
                words_cmp["moved"].append({"text": wa_["text"], "a_pt": wa_["pt"], "b_in_a_pt": wb_["pt"],
                                           "shift_pt": round(d, 2), "dir": wa_.get("dir"), "block": k})
        words_cmp["only_a"] = [w for w in words_cmp["only_a"] if id(w) not in gone_a]
        words_cmp["only_b"] = [w for w in words_cmp["only_b"] if id(w) not in gone_b]
        mv["words"] = len(mv.pop("a_words"))
        mv.pop("b_words")
    inv = _invert(m)
    to_b_page = lambda rect: _rect_b_to_a(inv, rect)   # noqa: E731
    # Внутри перемещённого блока текст сравнивается в координатах блока: B
    # сдвигается обратно на вектор, «7,5 → 9,0» — изменение, а не удалено + добавлено.
    block_changes: list[dict[str, Any]] = []
    for k, mv in enumerate(moves, 1):
        ba, bb = mv["bbox_a_pt"], mv["bbox_b_in_a_pt"]
        dx, dy = mv["vector"]
        in_a = [w for w in words_cmp["only_a"] if _in_rects(w["pt"], [ba])]
        in_b = [w for w in words_cmp["only_b"] if _in_rects(w["pt"], [bb])]
        if not (in_a or in_b):
            continue
        ids_a, ids_b = {id(w) for w in in_a}, {id(w) for w in in_b}
        words_cmp["only_a"] = [w for w in words_cmp["only_a"] if id(w) not in ids_a]
        words_cmp["only_b"] = [w for w in words_cmp["only_b"] if id(w) not in ids_b]
        for c in textdiff.build(in_a, textdiff.shifted(in_b, -dx, -dy), []):
            if c["b_in_a_bbox"]:
                bx_ = c["b_in_a_bbox"]
                c["b_in_a_bbox"] = [round(bx_[0] + dx, 2), round(bx_[1] + dy, 2), round(bx_[2] + dx, 2),
                                    round(bx_[3] + dy, 2)]
                c["b_bbox"] = [round(v, 2) for v in to_b_page(c["b_in_a_bbox"])]
            c["block"] = f"b{k}"
            if c["kind"] == "changed" and c["before"].split() == c["after"].split():
                # Тот же текст со своим небольшим сдвигом внутри блока — сдвиг надписи.
                (ax, ay), (bx, by) = _center(c["a_bbox"]), _center(c["b_in_a_bbox"])
                c.update(kind="moved", shift_pt=[round(bx - ax, 2), round(by - ay, 2)], norm=None, numbers=[])
            block_changes.append(c)
        # Слова остаются в списках «только A/B» для отчёта, но уже учтены в изменениях блока.
        words_cmp["only_a"] += in_a
        words_cmp["only_b"] += in_b
        mv["only_words"] = (ids_a, ids_b)
    skip_a = {i for mv in moves for i in mv.get("only_words", (set(), set()))[0]}
    skip_b = {i for mv in moves for i in mv.get("only_words", (set(), set()))[1]}
    text_changes = textdiff.finalize(
        textdiff.build([w for w in words_cmp["only_a"] if id(w) not in skip_a],
                       [w for w in words_cmp["only_b"] if id(w) not in skip_b], words_cmp["moved"],
                       to_b_page=to_b_page) + block_changes)
    for mv in moves:
        mv.pop("only_words", None)

    # Регионы отличий: без порога площади (изолированная точка — регион, план §5 шаг 4).
    union = (a_only | b_only | light_a | light_b | color | style).astype(np.uint8)
    close_px = max(1, int(round(settings.REGION_CLOSE_MM * dpi / 25.4)))
    grouped = cv2.morphologyEx(union, cv2.MORPH_CLOSE, _disk(close_px // 2 + 1)) | union
    n, labels, stats, _ = cv2.connectedComponentsWithStats(grouped, connectivity=8)
    # Места сдвинутых надписей (старое и новое, с допуском): пиксели региона вне
    # их — уже не сдвиг, а изменение рядом (линия у сдвинутого слова).
    moved_area = np.zeros(shape, np.uint8)
    for mv_w in words_cmp["moved"]:
        for rect in (mv_w["a_pt"], mv_w["b_in_a_pt"]):
            x0, y0, x1, y1 = canvas.px(rect)
            moved_area[max(0, y0 - r - 1):max(0, y1 + r + 2), max(0, x0 - r - 1):max(0, x1 + r + 2)] = 1
    counts = {}
    for name, layer in (("a", a_only), ("b", b_only), ("la", light_a), ("lb", light_b), ("c", color),
                        ("s", style), ("out", union & (1 - moved_area))):
        counts[name] = np.bincount(labels[layer > 0], minlength=n)
    del moved_area
    shift_diff = (a_only | b_only | light_a | light_b | style).astype(np.uint8)
    block_boxes = [b for mv in moves for b in (mv["bbox_a_pt"], mv["bbox_b_in_a_pt"])]

    # Одно изменение текста — один регион: куски отличий внутри слова или фразы
    # (разные цифры даты, старое и новое место сдвинутой надписи) объединяются.
    parent = list(range(n))

    def find(i: int) -> int:
        while parent[i] != i:
            parent[i] = parent[parent[i]]
            i = parent[i]
        return i
    word_rects = [wd["pt"] for wd in words_cmp["only_a"] + words_cmp["only_b"]]
    word_rects += [mv[k] for mv in words_cmp["moved"] for k in ("a_pt", "b_in_a_pt")]
    word_rects += [c[k] for c in text_changes for k in ("a_bbox", "b_in_a_bbox") if c[k]]
    for rect in word_rects:
        if block_boxes and _in_rects(rect, block_boxes):
            continue        # слова перемещённого блока не склеивают его старое и новое место
        x0, y0, x1, y1 = canvas.px(rect)
        x0, y0, x1, y1 = max(0, x0), max(0, y0), min(shape[1], x1 + 1), min(shape[0], y1 + 1)
        if x1 <= x0 or y1 <= y0:
            continue
        ids = np.unique(labels[y0:y1, x0:x1])
        ids = [int(v) for v in ids if v > 0]
        for other in ids[1:]:
            parent[find(other)] = find(ids[0])
    groups: dict[int, list[int]] = {}
    for i in range(1, n):
        groups.setdefault(find(i), []).append(i)

    regions = []
    unreadable = []
    for i, members in groups.items():
        x = min(int(stats[j][0]) for j in members)
        y = min(int(stats[j][1]) for j in members)
        x_end = max(int(stats[j][0] + stats[j][2]) for j in members)
        y_end = max(int(stats[j][1] + stats[j][3]) for j in members)
        w, h = x_end - x, y_end - y
        box_px = [x, y, x_end, y_end]
        box_pt = canvas.pt(box_px)
        def touching(r_: list[float], box=box_pt) -> bool:
            return not (r_[2] < box[0] or r_[0] > box[2] or r_[3] < box[1] or r_[1] > box[3])
        text_a = [wd["text"] for wd in words_cmp["only_a"] if touching(wd["pt"])]
        text_b = [wd["text"] for wd in words_cmp["only_b"] if touching(wd["pt"])]
        moved_w = [mv for mv in words_cmp["moved"] if touching(mv["a_pt"]) or touching(mv["b_in_a_pt"])]
        moved = [mv["text"] for mv in moved_w]
        has_words = any(touching(wd["pt"]) for wd in words_all)
        tchanges = [c["id"] for c in text_changes
                    if (c["a_bbox"] and touching(c["a_bbox"])) or (c["b_in_a_bbox"] and touching(c["b_in_a_bbox"]))]
        ca, cb, cla, clb, cc, cs = (int(sum(counts[k][j] for j in members)) for k in ("a", "b", "la", "lb", "c", "s"))
        out_moved = int(sum(counts["out"][j] for j in members))
        if text_a or text_b:
            kind = "text"
        elif moved and out_moved <= settings.NOISE_MAX_PX:
            # Только сдвиг тех же надписей — не изменение (решение 15.10).
            kind = "text_moved"
        elif ca and cb:
            kind = "changed"
        elif cb:
            kind = "added"
        elif ca:
            kind = "removed"
        elif cs:
            # Цвет и светлые края рядом с изменённой толщиной — следствие того же
            # (например, линия подложки перестала перекрывать кабель на пересечении).
            kind = "style"
        elif cc and _overdraw(rgb_a, rgb_b, color, dark_a, dark_b, box_px, 2 * r + 4):
            kind = "style"
        elif cc:
            kind = "color"
        else:
            kind = "light"
        if kind in ("added", "removed", "changed", "style", "light"):
            # Знак или линия переехали вместе с соседней надписью / блоком — тоже сдвиг.
            near = settings.SHIFT_NEAR_MM * mm_px / canvas.s
            def close(r_: list[float], box=box_pt, d=near) -> bool:
                return not (r_[2] < box[0] - d or r_[0] > box[2] + d or r_[3] < box[1] - d or r_[1] > box[3] + d)
            vecs = {(round(v[0], 1), round(v[1], 1)) for v in
                    [(_center(mv["b_in_a_pt"])[0] - _center(mv["a_pt"])[0], _center(mv["b_in_a_pt"])[1] - _center(mv["a_pt"])[1])
                     for mv in words_cmp["moved"] if close(mv["a_pt"]) or close(mv["b_in_a_pt"])]
                    + [tuple(mv["vector"]) for mv in moves if close(mv["bbox_a_pt"]) or close(mv["bbox_b_in_a_pt"])]}
            if vecs and _shift_explains(sorted(vecs), shift_diff, any_a, any_b, box_px, canvas):
                kind = "text_moved" if moved else "shifted"
        flagged = False
        if kind in ("added", "removed", "changed") and not has_words:
            # Слов текстового слоя здесь нет ни в A, ни в B: если пятна похожи
            # на строку знаков — это надпись кривыми (SHX/скан), текст не сравнён.
            sub = (a_only[y:y + h, x:x + w] | b_only[y:y + h, x:x + w])
            if _glyph_line(sub, mm_px, common_dark[y:y + h, x:x + w]):
                flagged = True
                unreadable.append(i)
        b0 = _apply(inv, box_pt[0], box_pt[1])
        b1 = _apply(inv, box_pt[2], box_pt[3])
        regions.append({
            "id": f"r{i}", "class": kind, "bbox_px": box_px,
            "bbox_a_pt": [round(v, 2) for v in box_pt],
            "bbox_b_pt": [round(min(b0[0], b1[0]), 2), round(min(b0[1], b1[1]), 2),
                          round(max(b0[0], b1[0]), 2), round(max(b0[1], b1[1]), 2)],
            "a_only_px": ca, "b_only_px": cb, "light_a_px": cla, "light_b_px": clb, "color_px": cc,
            "style_px": cs,
            "text_before": text_a[:40], "text_after": text_b[:40], "moved_text": moved[:40],
            "text_unreadable": flagged, "text_changes": tchanges,
        })
    for k, mv in enumerate(moves, 1):
        ba, bb = mv["bbox_a_pt"], mv["bbox_b_in_a_pt"]
        box_pt = [min(ba[0], bb[0]), min(ba[1], bb[1]), max(ba[2], bb[2]), max(ba[3], bb[3])]
        b0, b1 = _apply(inv, bb[0], bb[1]), _apply(inv, bb[2], bb[3])
        tchanges = [c["id"] for c in text_changes if c["kind"] == "moved" and c["a_bbox"] and _in_rects(c["a_bbox"], [ba])]
        regions.append({
            "id": f"b{k}", "class": "block_moved", "bbox_px": canvas.px(box_pt), "bbox_a_pt": [round(v, 2) for v in ba],
            "bbox_b_pt": [round(min(b0[0], b1[0]), 2), round(min(b0[1], b1[1]), 2),
                          round(max(b0[0], b1[0]), 2), round(max(b0[1], b1[1]), 2)],
            "bbox_b_in_a_pt": [round(v, 2) for v in bb], "shift_pt": [round(v, 2) for v in mv["vector"]],
            "words": mv["words"], "share": mv["share"],
            "a_only_px": 0, "b_only_px": 0, "light_a_px": 0, "light_b_px": 0, "color_px": 0, "style_px": 0,
            "text_before": [], "text_after": [], "moved_text": [], "text_unreadable": False, "text_changes": tchanges,
        })
    regions.sort(key=lambda rg: (rg["bbox_px"][1] // max(1, int(20 * mm_px)), rg["bbox_px"][0]))
    del rgb_a, rgb_b

    tol_n, _tol_lab = cv2.connectedComponents(within_tol, connectivity=8)
    noise_n, _ = cv2.connectedComponents(noise, connectivity=8)
    within = {"components": int(tol_n - 1), "px": int(within_tol.sum()), "tolerance_px": r,
              "noise_components": int(noise_n - 1), "noise_px": int(noise.sum()),
              "note": ("отличия не больше допуска: изменение толщины линий или субпиксельный сдвиг "
                       "(совмещение не тождественное либо координаты в PDF округлены иначе при пересохранении)")}

    # Каналы основной области.
    # Оформление (толщина, тон, порядок наложения) учитывается вместе с цветом:
    # это отличие вида линии, а не её положения.
    # Сдвиг надписи и перенос блока целиком изменением не считаются (решение
    # 15.10): содержание то же; изменения внутри блока — отдельные регионы.
    shift_regions = sum(1 for rg in regions if rg["class"] in SHIFT_CLASSES)
    geom_regions = [rg for rg in regions if rg["class"] not in ("color", "style") + SHIFT_CLASSES]
    style_regions = sum(1 for rg in regions if rg["class"] == "style")
    color_regions = sum(1 for rg in regions if rg["class"] == "color")
    channels["geometry"] = st.checked(len(geom_regions), within_tolerance_components=within["components"],
                                      shifts=shift_regions)
    channels["color"] = st.checked(color_regions + style_regions, color=color_regions, style=style_regions)
    norm_counts = {k: sum(1 for c in text_changes if c.get("norm") == k) for k in ("spaces", "symbol", "number_format")}
    channels["text"] = st.checked(textdiff.significant(text_changes), words_only_a=len(words_cmp["only_a"]),
                                  words_only_b=len(words_cmp["only_b"]), moved=len(words_cmp["moved"]),
                                  same=words_cmp["same"], **norm_counts,
                                  mode="равенство слов с координатами (допуск %.2f pt)" % max(tol_pt, 0.25))
    if not wa and not wb and (dark_a.any() or dark_b.any()):
        channels["text_coverage"] = st.unchecked("нет текстового слоя: надписи сравнены только графически")
        reasons.append("NO_TEXT_LAYER")
    elif unreadable:
        channels["text_coverage"] = st.unchecked("в областях отличий есть надписи без текстового слоя",
                                                 regions=[f"r{i}" for i in unreadable])
        reasons.append("TEXT_UNREADABLE_IN_DIFF_AREA")
    else:
        channels["text_coverage"] = st.checked(0)

    # Служебные зоны: каждая отдельно, плюс отделённые наложения.
    zone_results = []
    zone_words: dict[str, tuple[list[str], list[str]]] = {}
    for z_type in sorted({z["type"] for z in zones_a} | {z["type"] for z in zones_b}):
        rects = [canvas.px(z["bbox"]) for z in zones_a if z["type"] == z_type] + \
                [canvas.px(_rect_b_to_a(m, z["bbox"])) for z in zones_b if z["type"] == z_type]
        zm = _rect_mask(shape, rects)
        # Тот же допуск, что в основной области: субпиксельные сдвиги надписей
        # (например, округление координат при пересохранении) не считаются изменением.
        diff_px = int((((dark_a & (1 - dil_dark_b)) | (dark_b & (1 - dil_dark_a))) & zm).sum())
        strict_px = int(((dark_a ^ dark_b) & zm).sum())
        zr = [canvas.pt(rc) for rc in rects]
        za = [w for w in _words_in_canvas(words_a, canvas, None) if _in_rects(w["pt"], zr)]
        zb = [w for w in _words_in_canvas(words_b, canvas, m) if _in_rects(w["pt"], zr)]
        zc = _compare_words(za, zb, max(tol_pt, 0.25))
        zone_words[z_type] = ([w["text"] for w in _reading_order_words(za)], [w["text"] for w in _reading_order_words(zb)])
        zone_results.append({"type": z_type, "changed": bool(diff_px or zc["only_a"] or zc["only_b"]),
                             "diff_px": diff_px, "within_tolerance_px": strict_px - diff_px,
                             "words_only_a": [w["text"] for w in zc["only_a"]][:60],
                             "words_only_b": [w["text"] for w in zc["only_b"]][:60],
                             # Те же надписи на другом месте: «изменено» без смены текста.
                             "words_moved": [mv["text"] for mv in zc["moved"]][:60]})
    overlays = _compare_overlays(sep_a.separated, sep_b.separated)
    zone_changes = sum(1 for z in zone_results if z["changed"]) + sum(
        1 for o in overlays if o["kind"] != "warning" and o["change"] != "same_text")
    # Поля служебных зон «было → стало» (план §5 шаг 6): по исходным страницам.
    rel_words = zone_words.get("release_stamp", ([], []))
    service_fields = service.compare(
        rec_a, rec_b, plain_a, plain_a[pa], plain_b, plain_b[pb],
        service.release_text(rec_a, sep_a.separated, rel_words[0]),
        service.release_text(rec_b, sep_b.separated, rel_words[1]))
    if service_fields and not zone_changes:
        zone_changes = len(service_fields)
    channels["zones"] = st.checked(zone_changes, fields=len(service_fields))

    ann = _compare_annotations(annots_a, annots_b, m)
    ann_diffs = len(ann["added"]) + len(ann["removed"]) + len(ann["changed"])
    channels["annotations"] = (st.checked(ann_diffs) if (annots_a or annots_b)
                               else st.not_applicable("аннотаций нет ни в A, ни в B"))

    warn_rejected = [r_ for r_ in sep_a.rejected + sep_b.rejected if r_.get("kind") == "warning"]
    warn_sep = [o for o in overlays if o["kind"] == "warning"]
    baked = {"a": _warning_lines(wa), "b": _warning_lines(wb)}
    if baked["a"] or baked["b"]:
        channels["warnings"] = st.unchecked("предупреждение вшито в содержимое листа и не отделено: отличия под ним "
                                            "могут быть скрыты им самим", baked=baked)
        reasons.append("PARTIAL_COVERAGE")
    elif warn_rejected:
        channels["warnings"] = st.unchecked("предупреждение не удалось отделить безопасно; площадь под ним не проверена",
                                            share=max((r_.get("painted_share") or 0) for r_ in warn_rejected))
        reasons.append("PARTIAL_COVERAGE")
    elif warn_sep:
        changes = {o["change"] for o in warn_sep}
        note = ("предупреждение появилось в B" if changes == {"added"}
                else "исчезновение предупреждения не подтверждает согласования" if "removed" in changes
                else "текст предупреждения в A и B сравнён отдельно от листа")
        channels["warnings"] = st.checked(sum(1 for o in warn_sep if o["change"] != "same_text"), note=note)
    else:
        channels["warnings"] = st.not_applicable("предупреждений не обнаружено")

    main_diffs = channels["geometry"]["diffs"] + channels["color"]["diffs"] + channels["text"]["diffs"]
    if is_spec:
        channels["spec_positions"] = (st.checked(0, basis="лист совпадает по графике и тексту") if main_diffs == 0
                                      else st.unchecked("позиции СО не сравнивались (этап 2)"))
    else:
        channels["spec_positions"] = st.not_applicable("не спецификация")

    status, reasons = st.decide(channels, reasons)

    layers = {"common": ((dark_a | dark_b) & (1 - a_only) & (1 - b_only)).astype(np.uint8),
              "a_only": a_only, "b_only": b_only, "light_a": light_a, "light_b": light_b, "color": color,
              "style": style, "moved": moved_px,
              "within_tol": within_tol, "noise": noise, "zones": zones_mask, "ink_any": (any_a | any_b).astype(np.uint8)}
    np.savez_compressed(out_dir / "masks.npz", shape=np.array(shape), **{k: _pack(v) for k, v in layers.items()})
    prev = _preview(layers)
    cv2.imwrite(str(out_dir / "preview.png"), cv2.cvtColor(prev, cv2.COLOR_RGB2BGR))

    result = base | {
        "identical_content": False, "status": status, "reasons": reasons, "channels": channels,
        "transform": reg, "canvas": canvas.describe(), "regions": regions,
        "text": {"changes": text_changes[:2000], "only_a": words_cmp["only_a"][:500],
                 "only_b": words_cmp["only_b"][:500], "moved": words_cmp["moved"][:500]},
        "within_tolerance": within, "zones": zone_results, "overlays": overlays, "service": service_fields,
        "part": part_info,
        "annotations": ann,
        "separated": {"a": sep_a.separated, "b": sep_b.separated,
                      "a_rejected": sep_a.rejected, "b_rejected": sep_b.rejected},
        "preview": {"file": "preview.png", "width": int(prev.shape[1]), "height": int(prev.shape[0])},
        "elapsed_s": round(time.time() - started, 3),
    }
    (out_dir / "result.json").write_text(json.dumps(result, ensure_ascii=False), encoding="utf-8")
    return {"status": status, "regions": len(regions) - shift_regions, "style_regions": style_regions,
            "shift_regions": shift_regions, "identical": False}
