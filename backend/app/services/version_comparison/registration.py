"""Выравнивание листа B к листу A (план §5 шаг 3).

Только подобие: масштаб, поворот, сдвиг. Локальных деформаций нет.
1. Опорные слова: уникальные на обеих страницах слова вне служебных зон,
   RANSAC по центрам (``cv2.estimateAffinePartial2D``).
2. Запасной путь — фазовая корреляция масок 36 dpi (только сдвиг).
3. Контроль — совпадение масок (Dice) после совмещения.

Преобразование задаётся матрицей 2×3 из координат B (pt, повёрнутая
система) в координаты A.
"""
from __future__ import annotations

import math
from collections import Counter
from typing import Any

import cv2
import numpy as np

from . import pdfio, settings


def _inside_any(cx: float, cy: float, rects: list[list[float]]) -> bool:
    return any(r[0] <= cx <= r[2] and r[1] <= cy <= r[3] for r in rects)


def _anchors(words: list[list[Any]], zones: list[list[float]]) -> dict[str, tuple[float, float]]:
    texts = Counter()
    keep = []
    for w in words:
        t = pdfio.normalize_token(w[4])
        if len(t) < 3 or not any(ch.isalnum() for ch in t):
            continue
        cx, cy = (w[0] + w[2]) / 2, (w[1] + w[3]) / 2
        if _inside_any(cx, cy, zones):
            continue
        texts[t] += 1
        keep.append((t, cx, cy))
    return {t: (cx, cy) for t, cx, cy in keep if texts[t] == 1}


def _decompose(m: np.ndarray) -> dict[str, float]:
    scale = math.hypot(m[0, 0], m[1, 0])
    rot = math.degrees(math.atan2(m[1, 0], m[0, 0]))
    return {"scale": scale, "rot_deg": rot, "dx": float(m[0, 2]), "dy": float(m[1, 2])}


def identity() -> np.ndarray:
    return np.array([[1.0, 0.0, 0.0], [0.0, 1.0, 0.0]])


def _snap(m: np.ndarray) -> tuple[np.ndarray, bool]:
    d = _decompose(m)
    if abs(d["scale"] - 1) < 1e-3 and abs(d["rot_deg"]) < 0.05 and abs(d["dx"]) < 0.5 and abs(d["dy"]) < 0.5:
        return identity(), True
    if abs(d["scale"] - 1) < 1e-3 and abs(d["rot_deg"]) < 0.05:
        return np.array([[1.0, 0.0, d["dx"]], [0.0, 1.0, d["dy"]]]), False
    return m, False


def by_anchors(words_a, words_b, zones_a, zones_b) -> dict[str, Any] | None:
    aa, bb = _anchors(words_a, zones_a), _anchors(words_b, zones_b)
    common = [t for t in aa if t in bb]
    if len(common) < 3:
        return {"method": "anchors", "accepted": False, "anchors": len(common), "reason": "мало общих слов"}
    src = np.float32([bb[t] for t in common])
    dst = np.float32([aa[t] for t in common])
    m, inl = cv2.estimateAffinePartial2D(src, dst, method=cv2.RANSAC,
                                         ransacReprojThreshold=settings.ANCHOR_RANSAC_PT,
                                         maxIters=4000, confidence=0.999)
    if m is None or inl is None:
        return {"method": "anchors", "accepted": False, "anchors": len(common), "reason": "RANSAC не сошёлся"}
    inl = inl.ravel().astype(bool)
    proj = src @ m[:, :2].T + m[:, 2]
    res = np.hypot(*(proj - dst).T)
    inliers = int(inl.sum())
    median = float(np.median(res[inl])) if inliers else 1e9
    need = max(settings.ANCHOR_MIN_INLIERS, int(math.ceil(settings.ANCHOR_MIN_RATIO * len(common))))
    accepted = inliers >= need and median <= settings.ANCHOR_MAX_MEDIAN_RESIDUAL_PT
    snapped, is_identity = _snap(m)
    return {"method": "anchors", "accepted": bool(accepted), "anchors": len(common), "inliers": inliers,
            "median_residual_pt": round(median, 3), "matrix": snapped.tolist(), "identity": is_identity}


def by_phase(mask_a: np.ndarray, mask_b: np.ndarray, dpi: int) -> dict[str, Any]:
    h = max(mask_a.shape[0], mask_b.shape[0])
    w = max(mask_a.shape[1], mask_b.shape[1])
    a = np.zeros((h, w), np.float32)
    b = np.zeros((h, w), np.float32)
    a[:mask_a.shape[0], :mask_a.shape[1]] = mask_a
    b[:mask_b.shape[0], :mask_b.shape[1]] = mask_b
    win = cv2.createHanningWindow((w, h), cv2.CV_32F)
    (dx, dy), response = cv2.phaseCorrelate(b * win, a * win)
    k = 72.0 / dpi
    m = np.array([[1.0, 0.0, dx * k], [0.0, 1.0, dy * k]])
    snapped, is_identity = _snap(m)
    return {"method": "phase", "accepted": response > 0.05, "response": round(float(response), 4),
            "matrix": snapped.tolist(), "identity": is_identity}


def dice(mask_a: np.ndarray, mask_b: np.ndarray, matrix: np.ndarray, dpi: int,
         zones_a: list[list[float]]) -> float:
    """Совпадение масок 36 dpi после переноса B в систему A (с допуском 1 px)."""
    k = dpi / 72.0
    m = np.array(matrix, dtype=np.float64).copy()
    m[:, 2] *= k
    warped = cv2.warpAffine(mask_b, m, (mask_a.shape[1], mask_a.shape[0]), flags=cv2.INTER_NEAREST)
    a = mask_a.copy()
    for z in zones_a:
        a[int(z[1] * k):int(z[3] * k) + 1, int(z[0] * k):int(z[2] * k) + 1] = 0
        warped[int(z[1] * k):int(z[3] * k) + 1, int(z[0] * k):int(z[2] * k) + 1] = 0
    kernel = np.ones((3, 3), np.uint8)
    inter_a = int((a & cv2.dilate(warped, kernel)).sum())
    inter_b = int((warped & cv2.dilate(a, kernel)).sum())
    total = int(a.sum()) + int(warped.sum())
    if total == 0:
        return 1.0
    return (inter_a + inter_b) / total


def register(words_a, words_b, zones_a, zones_b, mask_a, mask_b, mask_dpi: int) -> dict[str, Any]:
    """Итоговое преобразование B→A с методом, качеством и причинами."""
    anchor = by_anchors(words_a, words_b, zones_a, zones_b)
    chosen = anchor if anchor and anchor.get("accepted") else None
    tried = [anchor]
    if chosen is None and mask_a is not None and mask_b is not None:
        phase = by_phase(mask_a, mask_b, mask_dpi)
        tried.append(phase)
        if phase["accepted"]:
            chosen = phase
    if chosen is None:
        chosen = {"method": "identity", "accepted": False, "matrix": identity().tolist(), "identity": True}
    matrix = np.array(chosen["matrix"])
    quality = dice(mask_a, mask_b, matrix, mask_dpi, zones_a) if mask_a is not None and mask_b is not None else None
    reliable = bool(chosen.get("accepted")) and (chosen["method"] == "anchors" or (quality or 0) >= settings.ALIGN_MIN_DICE)
    out = {"method": chosen["method"], "matrix": matrix.tolist(), "identity": bool(chosen.get("identity")),
           "reliable": reliable, "dice36": None if quality is None else round(quality, 4),
           "attempts": [{k: v for k, v in t.items() if k != "matrix"} for t in tried if t]}
    out.update({k: round(v, 5) for k, v in _decompose(matrix).items()})
    if chosen["method"] == "anchors":
        out.update(anchors=chosen["anchors"], inliers=chosen["inliers"], median_residual_pt=chosen["median_residual_pt"])
    return out
