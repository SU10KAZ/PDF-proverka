"""Детерминированное геометрическое совмещение уже принятых пар листов.

Модуль намеренно не ищет изменения и не меняет ``page_alignment``.  Он строит
только affine/similarity transform из координат V3 в систему координат V2 и
сохраняет диагностические изображения.  Сложные деформации здесь запрещены:
иначе совмещение могло бы скрыть реальное изменение чертежа.
"""
from __future__ import annotations

import json
import math
import os
import re
import tempfile
import unicodedata
from collections import Counter
from pathlib import Path
from typing import Any

import cv2
import numpy as np


SCHEMA_VERSION = 1
_WORD = re.compile(r"[^0-9a-zа-яё]+", re.IGNORECASE)
_MAX_ROTATION_DEG = 3.0
_MIN_SCALE, _MAX_SCALE = 0.94, 1.06
_MAX_TRANSLATION_SHARE = 0.15


def _round(value: float | None, digits: int = 6) -> float | None:
    return round(float(value), digits) if value is not None else None


def _matrix(value: Any) -> np.ndarray:
    result = np.asarray(value, dtype=np.float64)
    if result.shape == (2, 3):
        result = np.vstack([result, [0.0, 0.0, 1.0]])
    if result.shape != (3, 3):
        raise ValueError("expected_affine_3x3_matrix")
    return result


def transform_points(matrix: Any, points: list[tuple[float, float]] | np.ndarray) -> np.ndarray:
    """Перевести точки V3 в систему V2 по сохранённой матрице."""
    source = np.asarray(points, dtype=np.float64).reshape((-1, 2))
    if not len(source):
        return source
    homogeneous = np.column_stack([source, np.ones(len(source))])
    transformed = (homogeneous @ _matrix(matrix).T)[:, :2]
    return transformed


def transform_bbox(matrix: Any, bbox: list[float] | tuple[float, float, float, float]) -> list[float]:
    """Перевести bbox V3 → V2; сохраняет осевой bbox преобразованных углов."""
    x0, y0, x1, y1 = (float(value) for value in bbox)
    mapped = transform_points(matrix, [(x0, y0), (x1, y0), (x1, y1), (x0, y1)])
    return [_round(mapped[:, 0].min()), _round(mapped[:, 1].min()), _round(mapped[:, 0].max()), _round(mapped[:, 1].max())]


def _matrix_info(matrix: np.ndarray) -> dict[str, Any]:
    linear = matrix[:2, :2]
    sx = float(np.linalg.norm(linear[:, 0]))
    sy = float(np.linalg.norm(linear[:, 1]))
    rotation = math.degrees(math.atan2(float(linear[1, 0]), float(linear[0, 0]))) if sx else 0.0
    return {
        "matrix": [[_round(value) for value in row] for row in matrix.tolist()],
        "translation_x": _round(matrix[0, 2]), "translation_y": _round(matrix[1, 2]),
        "scale": _round((sx + sy) / 2), "scale_x": _round(sx), "scale_y": _round(sy),
        "rotation_deg": _round(rotation),
    }


def estimate_similarity_transform(source_points, destination_points, *, ransac: bool = True) -> tuple[np.ndarray | None, np.ndarray]:
    """Оценить similarity transform source → destination c RANSAC.

    Это отдельная публичная функция: следующий этап сможет безопасно применять
    сохранённую matrix к bbox, а unit-тесты проверяют детерминизм геометрии без
    зависимости от PDF object order.
    """
    source = np.asarray(source_points, dtype=np.float64).reshape((-1, 2))
    destination = np.asarray(destination_points, dtype=np.float64).reshape((-1, 2))
    if len(source) != len(destination) or len(source) < 2:
        return None, np.zeros(len(source), dtype=bool)
    method = cv2.RANSAC if ransac and len(source) >= 3 else cv2.LMEDS
    # OpenCV 4.13 accepts CV_32F here reliably; keep the returned matrix in
    # float64 below for PDF-coordinate precision.
    affine, inliers = cv2.estimateAffinePartial2D(
        source.astype(np.float32), destination.astype(np.float32), method=method, ransacReprojThreshold=3.0,
        maxIters=3000, confidence=0.995, refineIters=20,
    )
    if affine is None:
        return None, np.zeros(len(source), dtype=bool)
    return _matrix(affine), np.asarray(inliers, dtype=bool).reshape(-1) if inliers is not None else np.ones(len(source), dtype=bool)


def _norm_token(value: str) -> str:
    normalized = unicodedata.normalize("NFKC", value or "").lower().replace("ё", "е")
    return _WORD.sub("", normalized)


def _word_anchors(left_page, right_page) -> tuple[np.ndarray, np.ndarray, dict[str, Any]]:
    """Только уникальные нештамповые текстовые якоря; V3-точки → V2-точки."""
    def words(page):
        width, height = float(page.rect.width), float(page.rect.height)
        values = []
        for word in page.get_text("words") or []:
            x0, y0, x1, y1, raw = word[:5]
            token = _norm_token(str(raw))
            # Числа, даты и одиночные буквы слишком часто принадлежат штампу/
            # осям. Нижний правый штамп дополнительно исключается геометрически.
            if len(token) < 3 or token.isdigit() or (x0 + x1) / 2 > width * .63 and (y0 + y1) / 2 > height * .64:
                continue
            values.append((token, ((x0 + x1) / 2, (y0 + y1) / 2)))
        counts = Counter(token for token, _ in values)
        return {token: point for token, point in values if counts[token] == 1}

    left, right = words(left_page), words(right_page)
    common = sorted(set(left) & set(right))
    source = np.asarray([right[token] for token in common], dtype=np.float64).reshape((-1, 2))
    destination = np.asarray([left[token] for token in common], dtype=np.float64).reshape((-1, 2))
    return source, destination, {"unique_left": len(left), "unique_right": len(right), "common": len(common), "tokens": common[:80]}


def _render(page, target_long_side: int = 1700) -> np.ndarray:
    import fitz
    longest = max(float(page.rect.width), float(page.rect.height), 1.0)
    scale = target_long_side / longest
    pix = page.get_pixmap(matrix=fitz.Matrix(scale, scale), colorspace=fitz.csGRAY, alpha=False)
    image = np.frombuffer(pix.samples, dtype=np.uint8).reshape((pix.height, pix.width))
    return image


def _raster_candidate(left_page, right_page) -> tuple[np.ndarray | None, dict[str, Any]]:
    """ORB + RANSAC как независимая классическая raster-проверка.

    В штампе часто меняются даты и ревизии, поэтому его нижняя правая область
    не участвует в feature detection. Белый фон не участвует вообще: ORB берёт
    только реальные контуры/текст/графику.
    """
    left_image, right_image = _render(left_page), _render(right_page)
    mask_left, mask_right = np.full(left_image.shape, 255, np.uint8), np.full(right_image.shape, 255, np.uint8)
    for mask in (mask_left, mask_right):
        height, width = mask.shape
        mask[int(height * .64):, int(width * .63):] = 0
    detector = cv2.ORB_create(nfeatures=2500, fastThreshold=10, edgeThreshold=15)
    left_keypoints, left_descriptors = detector.detectAndCompute(left_image, mask_left)
    right_keypoints, right_descriptors = detector.detectAndCompute(right_image, mask_right)
    meta = {"left_keypoints": len(left_keypoints or []), "right_keypoints": len(right_keypoints or []), "matches": 0, "inliers": 0}
    if left_descriptors is None or right_descriptors is None or len(left_descriptors) < 4 or len(right_descriptors) < 4:
        return None, meta
    raw = cv2.BFMatcher(cv2.NORM_HAMMING).knnMatch(right_descriptors, left_descriptors, k=2)
    good = [pair[0] for pair in raw if len(pair) == 2 and pair[0].distance < .72 * pair[1].distance]
    meta["matches"] = len(good)
    if len(good) < 4:
        return None, meta
    source = np.asarray([right_keypoints[match.queryIdx].pt for match in good], dtype=np.float64)
    destination = np.asarray([left_keypoints[match.trainIdx].pt for match in good], dtype=np.float64)
    image_matrix, inliers = estimate_similarity_transform(source, destination)
    meta["inliers"] = int(inliers.sum())
    if image_matrix is None or int(inliers.sum()) < 4:
        return None, meta
    # image pixels → original PDF coordinates (две страницы могут иметь разные
    # dimensions; матрицу сохраняем только в PDF coordinate space).
    left_to_pdf = np.array([[left_page.rect.width / left_image.shape[1], 0, 0], [0, left_page.rect.height / left_image.shape[0], 0], [0, 0, 1]], dtype=np.float64)
    pdf_to_right = np.array([[right_image.shape[1] / right_page.rect.width, 0, 0], [0, right_image.shape[0] / right_page.rect.height, 0], [0, 0, 1]], dtype=np.float64)
    matrix = left_to_pdf @ image_matrix @ pdf_to_right
    inlier_source_pdf = transform_points(np.linalg.inv(pdf_to_right), source[inliers])
    inlier_destination_pdf = transform_points(left_to_pdf, destination[inliers])
    meta["source_points"] = inlier_source_pdf
    meta["destination_points"] = inlier_destination_pdf
    return matrix, meta


def _residual(matrix: np.ndarray, source: np.ndarray, destination: np.ndarray, diagonal: float) -> tuple[float | None, np.ndarray]:
    if not len(source):
        return None, np.asarray([], dtype=np.float64)
    errors = np.linalg.norm(transform_points(matrix, source) - destination, axis=1)
    return float(np.median(errors) / max(diagonal, 1.0)), errors


def _coverage(points: np.ndarray, width: float, height: float) -> float:
    if len(points) < 2:
        return 0.0
    # Сетка устойчива к длинной линии и не даёт нескольким точкам в одном месте
    # искусственно сделать coverage высоким.
    cells = {(min(3, max(0, int(x / max(width, 1) * 4))), min(3, max(0, int(y / max(height, 1) * 4)))) for x, y in points}
    return len(cells) / 16.0


def _matrix_distance(first: np.ndarray, second: np.ndarray, width: float, height: float) -> float:
    probes = np.asarray([[0, 0], [width, 0], [0, height], [width, height], [width / 2, height / 2]], dtype=np.float64)
    return float(np.median(np.linalg.norm(transform_points(first, probes) - transform_points(second, probes), axis=1)) / max(math.hypot(width, height), 1.0))


def _vector_count(page) -> int:
    try:
        return len(page.get_drawings() or [])
    except Exception:
        return 0


def _diagnostic_images(left_page, right_page, matrix: np.ndarray | None, output_dir: Path, stem: str) -> dict[str, str]:
    """Создать normal alpha overlay и color overlay. Это не diff-артефакты."""
    left, right = _render(left_page), _render(right_page)
    if matrix is None:
        warped = cv2.resize(right, (left.shape[1], left.shape[0]), interpolation=cv2.INTER_AREA)
    else:
        left_to_image = np.array([[left.shape[1] / left_page.rect.width, 0, 0], [0, left.shape[0] / left_page.rect.height, 0], [0, 0, 1]], dtype=np.float64)
        image_to_right = np.array([[right_page.rect.width / right.shape[1], 0, 0], [0, right_page.rect.height / right.shape[0], 0], [0, 0, 1]], dtype=np.float64)
        image_matrix = left_to_image @ matrix @ image_to_right
        warped = cv2.warpAffine(right, image_matrix[:2], (left.shape[1], left.shape[0]), flags=cv2.INTER_LINEAR, borderValue=255)
    alpha = cv2.addWeighted(left, .5, warped, .5, 0)
    # V2 is red, transformed V3 is cyan. Совпадение становится близким к серому.
    color = np.full((left.shape[0], left.shape[1], 3), 255, dtype=np.uint8)
    color[:, :, 2] = left
    color[:, :, 1] = warped
    color[:, :, 0] = warped
    output_dir.mkdir(parents=True, exist_ok=True)
    overlay, color_overlay = output_dir / f"{stem}_overlay.png", output_dir / f"{stem}_color.png"
    cv2.imwrite(str(overlay), alpha); cv2.imwrite(str(color_overlay), color)
    return {"overlay": str(overlay), "color_overlay": str(color_overlay)}


def align_pdf_pages(left_pdf, right_pdf, left_page_number: int, right_page_number: int, *, diagnostics_dir: str | Path | None = None) -> dict:
    """Совместить одну принятую пару. Никаких findings/diff/page-map write."""
    import fitz
    try:
        left_document, right_document = fitz.open(str(left_pdf)), fitz.open(str(right_pdf))
    except Exception as exc:
        return {"left_page": left_page_number, "right_page": right_page_number, "status": "failed", "reason": f"pdf_open_failed:{exc}"}
    try:
        if not (1 <= left_page_number <= left_document.page_count and 1 <= right_page_number <= right_document.page_count):
            return {"left_page": left_page_number, "right_page": right_page_number, "status": "failed", "reason": "page_out_of_range"}
        left_page, right_page = left_document[left_page_number - 1], right_document[right_page_number - 1]
        width, height = float(left_page.rect.width), float(left_page.rect.height)
        diagonal = math.hypot(width, height)
        text_source, text_destination, text_meta = _word_anchors(left_page, right_page)
        text_matrix, text_inliers = estimate_similarity_transform(text_source, text_destination)
        if text_matrix is not None:
            text_source, text_destination = text_source[text_inliers], text_destination[text_inliers]
        raster_matrix, raster_meta = _raster_candidate(left_page, right_page)
        raster_source = np.asarray(raster_meta.pop("source_points", []), dtype=np.float64).reshape((-1, 2))
        raster_destination = np.asarray(raster_meta.pop("destination_points", []), dtype=np.float64).reshape((-1, 2))
        candidates = []
        if text_matrix is not None and len(text_source) >= 2:
            residual, _ = _residual(text_matrix, text_source, text_destination, diagonal)
            candidates.append(("text_anchors", text_matrix, text_source, text_destination, residual))
        if raster_matrix is not None and len(raster_source) >= 2:
            residual, _ = _residual(raster_matrix, raster_source, raster_destination, diagonal)
            candidates.append(("vector_raster_orb", raster_matrix, raster_source, raster_destination, residual))
        # Text coordinates are source of truth where enough anchors survived;
        # otherwise ORB is a conservative fallback. We do not average matrices.
        chosen = next((candidate for candidate in candidates if candidate[0] == "text_anchors" and len(candidate[2]) >= 4), None)
        if chosen is None and candidates:
            chosen = candidates[0]
        if chosen:
            method, matrix, source, destination, residual = chosen
        else:
            method, matrix, source, destination, residual = "insufficient_anchors", None, np.empty((0, 2)), np.empty((0, 2)), None
        vector_left, vector_right = _vector_count(left_page), _vector_count(right_page)
        vector_support = min(1.0, math.sqrt(min(vector_left, vector_right) / 80.0)) if min(vector_left, vector_right) else 0.0
        text_support = min(1.0, len(text_source) / 20.0)
        raster_support = min(1.0, int(raster_meta.get("inliers") or 0) / 50.0)
        coverage = _coverage(destination, width, height)
        disagreement = _matrix_distance(text_matrix, raster_matrix, width, height) if text_matrix is not None and raster_matrix is not None else None
        info = _matrix_info(matrix) if matrix is not None else None
        geometry_ok = bool(info and _MIN_SCALE <= float(info["scale_x"]) <= _MAX_SCALE and _MIN_SCALE <= float(info["scale_y"]) <= _MAX_SCALE and abs(float(info["rotation_deg"])) <= _MAX_ROTATION_DEG and abs(float(info["translation_x"])) <= width * _MAX_TRANSLATION_SHARE and abs(float(info["translation_y"])) <= height * _MAX_TRANSLATION_SHARE)
        residual_value = float(residual) if residual is not None else 1.0
        confidence = max(0.0, min(1.0, .18 * vector_support + .34 * text_support + .30 * raster_support + .18 * min(1.0, coverage / .25) - min(.45, residual_value * 15) - (0.22 if disagreement is not None and disagreement > .015 else 0.0)))
        anchors = len(source)
        if matrix is None or not geometry_ok or (disagreement is not None and disagreement > .06):
            status = "failed"
        elif anchors >= 5 and coverage >= .125 and residual_value <= .008 and confidence >= .50:
            status = "aligned"
        else:
            status = "weak_alignment"
        reasons = []
        if matrix is None: reasons.append("insufficient_independent_anchors")
        if info and not geometry_ok: reasons.append("transform_outside_conservative_limits")
        if residual is not None and residual_value > .008: reasons.append("high_residual_error")
        if coverage < .125: reasons.append("low_anchor_coverage")
        if disagreement is not None and disagreement > .015: reasons.append("text_and_raster_disagree")
        image_paths = {}
        if diagnostics_dir is not None:
            image_paths = _diagnostic_images(left_page, right_page, matrix, Path(diagnostics_dir), f"v2_{left_page_number:03d}_v3_{right_page_number:03d}")
        return {
            "left_page": left_page_number, "right_page": right_page_number, "status": status,
            "method": method if matrix is not None else "none", "transform": info,
            "quality": {
                "vector_support": _round(vector_support), "text_anchor_support": _round(text_support), "raster_support": _round(raster_support),
                "residual_error": _round(residual), "matched_anchor_count": anchors, "anchor_count": anchors,
                "coverage": _round(coverage), "confidence": _round(confidence),
                "text_raster_disagreement": _round(disagreement),
                "vector_primitive_counts": {"left": vector_left, "right": vector_right},
                "text_anchor_candidates": text_meta, "raster": raster_meta,
                "blank_area_excluded": True,
            },
            "diagnostics": image_paths, "reason": "; ".join(reasons) if reasons else "conservative_similarity_transform",
        }
    except Exception as exc:
        return {"left_page": left_page_number, "right_page": right_page_number, "status": "failed", "reason": f"alignment_failed:{exc}"}
    finally:
        left_document.close(); right_document.close()


def evaluate_sheet_alignment(left_document: dict, right_document: dict, *, left_pdf, right_pdf, sheet_matching: dict, alignment_items: list[dict] | None = None, diagnostics_dir: str | Path | None = None) -> dict:
    """Проверить только accepted matcher/manual pairs; uncertain rows не трогаем."""
    pairs, seen = [], set()
    for match in sheet_matching.get("matches") or []:
        if match.get("status") == "matched":
            key = (int(match["left_page"]), int(match["right_page"]))
            pairs.append({"left_page": key[0], "right_page": key[1], "source": "sheet_matcher"}); seen.add(key)
    for item in alignment_items or []:
        if str(item.get("mode") or "") != "manual" or item.get("left_page") is None or item.get("right_page") is None:
            continue
        key = (int(item["left_page"]), int(item["right_page"]))
        if key not in seen:
            pairs.append({"left_page": key[0], "right_page": key[1], "source": "manual_alignment"}); seen.add(key)
    rows = []
    for pair in sorted(pairs, key=lambda item: (item["left_page"], item["right_page"])):
        item = align_pdf_pages(left_pdf, right_pdf, pair["left_page"], pair["right_page"], diagnostics_dir=diagnostics_dir)
        item["source"] = pair["source"]
        rows.append(item)
    return {
        "schema_version": SCHEMA_VERSION, "kind": "stage_comparison_sheet_alignment",
        "settings": {"llm_used": False, "findings_created": False, "page_map_changed": False, "model": "similarity_affine_only", "direction": "V3_to_V2"},
        "input": {"left": left_document.get("document") or {}, "right": right_document.get("document") or {}}, "items": rows,
        "summary": {status: sum(item.get("status") == status for item in rows) for status in ("aligned", "weak_alignment", "failed")},
    }


def _atomic_write(path: Path, payload: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(prefix=path.name + ".", suffix=".tmp", dir=path.parent)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as output:
            output.write(payload); output.flush(); os.fsync(output.fileno())
        os.replace(temporary, path)
    except BaseException:
        try: os.unlink(temporary)
        except OSError: pass
        raise
    return path


def write_sheet_alignment_report(directory: str | Path, report: dict) -> tuple[Path, Path]:
    directory = Path(directory)
    json_path, md_path = directory / "sheet_alignment.json", directory / "sheet_alignment.md"
    _atomic_write(json_path, json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n")
    lines = ["# Геометрическое совмещение сопоставленных листов", "", "Направление матрицы: **V3 PDF coordinates → V2 PDF coordinates**. Это диагностический этап: карта листов и расхождения не изменялись.", "", "| V2 | V3 | Status | Method | ΔX | ΔY | Scale | Rotation | Residual | Anchors | Coverage | Confidence | Overlay | Color |", "| ---: | ---: | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- |"]
    for item in report.get("items") or []:
        transform, quality, diagnostics = item.get("transform") or {}, item.get("quality") or {}, item.get("diagnostics") or {}
        overlay, color = diagnostics.get("overlay"), diagnostics.get("color_overlay")
        overlay_link = f"[overlay]({os.path.relpath(overlay, directory)})" if overlay else "—"
        color_link = f"[color]({os.path.relpath(color, directory)})" if color else "—"
        lines.append("| " + " | ".join([str(item.get("left_page")), str(item.get("right_page")), str(item.get("status") or "—"), str(item.get("method") or "—"), str(transform.get("translation_x", "—")), str(transform.get("translation_y", "—")), str(transform.get("scale", "—")), str(transform.get("rotation_deg", "—")), str(quality.get("residual_error", "—")), str(quality.get("matched_anchor_count", "—")), str(quality.get("coverage", "—")), str(quality.get("confidence", "—")), overlay_link, color_link]) + " |")
    lines.extend(["", "## Счётчики", "", *[f"- {status}: {count}" for status, count in (report.get("summary") or {}).items()], ""])
    _atomic_write(md_path, "\n".join(lines))
    return json_path, md_path
