"""Детерминированный grounding ``highlight_regions`` по текстовому слою PDF.

Замечание уже содержит редкие обозначения чертежа (``СВ-1.6``, ``QF12.3``,
``П.1``). Модуль ищет эти обозначения среди ``fitz.Page.get_text("words")``,
строго обрезанных областью исходного блока, и переводит union bbox найденных
слов из координат PDF в координаты кропа ``coords_norm``.

Никаких LLM/OCR-вызовов здесь нет. Все ошибки fail-soft: отсутствие PDF,
текстового слоя, блока, страницы или валидных координат даёт no-op.
"""
from __future__ import annotations

import json
import math
import re
import time
import unicodedata
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional, Sequence

from backend.app.pipeline.stages.block_grounding.singleline_graph_geometry import (
    _clip_words_to_bbox,
    _clip_words_to_polygon,
    _result_blocks_vector_index,
)
from backend.app.services.findings.verdict_preservation import (
    _salient_numbers,
)

SHADOW_FILENAME = "textlayer_highlights_shadow.json"
SCHEMA_VERSION = 1
DEFAULT_PADDING = 0.01
MAX_REGIONS_PER_FINDING = 3
MAX_REGION_WIDTH = 0.60
MAX_REGION_HEIGHT = 0.35

# Буквенный префикс + цифры + хотя бы один разделитель. Намеренно не ловит
# голые ``100``/``160``: числа разрешены лишь как слабые со-локаторы.
_STRONG_ANCHOR_RE = re.compile(
    r"(?<![\w])"
    r"([A-ZА-ЯЁ]{1,4}"
    r"(?=[0-9.\-–—−]*\d)"
    r"(?=[0-9.\-–—−]*[.\-–—−])"
    r"[0-9.\-–—−]*\d)"
    r"(?![\w.])",
    re.IGNORECASE,
)

_GENERIC_REFERENCE_PREFIXES = {
    "изм", "лист", "поз", "рис", "стр", "табл",
}

_UNIT = (
    r"мм(?:[²2])?|см(?:[²2])?|м(?:[²2])?|"
    r"а|ма|ка|в|кв|вт|квт|ва|ква|ом|кг|т|л|л/с|м3/ч|м³/ч"
)
_WEAK_ANCHOR_RE = re.compile(
    rf"(?<![\w])(?P<number>\d+(?:[.,]\d+)?)\s*(?P<unit>{_UNIT})(?![\w])",
    re.IGNORECASE,
)

_DASH_TRANSLATION = str.maketrans({
    "–": "-", "—": "-", "−": "-", "‒": "-", "―": "-",
})
_HOMOGLYPH_TRANSLATION = str.maketrans({
    "а": "a", "в": "b", "е": "e", "к": "k", "м": "m",
    "н": "h", "о": "o", "р": "p", "с": "c", "т": "t",
    "у": "y", "х": "x",
})


@dataclass(frozen=True)
class Anchor:
    text: str
    canonical: str
    kind: str  # strong | weak


@dataclass
class Occurrence:
    anchor: Anchor
    words: list
    bbox: tuple[float, float, float, float]


@dataclass(frozen=True)
class BlockSource:
    block_id: str
    pdf_path: Path
    result_path: Path
    page_index: int
    vector_text: str
    coords_norm: tuple[float, float, float, float]
    polygon_norm: Optional[tuple[tuple[float, float], ...]]


def _canonical(value: Any) -> str:
    """Консервативный канон обозначения: формат/гомоглифы, но не цифры."""
    text = unicodedata.normalize("NFKC", str(value or "")).lower()
    text = text.translate(_DASH_TRANSLATION).replace("ё", "е")
    text = re.sub(r"(?<=\d),(?=\d)", ".", text)
    text = text.translate(_HOMOGLYPH_TRANSLATION).replace("×", "x")
    # Снимаем пробелы и внешнюю пунктуацию; точка/дефис внутри кода значимы.
    return re.sub(r"[^\w.\-]+", "", text, flags=re.UNICODE).strip("._-")


def extract_anchors(problem: Any, description: Any) -> tuple[list[Anchor], list[Anchor]]:
    """Извлечь сильные и слабые якоря только из problem+description.

    ``norm_quote`` намеренно не является аргументом: текст нормы обычно не
    присутствует на чертеже и создаёт ложный grounding.
    """
    text = " ".join(str(part or "") for part in (problem, description)).strip()
    strong: list[Anchor] = []
    weak: list[Anchor] = []
    seen_strong: set[str] = set()
    seen_weak: set[str] = set()

    for match in _STRONG_ANCHOR_RE.finditer(text):
        raw = match.group(1)
        # Regex может использовать следующий дефис как обязательный
        # разделитель и обрезать первый сегмент block_id: ``WQV4-FRLA`` →
        # ``WQV4``. Два и более буквенных символа после дефиса — признак такого
        # служебного ID; диапазон осей ``П.1-П.12`` сюда не попадает.
        suffix = text[match.end():]
        if re.match(r"^[\-–—−][A-ZА-ЯЁ]{2,}", suffix, re.IGNORECASE):
            continue
        prefix_match = re.match(r"[A-ZА-ЯЁ]+", raw, re.IGNORECASE)
        prefix = (prefix_match.group(0).casefold() if prefix_match else "")
        if prefix in _GENERIC_REFERENCE_PREFIXES:
            continue
        # Внутренняя ссылка на finding, а не обозначение чертежа.
        if re.fullmatch(r"F-\d{3}", raw, re.IGNORECASE):
            continue
        # Нижний регистр ``п.1.7.137`` почти всегда пункт нормы. Ось ``П.1``
        # из постановки остаётся сильной; контекст «оси/осях» также разрешает.
        if prefix in {"п", "p"}:
            context = text[max(0, match.start() - 16):match.start()].casefold()
            if "ос" not in context and (raw[:1].islower() or raw.count(".") > 1):
                continue
        canonical = _canonical(raw)
        if canonical and canonical not in seen_strong:
            seen_strong.add(canonical)
            strong.append(Anchor(raw, canonical, "strong"))

    # Переиспользуем числовую нормализацию stable-finding fingerprint и
    # добавляем единицу, чтобы ``100 мм`` не спутать с любым другим ``100``.
    salient = set(_salient_numbers(text))
    for match in _WEAK_ANCHOR_RE.finditer(text):
        number = match.group("number").replace(",", ".")
        if number not in salient:
            continue
        raw = match.group(0)
        canonical = _canonical(raw)
        if canonical and canonical not in seen_weak:
            seen_weak.add(canonical)
            weak.append(Anchor(raw, canonical, "weak"))

    return strong, weak


def _bbox(words: Sequence) -> tuple[float, float, float, float]:
    return (
        min(float(word[0]) for word in words),
        min(float(word[1]) for word in words),
        max(float(word[2]) for word in words),
        max(float(word[3]) for word in words),
    )


def _line_groups(words: Sequence) -> list[list]:
    """Сгруппировать fitz words по строкам, сохранив порядок слов."""
    grouped: dict[tuple[Any, Any], list[tuple[int, int, Any]]] = {}
    loose: list[tuple[int, Any]] = []
    for order, word in enumerate(words):
        if not isinstance(word, (list, tuple)) or len(word) < 5:
            continue
        if len(word) >= 8:
            key = (word[5], word[6])
            try:
                word_order = int(word[7])
            except (TypeError, ValueError):
                word_order = order
            grouped.setdefault(key, []).append((word_order, order, word))
        else:
            loose.append((order, word))
    lines = [
        [item[2] for item in sorted(items, key=lambda item: (item[0], item[1]))]
        for items in grouped.values()
    ]
    if loose:
        lines.append([item[1] for item in sorted(loose)])
    return lines


def _find_occurrences(words: Sequence, anchors: Sequence[Anchor], *, max_join: int = 4) -> list[Occurrence]:
    """Найти якорь как одно слово либо склейку соседних fitz-слов."""
    if not words or not anchors:
        return []
    by_canonical = {anchor.canonical: anchor for anchor in anchors}
    out: list[Occurrence] = []
    seen: set[tuple[str, tuple[float, float, float, float]]] = set()

    for line in _line_groups(words):
        for start in range(len(line)):
            parts: list[str] = []
            window: list = []
            for end in range(start, min(len(line), start + max_join)):
                word = line[end]
                window.append(word)
                parts.append(str(word[4]))
                candidate = _canonical("".join(parts))
                anchor = by_canonical.get(candidate)
                if anchor is None:
                    continue
                box = _bbox(window)
                key = (anchor.canonical, tuple(round(value, 3) for value in box))
                if key not in seen:
                    seen.add(key)
                    out.append(Occurrence(anchor=anchor, words=list(window), bbox=box))
                # Самое короткое точное окно достаточно; длинное даст дубль.
                break
    return out


def _point_in_polygon(x: float, y: float, polygon: Sequence[Sequence[float]]) -> bool:
    inside = False
    j = len(polygon) - 1
    for i in range(len(polygon)):
        xi, yi = float(polygon[i][0]), float(polygon[i][1])
        xj, yj = float(polygon[j][0]), float(polygon[j][1])
        if ((yi > y) != (yj > y)) and (
            x < (xj - xi) * (y - yi) / ((yj - yi) or 1e-12) + xi
        ):
            inside = not inside
        j = i
    return inside


def _strict_clip_words(
    words: Sequence,
    coords_norm: Sequence[float],
    polygon_norm: Optional[Sequence[Sequence[float]]],
    page_w: float,
    page_h: float,
) -> list:
    """Переиспользовать штатный clip и убрать его fail-open для highlights.

    Штатные helpers при подозрительно малом результате возвращают все слова
    страницы — это правильно для Вектографа, но опасно для подсветки. Поэтому
    здесь после helper идёт строгая проверка центра слова внутри блока.
    """
    clipped = (
        _clip_words_to_polygon(words, polygon_norm, page_w, page_h)
        if polygon_norm
        else _clip_words_to_bbox(words, coords_norm, page_w, page_h, margin=0.0)
    )
    x0, y0, x1, y1 = (float(value) for value in coords_norm[:4])
    strict: list = []
    for word in clipped:
        try:
            cx = ((float(word[0]) + float(word[2])) / 2.0) / page_w
            cy = ((float(word[1]) + float(word[3])) / 2.0) / page_h
        except (TypeError, ValueError, ZeroDivisionError, IndexError):
            continue
        if not (x0 <= cx <= x1 and y0 <= cy <= y1):
            continue
        if polygon_norm and not _point_in_polygon(cx, cy, polygon_norm):
            continue
        strict.append(word)
    return strict


def _rect_gap(
    left: tuple[float, float, float, float],
    right: tuple[float, float, float, float],
) -> tuple[float, float]:
    x_gap = max(0.0, max(left[0], right[0]) - min(left[2], right[2]))
    y_gap = max(0.0, max(left[1], right[1]) - min(left[3], right[3]))
    return x_gap, y_gap


def _strong_near(left: Occurrence, right: Occurrence, crop_w: float, crop_h: float) -> bool:
    # Повтор одного обозначения в разных колонках — разные occurrence, а не
    # одна рамка на всю строку. Иначе ряд из 16 ``ВА-103`` схлопывался в bbox
    # шириной почти во весь блок.
    if left.anchor.canonical == right.anchor.canonical:
        return False
    x_gap, y_gap = _rect_gap(left.bbox, right.bbox)
    left_h = left.bbox[3] - left.bbox[1]
    right_h = right.bbox[3] - right.bbox[1]
    same_row = y_gap == 0.0 or abs(
        (left.bbox[1] + left.bbox[3]) / 2.0 - (right.bbox[1] + right.bbox[3]) / 2.0
    ) <= 1.5 * max(left_h, right_h, 1.0)
    return (
        (same_row and x_gap <= 0.08 * crop_w)
        or (x_gap <= 0.12 * crop_w and y_gap <= 0.08 * crop_h)
    )


def _clusters(strong: Sequence[Occurrence], crop_w: float, crop_h: float) -> list[list[Occurrence]]:
    parent = list(range(len(strong)))

    def find(value: int) -> int:
        while parent[value] != value:
            parent[value] = parent[parent[value]]
            value = parent[value]
        return value

    def union(left: int, right: int) -> None:
        a, b = find(left), find(right)
        if a != b:
            parent[b] = a

    for i in range(len(strong)):
        for j in range(i + 1, len(strong)):
            if _strong_near(strong[i], strong[j], crop_w, crop_h):
                union(i, j)
    grouped: dict[int, list[Occurrence]] = {}
    for i, occurrence in enumerate(strong):
        grouped.setdefault(find(i), []).append(occurrence)
    return list(grouped.values())


def _assign_weak(
    clusters: list[list[Occurrence]],
    weak: Sequence[Occurrence],
    crop_w: float,
    crop_h: float,
) -> None:
    """Добавить не более одного ближайшего occurrence каждого слабого якоря."""
    best: dict[tuple[int, str], tuple[float, Occurrence]] = {}
    for occurrence in weak:
        nearest: Optional[tuple[float, int]] = None
        for cluster_index, cluster in enumerate(clusters):
            cluster_bbox = _bbox([word for item in cluster for word in item.words])
            x_gap, y_gap = _rect_gap(cluster_bbox, occurrence.bbox)
            if x_gap > 0.20 * crop_w or y_gap > 0.12 * crop_h:
                continue
            distance = math.hypot(x_gap / max(crop_w, 1.0), y_gap / max(crop_h, 1.0))
            if nearest is None or distance < nearest[0]:
                nearest = (distance, cluster_index)
        if nearest is not None:
            distance, cluster_index = nearest
            key = (cluster_index, occurrence.anchor.canonical)
            if key not in best or distance < best[key][0]:
                best[key] = (distance, occurrence)
    for (cluster_index, _), (_, occurrence) in best.items():
        clusters[cluster_index].append(occurrence)


def _clamp01(value: float) -> float:
    return max(0.0, min(1.0, value))


def _region_from_cluster(
    block_id: str,
    cluster: Sequence[Occurrence],
    crop_bbox: tuple[float, float, float, float],
    *,
    padding: float,
) -> Optional[dict]:
    words = [word for occurrence in cluster for word in occurrence.words]
    if not words:
        return None
    ux0, uy0, ux1, uy1 = _bbox(words)
    cx0, cy0, cx1, cy1 = crop_bbox
    crop_w, crop_h = cx1 - cx0, cy1 - cy0
    if crop_w <= 0 or crop_h <= 0:
        return None
    x0 = _clamp01((ux0 - cx0) / crop_w - padding)
    y0 = _clamp01((uy0 - cy0) / crop_h - padding)
    x1 = _clamp01((ux1 - cx0) / crop_w + padding)
    y1 = _clamp01((uy1 - cy0) / crop_h + padding)
    if x1 <= x0 or y1 <= y0:
        return None
    if x1 - x0 > MAX_REGION_WIDTH or y1 - y0 > MAX_REGION_HEIGHT:
        return None
    labels: list[str] = []
    for occurrence in cluster:
        if occurrence.anchor.text not in labels:
            labels.append(occurrence.anchor.text)
    return {
        "block_id": block_id,
        "x": round(x0, 5),
        "y": round(y0, 5),
        "w": round(x1 - x0, 5),
        "h": round(y1 - y0, 5),
        "label": "; ".join(labels),
    }


def _ground_block(
    page: Any,
    source: BlockSource,
    strong_anchors: Sequence[Anchor],
    weak_anchors: Sequence[Anchor],
    *,
    padding: float = DEFAULT_PADDING,
) -> list[dict]:
    """Посчитать crop-normalized regions для одного блока."""
    try:
        page_w, page_h = float(page.rect.width), float(page.rect.height)
        bx0, by0, bx1, by1 = source.coords_norm
        if not (
            page_w > 0 and page_h > 0
            and 0.0 <= bx0 < bx1 <= 1.0
            and 0.0 <= by0 < by1 <= 1.0
        ):
            return []
        # Блок обязан иметь собственный вектор-текст и хотя бы один сильный
        # якорь в нём. Это гасит совпадения с чужим текстом страницы/сканом.
        vector_canonical = _canonical(source.vector_text)
        if not vector_canonical or not any(
            anchor.canonical in vector_canonical for anchor in strong_anchors
        ):
            return []

        words = page.get_text("words") or []
        words = _strict_clip_words(
            words, source.coords_norm, source.polygon_norm, page_w, page_h,
        )
        if not words:
            return []
        strong = _find_occurrences(words, strong_anchors)
        if not strong:
            return []
        weak = _find_occurrences(words, weak_anchors)

        crop_bbox = (bx0 * page_w, by0 * page_h, bx1 * page_w, by1 * page_h)
        crop_w, crop_h = crop_bbox[2] - crop_bbox[0], crop_bbox[3] - crop_bbox[1]
        grouped = _clusters(strong, crop_w, crop_h)
        _assign_weak(grouped, weak, crop_w, crop_h)

        # Сначала наиболее информативные/плотные кластеры. При повторении
        # одного якоря возвращаем максимум три небольших occurrence-региона.
        def score(cluster: Sequence[Occurrence]) -> tuple[int, int, int, float]:
            strong_names = {item.anchor.canonical for item in cluster if item.anchor.kind == "strong"}
            weak_names = {item.anchor.canonical for item in cluster if item.anchor.kind == "weak"}
            box = _bbox([word for item in cluster for word in item.words])
            area = ((box[2] - box[0]) / max(crop_w, 1.0)) * (
                (box[3] - box[1]) / max(crop_h, 1.0)
            )
            return (len(strong_names), len(weak_names), len(cluster), -area)

        grouped.sort(key=score, reverse=True)
        regions: list[dict] = []
        for cluster in grouped:
            region = _region_from_cluster(source.block_id, cluster, crop_bbox, padding=padding)
            if region and region not in regions:
                regions.append(region)
            if len(regions) >= MAX_REGIONS_PER_FINDING:
                break
        return regions
    except Exception:
        return []


def _normalise_block_id(value: Any) -> str:
    block_id = str(value or "").strip()
    if block_id.startswith("block_"):
        block_id = block_id[6:]
    if block_id.endswith(".png"):
        block_id = block_id[:-4]
    return block_id


def _finding_block_ids(finding: dict) -> list[str]:
    out: list[str] = []

    def add(value: Any) -> None:
        block_id = _normalise_block_id(value)
        if block_id and block_id not in out:
            out.append(block_id)

    for key in ("source_block_ids", "related_block_ids"):
        for value in finding.get(key) or []:
            add(value)
    add(finding.get("block_evidence"))
    for evidence in finding.get("evidence") or []:
        if isinstance(evidence, dict) and evidence.get("type") == "image":
            add(evidence.get("block_id"))
    return out


def _load_block_sources(project_dir: Path) -> dict[str, BlockSource]:
    """Собрать block_id → PDF/page/coords через штатные source-resolvers."""
    try:
        from backend.app.pipeline.stages.crop_blocks.blocks import (
            _load_project_info,
            _select_source_pdf,
            _source_files,
            detect_all_result_jsons,
        )

        info = _load_project_info(project_dir)
        resolved = _source_files(project_dir, info)
        pdf_files = info.get("pdf_files") or []
        if not pdf_files and info.get("pdf_file"):
            pdf_files = [info["pdf_file"]]
        result_paths = detect_all_result_jsons(str(project_dir))
    except Exception:
        return {}

    out: dict[str, BlockSource] = {}
    for result_path in result_paths:
        try:
            payload = json.loads(result_path.read_text(encoding="utf-8"))
            vector_index = _result_blocks_vector_index(
                str(result_path), result_path.stat().st_mtime,
            )
            pdf_path = _select_source_pdf(project_dir, result_path, pdf_files, resolved)
            if pdf_path is None or not Path(pdf_path).is_file():
                continue
            for ordinal, result_page in enumerate(payload.get("pages") or []):
                raw_page_number = result_page.get("page_number")
                try:
                    page_index = int(raw_page_number) - 1
                    if page_index < 0:
                        page_index = ordinal
                except (TypeError, ValueError):
                    page_index = ordinal
                for block in result_page.get("blocks") or []:
                    if str(block.get("block_type") or "").lower() != "image":
                        continue
                    block_id = _normalise_block_id(block.get("id") or block.get("block_id"))
                    indexed = vector_index.get(block_id) or {}
                    coords = indexed.get("bbox_norm") or block.get("coords_norm")
                    vector_text = indexed.get("text") or block.get("pdfplumber_text") or ""
                    polygon = indexed.get("polygon_norm") or block.get("polygon_points_norm")
                    try:
                        coords_tuple = tuple(float(value) for value in coords[:4])
                        if len(coords_tuple) != 4:
                            continue
                        polygon_tuple = None
                        if polygon and len(polygon) >= 3:
                            polygon_tuple = tuple(
                                (float(point[0]), float(point[1])) for point in polygon
                            )
                    except (TypeError, ValueError, IndexError):
                        continue
                    if block_id and vector_text:
                        out.setdefault(block_id, BlockSource(
                            block_id=block_id,
                            pdf_path=Path(pdf_path),
                            result_path=result_path,
                            page_index=page_index,
                            vector_text=str(vector_text),
                            coords_norm=coords_tuple,
                            polygon_norm=polygon_tuple,
                        ))
        except (OSError, json.JSONDecodeError, TypeError):
            continue
    return out


def _region_iou(left: dict, right: dict) -> float:
    try:
        lx0, ly0 = float(left["x"]), float(left["y"])
        lx1, ly1 = lx0 + float(left["w"]), ly0 + float(left["h"])
        rx0, ry0 = float(right["x"]), float(right["y"])
        rx1, ry1 = rx0 + float(right["w"]), ry0 + float(right["h"])
    except (KeyError, TypeError, ValueError):
        return 0.0
    ix = max(0.0, min(lx1, rx1) - max(lx0, rx0))
    iy = max(0.0, min(ly1, ry1) - max(ly0, ry0))
    intersection = ix * iy
    union = max(0.0, (lx1 - lx0) * (ly1 - ly0)) + max(
        0.0, (rx1 - rx0) * (ry1 - ry0)
    ) - intersection
    return intersection / union if union > 0 else 0.0


def _agreement(proposed: Sequence[dict], existing: Sequence[dict]) -> Optional[float]:
    scores: list[float] = []
    for region in proposed:
        block_id = _normalise_block_id(region.get("block_id"))
        candidates = [
            old for old in existing
            if isinstance(old, dict)
            and _normalise_block_id(old.get("block_id")) == block_id
        ]
        if candidates:
            scores.append(max(_region_iou(region, old) for old in candidates))
    return sum(scores) / len(scores) if scores else None


def _atomic_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix + ".tmp")
    temp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    temp.replace(path)


def _flag(name: str, default: bool) -> bool:
    try:
        from backend.app.core import config
        return bool(getattr(config, name, default))
    except Exception:
        return default


def backfill_textlayer_highlights(
    project_dir: str | Path,
    output_dir: str | Path | None = None,
    *,
    enabled: Optional[bool] = None,
    shadow: Optional[bool] = None,
    override_existing: Optional[bool] = None,
    padding: float = DEFAULT_PADDING,
) -> dict:
    """Посчитать text-layer highlights и записать shadow/live результат.

    По умолчанию параметры берутся из config. При ``shadow=True`` меняется
    только ``textlayer_highlights_shadow.json``. В live-режиме заполняются лишь
    пустые ``highlight_regions``; существующие перезаписываются только при
    отдельном ``override_existing=True``.
    """
    started = time.perf_counter()
    enabled = _flag("PIPELINE_TEXTLAYER_HIGHLIGHTS_ENABLED", False) if enabled is None else bool(enabled)
    shadow = _flag("PIPELINE_TEXTLAYER_HIGHLIGHTS_SHADOW", True) if shadow is None else bool(shadow)
    override_existing = (
        _flag("PIPELINE_TEXTLAYER_HIGHLIGHTS_OVERRIDE_EXISTING", False)
        if override_existing is None else bool(override_existing)
    )
    base_result = {
        "enabled": enabled,
        "shadow": shadow,
        "checked": 0,
        "grounded": 0,
        "fixed": 0,
        "coverage": 0.0,
        "agreement_iou_mean": None,
        "artifact": None,
    }
    if not enabled:
        return base_result

    project_dir = Path(project_dir)
    output_dir = Path(output_dir) if output_dir is not None else project_dir / "_output"
    findings_path = output_dir / "03_findings.json"
    if not findings_path.is_file():
        return base_result
    try:
        payload = json.loads(findings_path.read_text(encoding="utf-8"))
        findings = payload.get("findings") or []
        if not isinstance(findings, list):
            return base_result
    except (OSError, json.JSONDecodeError):
        return base_result

    block_sources = _load_block_sources(project_dir)
    records: list[dict] = []
    agreements: list[float] = []
    grounded = 0
    fixed = 0
    empty_total = 0
    grounded_empty = 0
    changed = False
    documents: dict[Path, Any] = {}

    try:
        import fitz

        for index, finding in enumerate(findings):
            if not isinstance(finding, dict):
                continue
            finding_id = str(finding.get("id") or f"finding-{index + 1}")
            existing = finding.get("highlight_regions") or []
            if not isinstance(existing, list):
                existing = []
            if not existing:
                empty_total += 1
            strong, weak = extract_anchors(
                finding.get("problem"), finding.get("description"),
            )
            block_ids = _finding_block_ids(finding)
            proposed: list[dict] = []
            block_status: list[dict] = []

            if not strong:
                reason = "no_strong_anchor"
            elif not block_ids:
                reason = "no_source_block"
            else:
                reason = "no_textlayer_match"
                for block_id in block_ids:
                    source = block_sources.get(block_id)
                    if source is None:
                        block_status.append({"block_id": block_id, "status": "source_unavailable"})
                        continue
                    try:
                        document = documents.get(source.pdf_path)
                        if document is None:
                            document = fitz.open(str(source.pdf_path))
                            documents[source.pdf_path] = document
                        if not (0 <= source.page_index < document.page_count):
                            block_status.append({"block_id": block_id, "status": "page_out_of_range"})
                            continue
                        regions = _ground_block(
                            document[source.page_index], source, strong, weak, padding=padding,
                        )
                    except Exception:
                        regions = []
                    proposed.extend(region for region in regions if region not in proposed)
                    block_status.append({
                        "block_id": block_id,
                        "status": "grounded" if regions else "no_match",
                        "regions": len(regions),
                    })
                if proposed:
                    proposed = proposed[:MAX_REGIONS_PER_FINDING]
                    reason = "grounded"

            iou = _agreement(proposed, existing) if proposed and existing else None
            if iou is not None:
                agreements.append(iou)
            if proposed:
                grounded += 1
                if not existing:
                    grounded_empty += 1
                if not shadow and (not existing or override_existing):
                    finding["highlight_regions"] = proposed
                    fixed += 1
                    changed = True

            records.append({
                "finding_id": finding_id,
                "status": reason,
                "had_existing_highlights": bool(existing),
                "strong_anchors": [anchor.text for anchor in strong],
                "weak_anchors": [anchor.text for anchor in weak],
                "source_block_ids": block_ids,
                "computed_highlight_regions": proposed,
                "agreement_iou": round(iou, 5) if iou is not None else None,
                "blocks": block_status,
            })
    except Exception:
        # Даже отсутствие PyMuPDF не должно ломать findings_merge.
        pass
    finally:
        for document in documents.values():
            try:
                document.close()
            except Exception:
                pass

    total = len(findings)
    agreement_mean = sum(agreements) / len(agreements) if agreements else None
    report = {
        "schema_version": SCHEMA_VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "mode": "shadow" if shadow else "live",
        "coordinate_basis": "coords_norm block crop (no extra margin)",
        "settings": {
            "padding": padding,
            "max_regions_per_finding": MAX_REGIONS_PER_FINDING,
            "max_region_width": MAX_REGION_WIDTH,
            "max_region_height": MAX_REGION_HEIGHT,
            "override_existing": override_existing,
        },
        "summary": {
            "findings_total": total,
            "findings_empty_before": empty_total,
            "findings_grounded": grounded,
            "findings_empty_grounded": grounded_empty,
            "coverage": round(grounded / total, 5) if total else 0.0,
            "empty_coverage": round(grounded_empty / empty_total, 5) if empty_total else 0.0,
            "findings_with_iou": len(agreements),
            "agreement_iou_mean": round(agreement_mean, 5) if agreement_mean is not None else None,
            "findings_written": fixed,
            "llm_tokens": 0,
            "elapsed_ms": round((time.perf_counter() - started) * 1000, 1),
        },
        "records": records,
    }
    artifact_path = output_dir / SHADOW_FILENAME
    try:
        _atomic_json(artifact_path, report)
        artifact: Optional[str] = str(artifact_path)
    except OSError:
        artifact = None

    if changed:
        try:
            _atomic_json(findings_path, payload)
        except OSError:
            fixed = 0

    return {
        "enabled": True,
        "shadow": shadow,
        "checked": total,
        "grounded": grounded,
        "fixed": fixed,
        "coverage": report["summary"]["coverage"],
        "agreement_iou_mean": report["summary"]["agreement_iou_mean"],
        "artifact": artifact,
    }


__all__ = [
    "SHADOW_FILENAME",
    "backfill_textlayer_highlights",
    "extract_anchors",
]
