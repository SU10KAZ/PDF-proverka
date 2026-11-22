"""Поиск одинаковых листов среди проектов одной сборки.

Сборка группы склеивает несколько проектов, и один и тот же лист часто
вложен в каждый из них: альбом технических решений в ЭМ каждого корпуса,
типовые узлы, повторно загруженный документ. Такие копии раздувают сборку и
каждую стадию сравнения после неё. Модуль находит копии; удаляет их сборка —
только после согласования инженером (``service.submit_duplicate_review``).

Дубль — лист того же формата, у которого совпадают и графика, и текст на
поле листа. Не учитываются (решение инженера 22.11.2026):

* штамп — основная надпись с шифром, номером листа, таблицей изменений,
  подписями и датами (блок ``stamp`` распознавания в ``blocks.json``);
* отметка выпуска «В ПРОИЗВОДСТВО РАБОТ» — плашка с организацией, ГИП и
  датой — и QR-код рядом с ней: у каждого проекта свои.

Любое другое отличие, даже мелкое, оставляет лист самостоятельным. Поэтому
проверка строгая: вне перечисленных зон не должен отличаться ни один пиксель
и ни одно слово текстового слоя. Допуск по площади намеренно не вводится —
1–2 % площади занимает и QR-код, и облако реальной правки.

Модуль детерминирован, не вызывает OCR и модели. Распознавание (Markdown)
для сравнения не годится: одна и та же страница распознаётся по-разному.

Координаты текста и графики PyMuPDF отдаёт в неповёрнутой системе страницы,
а рендер и ``blocks.json`` — в повёрнутой; все зоны здесь хранятся в
повёрнутой системе (``page.rect``).
"""
from __future__ import annotations

import hashlib
import json
import re
from collections import OrderedDict, defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Iterable

SCHEMA = "project_assembly_duplicates/1"
DETECTOR_VERSION = 1

# Отметка выпуска: текст плашки и цвет её рамки у системы выпуска РД.
APPROVAL_ANCHORS = ("В ПРОИЗВОДСТВО РАБОТ",)
_APPROVAL_FRAME_FILL = (0.110, 0.533, 0.749)
_APPROVAL_COLOR_TOLERANCE = 0.04
# Рамка плашки и QR ищутся рядом с найденным текстом отметки.
_APPROVAL_SEARCH_PT = 170.0
_QR_MIN_SIDE_PT = 30.0
_QR_MAX_SIDE_PT = 180.0
_QR_MIN_ITEMS = 40
_ZONE_MARGIN_PT = 2.0
_FRAME_MAX_THICKNESS_PT = 4.0
# Блок штампа из распознавания чуть уже настоящей рамки основной надписи.
_STAMP_MARGIN_PT = 6.0

# Кандидаты — по уменьшенному рендеру; проверка — в полном разрешении.
_COARSE_DPI = 30
_HASH_SIZE = 16
_CANDIDATE_MAX_DISTANCE = 24  # из 256 бит
_VERIFY_DPI = 100
_VERIFY_MAX_SIDE_PX = 4000
_PIXEL_THRESHOLD = 48  # разница яркости, которая считается отличием
_RENDER_CACHE = 4

_SPACE_RE = re.compile(r"\s+")

Heartbeat = Callable[[str, int, int], None]


@dataclass
class _Page:
    order: int
    source_index: int
    source_page: int  # 1-based
    size_key: tuple[int, int]
    text_key: str
    dhash: int


@dataclass
class _Source:
    index: int
    document_code: str
    pdf_sha256: str
    pdf_path: Path
    stamps: dict[int, list[list[float]]] = field(default_factory=dict)  # page_index → coords_norm
    sheets: dict[int, str] = field(default_factory=dict)  # 1-based page → подпись листа
    doc: Any = None


def _norm_text(value: str) -> str:
    return _SPACE_RE.sub(" ", value).strip().casefold()


def _rotated(page, rect) -> Any:
    import fitz

    out = fitz.Rect(rect) * page.rotation_matrix
    out.normalize()
    return out


def _expanded(rect, margin: float, bounds) -> Any:
    import fitz

    grown = fitz.Rect(rect.x0 - margin, rect.y0 - margin, rect.x1 + margin, rect.y1 + margin)
    return grown & bounds


def _stamp_zones(page, coords: Iterable[list[float]]) -> list[Any]:
    import fitz

    bounds = page.rect
    zones = []
    for x0, y0, x1, y1 in coords:
        rect = fitz.Rect(x0 * bounds.width, y0 * bounds.height, x1 * bounds.width, y1 * bounds.height)
        zones.append(_expanded(rect, _STAMP_MARGIN_PT, bounds))
    return zones


def _approval_anchors(page) -> list[Any]:
    """Текст отметки выпуска, в том числе за обрезом листа.

    Плашка фиксированного размера на малом формате выходит за край страницы
    («…ОДСТВО РАБОТ» на вкладыше A6), поэтому поиск идёт без обрезки по листу.
    """
    import fitz

    flags = fitz.TEXT_DEHYPHENATE | fitz.TEXT_PRESERVE_WHITESPACE | fitz.TEXT_PRESERVE_LIGATURES
    anchors: list[Any] = []
    for text in APPROVAL_ANCHORS:
        for rect in page.search_for(text, flags=flags, clip=fitz.INFINITE_RECT()):
            rotated = _rotated(page, rect)
            if not any(abs(rotated.x0 - seen.x0) < 1 and abs(rotated.y0 - seen.y0) < 1 for seen in anchors):
                anchors.append(rotated)
    return anchors


def _close_color(color: Any, target: tuple[float, float, float]) -> bool:
    if not isinstance(color, (tuple, list)) or len(color) != 3:
        return False
    return all(abs(float(a) - b) <= _APPROVAL_COLOR_TOLERANCE for a, b in zip(color, target))


def _squareish(rect, low: float, high: float) -> bool:
    side = max(rect.width, rect.height)
    return low <= side <= high and abs(rect.width - rect.height) <= 0.12 * side


def _merged(zones: list[Any]) -> list[Any]:
    merged: list[Any] = []
    for zone in zones:
        zone = type(zone)(zone)
        changed = True
        while changed:
            changed = False
            for other in list(merged):
                if zone.intersects(other):
                    zone |= other
                    merged.remove(other)
                    changed = True
        merged.append(zone)
    return merged


def _is_frame_path(path: dict[str, Any], rect) -> bool:
    """Контур рамки плашки: тонкие полосы или обводка её цвета.

    Залитая область того же цвета на чертеже рамкой не считается: каждая
    часть заливки должна быть полосой не толще ``_FRAME_MAX_THICKNESS_PT``.
    """
    if _close_color(path.get("color"), _APPROVAL_FRAME_FILL) and "s" in str(path.get("type") or ""):
        return True
    if not _close_color(path.get("fill"), _APPROVAL_FRAME_FILL):
        return False
    if min(rect.width, rect.height) <= _FRAME_MAX_THICKNESS_PT:
        return True
    items = path.get("items") or ()
    return bool(items) and all(
        item[0] == "re" and min(abs(item[1][2] - item[1][0]), abs(item[1][3] - item[1][1])) <= _FRAME_MAX_THICKNESS_PT
        for item in items
    )


def _approval_zones(page, anchors: list[Any]) -> list[Any]:
    """Точные зоны отметки выпуска: рамка плашки и QR-код.

    QR опознаётся и без текста плашки: чёрная фигура из сотен элементов на
    белой квадратной подложке (подложка закрывает чертёж под собой и тоже
    входит в зону). Рамка плашки — тонкие прямоугольники её цвета рядом с
    текстом отметки или с QR.
    """
    import fitz

    bounds = page.rect
    drawings = [(path, _rotated(page, path["rect"])) for path in page.get_cdrawings()]
    frames = [rect for path, rect in drawings if _is_frame_path(path, rect)]
    backgrounds = [
        rect for path, rect in drawings
        if _close_color(path.get("fill"), (1.0, 1.0, 1.0)) and _squareish(rect, _QR_MIN_SIDE_PT, _QR_MAX_SIDE_PT * 1.4)
    ]
    codes = []
    for path, rect in drawings:
        if not (_close_color(path.get("fill"), (0.0, 0.0, 0.0)) and len(path.get("items") or ()) >= _QR_MIN_ITEMS
                and _squareish(rect, _QR_MIN_SIDE_PT, _QR_MAX_SIDE_PT)):
            continue
        holder = [background for background in backgrounds if background.contains(rect)]
        near_anchor = any(rect.intersects(_expanded(anchor, _APPROVAL_SEARCH_PT, fitz.INFINITE_RECT()))
                          for anchor in anchors)
        # Без подложки чёрный квадрат признаётся QR только рядом с текстом отметки.
        if holder or near_anchor:
            code = fitz.Rect(rect)
            for background in holder:
                code |= background
            codes.append(code)
    zones = []
    for seed in [*anchors, *codes]:
        zone = fitz.Rect(seed)
        area = _expanded(seed, _APPROVAL_SEARCH_PT, fitz.INFINITE_RECT())
        for frame in frames:
            if frame.intersects(area):
                zone |= frame
        zones.append(zone)
    result = []
    for zone in _merged(zones):
        clipped = _expanded(zone, _ZONE_MARGIN_PT, bounds)
        if not clipped.is_empty:
            result.append(clipped)
    return result


def _rough_approval_zones(page, anchors: list[Any]) -> list[Any]:
    """Грубые зоны для отбора кандидатов: окрестность текста отметки."""
    return [_expanded(anchor, _APPROVAL_SEARCH_PT, page.rect) for anchor in anchors]


def _inside_any(rect, zones: list[Any]) -> bool:
    cx, cy = (rect.x0 + rect.x1) / 2, (rect.y0 + rect.y1) / 2
    return any(zone.x0 <= cx <= zone.x1 and zone.y0 <= cy <= zone.y1 for zone in zones)


def _text_outside(page, zones: list[Any]) -> str:
    import fitz

    words = []
    for x0, y0, x1, y1, word, *_ in page.get_text("words"):
        if not _inside_any(_rotated(page, fitz.Rect(x0, y0, x1, y1)), zones):
            words.append(word)
    return _norm_text(" ".join(words))


def _render(page, dpi: float, zones: list[Any]):
    import fitz
    import numpy as np

    pix = page.get_pixmap(dpi=dpi, colorspace=fitz.csGRAY, alpha=False, annots=True)
    gray = np.frombuffer(pix.samples, dtype=np.uint8).reshape(pix.height, pix.stride)[:, :pix.width].copy()
    sx, sy = pix.width / page.rect.width, pix.height / page.rect.height
    for zone in zones:
        x0, x1 = max(0, int(zone.x0 * sx)), min(pix.width, int(zone.x1 * sx) + 1)
        y0, y1 = max(0, int(zone.y0 * sy)), min(pix.height, int(zone.y1 * sy) + 1)
        gray[y0:y1, x0:x1] = 255
    return gray


def _dhash(gray) -> int:
    import numpy as np

    height, width = gray.shape
    rows = np.linspace(0, height, _HASH_SIZE + 1).astype(int)
    cols = np.linspace(0, width, _HASH_SIZE + 2).astype(int)
    small = np.zeros((_HASH_SIZE, _HASH_SIZE + 1))
    data = gray.astype(np.float64)
    for i in range(_HASH_SIZE):
        band = data[rows[i]:max(rows[i + 1], rows[i] + 1)]
        for j in range(_HASH_SIZE + 1):
            small[i, j] = band[:, cols[j]:max(cols[j + 1], cols[j] + 1)].mean()
    value = 0
    for bit in (small[:, 1:] > small[:, :-1]).flatten():
        value = (value << 1) | int(bit)
    return value


def _normalized(rect, page) -> list[float]:
    width, height = page.rect.width, page.rect.height
    return [round(rect.x0 / width, 5), round(rect.y0 / height, 5), round(rect.x1 / width, 5), round(rect.y1 / height, 5)]


def pair_key(first: dict[str, Any], second: dict[str, Any]) -> str:
    """Ключ решения инженера: пара страниц независимо от порядка склейки."""
    ends = sorted(f"{side['pdf_sha256']}:{int(side['source_page'])}" for side in (first, second))
    return "pk_" + hashlib.sha256("|".join(ends).encode("utf-8")).hexdigest()[:32]


class _Scanner:
    def __init__(self, sources: list[_Source]):
        self.sources = sources
        self._zones: dict[tuple[int, int], list[Any]] = {}
        self._renders: OrderedDict[tuple, Any] = OrderedDict()

    def page(self, ref: _Page):
        return self.sources[ref.source_index].doc[ref.source_page - 1]

    def zones(self, ref: _Page) -> list[Any]:
        key = (ref.source_index, ref.source_page)
        if key not in self._zones:
            page = self.page(ref)
            source = self.sources[ref.source_index]
            self._zones[key] = [
                *_stamp_zones(page, source.stamps.get(ref.source_page - 1, [])),
                *_approval_zones(page, _approval_anchors(page)),
            ]
        return self._zones[key]

    def render(self, ref: _Page, dpi: float, zones: list[Any]):
        key = (ref.source_index, ref.source_page, round(dpi, 3), tuple(tuple(round(v, 2) for v in z) for z in zones))
        if key in self._renders:
            self._renders.move_to_end(key)
            return self._renders[key]
        gray = _render(self.page(ref), dpi, zones)
        self._renders[key] = gray
        while len(self._renders) > _RENDER_CACHE:
            self._renders.popitem(last=False)
        return gray

    def fingerprint(self, order: int, source: _Source, page_index: int) -> _Page:
        page = source.doc[page_index]
        rough = [
            *_stamp_zones(page, source.stamps.get(page_index, [])),
            *_rough_approval_zones(page, _approval_anchors(page)),
        ]
        text = _text_outside(page, rough)
        return _Page(
            order=order,
            source_index=source.index,
            source_page=page_index + 1,
            size_key=(round(page.rect.width), round(page.rect.height)),
            text_key=hashlib.sha256(text.encode("utf-8")).hexdigest(),
            dhash=_dhash(_render(page, _COARSE_DPI, rough)),
        )

    def same(self, first: _Page, second: _Page) -> dict[str, Any] | None:
        """Строгая проверка: вне штампа и отметки выпуска страницы совпадают целиком."""
        import numpy as np

        page_a, page_b = self.page(first), self.page(second)
        if abs(page_a.rect.width - page_b.rect.width) > 0.5 or abs(page_a.rect.height - page_b.rect.height) > 0.5:
            return None
        zones_a, zones_b = self.zones(first), self.zones(second)
        zones = [*zones_a, *zones_b]
        if _text_outside(page_a, zones) != _text_outside(page_b, zones):
            return None
        dpi = min(_VERIFY_DPI, _VERIFY_MAX_SIDE_PX * 72 / max(page_a.rect.width, page_a.rect.height))
        gray_a, gray_b = self.render(first, dpi, zones), self.render(second, dpi, zones)
        if gray_a.shape != gray_b.shape:
            return None
        differing = int(np.count_nonzero(np.abs(gray_a.astype(np.int16) - gray_b.astype(np.int16)) > _PIXEL_THRESHOLD))
        if differing:
            return None
        return {
            "original": [_normalized(zone, page_a) for zone in zones_a],
            "copy": [_normalized(zone, page_b) for zone in zones_b],
        }


def _load_source(index: int, row: dict[str, Any]) -> _Source:
    import fitz

    from backend.app.services.stage_comparison import sheet_matching

    stamps: dict[int, list[list[float]]] = defaultdict(list)
    blocks_path = row.get("blocks_path")
    if blocks_path:
        blocks = json.loads(Path(blocks_path).read_text(encoding="utf-8"))
        for block in blocks.get("blocks") or []:
            coords = block.get("coords_norm") if isinstance(block, dict) else None
            if block.get("block_type") == "stamp" and isinstance(coords, list) and len(coords) == 4:
                stamps[int(block["page_index"])].append([float(value) for value in coords])
    sheets: dict[int, str] = {}
    html_path = row.get("html_path")
    if html_path:
        html_text = Path(html_path).read_text(encoding="utf-8")
        for sheet in sheet_matching.extract_sheet_index_from_results_html(html_text):
            try:
                sheets[int(sheet["pdf_page"])] = str(sheet.get("display") or "")
            except (KeyError, TypeError, ValueError):
                continue
    return _Source(
        index=index,
        document_code=str(row.get("document_code") or ""),
        pdf_sha256=str(row.get("pdf_sha256") or ""),
        pdf_path=Path(row["pdf_path"]),
        stamps=dict(stamps),
        sheets=sheets,
        doc=fitz.open(row["pdf_path"]),
    )


def scan(source_rows: list[dict[str, Any]], *, heartbeat: Heartbeat | None = None) -> dict[str, Any]:
    """Найти копии листов в порядке склейки.

    ``source_rows`` — проекты сборки по порядку: ``pdf_path``, ``blocks_path``,
    ``html_path``, ``document_code``, ``pdf_sha256``. Оригинал — первое
    вхождение листа; каждая копия образует пару с ним.
    """
    sources = [_load_source(index, row) for index, row in enumerate(source_rows)]
    try:
        scanner = _Scanner(sources)
        total = sum(source.doc.page_count for source in sources)
        pages: list[_Page] = []
        for source in sources:
            for page_index in range(source.doc.page_count):
                pages.append(scanner.fingerprint(len(pages), source, page_index))
                if heartbeat and len(pages) % 10 == 0:
                    heartbeat("fingerprint", len(pages), total)
        offsets: list[int] = []
        running = 0
        for source in sources:
            offsets.append(running)
            running += source.doc.page_count

        buckets: dict[tuple, list[_Page]] = defaultdict(list)
        pairs: list[dict[str, Any]] = []
        for done, page in enumerate(pages, start=1):
            originals = buckets[(page.size_key, page.text_key)]
            match = None
            for original in originals:
                if bin(original.dhash ^ page.dhash).count("1") > _CANDIDATE_MAX_DISTANCE:
                    continue
                zones = scanner.same(original, page)
                if zones is not None:
                    match = (original, zones)
                    break
            if match is None:
                originals.append(page)
            else:
                original, zones = match
                pairs.append(_pair(sources, offsets, original, page, zones))
            if heartbeat and done % 10 == 0:
                heartbeat("verify", done, total)
        return {
            "schema": SCHEMA,
            "detector_version": DETECTOR_VERSION,
            "page_count": total,
            "sources": [
                {"source_index": source.index, "document_code": source.document_code,
                 "pdf_sha256": source.pdf_sha256, "page_count": source.doc.page_count}
                for source in sources
            ],
            "pairs": pairs,
        }
    finally:
        for source in sources:
            if source.doc is not None:
                source.doc.close()


def _side(sources: list[_Source], offsets: list[int], ref: _Page) -> dict[str, Any]:
    source = sources[ref.source_index]
    return {
        "source_index": ref.source_index,
        "document_code": source.document_code,
        "pdf_sha256": source.pdf_sha256,
        "source_page": ref.source_page,
        # Номер страницы в сборке без удалений: по нему инженер найдёт лист в составе.
        "combined_page": offsets[ref.source_index] + ref.source_page,
        "sheet": source.sheets.get(ref.source_page, ""),
    }


def _pair(sources: list[_Source], offsets: list[int], original: _Page, copy: _Page,
          zones: dict[str, Any]) -> dict[str, Any]:
    first, second = _side(sources, offsets, original), _side(sources, offsets, copy)
    key = pair_key(first, second)
    return {
        "pair_id": "dup_" + key[3:19],
        "pair_key": key,
        "original": first,
        "copy": second,
        # Зоны, которые при сравнении не учитывались (доли страницы, x0 y0 x1 y1).
        "ignored_zones": zones,
    }


__all__ = ["APPROVAL_ANCHORS", "DETECTOR_VERSION", "SCHEMA", "pair_key", "scan"]
