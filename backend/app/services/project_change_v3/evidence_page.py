"""Read-only PDF page location for saved evidence; no OCR or model calls.

Only unique literal token sequences inside the cited source block are located.
Ellipsis-separated fragments may be located independently and are labelled as
partial quotes. A source block is a visibly labelled fallback, never a quote.
"""
from __future__ import annotations

import math
import re
import unicodedata
from typing import Any

import fitz

_TOKEN = re.compile(r"\d+(?:[.,]\d+)*|[^\W\d_]+|[+\-−≤≥<>=/%°]", re.UNICODE)


def _tokens(value: str) -> list[str]:
    return _TOKEN.findall(unicodedata.normalize('NFKC', value).casefold().replace('−', '-'))


def _block_rect(page, bbox) -> fitz.Rect | None:
    if not isinstance(bbox, (tuple, list)) or len(bbox) != 4:
        return None
    try:
        x0, y0, x1, y1 = [float(x) for x in bbox]
    except (TypeError, ValueError):
        return None
    if not all(math.isfinite(x) for x in (x0, y0, x1, y1)) or not (0 <= x0 < x1 <= 1 and 0 <= y0 < y1 <= 1):
        return None
    r = page.rect
    return fitz.Rect(x0 * r.width, y0 * r.height, x1 * r.width, y1 * r.height)


def _box(rect, page) -> dict[str, float]:
    rect = rect & page.rect
    return {'x': rect.x0 / page.rect.width, 'y': rect.y0 / page.rect.height,
            'width': rect.width / page.rect.width, 'height': rect.height / page.rect.height}


def locate(page, evidence: dict[str, Any]) -> dict[str, Any]:
    """Rectangles use the displayed (rotated) PDF page coordinate system."""
    quote = str(evidence.get('quote') or '')
    block = _block_rect(page, evidence.get('bbox'))
    result = {'page_width': page.rect.width, 'page_height': page.rect.height,
              'quote': quote, 'highlights': [], 'matched_fragments': [], 'kind': 'NOT_FOUND',
              'message': 'Точное место цитаты не найдено. Показан лист целиком.'}
    if evidence.get('source_type') == 'GRAPHIC':
        if block is not None:
            result.update(kind='SOURCE_BLOCK', highlights=[_box(block, page)],
                          message='Красная рамка показывает сохранённую область чертежа, на которую ссылается программа.')
        else:
            result['message'] = 'Координаты области чертежа не сохранены. Показан лист целиком.'
        return result

    words = page.get_text('words', sort=True)
    entries = []
    for word in words:
        rect = fitz.Rect(word[:4]) * page.rotation_matrix
        center = (rect.tl + rect.br) / 2
        # Do not match a repeated note outside the source block named by Miner.
        if block is not None and center not in block:
            continue
        for token in _tokens(str(word[4])):
            entries.append((token, rect, (word[5], word[6])))
    tokens = [entry[0] for entry in entries]

    def occurrences(part):
        needle = _tokens(part)
        if not needle:
            return []
        found = []
        for i, token in enumerate(tokens):
            if token == needle[0] and tokens[i:i + len(needle)] == needle:
                found.append((i, i + len(needle)))
                if len(found) == 2:  # two matches already mean ambiguous
                    break
        return found

    pieces = [p.strip() for p in re.split(r'\.{3,}|…+', quote) if p.strip()]
    has_ellipsis = len(pieces) != 1 or bool(re.search(r'\.{3,}|…', quote))
    found_ranges = []
    ambiguous = False
    if quote.strip() and not has_ellipsis:
        matches = occurrences(quote)
        ambiguous = len(matches) > 1
        if len(matches) == 1:
            found_ranges = matches
            result.update(kind='EXACT_QUOTE', matched_fragments=[quote],
                          message='Красным выделена цитата, найденная в текстовом слое PDF.')
    elif has_ellipsis:
        matched = []
        for piece in pieces:
            # Short detached values are not enough to locate a partial quote.
            if len(_tokens(piece)) < 4 or len(piece) < 20:
                continue
            matches = occurrences(piece)
            ambiguous |= len(matches) > 1
            if len(matches) == 1:
                found_ranges.extend(matches)
                matched.append(piece)
        if found_ranges:
            result.update(kind='PARTIAL_QUOTE', matched_fragments=matched,
                          message='Красным выделены найденные части цитаты. Полное совпадение цитаты не установлено.')

    if found_ranges:
        lines = {}
        for start, end in found_ranges:
            for _, rect, line in entries[start:end]:
                # Separate fragments on the same line remain separate rectangles.
                key = (start, line)
                lines[key] = (lines[key] | rect) if key in lines else fitz.Rect(rect)
        result['highlights'] = [_box(rect, page) for rect in lines.values()]
    else:
        reason = ('Цитата встречается несколько раз; точное место не выбрано.' if ambiguous else
                  'В PDF нет доступного текстового слоя.' if not words else
                  'Точное место цитаты не найдено в текстовом слое PDF.')
        if block is not None:
            result.update(kind='SOURCE_BLOCK', highlights=[_box(block, page)],
                          message=reason + ' Красная пунктирная рамка показывает исходный блок, а не точную цитату.')
        else:
            result['message'] = reason + ' Показан лист целиком.'
    return result


def render_page(page) -> bytes:
    scale = min(2400 / max(page.rect.width, page.rect.height), 3.0)
    return page.get_pixmap(matrix=fitz.Matrix(scale, scale), alpha=False).tobytes('png')
