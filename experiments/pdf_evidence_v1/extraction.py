"""Native PDF text, with everything the current preprocessing throws away.

Three channels carry printed text out of these documents, and only the first
one was ever read:

1. **the text layer** — spans with a rectangle, a font and a size;
2. **the line** — a value printed as several spans (``ГРЩ1-РП1-3`` + ``2х`` +
   ``ППГнг(А)-HF``) exists as a string on the sheet and as nothing at all in a
   per-span index.  Joining spans along the line recovers it;
3. **``AutoCAD SHX Text`` annotations** — AutoCAD exports SHX shape text as
   drawn vectors *plus* a comment annotation holding the readable string and
   its rectangle.  There is no glyph in the text layer at all.  On this corpus
   that is 8 323 annotations, and on ``IOS1.1/LEFT`` 4 637 of 4 765 of those
   strings occur neither in the text layer nor in the recognized Markdown.

Every rectangle is reported in displayed space (``page.rotation_matrix``
applied), because a rotated sheet read in raw coordinates puts its title block
in the middle of the page.  Measured on this corpus: on a 270° sheet 585 of 585
spans and 1 561 of 1 561 annotations land inside the page only after the
matrix is applied.

The module reports what it dropped as well as what it found — a channel that
only counts its successes cannot notice that it never saw half the page.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Iterator, Mapping, Sequence

import numpy as np

from . import geometry as geometry_module
from .contract import DECODED_NATIVE
from .decoding import DecodingProfile, build_profile

#: Title block of the displayed page — the production bound, taken from
#: ``sheet_identity.STAMP_ZONE_MIN_Y0`` / ``STAMP_ZONE_MIN_X1`` so that this
#: layer and the production sheet reader mean the same region by "stamp".
STAMP_ZONE_MIN_Y0 = 0.85
STAMP_ZONE_MIN_X1 = 0.55

#: Gap, as a share of the font size, above which two spans of one line are
#: joined with a space rather than concatenated.
SPACE_GAP_EM = 0.22

#: The annotation title AutoCAD writes for shape text.  Other annotation kinds
#: (a review stamp, a reviewer's note) are kept too, but tagged, because a
#: reviewer's comment is not something the sheet prints.
SHX_TITLE = "AutoCAD SHX Text"


def _is_white(color: Any) -> bool:
    if color is None:
        return False
    try:
        return all(float(channel) >= 0.98 for channel in color)
    except (TypeError, ValueError):
        return False


@dataclass
class PageSource:
    """One page, as the PDF actually carries it."""

    page: int
    rotation: int
    width: float
    height: float
    spans: list[dict[str, Any]] = field(default_factory=list)
    lines: list[dict[str, Any]] = field(default_factory=list)
    paragraphs: list[dict[str, Any]] = field(default_factory=list)
    annotations: list[dict[str, Any]] = field(default_factory=list)
    images: list[list[float]] = field(default_factory=list)
    geometry: geometry_module.CompactGeometry = field(
        default_factory=geometry_module.CompactGeometry
    )
    counters: dict[str, int] = field(default_factory=dict)

    @property
    def has_text_layer(self) -> bool:
        return bool(self.spans)

    def in_stamp_zone(self, bbox: Sequence[float]) -> bool:
        if not self.width or not self.height:
            return False
        return (
            float(bbox[1]) / self.height >= STAMP_ZONE_MIN_Y0
            and float(bbox[2]) / self.width >= STAMP_ZONE_MIN_X1
        )


def font_spans(pdf_path: str) -> Iterator[dict[str, Any]]:
    """Cheap text-only pass: what each font prints, before anything is decoded."""
    import fitz

    document = fitz.open(str(pdf_path))
    try:
        for page in document:
            for block in page.get_text("dict")["blocks"]:
                if block["type"] != 0:
                    continue
                for line in block["lines"]:
                    for span in line["spans"]:
                        yield {
                            "font": span["font"],
                            "text": span["text"],
                            "page": page.number + 1,
                        }
    finally:
        document.close()


def document_profile(
    pdf_path: str, bodies: Mapping[int, str] | None = None
) -> DecodingProfile:
    """Font profile of a document.

    ``bodies`` is the recognized Markdown per page.  It validates the CAD
    codec — whether these bytes decoded this way ever produced a string an
    independent reading also saw — and nothing else; no fact of the layer is
    gated on it.
    """
    return build_profile(font_spans(pdf_path), bodies)


def _join_line(spans: Sequence[dict[str, Any]], vertical: bool) -> str:
    """Reassemble the printed string from the spans of one line."""
    parts: list[str] = []
    previous: dict[str, Any] | None = None
    for span in spans:
        text = span["text"]
        if previous is not None:
            size = max(float(previous.get("size") or 0.0), float(span.get("size") or 0.0))
            if vertical:
                gap = float(span["bbox"][1]) - float(previous["bbox"][3])
            else:
                gap = float(span["bbox"][0]) - float(previous["bbox"][2])
            if gap > SPACE_GAP_EM * size and not parts[-1].endswith(" ") and not text.startswith(" "):
                parts.append(" ")
        parts.append(text)
        previous = span
    return "".join(parts)


def _union(boxes: Sequence[Sequence[float]]) -> list[float]:
    return [
        min(float(box[0]) for box in boxes),
        min(float(box[1]) for box in boxes),
        max(float(box[2]) for box in boxes),
        max(float(box[3]) for box in boxes),
    ]


def _text_rows(
    page: Any, matrix: Any, profile: DecodingProfile
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, int]]:
    import fitz

    spans: list[dict[str, Any]] = []
    lines: list[dict[str, Any]] = []
    counters = {
        "spans_read": 0,
        "spans_blank": 0,
        "spans_repaired": 0,
        "spans_undecodable": 0,
        "lines_read": 0,
        "lines_multi_span": 0,
    }
    for block in page.get_text("dict")["blocks"]:
        if block["type"] != 0:
            continue
        for line in block["lines"]:
            vertical = abs(float(line["dir"][1])) > abs(float(line["dir"][0]))
            members: list[dict[str, Any]] = []
            for span in line["spans"]:
                counters["spans_read"] += 1
                text, decoding, repaired = profile.decode(span["text"], span["font"])
                if not text.strip():
                    counters["spans_blank"] += 1
                    continue
                if repaired:
                    counters["spans_repaired"] += 1
                if decoding == "UNDECODABLE":
                    counters["spans_undecodable"] += 1
                rect = fitz.Rect(span["bbox"]) * matrix
                rect.normalize()
                row = {
                    "index": len(spans),
                    "text": text,
                    "bbox": [rect.x0, rect.y0, rect.x1, rect.y1],
                    "size": round(float(span["size"]), 2),
                    "font": str(span["font"]),
                    "vertical": vertical,
                    "decoding": decoding,
                    "repaired_chars": repaired,
                }
                spans.append(row)
                members.append(row)
            if not members:
                continue
            counters["lines_read"] += 1
            if len(members) > 1:
                counters["lines_multi_span"] += 1
            # A line's decoding is the weakest of its spans: a joined string is
            # only as trustworthy as its least trustworthy character.
            decodings = {row["decoding"] for row in members}
            if "UNDECODABLE" in decodings:
                line_decoding = "UNDECODABLE"
            elif "DECODED_CAD_UNRESOLVED" in decodings:
                line_decoding = "DECODED_CAD_UNRESOLVED"
            elif "DECODED_CAD_REPAIRED" in decodings:
                line_decoding = "DECODED_CAD_REPAIRED"
            else:
                line_decoding = DECODED_NATIVE
            lines.append({
                "index": len(lines),
                "text": _join_line(members, vertical),
                "bbox": _union([row["bbox"] for row in members]),
                "size": max(float(row["size"]) for row in members),
                "font": members[0]["font"],
                "vertical": vertical,
                "decoding": line_decoding,
                "repaired_chars": sum(int(row["repaired_chars"]) for row in members),
                "span_indices": [row["index"] for row in members],
            })
    return spans, lines, counters


def _paragraph_rows(page: Any, matrix: Any, profile: DecodingProfile) -> list[dict[str, Any]]:
    import fitz

    rows: list[dict[str, Any]] = []
    for block in page.get_text("blocks"):
        text = str(block[4]).strip()
        if not text:
            continue
        decoded, decoding, repaired = profile.decode(text, None)
        rect = fitz.Rect(block[:4]) * matrix
        rect.normalize()
        rows.append({
            "text": decoded,
            "bbox": [rect.x0, rect.y0, rect.x1, rect.y1],
            "decoding": decoding,
            "repaired_chars": repaired,
        })
    return rows


def _annotation_rows(page: Any, matrix: Any) -> tuple[list[dict[str, Any]], dict[str, int]]:
    import fitz

    rows: list[dict[str, Any]] = []
    counters = {"annotations": 0, "annotations_with_text": 0, "annotations_shx": 0}
    for annotation in page.annots() or []:
        counters["annotations"] += 1
        info = annotation.info or {}
        content = str(info.get("content") or "").strip()
        title = str(info.get("title") or "").strip()
        if not content:
            continue
        counters["annotations_with_text"] += 1
        shx = title == SHX_TITLE
        counters["annotations_shx"] += int(shx)
        rect = fitz.Rect(annotation.rect) * matrix
        rect.normalize()
        rows.append({
            "text": content,
            "bbox": [rect.x0, rect.y0, rect.x1, rect.y1],
            "annotation_type": str(annotation.type[1]),
            "annotation_title": title,
            "printed_by_the_drawing": shx,
        })
    return rows, counters


def _drawing_rows(page: Any, matrix: Any) -> tuple[np.ndarray, list[list[float]], dict[str, int]]:
    import fitz

    segments: list[tuple[float, float, float, float]] = []
    rectangles: list[list[float]] = []
    counters = {
        "paths": 0, "paths_white_dropped": 0, "items": 0,
        "items_line": 0, "items_rect": 0, "items_quad": 0, "items_curve": 0,
    }
    steps = geometry_module.CURVE_STEPS
    for drawing in page.get_drawings():
        counters["paths"] += 1
        stroke = drawing.get("color")
        fill = drawing.get("fill")
        if _is_white(stroke) and (fill is None or _is_white(fill)):
            counters["paths_white_dropped"] += 1
            continue
        for item in drawing["items"]:
            counters["items"] += 1
            op = item[0]
            if op == "l":
                p0 = fitz.Point(item[1]) * matrix
                p1 = fitz.Point(item[2]) * matrix
                segments.append((p0.x, p0.y, p1.x, p1.y))
                counters["items_line"] += 1
            elif op == "re":
                rect = fitz.Rect(item[1]) * matrix
                rect.normalize()
                rectangles.append([rect.x0, rect.y0, rect.x1, rect.y1])
                segments.extend([
                    (rect.x0, rect.y0, rect.x1, rect.y0),
                    (rect.x1, rect.y0, rect.x1, rect.y1),
                    (rect.x1, rect.y1, rect.x0, rect.y1),
                    (rect.x0, rect.y1, rect.x0, rect.y0),
                ])
                counters["items_rect"] += 1
            elif op == "qu":
                quad = item[1]
                points = [fitz.Point(point) * matrix for point in (quad.ul, quad.ur, quad.lr, quad.ll)]
                for start, end in zip(points, points[1:] + points[:1]):
                    segments.append((start.x, start.y, end.x, end.y))
                counters["items_quad"] += 1
            elif op == "c":
                control = [fitz.Point(point) * matrix for point in item[1:5]]
                previous = control[0]
                for step in range(1, steps + 1):
                    t = step / steps
                    x = ((1 - t) ** 3 * control[0].x + 3 * (1 - t) ** 2 * t * control[1].x
                         + 3 * (1 - t) * t * t * control[2].x + t ** 3 * control[3].x)
                    y = ((1 - t) ** 3 * control[0].y + 3 * (1 - t) ** 2 * t * control[1].y
                         + 3 * (1 - t) * t * t * control[2].y + t ** 3 * control[3].y)
                    segments.append((previous.x, previous.y, x, y))
                    previous = fitz.Point(x, y)
                counters["items_curve"] += 1
    array = np.asarray(segments, dtype=np.float64) if segments else np.zeros((0, 4))
    return array, rectangles, counters


def read_page(pdf_path: str, page_index: int, profile: DecodingProfile) -> PageSource:
    """Read one page into displayed-space evidence material."""
    import fitz

    document = fitz.open(str(pdf_path))
    try:
        page = document[page_index]
        matrix = page.rotation_matrix
        rect = fitz.Rect(page.rect)
        spans, lines, counters = _text_rows(page, matrix, profile)
        paragraphs = _paragraph_rows(page, matrix, profile)
        annotations, annotation_counters = _annotation_rows(page, matrix)
        segments, rectangles, draw_counters = _drawing_rows(page, matrix)
        images: list[list[float]] = []
        for block in page.get_text("dict")["blocks"]:
            if block["type"] == 0:
                continue
            image_rect = fitz.Rect(block["bbox"]) * matrix
            image_rect.normalize()
            images.append([image_rect.x0, image_rect.y0, image_rect.x1, image_rect.y1])
        source = PageSource(
            page=page_index + 1,
            rotation=int(page.rotation),
            width=float(rect.width),
            height=float(rect.height),
            spans=spans,
            lines=lines,
            paragraphs=paragraphs,
            annotations=annotations,
            images=images,
            geometry=geometry_module.compact(
                segments, rectangles,
                raw_paths=draw_counters["paths"], counters=draw_counters,
            ),
            counters={**counters, **annotation_counters, **draw_counters},
        )
    finally:
        document.close()
    return source


def page_count(pdf_path: str) -> int:
    import fitz

    document = fitz.open(str(pdf_path))
    try:
        return len(document)
    finally:
        document.close()


__all__ = [
    "SHX_TITLE",
    "SPACE_GAP_EM",
    "STAMP_ZONE_MIN_X1",
    "STAMP_ZONE_MIN_Y0",
    "PageSource",
    "document_profile",
    "font_spans",
    "page_count",
    "read_page",
]
