"""One page read once: its strokes, its regions and its printed strings.

Three consumers need the same page and none of them may disagree with the
others about what is on it, so the page is opened once and the three views are
built from that single read.

* the strokes come from :mod:`strokes`;
* the regions come from **V1's region model, unchanged** — the lattice, the
  closed box, the title block and the sheet frame are exactly what the evidence
  layer means by those words, and re-deriving them here would let the two
  layers drift apart silently;
* the printed strings come from **V1's text channels, unchanged** — the text
  layer, the multi-span line and the ``AutoCAD SHX Text`` annotation.

Nothing here classifies anything.  This module answers "what is drawn and what
is printed"; every question of the form "what does it mean" belongs downstream.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Sequence

import numpy as np

from experiments.pdf_evidence_v1 import structure as v1_structure
from experiments.pdf_evidence_v1.decoding import DecodingProfile
from experiments.pdf_evidence_v1.extraction import (
    STAMP_ZONE_MIN_X1,
    STAMP_ZONE_MIN_Y0,
    _annotation_rows,
    _paragraph_rows,
    _text_rows,
)

from . import strokes as strokes_module
from .strokes import PageStrokes


@dataclass
class _GeometryView:
    """The shape V1's region builder expects, filled from V2's welded edges."""

    horizontal: np.ndarray
    vertical: np.ndarray


@dataclass
class _SourceView:
    """A stand-in for V1's ``PageSource`` carrying only what regions need."""

    page: int
    width: float
    height: float
    geometry: _GeometryView
    paragraphs: list[dict[str, Any]]

    def in_stamp_zone(self, bbox: Sequence[float]) -> bool:
        if not self.width or not self.height:
            return False
        return (
            float(bbox[1]) / self.height >= STAMP_ZONE_MIN_Y0
            and float(bbox[2]) / self.width >= STAMP_ZONE_MIN_X1
        )


@dataclass
class PageData:
    """Everything one physical page carries, in displayed space."""

    document: str
    page: int
    rotation: int
    width: float
    height: float
    strokes: PageStrokes
    regions: list[v1_structure.Region] = field(default_factory=list)
    lines: list[dict[str, Any]] = field(default_factory=list)
    annotations: list[dict[str, Any]] = field(default_factory=list)
    counters: dict[str, int] = field(default_factory=dict)

    @property
    def labels(self) -> list[dict[str, Any]]:
        """Every printed string of the page, in one list, in reading order.

        A label is a label whichever channel carried it.  The SHX annotation is
        not a lesser string than a text-layer span — on the left document of
        this corpus it is the only channel that carries four thousand of them.
        """
        rows: list[dict[str, Any]] = []
        for index, line in enumerate(self.lines):
            rows.append({
                "label_id": f"l:p{self.page:04d}:t{index:05d}",
                "text": str(line["text"]),
                "bbox": [float(value) for value in line["bbox"]],
                "size": float(line["size"]),
                "vertical": bool(line["vertical"]),
                "decoding": str(line["decoding"]),
                "provenance": (
                    "NATIVE_PDF_TEXT_CAD_REPAIRED" if int(line["repaired_chars"])
                    else "NATIVE_PDF_TEXT"
                ),
            })
        for index, annotation in enumerate(self.annotations):
            box = [float(value) for value in annotation["bbox"]]
            rows.append({
                "label_id": f"l:p{self.page:04d}:a{index:05d}",
                "text": str(annotation["text"]),
                "bbox": box,
                "size": max(min(box[3] - box[1], box[2] - box[0]), 1e-6),
                "vertical": (box[3] - box[1]) > (box[2] - box[0]),
                "decoding": "DECODED_NATIVE",
                "provenance": "NATIVE_PDF_ANNOTATION",
            })
        rows.sort(key=lambda row: (round(row["bbox"][1], 1), round(row["bbox"][0], 1), row["label_id"]))
        return rows

    def region_index(self) -> v1_structure.RegionIndex:
        return v1_structure.build_index(self._source_view(), self.regions)

    def v1_ownership(self) -> dict[str, dict[str, Any]]:
        """V1's answer for the same strings, so before/after is like-for-like.

        The comparison this package makes is not against V1's published table
        but against V1's *rule*, run here on the same read of the same page.
        Only the target differs — a region there, a run here — so the delta
        cannot be an artefact of a different extraction.
        """
        source = self._source_view()
        index = v1_structure.build_index(source, self.regions)
        out: dict[str, dict[str, Any]] = {}
        for label in self.labels:
            out[str(label["label_id"])] = v1_structure.attribute(source, index, label)
        return out

    def _source_view(self) -> _SourceView:
        horizontal = self.strokes.edges[self.strokes.horizontal_mask]
        vertical = self.strokes.edges[~self.strokes.horizontal_mask]
        return _SourceView(
            page=self.page,
            width=self.width,
            height=self.height,
            geometry=_GeometryView(horizontal=horizontal, vertical=vertical),
            paragraphs=[],
        )


def read(document: str, pdf_path: str, page_index: int, profile: DecodingProfile) -> PageData:
    """Read one page's strokes, regions and printed strings from a single open."""
    import fitz

    page_strokes = strokes_module.read_page(str(pdf_path), page_index)
    handle = fitz.open(str(pdf_path))
    try:
        page = handle[page_index]
        matrix = page.rotation_matrix
        _, lines, text_counters = _text_rows(page, matrix, profile)
        annotations, annotation_counters = _annotation_rows(page, matrix)
        paragraphs = _paragraph_rows(page, matrix, profile) if not lines else []
    finally:
        handle.close()
    data = PageData(
        document=document,
        page=page_index + 1,
        rotation=page_strokes.rotation,
        width=page_strokes.width,
        height=page_strokes.height,
        strokes=page_strokes,
        lines=lines,
        annotations=annotations,
        counters={**page_strokes.counters, **text_counters, **annotation_counters},
    )
    source = data._source_view()
    source.paragraphs = paragraphs
    data.regions = v1_structure.build_regions(source)
    return data


__all__ = ["PageData", "read"]
