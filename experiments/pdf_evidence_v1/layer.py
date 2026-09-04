"""The evidence layer: units of printed text with geometry, scope and claim.

A unit is a printed string, its rectangle, how it was decoded, which structural
region owns it, and — derived from those — what it is allowed to assert.

Two decisions shape the unit boundary:

* **spans are joined into a string only when they share a structural owner.**
  A cable mark printed as three spans inside one callout is one string; three
  cells of a table row that happen to share a baseline are three strings.
  Joining by baseline alone would invent ``ГРЩ1-РП1-3 630А Корпус 3`` out of
  three unrelated columns — which is precisely the shape of the false
  ``500А → 3200А`` change this project has already paid for once.
* **a gap wider than a few ems ends the string.**  This can only ever split a
  unit; it never attributes one to a region, so it is not proximity reasoning.

``DOCUMENT_SHARED`` is decided at document scope, after every page is read: a
string printed *only* in title blocks, on more than one sheet, applies to the
document.  A string printed in one title block applies to that sheet.  Neither
ever becomes fragment-local — that is decision item 6, and the guard
``assert_scope_discipline`` enforces it structurally rather than by review.
"""
from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass, field
from typing import Any, Iterable, Mapping, Sequence

from . import structure as structure_module
from .contract import (
    DECODED_CAD_REPAIRED,
    DECODED_CAD_UNRESOLVED,
    DECODED_NATIVE,
    UNDECODABLE,
    EvidenceUnit,
    SCHEMA_VERSION,
    STAMP_ZONE,
    assert_scope_discipline,
)
from .decoding import DecodingProfile, is_undecodable
from .extraction import PageSource, read_page
from .textnorm import comparable, normalize

#: A gap wider than this many ems ends a printed string, even inside one owner.
MAX_JOIN_GAP_EM = 2.5

#: A title-block string printed on at least this many sheets applies to the
#: document rather than to one sheet.  Two is the smallest number that can mean
#: "more than this sheet" at all.
DOCUMENT_SHARED_MIN_PAGES = 2

_WEAKEST_FIRST = (
    UNDECODABLE,
    DECODED_CAD_UNRESOLVED,
    DECODED_CAD_REPAIRED,
    DECODED_NATIVE,
)


def _weakest(decodings: Iterable[str]) -> str:
    present = set(decodings)
    for value in _WEAKEST_FIRST:
        if value in present:
            return value
    return DECODED_NATIVE


def _union(boxes: Sequence[Sequence[float]]) -> tuple[float, float, float, float]:
    return (
        min(float(box[0]) for box in boxes),
        min(float(box[1]) for box in boxes),
        max(float(box[2]) for box in boxes),
        max(float(box[3]) for box in boxes),
    )


def _owner_key(attribution: Mapping[str, Any]) -> tuple[Any, ...]:
    return (
        attribution["ownership"],
        attribution["region_id"],
        tuple(attribution["cell"]) if attribution["cell"] else None,
    )


@dataclass
class PageLayer:
    """One page of the evidence layer."""

    page: int
    rotation: int
    width: float
    height: float
    units: list[EvidenceUnit] = field(default_factory=list)
    regions: list[structure_module.Region] = field(default_factory=list)
    cells: list[dict[str, Any]] = field(default_factory=list)
    compaction: dict[str, Any] = field(default_factory=dict)
    counters: dict[str, int] = field(default_factory=dict)
    has_text_layer: bool = False

    def to_dict(self, *, with_units: bool = False) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "page": self.page,
            "rotation": self.rotation,
            "width": round(self.width, 2),
            "height": round(self.height, 2),
            "has_text_layer": self.has_text_layer,
            "units": len(self.units),
            "regions": len(self.regions),
            "table_cells": len(self.cells),
            "compaction": self.compaction,
            "counters": dict(sorted(self.counters.items())),
        }
        if with_units:
            payload["region_index"] = [region.to_dict() for region in self.regions]
            payload["table_cell_index"] = self.cells
            payload["evidence_units"] = [unit.to_dict() for unit in self.units]
        return payload


def _text_units(
    document: str,
    source: PageSource,
    index: structure_module.RegionIndex,
) -> list[EvidenceUnit]:
    """Text-layer units: spans grouped by shared structural ownership."""
    attributions = [structure_module.attribute(source, index, span) for span in source.spans]
    by_span = {span["index"]: attribution for span, attribution in zip(source.spans, attributions)}
    units: list[EvidenceUnit] = []
    for line in source.lines:
        members = [source.spans[position] for position in line["span_indices"]]
        group: list[dict[str, Any]] = []
        group_attribution: Mapping[str, Any] | None = None
        for span in members:
            attribution = by_span[span["index"]]
            if group:
                previous = group[-1]
                size = max(float(previous["size"]), float(span["size"]), 1e-6)
                if line["vertical"]:
                    gap = float(span["bbox"][1]) - float(previous["bbox"][3])
                else:
                    gap = float(span["bbox"][0]) - float(previous["bbox"][2])
                split = (
                    _owner_key(attribution) != _owner_key(group_attribution or attribution)
                    or gap > MAX_JOIN_GAP_EM * size
                )
                if split:
                    units.append(_unit_from_group(document, source, group, group_attribution))
                    group, group_attribution = [], None
            group.append(span)
            group_attribution = attribution if group_attribution is None else group_attribution
        if group:
            units.append(_unit_from_group(document, source, group, group_attribution))
    return [unit for unit in units if unit is not None]


def _unit_from_group(
    document: str,
    source: PageSource,
    group: Sequence[Mapping[str, Any]],
    attribution: Mapping[str, Any] | None,
) -> EvidenceUnit | None:
    if not group or attribution is None:
        return None
    text_parts: list[str] = []
    for position, span in enumerate(group):
        if position:
            previous = group[position - 1]
            size = max(float(previous["size"]), float(span["size"]), 1e-6)
            gap = float(span["bbox"][0]) - float(previous["bbox"][2])
            if gap > 0.22 * size and text_parts and not text_parts[-1].endswith(" "):
                text_parts.append(" ")
        text_parts.append(str(span["text"]))
    text = "".join(text_parts).strip()
    if not text:
        return None
    decoding = _weakest(str(span["decoding"]) for span in group)
    repaired = sum(int(span["repaired_chars"]) for span in group)
    provenance = "NATIVE_PDF_TEXT_CAD_REPAIRED" if repaired else "NATIVE_PDF_TEXT"
    first = group[0]
    return EvidenceUnit(
        unit_id=f"{document}:p{source.page:04d}:t{int(first['index']):05d}",
        document=document,
        page=source.page,
        provenance=provenance,
        decoding=decoding,
        text=text,
        bbox=_union([span["bbox"] for span in group]),
        applicability=str(attribution["applicability"]),
        ownership=str(attribution["ownership"]),
        region_id=attribution["region_id"],
        cell=tuple(attribution["cell"]) if attribution["cell"] else None,
        font=str(first["font"]),
        size=float(first["size"]),
        vertical=bool(first["vertical"]),
        source_spans=len(group),
        repaired_chars=repaired,
    )


def _annotation_units(
    document: str,
    source: PageSource,
    index: structure_module.RegionIndex,
) -> list[EvidenceUnit]:
    """``AutoCAD SHX Text`` and other annotations, with their own rectangles.

    The SHX ones are printed by the drawing: AutoCAD drew the glyphs as vectors
    and put the readable string here.  Other annotation kinds — a review stamp,
    a reviewer's note — are kept and marked, because they are in the file but
    are not something the sheet prints, and a consumer must be able to tell.
    """
    units: list[EvidenceUnit] = []
    for position, annotation in enumerate(source.annotations):
        attribution = structure_module.attribute(source, index, annotation)
        text = str(annotation["text"]).strip()
        if not text:
            continue
        decoding = UNDECODABLE if is_undecodable(text) else DECODED_NATIVE
        notes = ("printed_by_the_drawing",) if annotation["printed_by_the_drawing"] else (
            "not_printed_by_the_drawing",
        )
        units.append(EvidenceUnit(
            unit_id=f"{document}:p{source.page:04d}:a{position:05d}",
            document=document,
            page=source.page,
            provenance="NATIVE_PDF_ANNOTATION",
            decoding=decoding,
            text=text,
            bbox=tuple(float(value) for value in annotation["bbox"]),
            applicability=str(attribution["applicability"]),
            ownership=str(attribution["ownership"]),
            region_id=attribution["region_id"],
            cell=tuple(attribution["cell"]) if attribution["cell"] else None,
            font=None,
            size=None,
            source_spans=1,
            notes=notes,
        ))
    return units


def build_page(document: str, source: PageSource, *, keep_edges: bool = False) -> PageLayer:
    """One page of the layer.

    ``keep_edges`` retains the welded edge arrays on every region.  The audit
    reads 278 pages and needs none of them after attribution, so they are
    released by default: a corpus-wide run otherwise carries hundreds of
    megabytes of geometry whose only remaining use is a bounding box.
    """
    regions = structure_module.build_regions(source)
    index = structure_module.build_index(source, regions)
    units = _text_units(document, source, index) + _annotation_units(document, source, index)
    if not keep_edges:
        import numpy as np

        for region in regions:
            region.edges = np.zeros((0, 4))
    return PageLayer(
        page=source.page,
        rotation=source.rotation,
        width=source.width,
        height=source.height,
        units=units,
        regions=regions,
        cells=structure_module.table_cells(regions),
        compaction=source.geometry.compaction(),
        counters=dict(source.counters),
        has_text_layer=source.has_text_layer,
    )


def promote_document_shared(pages: Sequence[PageLayer]) -> int:
    """A title-block string printed on several sheets applies to the document.

    Structural, not statistical: every occurrence of the string must sit in a
    title block, and it must do so on more than one sheet.  One occurrence
    outside a title block anywhere in the document is enough to refuse the
    promotion, because then the string is not a document-scope label.
    """
    occurrences: dict[str, list[EvidenceUnit]] = defaultdict(list)
    for page in pages:
        for unit in page.units:
            key = comparable(unit.text)
            if key:
                occurrences[key].append(unit)
    promoted = 0
    for units in occurrences.values():
        if not all(unit.ownership == STAMP_ZONE for unit in units):
            continue
        if len({unit.page for unit in units}) < DOCUMENT_SHARED_MIN_PAGES:
            continue
        for unit in units:
            unit.applicability = "DOCUMENT_SHARED"
            promoted += 1
    return promoted


@dataclass
class DocumentLayer:
    """The whole evidence layer of one document."""

    document: str
    pdf_path: str
    pages: list[PageLayer] = field(default_factory=list)
    decoding: dict[str, Any] = field(default_factory=dict)
    document_shared_units: int = 0

    @property
    def units(self) -> list[EvidenceUnit]:
        return [unit for page in self.pages for unit in page.units]

    def summary(self) -> dict[str, Any]:
        units = self.units
        provenance = Counter(unit.provenance for unit in units)
        applicability = Counter(unit.applicability for unit in units)
        claim = Counter(unit.claim for unit in units)
        ownership = Counter(unit.ownership for unit in units)
        decoding = Counter(unit.decoding for unit in units)
        raw = sum(int(page.compaction.get("raw_segments") or 0) for page in self.pages)
        kept = sum(int(page.compaction.get("welded_edges") or 0) for page in self.pages)
        return {
            "document": self.document,
            "pages": len(self.pages),
            "pages_without_a_text_layer": sum(1 for page in self.pages if not page.has_text_layer),
            "units": len(units),
            "units_by_provenance": dict(sorted(provenance.items())),
            "units_by_applicability": dict(sorted(applicability.items())),
            "units_by_claim": dict(sorted(claim.items())),
            "units_by_ownership": dict(sorted(ownership.items())),
            "units_by_decoding": dict(sorted(decoding.items())),
            "document_shared_units": self.document_shared_units,
            "table_cells": sum(len(page.cells) for page in self.pages),
            "regions": sum(len(page.regions) for page in self.pages),
            "raw_segments": raw,
            "welded_edges": kept,
            "geometry_compression": round(raw / kept, 1) if kept else None,
            "rotations": dict(sorted(Counter(page.rotation for page in self.pages).items())),
        }


def build_document(
    document: str,
    pdf_path: str,
    profile: DecodingProfile,
    *,
    pages: Sequence[PageSource] | None = None,
) -> DocumentLayer:
    """Build the layer for one document.

    ``pages`` may be supplied by a caller that already read the geometry, so a
    long audit reads each PDF once instead of once per measurement.
    """
    from .extraction import page_count

    if pages is not None:
        layers = [build_page(document, source) for source in pages]
    else:
        # Streamed on purpose: one page of geometry at a time, so a 3.8 million
        # segment document never exists in memory twice.
        layers = [
            build_page(document, read_page(str(pdf_path), index, profile))
            for index in range(page_count(str(pdf_path)))
        ]
    promoted = promote_document_shared(layers)
    layer = DocumentLayer(
        document=document,
        pdf_path=str(pdf_path),
        pages=layers,
        decoding=profile.to_dict(),
        document_shared_units=promoted,
    )
    assert_scope_discipline(layer.units)
    return layer


def envelope(layer: DocumentLayer, *, detail_pages: Sequence[int] = ()) -> dict[str, Any]:
    """The artifact form of the layer.

    Full unit detail only for the pages a caller names.  A layer with every
    unit of every page inlined is 45 000 rows of JSON that nobody reads and
    that no consumer needs to see in an audit artifact — the point of the
    envelope is the shape and the totals, and the shape is visible on one page.
    """
    wanted = set(int(page) for page in detail_pages)
    return {
        "schema_version": SCHEMA_VERSION,
        "kind": "pdf_evidence_layer",
        "model_calls": 0,
        "document": layer.document,
        "pdf_path": layer.pdf_path,
        "summary": layer.summary(),
        "decoding": layer.decoding,
        "pages": [page.to_dict(with_units=page.page in wanted) for page in layer.pages],
    }


__all__ = [
    "DOCUMENT_SHARED_MIN_PAGES",
    "MAX_JOIN_GAP_EM",
    "DocumentLayer",
    "PageLayer",
    "build_document",
    "build_page",
    "envelope",
    "promote_document_shared",
]
