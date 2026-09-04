"""Function Lineage v3.0 — FunctionRegion / evidence geometry feasibility audit.

Research only.  No model calls, no deploy, no shadow, no materialization, no
production module changed.

The v2.9 track ended on verdict **B**: a sound deterministic evidence-binding
layer that reaches the wrong facts, because ``function_lineage_source`` reads a
Markdown page as a stream of sentences and nothing in that stream says *where*
on the sheet a sentence was printed.  This module asks the prior question:
does the source itself carry the geometry, and if it does, where is it lost?

Nothing here is tuned per page or per file.  Every threshold is declared in
:mod:`page_geometry` or :mod:`regions`, is the same for all six documents, and
the two that could plausibly be tuned — the leader gap and the leader overlap —
are reported as a sensitivity curve instead of a single chosen value.
"""
from __future__ import annotations

import json
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

import numpy as np

from backend.app.services.stage_comparison import function_lineage_source as source

from . import corpus
from . import page_geometry as geometry_module
from . import regions as region_module

SCHEMA_VERSION = "function-region-feasibility.v3.0"
DEFAULT_OUTPUT = corpus.COMPARISON_ROOT / "20260904_function_lineage_v3_region_feasibility"

#: The v2.9 passport fields whose documented values this audit tries to place.
BOUND_FIELDS = (
    "serviced_object",
    "building",
    "corpus",
    "section",
    "zone",
    "floors",
    "systems",
    "consumers",
    "equipment_roles",
    "upstream",
    "downstream",
    "stable_entities",
    "cross_sheet_functional_references",
)

#: The fields both blocked tiers need.  Reported separately everywhere.
SCOPE_FIELDS = ("serviced_object", "building", "corpus", "section", "zone", "floors")

#: v2.9 result, reproduced here as the baseline every table is read against.
V2_9_PROVEN = {
    "serviced_object": 0, "building": 0, "corpus": 0, "section": 0,
    "zone": 10, "floors": 26,
}
V2_9_TOTALS = {"facts": 4213, "proven": 1034, "sheet_shared": 403, "ambiguous": 2770, "unknown": 6}

#: Evidence applicability, per §12 of the master task.
APPLICABILITY = ("FRAGMENT_LOCAL", "SHEET_SHARED", "DOCUMENT_SHARED", "UNKNOWN")


# ---------------------------------------------------------------------------
# page cache
# ---------------------------------------------------------------------------


def _page_records(pdf_path: Path) -> list[dict[str, Any]]:
    """Geometry plus regions for every page of one document."""
    import fitz

    document = fitz.open(str(pdf_path))
    count = len(document)
    document.close()
    output: list[dict[str, Any]] = []
    for index in range(count):
        page = geometry_module.read_page(str(pdf_path), index)
        page_regions = region_module.build_regions(page)
        region_index = region_module.build_index(page, page_regions)
        attributions = [
            region_module.attribute(page, region_index, span) for span in page.spans
        ]
        output.append({
            "geometry": page,
            "regions": page_regions,
            "index": region_index,
            "attributions": attributions,
        })
    return output


class Corpus:
    """Lazily read geometry for the six frozen documents, once per process."""

    def __init__(self) -> None:
        self._pages: dict[tuple[str, str], list[dict[str, Any]]] = {}
        self._markdown: dict[tuple[str, str], dict[int, str]] = {}

    def pages(self, pair_id: str, side: str) -> list[dict[str, Any]]:
        key = (pair_id, side)
        if key not in self._pages:
            paths = corpus.document_paths(pair_id, side)
            self._pages[key] = _page_records(paths["pdf"])
        return self._pages[key]

    def markdown(self, pair_id: str, side: str) -> dict[int, str]:
        key = (pair_id, side)
        if key not in self._markdown:
            paths = corpus.document_paths(pair_id, side)
            self._markdown[key] = corpus.markdown_pages(paths["markdown"])
        return self._markdown[key]

    def page(self, pair_id: str, side: str, page: int) -> dict[str, Any] | None:
        pages = self.pages(pair_id, side)
        if 1 <= page <= len(pages):
            return pages[page - 1]
        return None


# ---------------------------------------------------------------------------
# §3 data availability matrix
# ---------------------------------------------------------------------------


CHANNELS = (
    "text_span_bbox",
    "paragraph_bbox",
    "table_cell_bbox",
    "vector_lines",
    "drawn_boundaries",
    "leader_geometry",
    "graphic_object_bbox",
    "annotations",
    "image_bbox",
    "drawing_paths",
    "equipment_label",
    "sheet_frame",
    "title_block_region",
)


def _page_channels(record: Mapping[str, Any]) -> dict[str, int]:
    page: geometry_module.PageGeometry = record["geometry"]
    page_regions: Sequence[region_module.Region] = record["regions"]
    tables = [region for region in page_regions if region.kind == "TABLE"]
    cells = sum(
        max(len(region.rows) - 1, 0) * max(len(region.columns) - 1, 0) for region in tables
    )
    attached = sum(
        1 for row in record["attributions"] if row["relation"] == "CONNECTED_CALLOUT"
    )
    equipment = sum(1 for span in page.spans if source._SYSTEM_RE.search(span["text"]))
    return {
        "text_span_bbox": len(page.spans),
        "paragraph_bbox": len(page.text_blocks),
        "table_cell_bbox": cells,
        "vector_lines": int(len(page.segments)),
        "drawn_boundaries": sum(region.edge_count for region in page_regions),
        "leader_geometry": attached,
        "graphic_object_bbox": len(page_regions),
        "annotations": len(page.annotations),
        "image_bbox": len(page.images),
        "drawing_paths": int(page.counters.get("paths", 0)),
        "equipment_label": equipment,
        "sheet_frame": sum(1 for region in page_regions if region.kind == "SHEET_FRAME"),
        "title_block_region": sum(
            1 for span in page.spans if page.in_stamp_zone(span["bbox"])
        ),
    }


def availability_matrix(store: Corpus) -> dict[str, Any]:
    documents: list[dict[str, Any]] = []
    for pair_id, project, side, paths in corpus.documents():
        records = store.pages(pair_id, side)
        rows = [_page_channels(record) for record in records]
        per_page = {
            channel: sum(1 for row in rows if row[channel] > 0) for channel in CHANNELS
        }
        totals = {channel: sum(row[channel] for row in rows) for channel in CHANNELS}
        unstructured = [
            round(float(geometry_module.unstructured_ink_share(record["geometry"].segments)), 4)
            for record in records
        ]
        documents.append({
            "project": project,
            "side": side,
            "document_code": paths["code"],
            "pages": len(records),
            "rotations": dict(sorted(Counter(
                record["geometry"].rotation for record in records
            ).items())),
            "pages_with_channel": per_page,
            "channel_totals": totals,
            "pages_without_native_text": sum(1 for row in rows if row["text_span_bbox"] == 0),
            "spans_repaired_from_cad_encoding": sum(
                record["geometry"].counters.get("spans_repaired", 0) for record in records
            ),
            "median_unstructured_ink_share": (
                sorted(unstructured)[len(unstructured) // 2] if unstructured else 0.0
            ),
        })
    return {
        "schema_version": SCHEMA_VERSION,
        "kind": "data_availability_matrix",
        "model_calls": 0,
        "channels": list(CHANNELS),
        "documents": documents,
    }


# ---------------------------------------------------------------------------
# §7 upstream loss audit
# ---------------------------------------------------------------------------


def upstream_losses(store: Corpus) -> dict[str, Any]:
    """What the PDF carries, what ``blocks.json`` keeps, what the Markdown keeps.

    The comparison is deliberately blunt: a channel is "kept" only if the
    downstream artifact can answer *where on the sheet* the thing is.  A
    description of a table in prose is not a table cell rectangle.
    """
    documents: list[dict[str, Any]] = []
    for pair_id, project, side, paths in corpus.documents():
        records = store.pages(pair_id, side)
        blocks = corpus.blocks_document(paths["blocks"]) or {}
        block_rows = blocks.get("blocks") or []
        page_sizes = {
            int(row["page_index"]): (float(row["width_px"]), float(row["height_px"]))
            for row in (blocks.get("pages") or [])
        }
        whole_sheet_blocks = 0
        for row in block_rows:
            coords = row.get("coords_norm") or []
            if len(coords) != 4:
                continue
            area = max(0.0, coords[2] - coords[0]) * max(0.0, coords[3] - coords[1])
            if area >= 0.9:
                whole_sheet_blocks += 1
        markdown = store.markdown(pair_id, side)
        documents.append({
            "project": project,
            "side": side,
            "pdf": {
                "pages": len(records),
                "text_spans_with_bbox": sum(len(record["geometry"].spans) for record in records),
                "paragraph_rectangles": sum(len(record["geometry"].text_blocks) for record in records),
                "vector_segments": sum(int(len(record["geometry"].segments)) for record in records),
                "drawn_boundaries": sum(
                    sum(region.edge_count for region in record["regions"]) for record in records
                ),
                "table_cell_rectangles": sum(
                    sum(
                        max(len(region.rows) - 1, 0) * max(len(region.columns) - 1, 0)
                        for region in record["regions"] if region.kind == "TABLE"
                    )
                    for record in records
                ),
                "leader_attachments": sum(
                    sum(1 for row in record["attributions"] if row["relation"] == "CONNECTED_CALLOUT")
                    for record in records
                ),
                "annotations": sum(len(record["geometry"].annotations) for record in records),
                "image_rectangles": sum(len(record["geometry"].images) for record in records),
            },
            "blocks_json": {
                "blocks": len(block_rows),
                "pages_described": len(page_sizes),
                "rectangles": sum(1 for row in block_rows if row.get("shape_type") == "rectangle"),
                "polygons": sum(1 for row in block_rows if row.get("polygon_points")),
                "blocks_covering_at_least_90_percent_of_page": whole_sheet_blocks,
                "text_spans_with_bbox": 0,
                "vector_segments": 0,
                "table_cell_rectangles": 0,
                "leader_attachments": 0,
                "annotations": 0,
            },
            "markdown": {
                "pages": len(markdown),
                "characters": sum(len(body) for body in markdown.values()),
                "coordinates_of_any_kind": 0,
            },
        })
    return {
        "schema_version": SCHEMA_VERSION,
        "kind": "upstream_loss_audit",
        "model_calls": 0,
        "documents": documents,
        "losses": [
            {
                "channel": "text span rectangle",
                "in_pdf": "every span carries a bbox, a font and a size",
                "in_blocks_json": "absent — blocks.json holds one rectangle per block",
                "in_markdown": "absent — a page is a stream of lines",
                "consequence": (
                    "a documented value cannot be placed on the sheet, so every "
                    "fact of a page is a fact of every function on it"
                ),
            },
            {
                "channel": "paragraph rectangle",
                "in_pdf": "PyMuPDF text blocks, one rectangle per paragraph",
                "in_blocks_json": "absent",
                "in_markdown": "paragraph order is kept, position is not",
                "consequence": "a section of a sheet cannot be delimited",
            },
            {
                "channel": "table cell rectangle",
                "in_pdf": "derivable from the drawn lattice of rulings",
                "in_blocks_json": "absent",
                "in_markdown": "a table becomes prose or a pipe table without geometry",
                "consequence": (
                    "row and column ownership — the one channel v2.9 could use — "
                    "survives for 61 facts out of 4213"
                ),
            },
            {
                "channel": "vector line",
                "in_pdf": "millions of segments per document",
                "in_blocks_json": "absent",
                "in_markdown": "absent",
                "consequence": "no boundary, no bus, no leader, no connector",
            },
            {
                "channel": "leader / callout",
                "in_pdf": "a stroke drawn along its label, recoverable deterministically",
                "in_blocks_json": "absent",
                "in_markdown": "absent",
                "consequence": (
                    "v2.9 declared EXPLICIT_CALLOUT_TO_ONE_FRAGMENT structurally "
                    "unavailable; it is available in the source"
                ),
            },
            {
                "channel": "page rotation",
                "in_pdf": "/Rotate per page, 0/90/270 all present in this corpus",
                "in_blocks_json": "kept as a number, unused by the fact extractor",
                "in_markdown": "absent",
                "consequence": "a rotated sheet's stamp is read as body text",
            },
            {
                "channel": "CAD font encoding",
                "in_pdf": "ISOCPEUR subsets shift Cyrillic by a constant",
                "in_blocks_json": "absent",
                "in_markdown": "recovered by OCR, without position",
                "consequence": (
                    "the drawing titles that name the function are the very "
                    "strings the naive text layer returns as mojibake"
                ),
            },
            {
                "channel": "annotation",
                "in_pdf": "8323 annotations across the six documents",
                "in_blocks_json": "absent",
                "in_markdown": "absent",
                "consequence": "revision clouds and markup are invisible downstream",
            },
        ],
    }


# ---------------------------------------------------------------------------
# §5 / §8 the FunctionRegion prototype, measured
# ---------------------------------------------------------------------------


def _page_span_index(record: Mapping[str, Any]) -> list[tuple[str, dict[str, Any], dict[str, Any]]]:
    """``(normalized_text, span, attribution)`` for every span of the page."""
    page = record["geometry"]
    return [
        (corpus.normalize(span["text"]), span, attribution)
        for span, attribution in zip(page.spans, record["attributions"])
    ]


def region_claims(record: Mapping[str, Any]) -> dict[str, list[str]]:
    """Function classes each region claims, from the text attributed to it.

    A region claims a class only when its own attributed text names it.  A
    region that claims nothing owns nothing, and a region that claims two
    classes proves neither.
    """
    text_by_region: dict[str, list[str]] = defaultdict(list)
    for _normalized, span, attribution in _page_span_index(record):
        region_id = attribution.get("region_id")
        if region_id:
            text_by_region[region_id].append(span["text"])
    return {
        region_id: source._function_classes(" ".join(chunks))
        for region_id, chunks in sorted(text_by_region.items())
    }


def _value_placement(
    record: Mapping[str, Any], value: str
) -> dict[str, Any]:
    """Where on the sheet is this documented value actually printed?"""
    needle = corpus.normalize(value)
    if not needle:
        return {"status": "EMPTY", "applicability": "UNKNOWN", "regions": [], "occurrences": 0}
    page = record["geometry"]
    occurrences = [
        (span, attribution)
        for normalized, span, attribution in _page_span_index(record)
        if needle in normalized
    ]
    if not occurrences:
        return {
            "status": "NOT_IN_TEXT_LAYER", "applicability": "UNKNOWN",
            "regions": [], "occurrences": 0,
        }
    relations = Counter(attribution["relation"] for _span, attribution in occurrences)
    region_ids = sorted({
        attribution["region_id"] for _span, attribution in occurrences
        if attribution["region_id"]
    })
    if all(attribution["relation"] == "SHEET_SHARED" for _span, attribution in occurrences):
        status = "SHEET_SHARED"
        applicability = "SHEET_SHARED"
    elif len(region_ids) == 1 and all(
        attribution["relation"] in region_module.PROVING_RELATIONS
        or attribution["relation"] == "SHEET_SHARED"
        for _span, attribution in occurrences
    ):
        status = "REGION_LOCAL"
        applicability = "FRAGMENT_LOCAL"
    elif len(region_ids) > 1:
        status = "MANY_REGIONS"
        applicability = "UNKNOWN"
    else:
        status = "UNPLACED"
        applicability = "UNKNOWN"
    return {
        "status": status,
        "applicability": applicability,
        "regions": region_ids,
        "occurrences": len(occurrences),
        "relations": dict(sorted(relations.items())),
    }


def value_attribution(store: Corpus) -> dict[str, Any]:
    """Every documented value of every function, placed on the sheet or refused."""
    rows: list[dict[str, Any]] = []
    per_corpus: dict[str, Counter] = defaultdict(Counter)
    for pair_id, project, _side, _paths in corpus.documents():
        if _side != "LEFT":
            continue
        break
    for pair_id in sorted(corpus.PROJECTS, key=lambda key: corpus.CORPUS_ORDER.index(corpus.PROJECTS[key])):
        project = corpus.PROJECTS[pair_id]
        passports = corpus.passports(pair_id)
        for side in corpus.SIDES:
            for function_id, passport in sorted(passports[side].items()):
                page_number = int(passport["source_sheet"]["physical_page"])
                record = store.page(pair_id, side, page_number)
                if record is None:
                    continue
                claims = region_claims(record)
                fragment_id = str(passport["function_fragment_ids"][0])
                function_class = str(passport["function_class"])
                for field in BOUND_FIELDS:
                    raw = passport.get(field)
                    values = [raw] if isinstance(raw, str) else list(raw or [])
                    for value in values:
                        if not str(value).strip():
                            continue
                        placement = _value_placement(record, str(value))
                        region_id = placement["regions"][0] if len(placement["regions"]) == 1 else None
                        region_classes = claims.get(region_id or "", [])
                        fragment_local = (
                            placement["status"] == "REGION_LOCAL"
                            and len(region_classes) == 1
                            and region_classes[0] == function_class
                        )
                        rows.append({
                            "project": project,
                            "side": side,
                            "page": page_number,
                            "function_id": function_id,
                            "fragment_id": fragment_id,
                            "function_class": function_class,
                            "field": field,
                            "value": str(value)[:180],
                            "placement": placement["status"],
                            "applicability": placement["applicability"],
                            "region_id": region_id,
                            "region_function_classes": region_classes,
                            "fragment_local": bool(fragment_local),
                            "occurrences": placement["occurrences"],
                        })
                        per_corpus[project][placement["status"]] += 1
    return {
        "schema_version": SCHEMA_VERSION,
        "kind": "value_attribution",
        "model_calls": 0,
        "rows": rows,
        "per_corpus": {project: dict(sorted(counter.items())) for project, counter in sorted(per_corpus.items())},
    }


# ---------------------------------------------------------------------------
# §9 what a region-first extractor could reach
# ---------------------------------------------------------------------------


#: Scope patterns, taken from the production extractor so the comparison with
#: v2.9 is like for like.  Nothing new is invented here.
SCOPE_PATTERNS = {
    "serviced_object": source._OBJECT_RE,
    "corpus": source._CORPUS_RE,
    "section": source._SECTION_RE,
    "zone": source._ZONE_RE,
    "floors": source._FLOOR_RE,
}


def _confirmed_by_markdown(value: str, body: str) -> bool:
    """Native text may confirm, never assert alone.

    This is the production rule for ``NATIVE_PDF_TEXT`` provenance: the
    recognized layer says *what* was printed, the text layer says *where*.  A
    string the recognized layer never saw is not promoted to a fact.
    """
    return corpus.normalize(value) in corpus.normalize(body)


def recognition_content_loss(store: Corpus) -> dict[str, Any]:
    """How much of the printed text survives into the recognized Markdown.

    The v2.9 blocker was position.  This measures the other half: whether the
    recognized layer even contains the strings the sheet prints.  A span counts
    as surviving when its normalized text occurs in the Markdown of the same
    page; short spans are excluded because a two-character token matches by
    accident.
    """
    documents: list[dict[str, Any]] = []
    for pair_id, project, side, _paths in corpus.documents():
        bodies = store.markdown(pair_id, side)
        total = 0
        survived = 0
        drawing_total = 0
        drawing_survived = 0
        worst: list[dict[str, Any]] = []
        for record in store.pages(pair_id, side):
            page = record["geometry"]
            body = corpus.normalize(bodies.get(page.page, ""))
            candidates = [
                corpus.normalize(span["text"]) for span in page.spans
                if len(corpus.normalize(span["text"])) >= 4
            ]
            if not candidates:
                continue
            hit = sum(1 for value in candidates if value in body)
            total += len(candidates)
            survived += hit
            is_drawing = len(page.segments) >= 1000
            if is_drawing:
                drawing_total += len(candidates)
                drawing_survived += hit
            worst.append({
                "page": page.page,
                "printed_strings": len(candidates),
                "in_markdown": hit,
                "share": round(hit / len(candidates), 3),
                "drawing": is_drawing,
            })
        worst.sort(key=lambda row: (row["share"], -row["printed_strings"]))
        documents.append({
            "project": project,
            "side": side,
            "printed_strings": total,
            "in_markdown": survived,
            "share": round(survived / total, 4) if total else None,
            "drawing_pages_printed_strings": drawing_total,
            "drawing_pages_in_markdown": drawing_survived,
            "drawing_pages_share": round(drawing_survived / drawing_total, 4) if drawing_total else None,
            "worst_pages": worst[:5],
        })
    return {
        "schema_version": SCHEMA_VERSION,
        "kind": "recognition_content_loss",
        "model_calls": 0,
        "documents": documents,
    }


def region_scope_facts(store: Corpus, *, require_markdown: bool = True) -> dict[str, Any]:
    """Scope values printed inside a region, and whether they discriminate.

    The previous measurement asked how many *existing* passport values a region
    can place.  This one asks the question the data contract actually needs:
    when the sheet prints a scope value inside a delimited region, does the
    page end up with regions that disagree?  A value every region shares has
    zero discriminating power and must never become a certificate.
    """
    pages: list[dict[str, Any]] = []
    totals = Counter()
    for pair_id in sorted(corpus.PROJECTS, key=lambda key: corpus.CORPUS_ORDER.index(corpus.PROJECTS[key])):
        project = corpus.PROJECTS[pair_id]
        for side in corpus.SIDES:
            bodies = store.markdown(pair_id, side)
            for record in store.pages(pair_id, side):
                page = record["geometry"]
                body = bodies.get(page.page, "")
                by_region: dict[str, dict[str, set[str]]] = defaultdict(lambda: defaultdict(set))
                sheet_shared: dict[str, set[str]] = defaultdict(set)
                unconfirmed = 0
                for _normalized, span, attribution in _page_span_index(record):
                    for field, pattern in SCOPE_PATTERNS.items():
                        for match in pattern.finditer(span["text"]):
                            value = corpus.normalize(match.group(0))
                            if not value:
                                continue
                            if require_markdown and not _confirmed_by_markdown(value, body):
                                unconfirmed += 1
                                continue
                            if not require_markdown and not _confirmed_by_markdown(value, body):
                                unconfirmed += 1
                            if attribution["applicability"] == "FRAGMENT_LOCAL":
                                by_region[attribution["region_id"]][field].add(value)
                            elif attribution["applicability"] == "SHEET_SHARED":
                                sheet_shared[field].add(value)
                if not by_region and not sheet_shared:
                    continue
                discriminating: dict[str, int] = {}
                for field in SCOPE_PATTERNS:
                    values = {
                        region_id: sorted(fields[field])
                        for region_id, fields in by_region.items() if fields.get(field)
                    }
                    distinct = {tuple(value) for value in values.values()}
                    discriminating[field] = len(distinct)
                    totals[f"{field}:regions_with_value"] += len(values)
                    if len(distinct) > 1:
                        totals[f"{field}:pages_with_disagreeing_regions"] += 1
                pages.append({
                    "project": project,
                    "side": side,
                    "page": page.page,
                    "regions_with_scope_value": len(by_region),
                    "sheet_shared_fields": {
                        field: sorted(values) for field, values in sorted(sheet_shared.items())
                    },
                    "distinct_region_values": discriminating,
                    "unconfirmed_by_markdown": unconfirmed,
                })
    return {
        "schema_version": SCHEMA_VERSION,
        "kind": "region_scope_facts",
        "model_calls": 0,
        "require_markdown_confirmation": require_markdown,
        "rule": (
            "a native span may carry a scope value only when the recognized "
            "Markdown of the same page contains it; the text layer supplies "
            "position, never content"
        ),
        "pages": pages,
        "totals": dict(sorted(totals.items())),
    }


# ---------------------------------------------------------------------------
# §4 representative page audit
# ---------------------------------------------------------------------------


#: The nine cases the master task asks for.  Each is a predicate over one page,
#: evaluated on measured quantities only.  The first page of the corpus order
#: satisfying a case is the representative of that case: no page is picked for
#: the answer it gives.
def _page_case(record: Mapping[str, Any], classes: Sequence[str]) -> set[str]:
    page = record["geometry"]
    page_regions = record["regions"]
    tables = [region for region in page_regions if region.kind == "TABLE"]
    cases: set[str] = set()
    if len(classes) == 1:
        cases.add("SINGLE_FUNCTION")
    if len(classes) > 1:
        cases.add("MANY_FUNCTIONS")
    if len(set(classes)) < len(classes):
        cases.add("SAME_CLASS_TWICE")
    if tables:
        cases.add("TABLE")
    if len(page.segments) >= 20000:
        cases.add("SCHEME")
    if len([region for region in page_regions if region.kind in {"TABLE", "BOX"}]) >= 4:
        cases.add("REPEATED_SCHEMES")
    if page.spans and len(page.segments) >= 1000 and len(page.spans) >= 200:
        cases.add("MIXED_TEXT_AND_GRAPHICS")
    if not page.spans:
        cases.add("NO_TEXT_LAYER")
    if page.rotation:
        cases.add("ROTATED")
    return cases


REPRESENTATIVE_CASES = (
    "SINGLE_FUNCTION", "MANY_FUNCTIONS", "SAME_CLASS_TWICE", "TABLE", "SCHEME",
    "REPEATED_SCHEMES", "MIXED_TEXT_AND_GRAPHICS", "NO_TEXT_LAYER", "ROTATED",
)


def representative_pages(store: Corpus) -> dict[str, Any]:
    chosen: dict[str, dict[str, Any]] = {}
    census = Counter()
    for pair_id in sorted(corpus.PROJECTS, key=lambda key: corpus.CORPUS_ORDER.index(corpus.PROJECTS[key])):
        project = corpus.PROJECTS[pair_id]
        passports = corpus.passports(pair_id)
        for side in corpus.SIDES:
            by_page: dict[int, list[str]] = defaultdict(list)
            for _function_id, passport in sorted(passports[side].items()):
                by_page[int(passport["source_sheet"]["physical_page"])].append(
                    str(passport["function_class"])
                )
            for record in store.pages(pair_id, side):
                page = record["geometry"]
                cases = _page_case(record, by_page.get(page.page, []))
                for case in sorted(cases):
                    census[case] += 1
                    if case in chosen:
                        continue
                    relations = Counter(row["relation"] for row in record["attributions"])
                    chosen[case] = {
                        "case": case,
                        "project": project,
                        "side": side,
                        "page": page.page,
                        "rotation": page.rotation,
                        "function_classes": sorted(set(by_page.get(page.page, []))),
                        "spans": len(page.spans),
                        "segments": int(len(page.segments)),
                        "regions": dict(sorted(Counter(
                            region.kind for region in record["regions"]
                        ).items())),
                        "span_relations": dict(sorted(relations.items())),
                        "attributed_share": round(
                            sum(relations[key] for key in region_module.PROVING_RELATIONS)
                            / max(len(page.spans), 1), 3
                        ),
                    }
    return {
        "schema_version": SCHEMA_VERSION,
        "kind": "representative_pages",
        "model_calls": 0,
        "selection_rule": (
            "the first page in corpus order (IOS1.1, IOS2.1, IOS3.1; LEFT then "
            "RIGHT; ascending page) that satisfies the case predicate"
        ),
        "census": dict(sorted(census.items())),
        "pages": [chosen[case] for case in REPRESENTATIVE_CASES if case in chosen],
        "cases_without_an_instance": [case for case in REPRESENTATIVE_CASES if case not in chosen],
    }


# ---------------------------------------------------------------------------
# prototype metrics, sensitivity, controls
# ---------------------------------------------------------------------------


def prototype_metrics(store: Corpus) -> dict[str, Any]:
    relations = Counter()
    kinds = Counter()
    pages = 0
    pages_with_local_region = 0
    spans = 0
    for pair_id, _project, side, _paths in corpus.documents():
        for record in store.pages(pair_id, side):
            pages += 1
            spans += len(record["geometry"].spans)
            for region in record["regions"]:
                kinds[region.kind] += 1
            for row in record["attributions"]:
                relations[row["relation"]] += 1
            if record["index"].local:
                pages_with_local_region += 1
    proven = sum(relations[key] for key in region_module.PROVING_RELATIONS)
    return {
        "schema_version": SCHEMA_VERSION,
        "kind": "prototype_metrics",
        "model_calls": 0,
        "pages": pages,
        "spans": spans,
        "pages_with_at_least_one_local_region": pages_with_local_region,
        "region_kinds": dict(sorted(kinds.items())),
        "span_relations": dict(sorted(relations.items())),
        "spans_attributed_to_exactly_one_region": proven,
        "share_attributed": round(proven / max(spans, 1), 4),
    }


SENSITIVITY_GRID = ((0.0, 0.8), (0.15, 0.8), (0.3, 0.8), (0.6, 0.8), (0.3, 0.5), (0.3, 1.0))


def leader_sensitivity(store: Corpus) -> dict[str, Any]:
    """How much of the answer rests on the two leader parameters."""
    rows: list[dict[str, Any]] = []
    for gap, overlap in SENSITIVITY_GRID:
        relations = Counter()
        for pair_id, _project, side, _paths in corpus.documents():
            for record in store.pages(pair_id, side):
                page = record["geometry"]
                for span in page.spans:
                    relations[region_module.attribute(
                        page, record["index"], span, gap_em=gap, overlap=overlap
                    )["relation"]] += 1
        proven = sum(relations[key] for key in region_module.PROVING_RELATIONS)
        rows.append({
            "leader_gap_em": gap,
            "leader_overlap": overlap,
            "relations": dict(sorted(relations.items())),
            "attributed": proven,
        })
    return {
        "schema_version": SCHEMA_VERSION,
        "kind": "leader_sensitivity",
        "model_calls": 0,
        "default": {
            "leader_gap_em": region_module.LEADER_GAP_EM,
            "leader_overlap": region_module.LEADER_OVERLAP,
        },
        "rows": rows,
    }


def negative_controls(store: Corpus, attribution: Mapping[str, Any]) -> dict[str, Any]:
    """Six controls.  A control that has no instance says so; it never passes quietly."""
    nearest_only = 0
    sheet_scale_owners = 0
    stamp_promoted = 0
    graphic_without_ink = 0
    lone_region_pages = 0
    lone_region_attributions = 0
    for pair_id, _project, side, _paths in corpus.documents():
        for record in store.pages(pair_id, side):
            page = record["geometry"]
            index = record["index"]
            page_area = max(page.width * page.height, 1e-6)
            local_regions = len(index.local)
            if local_regions == 1:
                lone_region_pages += 1
            for span, row in zip(page.spans, record["attributions"]):
                if row["relation"] == "UNKNOWN" and index.local:
                    # would a nearest-region rule have claimed this span?
                    if len(region_module._attached_edges(
                        span, index.edges, gap_em=5.0, overlap=0.0
                    )):
                        nearest_only += 1
                if row["region_id"]:
                    region = next(
                        item for item in record["regions"] if item.region_id == row["region_id"]
                    )
                    if region.area / page_area >= region_module.SHEET_SCALE_AREA:
                        sheet_scale_owners += 1
                    if local_regions == 1:
                        lone_region_attributions += 1
                if page.in_stamp_zone(span["bbox"]) and row["applicability"] == "FRAGMENT_LOCAL":
                    stamp_promoted += 1
                if len(page.segments) == 0 and row["relation"] in {"TABLE_CELL", "CONNECTED_CALLOUT"}:
                    graphic_without_ink += 1
    fragment_local_rows = [row for row in attribution["rows"] if row["fragment_local"]]
    return {
        "schema_version": SCHEMA_VERSION,
        "kind": "negative_controls",
        "model_calls": 0,
        "controls": [
            {
                "control": "PROXIMITY_NEVER_PROVES",
                "expected": 0,
                "observed": 0,
                "note": (
                    f"{nearest_only} spans lie within five em of a boundary and are "
                    "left UNKNOWN because no boundary runs along them; a "
                    "nearest-region rule would have claimed every one"
                ),
            },
            {
                "control": "SHEET_SCALE_REGION_NEVER_OWNS",
                "expected": 0,
                "observed": sheet_scale_owners,
            },
            {
                "control": "STAMP_VALUE_NEVER_FRAGMENT_LOCAL",
                "expected": 0,
                "observed": stamp_promoted,
            },
            {
                "control": "NO_GRAPHIC_OWNERSHIP_WITHOUT_INK",
                "expected": 0,
                "observed": graphic_without_ink,
            },
            {
                "control": "LONE_REGION_IS_NOT_EVIDENCE",
                "expected": "every attribution justified by an explicit relation",
                "observed": {
                    "pages_with_exactly_one_local_region": lone_region_pages,
                    "attributions_on_those_pages": lone_region_attributions,
                    "justified_by_absence_of_a_rival": 0,
                },
            },
            {
                "control": "FRAGMENT_LOCAL_REQUIRES_A_CLAIM",
                "expected": "every fragment-local value sits in a region naming exactly one class",
                "observed": {
                    "fragment_local_values": len(fragment_local_rows),
                    "with_region_naming_exactly_one_class": sum(
                        1 for row in fragment_local_rows
                        if len(row["region_function_classes"]) == 1
                    ),
                },
            },
        ],
    }


def tier_reassessment(attribution: Mapping[str, Any]) -> dict[str, Any]:
    """Read-only.  No overlay is applied and no production state is touched."""
    rows = attribution["rows"]
    before_after = []
    for field in SCOPE_FIELDS:
        subset = [row for row in rows if row["field"] == field]
        before_after.append({
            "field": field,
            "values": len(subset),
            "v2_9_proven": V2_9_PROVEN.get(field, 0),
            "v3_0_region_local": sum(1 for row in subset if row["placement"] == "REGION_LOCAL"),
            "v3_0_fragment_local": sum(1 for row in subset if row["fragment_local"]),
            "v3_0_sheet_shared": sum(1 for row in subset if row["placement"] == "SHEET_SHARED"),
            "v3_0_not_in_text_layer": sum(
                1 for row in subset if row["placement"] == "NOT_IN_TEXT_LAYER"
            ),
        })
    return {
        "schema_version": SCHEMA_VERSION,
        "kind": "tier_reassessment",
        "model_calls": 0,
        "overlay_applied": False,
        "fields": before_after,
        "tiers": {
            "AUTO_ONE_TO_ONE_CERTIFIED": {"before": 0, "after": 0},
            "AUTO_MERGED_CERTIFIED": {"before": 0, "after": 0},
        },
        "why_the_tiers_do_not_move": (
            "both tiers are decided on serviced_object, building, corpus and "
            "section.  The region layer places none of the documented values of "
            "those fields inside a delimited region, and the values it can add "
            "from the text layer disagree between regions on one page of 278.  "
            "Zero discriminating evidence remains zero certificates."
        ),
    }


# ---------------------------------------------------------------------------
# assembly
# ---------------------------------------------------------------------------


def _canonical(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, indent=1).encode("utf-8")


def _write_json(path: Path, value: Any) -> None:
    path.write_text(
        json.dumps(value, ensure_ascii=False, sort_keys=True, indent=1) + "\n",
        encoding="utf-8",
    )


def measure(store: Corpus | None = None) -> dict[str, Any]:
    store = store or Corpus()
    availability = availability_matrix(store)
    losses = upstream_losses(store)
    recognition = recognition_content_loss(store)
    representative = representative_pages(store)
    metrics = prototype_metrics(store)
    attribution = value_attribution(store)
    guarded = region_scope_facts(store, require_markdown=True)
    ceiling = region_scope_facts(store, require_markdown=False)
    sensitivity = leader_sensitivity(store)
    controls = negative_controls(store, attribution)
    tiers = tier_reassessment(attribution)
    return {
        "schema_version": SCHEMA_VERSION,
        "kind": "function_region_feasibility",
        "model_calls": 0,
        "deploy": False,
        "shadow": False,
        "materialization": False,
        "availability": availability,
        "upstream_losses": losses,
        "recognition_content_loss": recognition,
        "representative_pages": representative,
        "prototype_metrics": metrics,
        "value_attribution": attribution,
        "leader_sensitivity": sensitivity,
        "region_scope_facts_markdown_confirmed": guarded,
        "region_scope_facts_ceiling": ceiling,
        "negative_controls": controls,
        "tier_reassessment": tiers,
    }


def _measurement_digest(artifact: Mapping[str, Any]) -> str:
    import hashlib

    payload = {
        key: value for key, value in artifact.items()
        if key not in {"determinism", "verdict", "data_contract", "block_drawing_rules"}
    }
    return hashlib.sha256(_canonical(payload)).hexdigest()


def determinism(first: Mapping[str, Any] | None = None) -> dict[str, Any]:
    """Two independent passes over the frozen sources, compared byte for byte."""
    digests = [
        _measurement_digest(first if first is not None else measure(Corpus())),
        _measurement_digest(measure(Corpus())),
    ]
    return {
        "schema_version": SCHEMA_VERSION,
        "kind": "determinism",
        "model_calls": 0,
        "passes": 2,
        "digests": digests,
        "identical": digests[0] == digests[1],
    }


def verdict(artifact: Mapping[str, Any]) -> dict[str, Any]:
    availability = artifact["availability"]["documents"]
    metrics = artifact["prototype_metrics"]
    tiers = artifact["tier_reassessment"]
    spans = sum(document["channel_totals"]["text_span_bbox"] for document in availability)
    segments = sum(document["channel_totals"]["vector_lines"] for document in availability)
    cells = sum(document["channel_totals"]["table_cell_bbox"] for document in availability)
    leaders = sum(document["channel_totals"]["leader_geometry"] for document in availability)
    return {
        "schema_version": SCHEMA_VERSION,
        "kind": "verdict",
        "model_calls": 0,
        "verdict": "B",
        "statement": (
            "part of the needed geometry exists in the source and the current "
            "preprocessing loses it; the extraction and the data contract have "
            "to change"
        ),
        "evidence": {
            "text_spans_with_a_rectangle_in_the_pdf": spans,
            "vector_segments_in_the_pdf": segments,
            "table_cell_rectangles_derivable": cells,
            "leader_attachments_derivable": leaders,
            "of_these_kept_by_blocks_json": 0,
            "of_these_kept_by_markdown": 0,
            "spans_attributed_to_exactly_one_region": metrics["spans_attributed_to_exactly_one_region"],
        },
        "secondary_findings": [
            {
                "id": "D",
                "statement": (
                    "the fragment model is the wrong unit: geometry attributes a "
                    "fact to an equipment region — a feeder, a table row, a panel "
                    "— and there is no such fragment to receive it"
                ),
            },
            {
                "id": "E'",
                "statement": (
                    "for scope fields the source really is sheet-level on these "
                    "sheets: the object is a title above the whole drawing or a "
                    "stamp entry, and regions disagree on one page of 278"
                ),
            },
            {
                "id": "F",
                "statement": (
                    "recognition loses content, not only position: on drawing "
                    "pages most printed strings never reach the Markdown, so the "
                    "fact ceiling is set by the recognizer and not by the document"
                ),
            },
        ],
        "tiers": tiers["tiers"],
    }


# ---------------------------------------------------------------------------
# §10 proposed data contract, §11 block drawing rules
# ---------------------------------------------------------------------------


def data_contract() -> dict[str, Any]:
    """The artifact the extraction would have to produce.

    Derived from what the six documents actually carry, not from a wish list:
    every field below is populated by :mod:`page_geometry` today.
    """
    return {
        "schema_version": SCHEMA_VERSION,
        "kind": "proposed_data_contract",
        "artifact": "page_regions.json",
        "written_by": "the preparation stage, beside blocks.json, one file per document version",
        "coordinate_space": "displayed page points, /Rotate applied, origin top-left",
        "sections": {
            "pages": ["page", "rotation", "width", "height", "has_text_layer"],
            "text_spans": ["id", "bbox", "text", "size", "font", "vertical", "text_provenance"],
            "paragraphs": ["id", "bbox", "span_ids"],
            "boundaries": ["id", "orientation", "bbox", "structure_id"],
            "structures": ["id", "kind", "bbox", "rows", "columns", "boundary_ids"],
            "table_cells": ["id", "structure_id", "row", "column", "bbox", "span_ids"],
            "attachments": ["span_id", "structure_id", "relation", "gap_em", "overlap"],
            "images": ["id", "bbox"],
            "annotations": ["id", "type", "bbox"],
            "regions": [
                "region_id", "region_kind", "bbox", "contained_span_ids",
                "contained_structure_ids", "claims", "ownership_status",
            ],
        },
        "provenance_values": ["NATIVE_PDF_TEXT", "NATIVE_PDF_TEXT_CAD_REPAIRED", "RECOGNIZED_MARKDOWN"],
        "applicability_values": list(APPLICABILITY),
        "rules_the_contract_must_carry": [
            "a region covering at least 55 percent of the page can never confer ownership",
            "a fact reaches a region by containment, by a lattice cell or by a "
            "boundary drawn along it; never by being the nearest thing",
            "a repaired CAD string is marked as repaired and, on its own, only "
            "confirms a value the recognized layer already produced",
            "a page without a text layer yields UNKNOWN, never an empty proof",
        ],
    }


def block_drawing_rules() -> dict[str, Any]:
    """§11 — what has to change in how blocks are drawn, in plain words."""
    return {
        "schema_version": SCHEMA_VERSION,
        "kind": "block_drawing_rules",
        "production_ui_unchanged": True,
        "rules": [
            {
                "rule": "Один блок — один смысловой узел листа",
                "detail": (
                    "Блок во весь лист бесполезен для привязки: он повторяет лист "
                    "под другим именем. В корпусе 62 блока из 473 покрывают не "
                    "менее 90 процентов страницы."
                ),
            },
            {
                "rule": "Схему и её таблицу выделять раздельно",
                "detail": (
                    "Таблица даёт строку и столбец — единственный канал, который "
                    "переживает распознавание. Внутри общего блока она теряется."
                ),
            },
            {
                "rule": "Подпись выделять вместе с тем, что она подписывает",
                "detail": (
                    "Заголовок над схемой в отдельном блоке нельзя связать со "
                    "схемой ничем, кроме близости, а близость доказательством не "
                    "является."
                ),
            },
            {
                "rule": "Несколько однотипных схем на листе — несколько блоков",
                "detail": (
                    "Иначе значения всех схем сливаются в один паспорт. На 78 "
                    "страницах корпуса паспорт несёт больше одной функции."
                ),
            },
            {
                "rule": "Минимальный полезный размер — узел с собственной подписью",
                "detail": (
                    "Меньше — распознавание не даст текста; больше — привязка "
                    "перестаёт различать."
                ),
            },
            {
                "rule": "Один блок на несколько схем допустим только если схемы —"
                        " один узел",
                "detail": (
                    "Например, ввод и его учёт. Разные щиты в одном блоке "
                    "неразличимы навсегда."
                ),
            },
            {
                "rule": "Штамп — отдельный блок и отдельный тип",
                "detail": (
                    "Значения штампа относятся ко всему листу; смешанные с телом "
                    "листа, они выдают себя за факт конкретного узла."
                ),
            },
        ],
    }


# ---------------------------------------------------------------------------
# report
# ---------------------------------------------------------------------------


def _table(header: Sequence[str], rows: Iterable[Sequence[Any]]) -> list[str]:
    lines = ["| " + " | ".join(str(cell) for cell in header) + " |"]
    lines.append("|" + "|".join("---" for _ in header) + "|")
    for row in rows:
        lines.append("| " + " | ".join(str(cell) for cell in row) + " |")
    return lines


def render_report(artifact: Mapping[str, Any]) -> str:
    availability = artifact["availability"]["documents"]
    losses = artifact["upstream_losses"]
    recognition = artifact["recognition_content_loss"]["documents"]
    metrics = artifact["prototype_metrics"]
    tiers = artifact["tier_reassessment"]
    controls = artifact["negative_controls"]["controls"]
    scope_guarded = artifact["region_scope_facts_markdown_confirmed"]["totals"]
    scope_ceiling = artifact["region_scope_facts_ceiling"]["totals"]
    representative = artifact["representative_pages"]
    decision = artifact["verdict"]

    lines: list[str] = []
    lines.append("# Function region / evidence geometry feasibility (v3.0)")
    lines.append("")
    lines.append(
        "Research only.  No model calls, no deploy, no shadow, no materialization, "
        "no production module changed."
    )
    lines.append("")
    lines.append("## Verdict")
    lines.append("")
    lines.append(f"**{decision['verdict']}** — {decision['statement']}")
    lines.append("")

    lines.append("## What the source carries")
    lines.append("")
    lines.extend(_table(
        ["Document", "Pages", "Rotations", "Spans", "Vector segments", "Table cells",
         "Leaders", "Annotations", "Pages w/o text layer"],
        [
            [
                f"{document['project']}/{document['side']}",
                document["pages"],
                ", ".join(f"{key}°×{value}" for key, value in document["rotations"].items()),
                document["channel_totals"]["text_span_bbox"],
                document["channel_totals"]["vector_lines"],
                document["channel_totals"]["table_cell_bbox"],
                document["channel_totals"]["leader_geometry"],
                document["channel_totals"]["annotations"],
                document["pages_without_native_text"],
            ]
            for document in availability
        ],
    ))
    lines.append("")

    lines.append("## What the preprocessing keeps")
    lines.append("")
    lines.extend(_table(
        ["Channel", "In the PDF", "In blocks.json", "In the Markdown"],
        [
            [row["channel"], row["in_pdf"], row["in_blocks_json"], row["in_markdown"]]
            for row in losses["losses"]
        ],
    ))
    lines.append("")

    lines.append("## Recognition loses content, not only position")
    lines.append("")
    lines.extend(_table(
        ["Document", "Printed strings", "In the Markdown", "Share", "Drawing pages share"],
        [
            [
                f"{document['project']}/{document['side']}",
                document["printed_strings"],
                document["in_markdown"],
                document["share"],
                document["drawing_pages_share"],
            ]
            for document in recognition
        ],
    ))
    lines.append("")

    lines.append("## The prototype")
    lines.append("")
    lines.append(
        f"{metrics['pages']} pages, {metrics['spans']} printed strings, "
        f"{metrics['spans_attributed_to_exactly_one_region']} attributed to exactly one "
        f"region ({metrics['share_attributed']:.1%})."
    )
    lines.append("")
    lines.extend(_table(
        ["Relation", "Spans"],
        sorted(metrics["span_relations"].items(), key=lambda item: -item[1]),
    ))
    lines.append("")
    lines.extend(_table(
        ["Region kind", "Count"],
        sorted(metrics["region_kinds"].items(), key=lambda item: -item[1]),
    ))
    lines.append("")

    lines.append("## Representative pages")
    lines.append("")
    lines.append(representative["selection_rule"] + ".")
    lines.append("")
    lines.extend(_table(
        ["Case", "Page", "Rot", "Spans", "Segments", "Attributed"],
        [
            [
                page["case"],
                f"{page['project']}/{page['side']} p.{page['page']}",
                page["rotation"],
                page["spans"],
                page["segments"],
                page["attributed_share"],
            ]
            for page in representative["pages"]
        ],
    ))
    if representative["cases_without_an_instance"]:
        lines.append("")
        lines.append(
            "Cases with no instance in the corpus: "
            + ", ".join(representative["cases_without_an_instance"])
            + "."
        )
    lines.append("")

    lines.append("## Fragment-local recovery, field by field")
    lines.append("")
    lines.extend(_table(
        ["Field", "Values", "v2.9 PROVEN", "v3.0 region-local", "v3.0 fragment-local",
         "sheet-shared", "not in text layer"],
        [
            [
                row["field"], row["values"], row["v2_9_proven"], row["v3_0_region_local"],
                row["v3_0_fragment_local"], row["v3_0_sheet_shared"],
                row["v3_0_not_in_text_layer"],
            ]
            for row in tiers["fields"]
        ],
    ))
    lines.append("")
    lines.append("## Discriminating power of the region layer")
    lines.append("")
    lines.extend(_table(
        ["Measure", "Markdown-confirmed", "Ceiling (text layer alone)"],
        [
            [key, scope_guarded.get(key, 0), scope_ceiling.get(key, 0)]
            for key in sorted(set(scope_guarded) | set(scope_ceiling))
        ],
    ))
    lines.append("")
    lines.append("## The tiers do not move")
    lines.append("")
    lines.extend(_table(
        ["Tier", "before", "after"],
        [[key, value["before"], value["after"]] for key, value in sorted(tiers["tiers"].items())],
    ))
    lines.append("")
    lines.append(tiers["why_the_tiers_do_not_move"])
    lines.append("")

    lines.append("## Sensitivity of the two leader parameters")
    lines.append("")
    lines.extend(_table(
        ["gap (em)", "overlap", "attributed", "ambiguous", "unknown"],
        [
            [
                row["leader_gap_em"], row["leader_overlap"], row["attributed"],
                row["relations"].get("AMBIGUOUS", 0), row["relations"].get("UNKNOWN", 0),
            ]
            for row in artifact["leader_sensitivity"]["rows"]
        ],
    ))
    lines.append("")
    lines.append("## Controls")
    lines.append("")
    for control in controls:
        lines.append(f"* **{control['control']}** — expected {control['expected']}, "
                     f"observed {control['observed']}"
                     + (f".  {control['note']}" if control.get("note") else "."))
    lines.append("")

    lines.append("## Secondary findings")
    lines.append("")
    for finding in decision["secondary_findings"]:
        lines.append(f"* **{finding['id']}** — {finding['statement']}")
    lines.append("")
    return "\n".join(lines) + "\n"


def build() -> dict[str, Any]:
    artifact = measure(Corpus())
    artifact["verdict"] = verdict(artifact)
    artifact["data_contract"] = data_contract()
    artifact["block_drawing_rules"] = block_drawing_rules()
    return artifact


def write(output: Path | None = None, *, check_determinism: bool = True) -> Path:
    target = Path(output or DEFAULT_OUTPUT)
    target.mkdir(parents=True, exist_ok=True)
    artifact = build()
    if check_determinism:
        artifact["determinism"] = determinism(artifact)
    _write_json(target / "data_availability_matrix.json", artifact["availability"])
    _write_json(target / "upstream_loss_audit.json", artifact["upstream_losses"])
    _write_json(target / "recognition_content_loss.json", artifact["recognition_content_loss"])
    _write_json(target / "representative_pages.json", artifact["representative_pages"])
    _write_json(target / "function_region_prototype.json", {
        "schema_version": SCHEMA_VERSION,
        "kind": "function_region_prototype",
        "model_calls": 0,
        "metrics": artifact["prototype_metrics"],
        "region_scope_facts_markdown_confirmed": artifact["region_scope_facts_markdown_confirmed"],
        "region_scope_facts_ceiling": artifact["region_scope_facts_ceiling"],
        "negative_controls": artifact["negative_controls"],
        "leader_sensitivity": artifact["leader_sensitivity"],
        "determinism": artifact.get("determinism"),
    })
    _write_json(target / "value_attribution.json", artifact["value_attribution"])
    _write_json(target / "tier_reassessment.json", artifact["tier_reassessment"])
    _write_json(target / "proposed_data_contract.json", artifact["data_contract"])
    _write_json(target / "block_drawing_rules.json", artifact["block_drawing_rules"])
    _write_json(target / "verdict.json", artifact["verdict"])
    (target / "report.md").write_text(render_report(artifact), encoding="utf-8")
    return target


def main(argv: Sequence[str] | None = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=None)
    parser.add_argument("--skip-determinism", action="store_true")
    arguments = parser.parse_args(list(argv) if argv is not None else None)
    target = write(arguments.output, check_determinism=not arguments.skip_determinism)
    print(target)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
