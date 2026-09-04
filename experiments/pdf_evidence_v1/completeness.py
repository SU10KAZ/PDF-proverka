"""Page completeness: how much of what the sheet prints was ever read.

This is a statement about **reading**, never about the document.  A page whose
recognized Markdown holds a tenth of its printed strings is a page we read
badly; it is not a page from which nine tenths of the content was removed.  The
distinction is the whole reason the four statuses below exist, and it is why
this module — like every other producer here — has no vocabulary for absence.

The statuses and their thresholds are deliberately the production ones from
``recognition_coverage.py`` (0.9 / 0.6, worst-page-wins), so that a page called
``PARTIAL`` here means the same thing it means there.

Two failure modes are reported separately because they need different answers:

* a page with **no native text layer** (a scan, or fonts converted to curves)
  is ``UNKNOWN``: there is no independent signal, and calling it complete or
  incomplete would both be inventions;
* a page with a native text layer and **no Markdown section at all** is
  ``INSUFFICIENT``: something printed was never read, and this corpus really
  contains such a page — ``IOS3.1/LEFT`` page 25, silently, since 26 PDF pages
  produced 25 Markdown sections.
"""
from __future__ import annotations

from typing import Any, Mapping, Sequence

from .contract import SCHEMA_VERSION
from .layer import DocumentLayer, PageLayer
from .textnorm import comparable, normalize

SUFFICIENT = "SUFFICIENT"
PARTIAL = "PARTIAL"
INSUFFICIENT = "INSUFFICIENT"
UNKNOWN = "UNKNOWN"
STATUSES = (SUFFICIENT, PARTIAL, INSUFFICIENT, UNKNOWN)

#: Worst wins.  Completeness is a claim about the worst place on the sheet, not
#: an average over it.
_SEVERITY = {INSUFFICIENT: 0, PARTIAL: 1, UNKNOWN: 2, SUFFICIENT: 3}

READ_SUFFICIENT = 0.9
READ_PARTIAL = 0.6


def _printed_strings(page: PageLayer) -> list[str]:
    """Comparable printed strings of a page, from every native channel."""
    out: list[str] = []
    for unit in page.units:
        if "not_printed_by_the_drawing" in unit.notes:
            continue
        value = comparable(unit.text)
        if value:
            out.append(value)
    return out


def page_completeness(page: PageLayer, markdown_body: str | None) -> dict[str, Any]:
    printed = _printed_strings(page)
    body = normalize(markdown_body or "")
    read = sum(1 for value in printed if value in body) if body else 0
    share = round(read / len(printed), 4) if printed else None
    if not printed:
        status, reason = UNKNOWN, "page_carries_no_native_text"
    elif markdown_body is None:
        status, reason = INSUFFICIENT, "page_has_no_markdown_section"
    elif not body:
        status, reason = INSUFFICIENT, "markdown_section_is_empty"
    elif share is not None and share >= READ_SUFFICIENT:
        status, reason = SUFFICIENT, "recognized_layer_agrees_with_the_printed_layer"
    elif share is not None and share >= READ_PARTIAL:
        status, reason = PARTIAL, "recognized_layer_agrees_in_part"
    else:
        status, reason = INSUFFICIENT, "most_printed_strings_are_not_in_the_recognized_layer"
    return {
        "page": page.page,
        "status": status,
        "reason": reason,
        "printed_strings": len(printed),
        "printed_strings_in_markdown": read,
        "read_share": share,
        "has_text_layer": page.has_text_layer,
        "has_markdown_section": markdown_body is not None,
        "markdown_chars": len(markdown_body or ""),
        "annotation_units": sum(
            1 for unit in page.units if unit.provenance == "NATIVE_PDF_ANNOTATION"
        ),
    }


def document_completeness(
    layer: DocumentLayer, bodies: Mapping[int, str]
) -> dict[str, Any]:
    rows = [page_completeness(page, bodies.get(page.page)) for page in layer.pages]
    status = min((row["status"] for row in rows), key=lambda value: _SEVERITY[value]) if rows else UNKNOWN
    printed = sum(row["printed_strings"] for row in rows)
    read = sum(row["printed_strings_in_markdown"] for row in rows)
    return {
        "document": layer.document,
        "status": status,
        "pages": len(rows),
        "pages_by_status": {
            value: sum(1 for row in rows if row["status"] == value) for value in STATUSES
        },
        "pages_without_a_markdown_section": [
            row["page"] for row in rows if not row["has_markdown_section"]
        ],
        "pages_without_a_text_layer": [
            row["page"] for row in rows if not row["has_text_layer"]
        ],
        "printed_strings": printed,
        "printed_strings_in_markdown": read,
        "read_share": round(read / printed, 4) if printed else None,
        "worst_pages": sorted(
            (row for row in rows if row["read_share"] is not None),
            key=lambda row: (row["read_share"], -row["printed_strings"]),
        )[:5],
        "page_rows": rows,
    }


def audit(documents: Sequence[tuple[DocumentLayer, Mapping[int, str]]]) -> dict[str, Any]:
    rows = [document_completeness(layer, bodies) for layer, bodies in documents]
    return {
        "schema_version": SCHEMA_VERSION,
        "kind": "page_completeness_audit",
        "model_calls": 0,
        "statement": (
            "this measures how much of the printed sheet the recognized layer "
            "contains; it says nothing about what the document does or does not "
            "contain"
        ),
        "thresholds": {"sufficient": READ_SUFFICIENT, "partial": READ_PARTIAL},
        "documents": rows,
        "totals": {
            "pages": sum(row["pages"] for row in rows),
            "pages_without_a_markdown_section": sum(
                len(row["pages_without_a_markdown_section"]) for row in rows
            ),
            "pages_without_a_text_layer": sum(
                len(row["pages_without_a_text_layer"]) for row in rows
            ),
            "printed_strings": sum(row["printed_strings"] for row in rows),
            "printed_strings_in_markdown": sum(row["printed_strings_in_markdown"] for row in rows),
            "pages_by_status": {
                value: sum(row["pages_by_status"][value] for row in rows) for value in STATUSES
            },
        },
    }


__all__ = [
    "INSUFFICIENT",
    "PARTIAL",
    "READ_PARTIAL",
    "READ_SUFFICIENT",
    "STATUSES",
    "SUFFICIENT",
    "UNKNOWN",
    "audit",
    "document_completeness",
    "page_completeness",
]
