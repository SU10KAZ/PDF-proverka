"""Canonical page identity shared by TEXT and GRAPHIC comparison artifacts."""
from __future__ import annotations

from typing import Any


PAGE_CONVENTION_VERSION = "pdf-page-1based-to-index-0based-v2"


class PageIdentityValidationError(ValueError):
    """A page value cannot be represented in the canonical 0-based space."""


def text_pdf_page_1based_to_canonical_index(pdf_page_1based: Any) -> int:
    """Convert a production TEXT PDF page number to a canonical 0-based index."""
    if (
        not isinstance(pdf_page_1based, int)
        or isinstance(pdf_page_1based, bool)
        or pdf_page_1based < 1
    ):
        raise PageIdentityValidationError(
            "text_pdf_page_1based: positive integer required"
        )
    return pdf_page_1based - 1


def graphic_page_index_0based_to_canonical_index(page_index_0based: Any) -> int:
    """Validate and return a production GRAPHIC 0-based page index."""
    if (
        not isinstance(page_index_0based, int)
        or isinstance(page_index_0based, bool)
        or page_index_0based < 0
    ):
        raise PageIdentityValidationError(
            "graphic_page_index_0based: non-negative integer required"
        )
    return page_index_0based


__all__ = [
    "PAGE_CONVENTION_VERSION",
    "PageIdentityValidationError",
    "graphic_page_index_0based_to_canonical_index",
    "text_pdf_page_1based_to_canonical_index",
]
