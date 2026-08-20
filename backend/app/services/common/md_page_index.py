"""Split recognized Markdown into page bodies for the general audit pipeline."""
from __future__ import annotations

import re
from dataclasses import dataclass

from . import results_md


_LEGACY_PAGE_RE = re.compile(
    r"^\s{0,3}#{1,6}\s+(?:СТРАНИЦА|Страница|PAGE|Page)\s*[:№#]?\s*(\d+)\s*$",
    re.MULTILINE,
)
_IMAGE_MARKER_RE = re.compile(r"block_id:\s*\S+")


@dataclass(frozen=True)
class MarkdownPage:
    number: int
    body: str
    section_class: str


def _section_class(body: str, has_image: bool) -> str:
    if has_image:
        return "drawing"
    return "other" if len((body or "").strip()) < 300 else "pz"


def index_markdown_pages(markdown: str) -> list[MarkdownPage]:
    """Return stable page slices without any comparison-specific semantics."""
    markdown = markdown or ""
    if results_md.is_results_md_text(markdown):
        document = results_md.parse_results_md(markdown)
        lines = markdown.splitlines()
        pages: list[MarkdownPage] = []
        for index, page in enumerate(document.pages):
            start = page.start_line
            end = document.pages[index + 1].start_line - 1 if index + 1 < len(document.pages) else len(lines)
            body = "\n".join(lines[start:end])
            has_image = any(block.is_image for block in page.blocks) or bool(_IMAGE_MARKER_RE.search(body))
            pages.append(MarkdownPage(page.number, body, _section_class(body, has_image)))
        if pages:
            return pages

    matches = list(_LEGACY_PAGE_RE.finditer(markdown))
    if not matches:
        return [MarkdownPage(1, markdown, _section_class(markdown, bool(_IMAGE_MARKER_RE.search(markdown))))]
    pages = []
    for index, match in enumerate(matches):
        end = matches[index + 1].start() if index + 1 < len(matches) else len(markdown)
        body = markdown[match.end():end]
        pages.append(MarkdownPage(
            int(match.group(1)), body, _section_class(body, bool(_IMAGE_MARKER_RE.search(body))),
        ))
    return pages


__all__ = ["MarkdownPage", "index_markdown_pages"]
