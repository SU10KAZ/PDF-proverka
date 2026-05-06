"""Shared parser for OCR Markdown block headers and sections."""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Iterator, Optional


BLOCK_HEADER_RE = re.compile(
    r"^### BLOCK \[(?P<type>IMAGE|TEXT)\]:\s*(?P<id>\S+)\s*$",
    re.MULTILINE,
)
PAGE_HEADER_RE = re.compile(r"^## СТРАНИЦА\s+\d+\s*$", re.MULTILINE)
ENRICHED_LINE_RE = re.compile(r"^\*\*\[ENRICHED [^\]]+\]\*\*\s*$", re.MULTILINE)


@dataclass(frozen=True)
class BlockHeader:
    type: str
    id: str
    start: int
    end: int
    line: str


@dataclass(frozen=True)
class BlockSection:
    type: str
    id: str
    header_start: int
    header_end: int
    body_start: int
    body_end: int
    header: str
    text: str
    body: str


def parse_block_header(line: str) -> Optional[BlockHeader]:
    """Parse one Markdown block header line."""
    clean_line = line.rstrip("\r\n")
    match = BLOCK_HEADER_RE.fullmatch(clean_line)
    if not match:
        return None
    return BlockHeader(
        type=match.group("type"),
        id=match.group("id"),
        start=0,
        end=len(clean_line),
        line=clean_line,
    )


def iter_block_headers(markdown_text: str) -> Iterator[BlockHeader]:
    """Yield all OCR block headers in document order."""
    for match in BLOCK_HEADER_RE.finditer(markdown_text):
        yield BlockHeader(
            type=match.group("type"),
            id=match.group("id"),
            start=match.start(),
            end=match.end(),
            line=match.group(0),
        )


def extract_block_sections(markdown_text: str) -> list[BlockSection]:
    """Return block sections bounded by the next block header or page header."""
    headers = list(iter_block_headers(markdown_text))
    if not headers:
        return []

    page_starts = [m.start() for m in PAGE_HEADER_RE.finditer(markdown_text)]
    sections: list[BlockSection] = []
    for idx, header in enumerate(headers):
        next_block_pos = headers[idx + 1].start if idx + 1 < len(headers) else len(markdown_text)
        next_page_pos = next((pos for pos in page_starts if pos > header.start), len(markdown_text))
        body_end = min(next_block_pos, next_page_pos)
        sections.append(BlockSection(
            type=header.type,
            id=header.id,
            header_start=header.start,
            header_end=header.end,
            body_start=header.end,
            body_end=body_end,
            header=header.line,
            text=markdown_text[header.start:body_end],
            body=markdown_text[header.end:body_end],
        ))
    return sections


def strip_enrichment_in_block(block_text: str) -> str:
    """Remove the generated Gemma enrichment tail from one block section."""
    match = ENRICHED_LINE_RE.search(block_text)
    if not match:
        return block_text
    return block_text[:match.start()].rstrip() + "\n\n"


def strip_gemma_enrichment_sections(markdown_text: str) -> str:
    """Remove generated Gemma enrichment sections from all parsed blocks."""
    sections = extract_block_sections(markdown_text)
    if not sections:
        return markdown_text

    parts: list[str] = []
    cursor = 0
    for section in sections:
        parts.append(markdown_text[cursor:section.header_start])
        parts.append(strip_enrichment_in_block(section.text))
        cursor = section.body_end
    parts.append(markdown_text[cursor:])
    return "".join(parts)


# Backward-compatible alias.
strip_qwen_enrichment_sections = strip_gemma_enrichment_sections
