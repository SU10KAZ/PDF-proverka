"""Shared parser for OCR Markdown block headers and sections.

Поддерживает ДВА формата исходного Markdown:

- старый Chandra-формат: ``## СТРАНИЦА N`` + ``### BLOCK [IMAGE|TEXT]: <id>``
  (регэкспы этого модуля, поведение не менялось);
- новый формат портала vibe (`*_results.md`, 2026-07): ``## Page N`` +
  ``### BLOCK #N [TEXT|IMAGE]: blk_<hex>`` — разбирается нативно через единый
  парсер :mod:`backend.app.services.common.results_md`.

Ветвление — по :func:`is_results_md_text` В НАЧАЛЕ каждой публичной функции;
старый код-путь не тронут. Маппинг полей нового формата в старый контракт:

- ``BLOCK #N`` → ``BlockHeader.ordinal`` / ``BlockSection.ordinal``
  (в старом формате поле = None);
- ``## Page N`` (страница PDF, 1-based) → ``BlockSection.page``;
- ``Sheet`` из мета-штампа блока → ``BlockSection.sheet`` (подпись,
  НЕ ключ: лист может быть пуст/неуникален, ключ листа = страница PDF);
- тело без мета-цитат ``> **…**`` → ``BlockSection.body_clean``; при этом
  ``text``/``body`` остаются сырыми срезами исходного текста
  (инвариант ``text == markdown_text[header_start:body_end]`` сохраняется —
  на нём построен round-trip в strip_gemma_enrichment_sections).
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Iterator, Optional

from backend.app.services.common.results_md import (
    BLOCK_HEADER_RE as RESULTS_BLOCK_HEADER_RE,
    PAGE_HEADER_RE as RESULTS_PAGE_HEADER_RE,
    is_results_md_text,
    parse_results_md,
)


BLOCK_HEADER_RE = re.compile(
    r"^### BLOCK \[(?P<type>IMAGE|TEXT)\]:\s*(?P<id>\S+)\s*$",
    re.MULTILINE,
)
PAGE_HEADER_RE = re.compile(r"^## СТРАНИЦА\s+\d+\s*$", re.MULTILINE)
ENRICHED_LINE_RE = re.compile(r"^\*\*\[ENRICHED [^\]]+\]\*\*\s*$", re.MULTILINE)
CHANDRA_TYPE_RE = re.compile(
    r"^\*\*\[(?:ИЗОБРАЖЕНИЕ|ТЕКСТ)\]\*\*\s*\|\s*Тип:\s*(?P<value>[^\r\n]+?)\s*$",
    re.MULTILINE | re.IGNORECASE,
)
CHANDRA_FIELD_RE = re.compile(
    r"^\*\*(?P<label>Краткое описание|Описание):\*\*\s*(?P<value>.*?)"
    r"(?=^\*\*[^\r\n]+:\*\*|^\*\*\[ENRICHED |\Z)",
    re.MULTILINE | re.DOTALL | re.IGNORECASE,
)


@dataclass(frozen=True)
class BlockHeader:
    type: str
    id: str
    start: int
    end: int
    line: str
    # Новый формат: сквозной номер из "### BLOCK #N …"; в старом формате None.
    ordinal: Optional[int] = None


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
    # Поля нового формата (*_results.md); в старом формате все = None.
    ordinal: Optional[int] = None      # сквозной номер из "### BLOCK #N …"
    page: Optional[int] = None         # страница PDF (1-based) из "## Page N"
    sheet: Optional[str] = None        # Sheet из мета-штампа (подпись, не ключ)
    body_clean: Optional[str] = None   # тело без мета-цитат "> **…**"


@dataclass(frozen=True)
class ChandraBlockDescription:
    """Исходное смысловое описание одного блока из Markdown Chandra.

    Поля ``Текст на чертеже`` и ``Сущности`` намеренно отсутствуют в контракте:
    они не должны участвовать ни в выборе профиля, ни в построении графа.
    """

    block_id: str
    block_type: Optional[str]
    short_description: Optional[str]
    description: Optional[str]

    @property
    def classification_text(self) -> str:
        """Только разрешённые поля для семантической классификации блока."""
        parts = []
        if self.block_type:
            parts.append(f"Тип блока: {self.block_type}")
        if self.short_description:
            parts.append(f"Краткое описание: {self.short_description}")
        if self.description:
            parts.append(f"Описание: {self.description}")
        return "\n".join(parts)


def parse_block_header(line: str) -> Optional[BlockHeader]:
    """Parse one Markdown block header line.

    Понимает оба формата заголовка:
    старый ``### BLOCK [IMAGE|TEXT]: <id>`` (ordinal=None) и новый
    ``### BLOCK #N [TEXT|IMAGE]: blk_<hex>`` (ordinal=N, id=blk_<hex>).
    """
    clean_line = line.rstrip("\r\n")
    match = BLOCK_HEADER_RE.fullmatch(clean_line)
    if match:
        return BlockHeader(
            type=match.group("type"),
            id=match.group("id"),
            start=0,
            end=len(clean_line),
            line=clean_line,
        )
    match = RESULTS_BLOCK_HEADER_RE.fullmatch(clean_line)
    if match:
        return BlockHeader(
            type=match.group("type"),
            id=match.group("block_id"),
            start=0,
            end=len(clean_line),
            line=clean_line,
            ordinal=int(match.group("ordinal")),
        )
    return None


def iter_block_headers(markdown_text: str) -> Iterator[BlockHeader]:
    """Yield all OCR block headers in document order (оба формата MD)."""
    if is_results_md_text(markdown_text):
        for match in RESULTS_BLOCK_HEADER_RE.finditer(markdown_text):
            yield BlockHeader(
                type=match.group("type"),
                id=match.group("block_id"),
                start=match.start(),
                end=match.end(),
                line=match.group(0),
                ordinal=int(match.group("ordinal")),
            )
        return
    for match in BLOCK_HEADER_RE.finditer(markdown_text):
        yield BlockHeader(
            type=match.group("type"),
            id=match.group("id"),
            start=match.start(),
            end=match.end(),
            line=match.group(0),
        )


def extract_block_sections(markdown_text: str) -> list[BlockSection]:
    """Return block sections bounded by the next block header or page header.

    Для нового формата (*_results.md) дополнительно заполняются поля
    ``ordinal``/``page``/``sheet``/``body_clean``; ``text``/``body`` в обоих
    форматах — сырые срезы исходного текста (см. докстринг модуля).
    """
    if is_results_md_text(markdown_text):
        return _extract_results_block_sections(markdown_text)

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


def _extract_results_block_sections(markdown_text: str) -> list[BlockSection]:
    """Секции блоков нового формата (*_results.md) с сохранением офсетов.

    Границы те же, что в старом пути: следующий заголовок блока или страницы.
    Семантика полей — через parse_results_md (сопоставление по индексу:
    оба прохода используют один и тот же RESULTS_BLOCK_HEADER_RE, поэтому
    порядок и количество совпадают 1:1).
    """
    headers = list(RESULTS_BLOCK_HEADER_RE.finditer(markdown_text))
    if not headers:
        return []

    parsed_blocks = parse_results_md(markdown_text).blocks
    page_starts = [m.start() for m in RESULTS_PAGE_HEADER_RE.finditer(markdown_text)]
    sections: list[BlockSection] = []
    for idx, match in enumerate(headers):
        next_block_pos = headers[idx + 1].start() if idx + 1 < len(headers) else len(markdown_text)
        next_page_pos = next((pos for pos in page_starts if pos > match.start()), len(markdown_text))
        body_end = min(next_block_pos, next_page_pos)
        block = parsed_blocks[idx] if idx < len(parsed_blocks) else None
        sections.append(BlockSection(
            type=match.group("type"),
            id=match.group("block_id"),
            header_start=match.start(),
            header_end=match.end(),
            body_start=match.end(),
            body_end=body_end,
            header=match.group(0),
            text=markdown_text[match.start():body_end],
            body=markdown_text[match.end():body_end],
            ordinal=int(match.group("ordinal")),
            page=(block.page or None) if block else None,
            sheet=block.sheet if block else None,
            body_clean=block.body if block else None,
        ))
    return sections


def strip_enrichment_in_block(block_text: str) -> str:
    """Remove the generated Gemma enrichment tail from one block section."""
    match = ENRICHED_LINE_RE.search(block_text)
    if not match:
        return block_text
    return block_text[:match.start()].rstrip() + "\n\n"


def _clean_field(value: Optional[str]) -> Optional[str]:
    clean = re.sub(r"\s+", " ", str(value or "")).strip()
    return clean or None


def extract_chandra_block_description(
    markdown_text: str, block_id: str
) -> Optional[ChandraBlockDescription]:
    """Вернуть исходное описание Chandra для точного ``block_id``.

    Разрешены только ``Тип``, ``Краткое описание`` и ``Описание``. Секция
    предварительно обрезается перед ``[ENRICHED ...]``; извлечённый Chandra
    ``Текст на чертеже`` и список ``Сущности`` игнорируются по контракту.

    Новый формат (*_results.md) мапится в те же поля:
    ``Type`` из строки ``**[IMAGE]**`` → block_type, ``**Summary:**`` →
    short_description, ``**Description:**`` → description; ``Entities`` и
    ``Verification`` игнорируются (аналог «Текст на чертеже»/«Сущности»).
    """
    if is_results_md_text(markdown_text):
        return _extract_results_block_description(markdown_text, block_id)

    image_sections = [section for section in extract_block_sections(markdown_text)
                      if section.type == "IMAGE"]
    target = next((section for section in image_sections if section.id == block_id), None)
    if target is None:
        folded = [section for section in image_sections
                  if section.id.casefold() == str(block_id).casefold()]
        if len(folded) == 1:
            target = folded[0]
    if target is None:
        return None

    source = strip_enrichment_in_block(target.body)
    type_match = CHANDRA_TYPE_RE.search(source)
    fields: dict[str, str] = {}
    for match in CHANDRA_FIELD_RE.finditer(source):
        fields[match.group("label").casefold()] = match.group("value")
    result = ChandraBlockDescription(
        block_id=target.id,
        block_type=_clean_field(type_match.group("value") if type_match else None),
        short_description=_clean_field(fields.get("краткое описание")),
        description=_clean_field(fields.get("описание")),
    )
    return result if result.classification_text else None


def _extract_results_block_description(
    markdown_text: str, block_id: str
) -> Optional[ChandraBlockDescription]:
    """Описание IMAGE-блока нового формата в контракте ChandraBlockDescription.

    Маппинг: ``Type`` (строка ``**[IMAGE]**``) → block_type, ``Summary`` →
    short_description, ``Description`` → description. ``Entities``,
    ``Verification`` и хвост ``[ENRICHED …]`` в контракт не попадают.
    """
    image_blocks = [b for b in parse_results_md(markdown_text).blocks if b.is_image]
    target = next((b for b in image_blocks if b.block_id == block_id), None)
    if target is None:
        folded = [b for b in image_blocks
                  if b.block_id.casefold() == str(block_id).casefold()]
        if len(folded) == 1:
            target = folded[0]
    if target is None:
        return None

    def _cut_enriched(value: Optional[str]) -> Optional[str]:
        # Паритет со старым путём: хвост [ENRICHED …] не должен попадать в
        # контракт, даже если он прилип к последней секции описания.
        if not value:
            return value
        match = ENRICHED_LINE_RE.search(value)
        return value[:match.start()] if match else value

    result = ChandraBlockDescription(
        block_id=target.block_id,
        block_type=_clean_field(target.image_meta.get("type")),
        short_description=_clean_field(_cut_enriched(target.image_sections.get("summary"))),
        description=_clean_field(_cut_enriched(target.image_sections.get("description"))),
    )
    return result if result.classification_text else None


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
