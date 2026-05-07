"""
context_models.py
------------------
Data models for finding context packages.

NOT connected to production pipeline. Read-only from project artifacts.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional


@dataclass
class BlockSnippet:
    """A short excerpt from a document block, used for context."""
    block_id: str
    page: int
    text: str                  # first ~400 chars of block text
    block_type: str = "text"   # "text" or "image"
    sheet: str = ""
    label: str = ""            # block label / section heading
    coords_norm: Optional[list] = None  # [x0, y0, x1, y1] normalized 0-1
    distance_rank: int = 0     # lower = closer to evidence block


@dataclass
class TableContextRow:
    """A single row from a table context (header, previous, next)."""
    row_type: str    # "header" | "prev" | "target" | "next" | "units"
    cells: list[str] = field(default_factory=list)
    raw_text: str = ""


@dataclass
class CrossReference:
    """A cross-reference found in block text pointing to another location."""
    ref_text: str          # e.g. "см. лист 3" / "узел 1-1"
    target_sheet: str = ""
    target_leaf: str = ""  # leaf/node reference
    resolved_block_id: str = ""
    resolved_text: str = ""  # first ~200 chars of target block


@dataclass
class RelatedFinding:
    """A related finding (same sheet, category, or potential duplicate)."""
    finding_id: str
    title: str
    category: str
    sheet: str
    page: Optional[int]
    similarity_type: str  # "same_category" | "same_sheet" | "possible_duplicate"
    similarity_score: float = 0.0


@dataclass
class FindingContextPackage:
    """
    Full context package assembled for one finding before LLM review.

    All fields are read-only snapshots from project artifacts.
    This package NEVER modifies any source data.
    """
    finding_id: str

    # Primary evidence blocks (already in finding.evidence)
    primary_blocks: list[BlockSnippet] = field(default_factory=list)

    # Neighboring blocks (±2..5 from evidence blocks, same/adjacent pages)
    neighbor_blocks: list[BlockSnippet] = field(default_factory=list)

    # Same-page blocks not in evidence
    same_page_blocks: list[BlockSnippet] = field(default_factory=list)

    # Table context rows (if finding relates to a table)
    table_context: list[TableContextRow] = field(default_factory=list)

    # General-notes / legend blocks from same sheet or document
    common_notes: list[BlockSnippet] = field(default_factory=list)

    # Cross-references found in evidence blocks
    cross_references: list[CrossReference] = field(default_factory=list)

    # Related findings from same project
    related_findings: list[RelatedFinding] = field(default_factory=list)

    # Spec / schedule context (blocks labeled as spec/schedule/statement)
    spec_context: list[BlockSnippet] = field(default_factory=list)

    # Human-readable summary of what was collected
    collected_context_summary: str = ""

    def to_dict(self) -> dict:
        """Serialize to plain dict for JSON output."""
        return {
            "finding_id": self.finding_id,
            "primary_blocks_count": len(self.primary_blocks),
            "neighbor_blocks_count": len(self.neighbor_blocks),
            "same_page_blocks_count": len(self.same_page_blocks),
            "table_context_rows": len(self.table_context),
            "common_notes_count": len(self.common_notes),
            "cross_references_count": len(self.cross_references),
            "related_findings_count": len(self.related_findings),
            "spec_context_count": len(self.spec_context),
            "collected_context_summary": self.collected_context_summary,
            # Serialized content
            "neighbor_blocks": [
                {
                    "block_id": b.block_id, "page": b.page,
                    "text": b.text[:400], "label": b.label,
                    "distance_rank": b.distance_rank,
                }
                for b in self.neighbor_blocks[:8]
            ],
            "common_notes": [
                {"block_id": b.block_id, "page": b.page, "text": b.text[:600], "label": b.label}
                for b in self.common_notes[:4]
            ],
            "table_context": [
                {"row_type": r.row_type, "cells": r.cells, "raw_text": r.raw_text[:200]}
                for r in self.table_context
            ],
            "cross_references": [
                {
                    "ref_text": c.ref_text, "target_sheet": c.target_sheet,
                    "resolved_text": c.resolved_text[:200],
                }
                for c in self.cross_references[:4]
            ],
            "related_findings": [
                {
                    "finding_id": r.finding_id, "title": r.title[:80],
                    "category": r.category, "similarity_type": r.similarity_type,
                }
                for r in self.related_findings[:4]
            ],
            "spec_context": [
                {"block_id": b.block_id, "page": b.page, "text": b.text[:300], "label": b.label}
                for b in self.spec_context[:3]
            ],
        }

    def to_llm_text(self, max_chars: int = 3000) -> str:
        """
        Render context as compact text for inclusion in LLM prompt.
        Prioritizes: common_notes > neighbor_blocks > cross_refs > table > spec.
        """
        parts = []
        budget = max_chars

        if self.common_notes:
            parts.append("### Общие указания / примечания (General Notes):")
            for b in self.common_notes[:3]:
                snippet = b.text[:500]
                line = f"[Лист {b.page}] {snippet}"
                parts.append(line)
                budget -= len(line)
                if budget < 200:
                    break
            parts.append("")

        if self.neighbor_blocks and budget > 400:
            parts.append("### Соседние блоки (Neighbor Blocks):")
            for b in self.neighbor_blocks[:5]:
                snippet = b.text[:300]
                line = f"[Блок {b.block_id[:8]}, стр.{b.page}] {snippet}"
                parts.append(line)
                budget -= len(line)
                if budget < 200:
                    break
            parts.append("")

        if self.cross_references and budget > 300:
            parts.append("### Перекрёстные ссылки (Cross-references):")
            for cr in self.cross_references[:3]:
                line = f"  {cr.ref_text}"
                if cr.resolved_text:
                    line += f" → {cr.resolved_text[:150]}"
                parts.append(line)
                budget -= len(line)
            parts.append("")

        if self.table_context and budget > 200:
            parts.append("### Контекст таблицы (Table Context):")
            for row in self.table_context[:4]:
                line = f"  [{row.row_type}] {' | '.join(str(c) for c in row.cells[:8])}"
                parts.append(line)
                budget -= len(line)
            parts.append("")

        if self.spec_context and budget > 200:
            parts.append("### Спецификация / ведомость (Spec Context):")
            for b in self.spec_context[:2]:
                line = f"[{b.label}] {b.text[:200]}"
                parts.append(line)
            parts.append("")

        if self.related_findings and budget > 100:
            parts.append("### Похожие замечания (Related Findings):")
            for rf in self.related_findings[:3]:
                parts.append(f"  [{rf.finding_id}] {rf.title[:60]} ({rf.similarity_type})")
            parts.append("")

        return "\n".join(parts).strip()

    @property
    def has_useful_context(self) -> bool:
        """True if at least one meaningful context element was collected."""
        return bool(
            self.common_notes
            or self.neighbor_blocks
            or self.cross_references
            or self.table_context
            or self.spec_context
        )

    @property
    def total_context_blocks(self) -> int:
        return (
            len(self.primary_blocks)
            + len(self.neighbor_blocks)
            + len(self.same_page_blocks)
            + len(self.common_notes)
            + len(self.spec_context)
        )
