"""
context_collector.py
---------------------
Main entry point for assembling FindingContextPackage for each finding.

Reads from:
  - document_graph.json (block text, page layout)
  - 02_blocks_analysis.json (enriched block labels, optional)
  - 03_findings.json (all findings, for related-finding lookup)

Does NOT write to production artifacts.
Writes only to output_dir when explicitly asked.

Usage:
    collector = ContextCollector(project_dir)
    packages = collector.collect_all(findings)

    # Or for benchmark:
    collector = ContextCollector.from_project_dir(project_dir)
    packages = collector.collect_all(findings)
    collector.save_artifact(packages, output_dir)
"""
from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from .common_notes import get_common_notes
from .context_models import BlockSnippet, FindingContextPackage
from .cross_references import get_cross_references
from .neighbor_blocks import get_neighbor_blocks
from .related_findings import get_related_findings
from .table_context import get_table_context


@dataclass
class ContextCollectionStats:
    """Statistics over a batch collection run."""
    total_findings: int = 0
    findings_with_neighbors: int = 0
    findings_with_common_notes: int = 0
    findings_with_cross_refs: int = 0
    findings_with_table_context: int = 0
    findings_with_related_findings: int = 0
    findings_with_spec_context: int = 0
    findings_with_any_context: int = 0
    avg_context_blocks: float = 0.0
    collection_ms: int = 0

    def to_dict(self) -> dict:
        return {
            "total_findings": self.total_findings,
            "findings_with_neighbors": self.findings_with_neighbors,
            "findings_with_common_notes": self.findings_with_common_notes,
            "findings_with_cross_refs": self.findings_with_cross_refs,
            "findings_with_table_context": self.findings_with_table_context,
            "findings_with_related_findings": self.findings_with_related_findings,
            "findings_with_spec_context": self.findings_with_spec_context,
            "findings_with_any_context": self.findings_with_any_context,
            "avg_context_blocks": round(self.avg_context_blocks, 2),
            "collection_ms": self.collection_ms,
        }


class ContextCollector:
    """
    Assembles FindingContextPackage for a list of findings.

    All operations are read-only relative to production artifacts.
    """

    def __init__(
        self,
        document_graph: dict,
        blocks_analysis: Optional[dict] = None,
        all_findings: Optional[list[dict]] = None,
    ):
        self._graph = document_graph
        self._blocks = blocks_analysis
        self._all_findings = all_findings or []
        # Pre-build spec block index (blocks labelled as spec/schedule/statement)
        self._spec_blocks = self._index_spec_blocks()

    # ── Factory ───────────────────────────────────────────────────────────────

    @classmethod
    def from_project_dir(cls, project_dir: Path) -> "ContextCollector":
        """
        Load necessary artifacts from a project directory and create collector.

        project_dir: path to project root (parent of _output/)
        """
        output_dir = project_dir / "_output"

        graph_path = output_dir / "document_graph.json"
        if not graph_path.exists():
            document_graph: dict = {"pages": []}
        else:
            document_graph = json.loads(graph_path.read_text(encoding="utf-8"))

        blocks_analysis: Optional[dict] = None
        blocks_path = output_dir / "02_blocks_analysis.json"
        if blocks_path.exists():
            try:
                blocks_analysis = json.loads(blocks_path.read_text(encoding="utf-8"))
            except (json.JSONDecodeError, OSError):
                blocks_analysis = None

        all_findings: list[dict] = []
        findings_path = output_dir / "03_findings.json"
        if findings_path.exists():
            try:
                raw = json.loads(findings_path.read_text(encoding="utf-8"))
                all_findings = raw.get("findings", raw.get("items", []))
            except (json.JSONDecodeError, OSError):
                pass

        return cls(
            document_graph=document_graph,
            blocks_analysis=blocks_analysis,
            all_findings=all_findings,
        )

    @classmethod
    def empty(cls) -> "ContextCollector":
        """Return a no-op collector for testing without project data."""
        return cls(document_graph={"pages": []})

    # ── Spec block index ──────────────────────────────────────────────────────

    _SPEC_LABEL_RE = None

    @classmethod
    def _build_spec_re(cls):
        import re
        if cls._SPEC_LABEL_RE is None:
            cls._SPEC_LABEL_RE = re.compile(
                r"спецификац|ведомост|schedule|statement|statement\s+of|перечень\s+элемент",
                re.IGNORECASE,
            )
        return cls._SPEC_LABEL_RE

    def _index_spec_blocks(self) -> list[BlockSnippet]:
        """Find specification/schedule blocks in document."""
        spec_re = self._build_spec_re()
        results = []
        pages = self._graph.get("pages", [])
        for pg in pages:
            pnum = pg.get("page") or pg.get("page_index") or 0
            try:
                pnum = int(pnum)
            except (TypeError, ValueError):
                pnum = 0
            for block in (pg.get("text_blocks") or []) + (pg.get("image_blocks") or []):
                bid = str(block.get("id") or block.get("block_id") or "")
                label = str(block.get("label") or block.get("ocr_label") or "")
                text = (block.get("text") or "").strip()
                if spec_re.search(label) or spec_re.search(text[:100]):
                    if text and len(text) >= 20:
                        results.append(BlockSnippet(
                            block_id=bid,
                            page=pnum,
                            text=text,
                            label=label,
                            block_type="text",
                        ))
        return results

    # ── Evidence block look-up ────────────────────────────────────────────────

    def _get_primary_blocks(self, finding: dict) -> list[BlockSnippet]:
        """Return evidence blocks already referenced by the finding."""
        evidence = finding.get("evidence") or []
        block_map: dict[str, tuple[str, int, str]] = {}
        for pg in self._graph.get("pages", []):
            pnum = pg.get("page") or pg.get("page_index") or 0
            try:
                pnum = int(pnum)
            except (TypeError, ValueError):
                pnum = 0
            for block in (pg.get("text_blocks") or []) + (pg.get("image_blocks") or []):
                bid = str(block.get("id") or block.get("block_id") or "")
                text = (block.get("text") or "").strip()
                label = str(block.get("label") or "")
                if bid and text:
                    block_map[bid] = (text, pnum, label)

        result = []
        for e in evidence:
            bid = str(e.get("block_id") or e.get("id") or "")
            if bid and bid in block_map:
                text, pnum, label = block_map[bid]
                result.append(BlockSnippet(
                    block_id=bid,
                    page=pnum,
                    text=text,
                    block_type=e.get("type", "text"),
                    label=label,
                ))
        return result

    def _get_spec_context(self, finding: dict) -> list[BlockSnippet]:
        """Return spec blocks relevant to this finding (same sheet preferred)."""
        if not self._spec_blocks:
            return []
        sheet = str(finding.get("sheet") or "").lower()
        same: list[BlockSnippet] = []
        other: list[BlockSnippet] = []
        for b in self._spec_blocks:
            if sheet and sheet in b.sheet.lower():
                same.append(b)
            else:
                other.append(b)
        return (same + other)[:3]

    # ── Main collection ───────────────────────────────────────────────────────

    def collect_one(self, finding: dict) -> FindingContextPackage:
        """
        Collect full context package for a single finding.
        Safe to call with an empty graph — returns empty package.
        """
        fid = str(finding.get("id") or finding.get("finding_id") or "")
        pkg = FindingContextPackage(finding_id=fid)

        pkg.primary_blocks = self._get_primary_blocks(finding)

        pkg.neighbor_blocks = get_neighbor_blocks(
            finding, self._graph, max_neighbors=6, index_window=4,
        )

        pkg.common_notes = get_common_notes(
            finding, self._graph, max_blocks=3,
        )

        pkg.table_context = get_table_context(
            finding, self._graph, self._blocks,
        )

        pkg.cross_references = get_cross_references(
            finding, self._graph, max_refs=4,
        )

        if self._all_findings:
            pkg.related_findings = get_related_findings(
                finding, self._all_findings, max_related=4,
            )

        pkg.spec_context = self._get_spec_context(finding)

        # Build summary string
        summary_parts = []
        if pkg.common_notes:
            summary_parts.append(f"общие_указания={len(pkg.common_notes)}")
        if pkg.neighbor_blocks:
            summary_parts.append(f"соседние_блоки={len(pkg.neighbor_blocks)}")
        if pkg.cross_references:
            summary_parts.append(f"ссылки={len(pkg.cross_references)}")
        if pkg.table_context:
            summary_parts.append(f"таблица={len(pkg.table_context)}_строк")
        if pkg.spec_context:
            summary_parts.append(f"спецификация={len(pkg.spec_context)}")
        if pkg.related_findings:
            summary_parts.append(f"похожие_замечания={len(pkg.related_findings)}")
        pkg.collected_context_summary = "; ".join(summary_parts) if summary_parts else "no_context"

        return pkg

    def collect_all(
        self,
        findings: list[dict],
    ) -> tuple[list[FindingContextPackage], ContextCollectionStats]:
        """
        Collect context packages for all findings in a list.

        Returns:
            (packages_list, stats)
        """
        t0 = time.monotonic()
        packages: list[FindingContextPackage] = []
        stats = ContextCollectionStats(total_findings=len(findings))
        total_ctx_blocks = 0

        for finding in findings:
            pkg = self.collect_one(finding)
            packages.append(pkg)

            if pkg.neighbor_blocks:
                stats.findings_with_neighbors += 1
            if pkg.common_notes:
                stats.findings_with_common_notes += 1
            if pkg.cross_references:
                stats.findings_with_cross_refs += 1
            if pkg.table_context:
                stats.findings_with_table_context += 1
            if pkg.related_findings:
                stats.findings_with_related_findings += 1
            if pkg.spec_context:
                stats.findings_with_spec_context += 1
            if pkg.has_useful_context:
                stats.findings_with_any_context += 1
            total_ctx_blocks += pkg.total_context_blocks

        if findings:
            stats.avg_context_blocks = total_ctx_blocks / len(findings)
        stats.collection_ms = int((time.monotonic() - t0) * 1000)
        return packages, stats

    @staticmethod
    def save_artifact(
        packages: list[FindingContextPackage],
        stats: ContextCollectionStats,
        output_dir: Path,
        filename: str = "critic_v2_context_packages.json",
    ) -> Path:
        """
        Save context packages as a JSON artifact to output_dir.

        output_dir must be a scratch/benchmark directory, NOT a production _output/ path.
        Returns the written file path.
        """
        output_dir.mkdir(parents=True, exist_ok=True)
        artifact = {
            "stats": stats.to_dict(),
            "packages": [p.to_dict() for p in packages],
        }
        path = output_dir / filename
        path.write_text(json.dumps(artifact, ensure_ascii=False, indent=2), encoding="utf-8")
        return path
