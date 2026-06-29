"""Load finding context: blocks, crops, MD excerpts, Gemma OCR."""
from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from backend.app.pipeline.stages.gemma_enrichment.gemma_enrichment_contract import (
    GEMMA_BASE_BLOCKS_DIRNAME,
    STAGE02_BLOCKS_DIRNAME,
)
from backend.app.services.common import version_service
from backend.app.services.findings.findings_service import (
    blocks_data_from_sources,
    compute_finding_block_map,
    compute_text_evidence,
)
from backend.app.services.findings.grounding_service import classify_grounding_level


_BLOCK_DIRS = (STAGE02_BLOCKS_DIRNAME, GEMMA_BASE_BLOCKS_DIRNAME, "blocks_gemma_300", "blocks")


@dataclass
class BlockContext:
    block_id: str
    png_path: Optional[Path] = None
    gemma_text: str = ""
    page: Optional[int] = None
    ocr_label: str = ""


@dataclass
class FindingContext:
    finding: dict
    project_id: str
    output_dir: Path
    section: str = ""
    grounding_level: str = ""
    graphic_block_ids: list = field(default_factory=list)
    text_block_ids: list = field(default_factory=list)
    blocks: list = field(default_factory=list)
    md_excerpt: str = ""
    md_path: Optional[Path] = None


def resolve_output_dir(project_id: str, version_id: Optional[str] = None) -> Path:
    try:
        return version_service.resolve_version_output_dir(project_id, version_id)
    except Exception:
        from backend.app.services.findings.kb_validation_service import _kb_validation_path
        p = _kb_validation_path(project_id, version_id)
        if p:
            return p.parent
        raise


def _load_json(path: Path) -> Optional[dict]:
    if not path.is_file():
        return None
    try:
        with path.open(encoding="utf-8") as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError):
        return None


def _find_block_png(output_dir: Path, block_id: str) -> Optional[Path]:
    bid = block_id.replace("block_", "")
    names = [f"block_{bid}.png", f"{bid}.png"]
    search_roots = [output_dir]
    runs = output_dir.parent / "runs"
    if runs.is_dir():
        for run in sorted(runs.iterdir(), reverse=True):
            if run.is_dir():
                search_roots.append(run)
    version_dir = output_dir.parent.parent if output_dir.name == "latest" else output_dir.parent
    legacy = version_dir / "_output"
    if legacy.is_dir():
        search_roots.append(legacy)

    for root in search_roots:
        for dirname in _BLOCK_DIRS:
            base = root / dirname
            if not base.is_dir():
                continue
            for name in names:
                candidate = base / name
                if candidate.is_file():
                    return candidate
            index = _load_json(base / "index.json")
            if index:
                for entry in index.get("blocks", index.get("items", [])):
                    if not isinstance(entry, dict):
                        continue
                    eid = str(entry.get("block_id", entry.get("id", ""))).replace("block_", "")
                    if eid == bid:
                        fname = entry.get("file") or entry.get("filename") or names[0]
                        candidate = base / fname
                        if candidate.is_file():
                            return candidate
    return None


def _blocks_analysis_text(blocks_analysis: dict) -> dict[str, str]:
    out: dict[str, str] = {}
    for b in (blocks_analysis.get("blocks") or blocks_analysis.get("block_analyses") or []):
        bid = str(b.get("block_id", "")).replace("block_", "")
        if not bid:
            continue
        parts: list[str] = []
        if (b.get("label") or "").strip():
            parts.append(b["label"].strip())
        if (b.get("summary") or "").strip():
            parts.append("Сводка: " + b["summary"].strip())
        kvr = b.get("key_values_read") or []
        if kvr:
            parts.append("Считанные значения: " + "; ".join(map(str, kvr))[:1200])
        if b.get("unreadable_text"):
            parts.append("(часть текста нечитаема)")
        if parts:
            out[bid] = "\n".join(parts)[:3000]
    return out


def _gemma_text_for_block(output_dir: Path, block_id: str) -> str:
    bid = block_id.replace("block_", "")
    search_roots = [output_dir]
    runs = output_dir.parent / "runs"
    if runs.is_dir():
        for run in sorted(runs.iterdir(), reverse=True):
            if run.is_dir():
                search_roots.append(run)
    for root in search_roots:
        for dirname in (GEMMA_BASE_BLOCKS_DIRNAME, "blocks_gemma_300"):
            json_path = root / dirname / f"block_{bid}.json"
            if json_path.is_file():
                data = _load_json(json_path)
                if data:
                    for key in ("content", "text", "ocr_text", "description"):
                        val = data.get(key)
                        if isinstance(val, str) and val.strip():
                            return val.strip()
    return ""


def _block_ocr_text(
    block_id: str,
    *,
    analysis_text: dict[str, str],
    output_dir: Path,
    block_info: dict,
) -> str:
    bid = block_id.replace("block_", "")
    text = (analysis_text.get(bid) or "").strip()
    if text:
        return text
    text = _gemma_text_for_block(output_dir, block_id).strip()
    if text:
        return text
    return str(block_info.get("ocr_label") or block_info.get("label") or "").strip()


def _find_md_source(output_dir: Path) -> Optional[Path]:
    version_dir = output_dir.parent.parent if output_dir.name == "latest" else output_dir.parent
    candidates = [
        version_dir / "01_input" / "source.md",
        version_dir / "01_input" / "document.md",
        output_dir / "01_text_analysis.json",
    ]
    input_dir = version_dir / "01_input"
    if input_dir.is_dir():
        for p in sorted(input_dir.glob("*.md")):
            candidates.insert(0, p)
    for c in candidates:
        if c.is_file():
            return c
    return None


def _md_excerpt(md_path: Optional[Path], finding: dict, max_chars: int = 4000) -> str:
    if not md_path or not md_path.is_file():
        return ""
    try:
        text = md_path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return ""
    if md_path.suffix == ".json":
        return text[:max_chars]
    needles = []
    for key in ("problem", "description", "norm"):
        val = finding.get(key)
        if isinstance(val, str) and len(val) > 20:
            needles.append(val[:80])
    sheet = finding.get("sheet")
    if sheet:
        needles.append(str(sheet))
    for needle in needles:
        pos = text.lower().find(needle.lower()[:40])
        if pos >= 0:
            start = max(0, pos - 800)
            return text[start:start + max_chars]
    return text[:max_chars]


def load_finding_context(
    project_id: str,
    finding: dict,
    *,
    version_id: Optional[str] = None,
    section: str = "",
) -> FindingContext:
    output_dir = resolve_output_dir(project_id, version_id)
    blocks_analysis = _load_json(output_dir / "02_blocks_analysis.json") or {}
    index_data = _load_json(output_dir / "document_graph.json")
    if index_data is None:
        runs = output_dir.parent / "runs"
        if runs.is_dir():
            for sub in sorted(runs.iterdir(), reverse=True):
                if sub.is_dir():
                    g = _load_json(sub / "document_graph.json")
                    if g:
                        index_data = g
                        break
    _, block_info, all_block_ids = blocks_data_from_sources(blocks_analysis, index_data)
    items = [finding]
    graphic_map = compute_finding_block_map(items, all_block_ids)
    text_map = compute_text_evidence(index_data or {}, {}, items)
    fid = finding.get("id", "")
    graphic_ids = graphic_map.get(fid, [])
    text_ids = text_map.get(fid, [])
    analysis_text = _blocks_analysis_text(blocks_analysis)

    blocks: list[BlockContext] = []
    for bid in graphic_ids[:3]:
        info = block_info.get(bid, {})
        ocr_text = _block_ocr_text(bid, analysis_text=analysis_text, output_dir=output_dir, block_info=info)
        blocks.append(BlockContext(
            block_id=bid,
            png_path=_find_block_png(output_dir, bid),
            gemma_text=ocr_text,
            page=info.get("page"),
            ocr_label=str(info.get("ocr_label") or info.get("label") or ""),
        ))

    md_path = _find_md_source(output_dir)
    ctx = FindingContext(
        finding=finding,
        project_id=project_id,
        output_dir=output_dir,
        section=section,
        grounding_level=finding.get("grounding_level") or classify_grounding_level(finding),
        graphic_block_ids=graphic_ids,
        text_block_ids=text_ids,
        blocks=blocks,
        md_excerpt=_md_excerpt(md_path, finding),
        md_path=md_path,
    )
    return ctx
