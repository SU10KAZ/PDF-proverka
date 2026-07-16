"""EV2 context loader — независимое разрешение контекста замечания.

Не зависит от backend/.../evidence_verifier/context_loader.py (его правит Cursor).
Использует только СТАБИЛЬНЫЕ helper'ы основного кода:
  - version_service.resolve_version_output_dir
  - findings_service.blocks_data_from_sources / compute_finding_block_map /
    compute_text_evidence   (ВЕРНАЯ сигнатура: graph, ocr_index, findings)

Cursor в своём context_loader.py:195 вызывает compute_text_evidence(items, block_info)
— это TypeError (нужно 3 аргумента graph/ocr_index/findings). Здесь сделано правильно.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from backend.app.pipeline.stages.gemma_enrichment.gemma_enrichment_contract import (
    GEMMA_BASE_BLOCKS_DIRNAME,
    STAGE02_BLOCKS_DIRNAME,
)
from backend.app.services.common import version_service
from backend.app.services.findings import findings_service as fs
from backend.app.services.findings.grounding_service import classify_grounding_level

_BLOCK_DIRS = (STAGE02_BLOCKS_DIRNAME, GEMMA_BASE_BLOCKS_DIRNAME, "blocks_gemma_300", "blocks")


def _tokens(s: str) -> set:
    return {w for w in "".join(c if c.isalnum() else " " for c in (s or "").lower()).split() if len(w) > 3}


@dataclass
class Block:
    block_id: str
    png_path: Optional[Path] = None
    gemma_text: str = ""
    page: Optional[int] = None
    ocr_label: str = ""


@dataclass
class Context:
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
    graph: dict = field(default_factory=dict)   # document_graph (для кросс-блок без перечтения)

    @property
    def has_png(self) -> bool:
        return any(b.png_path and b.png_path.is_file() for b in self.blocks)

    @property
    def primary_png(self) -> Optional[Path]:
        for b in self.blocks:
            if b.png_path and b.png_path.is_file():
                return b.png_path
        return None


def _load_json(path: Path) -> Optional[dict]:
    if not path or not path.is_file():
        return None
    try:
        with path.open(encoding="utf-8") as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError):
        return None


def _resolve_output_dir(project_id: str, version_id: Optional[str]) -> Optional[Path]:
    try:
        d = version_service.resolve_version_output_dir(project_id, version_id)
        if d and Path(d).is_dir():
            return Path(d)
    except Exception:
        pass
    return None


def _search_roots(output_dir: Path) -> list[Path]:
    roots = [output_dir]
    runs = output_dir.parent / "runs"
    if runs.is_dir():
        for run in sorted(runs.iterdir(), reverse=True):
            if run.is_dir():
                roots.append(run)
    version_dir = output_dir.parent.parent if output_dir.name == "latest" else output_dir.parent
    legacy = version_dir / "_output"
    if legacy.is_dir():
        roots.append(legacy)
    return roots


def _find_block_png(output_dir: Path, block_id: str) -> Optional[Path]:
    bid = block_id.replace("block_", "")
    names = [f"block_{bid}.png", f"{bid}.png"]
    for root in _search_roots(output_dir):
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


def _blocks_analysis_text(blocks_analysis: dict) -> dict:
    """{normalized_block_id -> текст блока} из 01_blocks_analysis.json.

    КЛЮЧЕВОЙ источник контекста для верификатора (диагностика 2026-06-27: per-block
    gemma JSON в v2-раскладке пуст, а здесь label заполнен у ~100% блоков). Без него
    модель смотрит картинку «на глаз» и не ловит misread → recall=0.
    """
    out: dict[str, str] = {}
    for b in (blocks_analysis.get("blocks") or blocks_analysis.get("block_analyses") or []):
        bid = str(b.get("block_id", "")).replace("block_", "")
        if not bid:
            continue
        parts = []
        label = (b.get("label") or "").strip()
        if label:
            parts.append(label)
        summ = (b.get("summary") or "").strip()
        if summ:
            parts.append(f"Сводка: {summ}")
        kvr = b.get("key_values_read") or []
        if kvr:
            parts.append("Считанные значения: " + "; ".join(str(x) for x in kvr)[:1200])
        if b.get("unreadable_text"):
            parts.append("(часть текста нечитаема)")
        if parts:
            out[bid] = "\n".join(parts)[:3000]
    return out


def _gemma_text_for_block(output_dir: Path, block_id: str) -> str:
    bid = block_id.replace("block_", "")
    for root in _search_roots(output_dir):
        for dirname in (GEMMA_BASE_BLOCKS_DIRNAME, "blocks_gemma_300", STAGE02_BLOCKS_DIRNAME):
            data = _load_json(root / dirname / f"block_{bid}.json")
            if data:
                for key in ("content", "text", "ocr_text", "description"):
                    val = data.get(key)
                    if isinstance(val, str) and val.strip():
                        return val.strip()
    return ""


def _find_md_source(output_dir: Path) -> Optional[Path]:
    version_dir = output_dir.parent.parent if output_dir.name == "latest" else output_dir.parent
    input_dir = version_dir / "01_input"
    candidates: list[Path] = []
    if input_dir.is_dir():
        candidates.extend(sorted(input_dir.glob("*.md")))
        candidates.append(input_dir / "source.md")
        candidates.append(input_dir / "document.md")
    candidates.append(output_dir / "02_text_analysis.json")
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
    for key in ("problem", "description", "norm"):
        val = finding.get(key)
        if isinstance(val, str) and len(val) > 20:
            pos = text.lower().find(val.lower()[:40])
            if pos >= 0:
                start = max(0, pos - 800)
                return text[start:start + max_chars]
    return text[:max_chars]


def load_context(
    project_id: str,
    finding: dict,
    *,
    version_id: Optional[str] = None,
    section: str = "",
    max_blocks: int = 3,
) -> Optional[Context]:
    """Загрузить контекст замечания. Вернёт None, если output_dir не разрешился."""
    output_dir = _resolve_output_dir(project_id, version_id)
    if output_dir is None:
        return None
    return _build_context(output_dir, project_id, finding, section=section, max_blocks=max_blocks)


def load_context_from_dir(
    output_dir, finding: dict, *, project_id: str = "", section: str = "", max_blocks: int = 3,
) -> Optional[Context]:
    """Построить Context из ИЗВЕСТНОГО version output_dir (минуя project_id-резолв).

    Для аудита по per-version expert_review: бьём точно в нужную версию, без F-ID-дрейфа.
    """
    output_dir = Path(output_dir)
    if not output_dir.is_dir():
        return None
    return _build_context(output_dir, project_id, finding, section=section, max_blocks=max_blocks)


def _build_context(
    output_dir: Path, project_id: str, finding: dict, *, section: str = "", max_blocks: int = 3,
) -> Context:
    blocks_analysis = _load_json(output_dir / "01_blocks_analysis.json") or {}
    graph = _load_json(output_dir / "document_graph.json") or {}
    if not graph.get("pages"):
        # поискать document_graph в runs/
        for root in _search_roots(output_dir):
            g = _load_json(root / "document_graph.json")
            if g and g.get("pages"):
                graph = g
                break

    _bp, block_info, all_block_ids = fs.blocks_data_from_sources(blocks_analysis, graph)
    items = [finding]
    try:
        graphic_map = fs.compute_finding_block_map(items, all_block_ids)
    except Exception:
        graphic_map = {}
    # ВЕРНАЯ сигнатура: (graph, ocr_index, findings). ocr_index пуст -> fallback внутри.
    # guard: у части findings evidence_text_refs — строки, не dict (крешит helper).
    try:
        text_map = fs.compute_text_evidence(graph, {}, items)
    except Exception:
        text_map = {}

    fid = finding.get("id", "")
    graphic_ids = graphic_map.get(fid, [])
    text_refs = text_map.get(fid, [])
    text_ids = [r.get("text_block_id") for r in text_refs if isinstance(r, dict)]

    # Приоритизация блоков (фикс корня false_reject: показывать модели блок, где
    # РЕАЛЬНО есть предмет замечания, а не просто первый):
    #   1) блок из image-evidence замечания (эксперт/пайплайн указали именно его);
    #   2) блок с наибольшим совпадением OCR-текста с текстом замечания;
    #   3) исходный порядок.
    evidence_img = {
        str(e.get("block_id", "")).replace("block_", "")
        for e in (finding.get("evidence") or [])
        if isinstance(e, dict) and e.get("type") == "image" and e.get("block_id")
    }
    needle = _tokens(
        f"{finding.get('problem','')} {finding.get('description','')} {finding.get('norm','')}"
    )

    ba_text = _blocks_analysis_text(blocks_analysis)

    blocks: list[Block] = []
    for bid in graphic_ids[:6]:   # кандидаты для ранжирования (IO-бюджет)
        info = block_info.get(bid, {})
        nbid = bid.replace("block_", "")
        blocks.append(Block(
            block_id=bid,
            png_path=_find_block_png(output_dir, bid),
            # приоритет: текст из 02_blocks_analysis (label/values), fallback на per-block JSON
            gemma_text=ba_text.get(nbid) or _gemma_text_for_block(output_dir, bid),
            page=info.get("page"),
            ocr_label=str(info.get("ocr_label", "")),
        ))

    def _rank(b: Block) -> tuple:
        is_ev = b.block_id.replace("block_", "") in evidence_img
        overlap = len(needle & _tokens(b.gemma_text)) if needle else 0
        has_png = bool(b.png_path and b.png_path.is_file())
        return (has_png, is_ev, overlap)

    blocks.sort(key=_rank, reverse=True)
    blocks = blocks[:max_blocks]

    md_path = _find_md_source(output_dir)
    return Context(
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
        graph=graph if isinstance(graph, dict) else {},
    )
