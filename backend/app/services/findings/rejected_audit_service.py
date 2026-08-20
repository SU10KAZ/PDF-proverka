"""Read-only, resumable audit of human-rejected PDF findings with Codex CLI.

The canonical input is each version's ``04_review/expert_review.json``.  The
global knowledge-base log is deliberately not used: it is de-duplicated without
the version id and therefore cannot reliably reconnect an F-id to its source
finding after a re-audit.

This module never writes into projects_v2, expert_review, or the knowledge base.
It only creates an immutable-ish manifest and append-only results under the
caller supplied report directory (normally ignored ``comparison/`` runtime
data).
"""
from __future__ import annotations

import asyncio
import csv
import hashlib
import itertools
import importlib
import json
import math
import os
import re
import shutil
import sys
from collections import Counter, defaultdict
from difflib import SequenceMatcher
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path
from typing import Any, Callable, Optional, Sequence
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from backend.app.core.config import ROOT_DIR


PROMPT_PATH = ROOT_DIR / "prompts" / "pipeline" / "ru" / "rejected_finding_expert_audit.md"
DEFAULT_PROJECTS_V2_ROOT = ROOT_DIR / "projects_v2"
DEFAULT_REPORT_ROOT = ROOT_DIR / "comparison" / "rejected_findings_audit"

VALID_VERDICTS = {
    "expert_correct",
    "expert_may_be_wrong",
    "insufficient_evidence",
}
VALID_ACTIONS = {"keep_rejected", "manual_recheck", "collect_context"}
VALID_BINDING_STATUSES = {"exact", "conflict", "missing"}
VALID_FACTUAL_VERDICTS = {"supported", "unsupported", "contradicted", "unclear"}
VALID_REPORT_VALUES = {"include", "merge", "downgrade", "remove", "unclear"}
VALID_REASON_QUALITIES = {"substantiated", "partial", "unsubstantiated", "contradicted", "missing"}
VALID_EVIDENCE_SOURCES = {
    "finding",
    "expert_reason",
    "graphic_block",
    "text_block",
    "document_text",
    "related_document",
    "norm_context",
}
CONCRETE_EVIDENCE_SOURCES = {
    "graphic_block",
    "text_block",
    "document_text",
    "related_document",
    "norm_context",
}
VALID_DECISION_EFFECTS = {
    "supports_rejection",
    "changes_rejection",
    "reason_only",
    "unclear",
}
VALID_REJECTION_BASES = {
    "factual",
    "scope_stage",
    "report_value",
    "duplicate",
    "construction_state",
    "mixed",
    "unknown",
}
VALID_PRACTICAL_IMPACTS = {"high", "medium", "low", "none", "unclear"}
VALID_SOURCE_ALIGNMENTS = {
    "not_visual",
    "confirmed_by_raster",
    "ocr_only_visual_claim",
    "raster_text_conflict",
    "unreadable",
}
VALID_SCOPE_CONTEXT_STATUSES = {
    "not_needed",
    "verified_same_version",
    "missing",
    "version_uncertain",
    "conflict",
}
VALID_OBSERVATION_BASES = {"raster", "pdf_text_layer", "ocr", "vector", "derived"}
VALID_VERIFICATION_STATES = {"corroborated", "single_source", "conflict", "unavailable"}
VALID_CLAIM_TYPES = {"text_token", "dimension", "geometry", "absence", "relation", "other"}
VALID_ABSENCE_SCOPES = {"none", "crop", "page", "document"}
VALID_REVIEW_PRIORITIES = {"none", "low", "medium", "high"}
_TRANSFER_REASON_RE = re.compile(
    r"^\s*↩|автоматически\s+перенес",
    re.IGNORECASE,
)
AUDIT_CONTRACT_VERSION = "rejected_finding_expert_audit.v4"
RETRIEVAL_CONTRACT_VERSION = "rejected_finding_context_retrieval.v1"
AUTO_RETRIEVAL_CONTRACT_VERSION = "rejected_finding_context_retrieval.v3"
DEEP_RETRIEVAL_CONTRACT_VERSION = "rejected_finding_context_recovery.v8"

_MODERN_BLOCK_ID_RE = re.compile(r"[A-Z0-9]{3,5}(?:-[A-Z0-9]{3,5}){2}", re.IGNORECASE)
_LEGACY_BLOCK_ID_RE = re.compile(r"blk_[A-Z0-9_-]{2,80}", re.IGNORECASE)
_REMOTE_CROP_MAX_BYTES = 25 * 1024 * 1024
_REMOTE_CROP_CASE_MAX_BYTES = 96 * 1024 * 1024
_REMOTE_CROP_RUN_MAX_BYTES = 512 * 1024 * 1024
_REMOTE_CROP_MIN_FREE_BYTES = 256 * 1024 * 1024
_SPECIFICATION_REQUEST_RE = re.compile(
    r"спецификац|ведомост|экспликац|таблиц|переч(?:ень|ня)|комплектовочн",
    re.IGNORECASE,
)
_SPECIFICATION_PAGE_RE = re.compile(
    r"спецификац|ведомост(?:ь|и)|экспликац|позиц(?:ия|ии)|"
    r"обозначение|наименование|количеств|масса|марка",
    re.IGNORECASE,
)
_NORM_LOCATOR_RE = re.compile(
    r"(?<![А-Яа-яЁё])(?P<kind>п(?:п|ункт(?:ы|а|ов)?)?|ст(?:атья|атьи|атей)?|"
    r"табл(?:ица|ицы|ице|ицу|иц|\.)?)\.?\s*[№#]?\s*"
    r"(?P<values>\d+(?:\.\d+)*(?:\s*(?:[-–—,;]|\bи\b)\s*\d+(?:\.\d+)*)*)",
    re.IGNORECASE,
)
_ADDITIONAL_NORM_CODE_RE = re.compile(
    r"(?<![А-Яа-яЁё])(?:СанПиН|СП)\s+\d+(?:\.\d+)+(?:-\d+)?",
    re.IGNORECASE,
)
_RELATED_DISCIPLINE_CUES: tuple[tuple[re.Pattern[str], tuple[str, ...]], ...] = (
    (re.compile(r"\bпз\b|пояснительн", re.IGNORECASE), ("PZ",)),
    (re.compile(r"\bпзу\b|генеральн\w*\s+план|генплан", re.IGNORECASE), ("GP", "PZ")),
    (re.compile(r"\bиос\b|гидравлическ|водоснабжен|канализац", re.IGNORECASE), ("VK", "PZ")),
    (re.compile(r"\bппр\b|организац\w*\s+строитель", re.IGNORECASE), ("POS",)),
    (re.compile(r"\bэом\b|электроснабжен|электрооборудован", re.IGNORECASE), ("EOM",)),
    (re.compile(r"пожарн\w*\s+безопасност", re.IGNORECASE), ("AR", "SS", "PZ")),
)

_RETRIEVAL_STOPWORDS = {
    "а", "без", "бы", "был", "была", "были", "было", "в", "во", "для",
    "до", "его", "ее", "её", "и", "из", "или", "их", "как", "к", "ко",
    "либо", "на", "над", "но", "о", "об", "от", "по", "под", "при", "с",
    "со", "то", "у", "что", "эта", "эти", "это", "этот",
    "графический", "графические", "область", "области", "фрагмент",
    "полный", "полное", "полная", "читаемый", "читаемое", "изображение",
    "изображения", "лист", "листа", "листе", "страница", "страницы",
    "пояснение", "принадлежность", "корректный", "проверяемый", "проверяемой",
    "фактический", "фактического", "однозначный", "однозначными",
}
_RETRIEVAL_RU_SUFFIXES = tuple(sorted({
    "иями", "ями", "ами", "ого", "его", "ему", "ому", "ыми", "ими",
    "иях", "ией", "ией", "ией", "ей", "ой", "ий", "ый", "ая", "яя",
    "ое", "ее", "ые", "ие", "ов", "ев", "ам", "ям", "ах", "ях", "ом",
    "ем", "ью", "иям", "ия", "ых", "их", "ую", "юю", "а", "я", "ы",
    "и", "у", "ю", "е", "о", "ь", "й",
}, key=len, reverse=True))


def _import_fitz() -> Any:
    """Load PyMuPDF from the active env or the existing local user site."""
    try:
        return importlib.import_module("fitz")
    except ModuleNotFoundError:
        import site

        candidates = site.getusersitepackages()
        if isinstance(candidates, str):
            candidates = [candidates]
        for candidate in candidates:
            path = str(candidate or "")
            if path and Path(path).is_dir() and path not in sys.path:
                sys.path.append(path)
        return importlib.import_module("fitz")


def _canonical_block_id(value: Any) -> str:
    """Normalize resolver prefixes without corrupting legacy ``blk_*`` ids."""
    block_id = str(value or "").strip()
    if block_id.startswith("block_"):
        block_id = block_id[len("block_"):]
    if _MODERN_BLOCK_ID_RE.fullmatch(block_id):
        return block_id.upper()
    if _LEGACY_BLOCK_ID_RE.fullmatch(block_id):
        return block_id.lower()
    return block_id


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def parse_month(
    month: str,
    timezone_name: str = "Europe/Moscow",
) -> tuple[datetime, datetime]:
    """Return a half-open calendar-month interval in the requested timezone."""
    match = re.fullmatch(r"(\d{4})-(\d{2})", str(month or "").strip())
    if not match:
        raise ValueError("month must have YYYY-MM format")
    year, mon = int(match.group(1)), int(match.group(2))
    if not 1 <= mon <= 12:
        raise ValueError("month must have YYYY-MM format")
    try:
        tz = ZoneInfo(timezone_name)
    except ZoneInfoNotFoundError as exc:
        raise ValueError(f"unknown timezone: {timezone_name}") from exc
    start = datetime(year, mon, 1, tzinfo=tz)
    if mon == 12:
        end = datetime(year + 1, 1, 1, tzinfo=tz)
    else:
        end = datetime(year, mon + 1, 1, tzinfo=tz)
    return start, end


def parse_timestamp(value: Any) -> Optional[datetime]:
    raw = str(value or "").strip()
    if not raw:
        return None
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _load_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        return None


def _atomic_write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    tmp.write_text(text, encoding="utf-8")
    os.replace(tmp, path)


def _atomic_write_json(path: Path, value: Any) -> None:
    _atomic_write_text(path, json.dumps(value, ensure_ascii=False, indent=2) + "\n")


def _decision_items(payload: Any) -> list[dict]:
    if isinstance(payload, list):
        raw = payload
    elif isinstance(payload, dict):
        raw = payload.get("decisions") or payload.get("reviews") or payload.get("items") or []
    else:
        raw = []
    return [item for item in raw if isinstance(item, dict)]


def _part_after(path: Path, marker: str) -> str:
    parts = path.parts
    try:
        return parts[parts.index(marker) + 1]
    except (ValueError, IndexError):
        return ""


def _analysis_dirs(version_dir: Path) -> list[Path]:
    candidates = [version_dir / "03_analysis" / "latest", version_dir / "_output"]
    return [path for path in candidates if path.is_dir()]


def _source_files(item_type: str) -> tuple[str, ...]:
    if item_type == "optimization":
        return ("optimization.json",)
    return ("03_findings.json", "03a_norms_verified.json")


def _items_from_artifact(payload: Any, item_type: str) -> list[dict]:
    if isinstance(payload, list):
        raw = payload
    elif not isinstance(payload, dict):
        raw = []
    elif item_type == "optimization":
        raw = payload.get("items") or payload.get("optimizations") or []
    else:
        raw = payload.get("findings") or payload.get("items") or []
    return [item for item in raw if isinstance(item, dict)]


def _source_binding_similarity(left: dict, right: dict) -> float:
    def identity_text(item: dict) -> str:
        value = f"{item.get('problem') or ''} {item.get('description') or ''}"
        return re.sub(r"\s+", " ", value).strip().casefold()
    left_text = identity_text(left)
    right_text = identity_text(right)
    if not left_text or not right_text:
        return 0.0
    return SequenceMatcher(None, left_text, right_text, autojunk=False).ratio()


@lru_cache(maxsize=512)
def _source_item_maps(output_dir_text: str, item_type: str) -> dict[str, dict[str, dict]]:
    output_dir = Path(output_dir_text)
    maps: dict[str, dict[str, dict]] = {}
    for filename in _source_files(item_type):
        payload = _load_json(output_dir / filename)
        maps[filename] = {
            str(item.get("id")): item
            for item in _items_from_artifact(payload, item_type)
            if item.get("id")
        }
    return maps


def load_exact_source_item(
    version_dir: Path,
    item_id: str,
    item_type: str = "finding",
) -> tuple[Optional[dict], Optional[Path], Optional[Path], str]:
    """Load the fresh same-version item and flag conflicting verified output."""
    for output_dir in _analysis_dirs(version_dir):
        maps = _source_item_maps(str(output_dir.resolve()), item_type)
        if item_type == "optimization":
            item = maps.get("optimization.json", {}).get(item_id)
            if item is not None:
                return dict(item), output_dir / "optimization.json", output_dir, "same_version_artifact"
            continue
        primary = maps.get("03_findings.json", {}).get(item_id)
        verified = maps.get("03a_norms_verified.json", {}).get(item_id)
        if primary is not None:
            quality = "same_version_artifact"
            if verified is not None and _source_binding_similarity(primary, verified) < 0.75:
                quality = "same_version_artifact_conflict"
            return dict(primary), output_dir / "03_findings.json", output_dir, quality
        if verified is not None:
            return dict(verified), output_dir / "03a_norms_verified.json", output_dir, "same_version_verified_only"
    output_dirs = _analysis_dirs(version_dir)
    return None, None, output_dirs[0] if output_dirs else None, "missing"


def _compact_value(value: Any, *, depth: int = 0) -> Any:
    """Bound manifest context while keeping all decision-relevant fields."""
    if depth > 5:
        return str(value)[:500]
    if isinstance(value, str):
        return value[:6000]
    if isinstance(value, dict):
        return {
            str(key): _compact_value(val, depth=depth + 1)
            for key, val in list(value.items())[:80]
        }
    if isinstance(value, (list, tuple)):
        return [_compact_value(item, depth=depth + 1) for item in list(value)[:40]]
    if value is None or isinstance(value, (bool, int, float)):
        return value
    return str(value)[:1000]


_FINDING_FIELDS = (
    "id",
    "severity",
    "category",
    "sheet",
    "page",
    "problem",
    "description",
    "solution",
    "risk",
    "norm",
    "norm_ref",
    "normative_ref",
    "grounding_level",
    "source_block_ids",
    "related_block_ids",
    "evidence",
    "evidence_text_refs",
    "text_evidence",
    "evidence_text",
    "md_excerpt",
    "context",
)


def compact_finding(finding: dict) -> dict:
    compact = {
        key: _compact_value(finding.get(key))
        for key in _FINDING_FIELDS
        if key in finding and finding.get(key) not in (None, "", [], {})
    }
    compact.setdefault("id", str(finding.get("id") or ""))
    return compact


def _block_refs(finding: dict) -> list[str]:
    refs: list[str] = []
    seen: set[str] = set()

    def add(value: Any) -> None:
        block_id = _canonical_block_id(value)
        if block_id and block_id not in seen:
            seen.add(block_id)
            refs.append(block_id)

    for field in ("source_block_ids", "related_block_ids", "primary_block_ids"):
        for block_id in finding.get(field) or []:
            add(block_id)
    for evidence in finding.get("evidence") or []:
        if isinstance(evidence, dict):
            add(evidence.get("block_id") or evidence.get("id"))
    for evidence_ref in finding.get("evidence_text_refs") or []:
        if isinstance(evidence_ref, dict):
            add(
                evidence_ref.get("text_block_id")
                or evidence_ref.get("block_id")
                or evidence_ref.get("id")
            )
    return refs


def _text_evidence(finding: dict) -> list[str]:
    parts: list[str] = []

    def add(value: Any) -> None:
        if isinstance(value, str) and value.strip():
            parts.append(value.strip())
        elif isinstance(value, dict):
            for key in ("text", "quote", "snippet", "content", "md_excerpt", "resolved_text"):
                nested = value.get(key)
                if isinstance(nested, str) and nested.strip():
                    parts.append(nested.strip())

    for evidence in finding.get("evidence") or []:
        if isinstance(evidence, dict) and evidence.get("type") in (None, "text"):
            add(evidence)
    for ref in finding.get("evidence_text_refs") or []:
        add(ref)
    for key in ("text_evidence", "evidence_text", "md_excerpt", "context"):
        add(finding.get(key))

    unique: list[str] = []
    seen: set[str] = set()
    for part in parts:
        marker = part[:240]
        if marker not in seen:
            seen.add(marker)
            unique.append(part)
    return unique


def _page_numbers(value: Any) -> list[int]:
    raw_values = value if isinstance(value, list) else [value]
    pages: list[int] = []
    for raw in raw_values:
        if isinstance(raw, (int, float)) and int(raw) > 0:
            pages.append(int(raw))
            continue
        for match in re.findall(r"\d+", str(raw or "")):
            page = int(match)
            if page > 0:
                pages.append(page)
    return list(dict.fromkeys(pages))


def _locator_page_numbers(value: Any) -> list[int]:
    """Read only explicit page/sheet locators, excluding version/code digits."""
    matches = re.findall(
        r"(?:страниц(?:а|е|ы|у)?|стр\.?|page|лист(?:е|а|у)?)"
        r"\s*(?:pdf\s*)?(?:№|#)?\s*(\d+)",
        str(value or ""),
        flags=re.IGNORECASE,
    )
    return list(dict.fromkeys(int(value) for value in matches if int(value) > 0))


@lru_cache(maxsize=4)
def _read_md_pages(path_text: str) -> tuple[str, dict[int, tuple[int, int]]]:
    path = Path(path_text)
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return "", {}
    matches = list(
        re.finditer(
            r"(?mi)^##\s+(?:Page|СТРАНИЦА)\s+(\d+)\s*$",
            text,
        )
    )
    spans: dict[int, tuple[int, int]] = {}
    for index, match in enumerate(matches):
        start = match.start()
        end = matches[index + 1].start() if index + 1 < len(matches) else len(text)
        spans[int(match.group(1))] = (start, end)
    return text, spans


def _bounded_md_excerpt(value: str, max_chars: int) -> str:
    """Keep both ends of long tables instead of silently dropping totals."""
    text = str(value or "").strip()
    max_chars = max(1, int(max_chars))
    if len(text) <= max_chars:
        return text
    marker = "\n\n[… середина блока сокращена …]\n\n"
    available = max(1, max_chars - len(marker))
    head = available // 2
    tail = available - head
    return text[:head].rstrip() + marker + text[-tail:].lstrip()


@lru_cache(maxsize=4)
def _read_md_text_blocks(path_text: str) -> dict[str, dict]:
    """Index canonical BLOCK [TEXT] sections with their document page."""
    text, page_spans = _read_md_pages(path_text)
    if not text:
        return {}
    matches = list(
        re.finditer(
            r"(?mi)^###\s+BLOCK\s+\[(TEXT|IMAGE)\]\s*:\s*"
            r"(?:block_)?([A-Z0-9-]+)\s*$",
            text,
        )
    )
    indexed: dict[str, dict] = {}
    for index, match in enumerate(matches):
        if match.group(1).upper() != "TEXT":
            continue
        block_id = match.group(2).removeprefix("block_")
        page = None
        page_end = len(text)
        for candidate_page, (start, end) in page_spans.items():
            if start <= match.start() < end:
                page = candidate_page
                page_end = end
                break
        next_block = (
            matches[index + 1].start()
            if index + 1 < len(matches)
            else len(text)
        )
        end = min(page_end, next_block)
        indexed[block_id] = {
            "block_id": block_id,
            "page": page,
            "text": text[match.start():end].strip(),
        }
    return indexed


def _find_document_md(output_dir: Path) -> Optional[Path]:
    version_dir = output_dir.parent.parent if output_dir.name == "latest" else output_dir.parent
    candidates = [
        version_dir / "02_work" / "document.md",
        version_dir / "01_input" / "document.md",
        version_dir / "01_input" / "source.md",
    ]
    input_dir = version_dir / "01_input"
    if input_dir.is_dir():
        candidates.extend(sorted(input_dir.glob("*.md")))
    for path in candidates:
        if path.is_file():
            return path.resolve()
    return None


def _document_md_context(
    output_dir: Optional[Path],
    finding: dict,
    *,
    max_chars: int = 12000,
) -> tuple[str, str, list[int], list[dict]]:
    if output_dir is None:
        return "", "", [], []
    md_path = _find_document_md(output_dir)
    if md_path is None:
        return "", "", [], []
    text, spans = _read_md_pages(str(md_path))
    if not text:
        return "", str(md_path), [], []

    excerpts: list[str] = []
    used_pages: list[int] = []
    exact_rows: list[dict] = []
    block_index = _read_md_text_blocks(str(md_path))
    matched_blocks = [
        block_index[block_id]
        for block_id in _block_refs(finding)
        if block_id in block_index
    ]
    if matched_blocks:
        per_block_limit = max(600, max_chars // len(matched_blocks))
        for row in matched_blocks:
            block_text = _bounded_md_excerpt(row["text"], per_block_limit)
            exact_rows.append({
                "block_id": row["block_id"],
                "page": row["page"],
                "ocr_label": "",
                "ocr_or_description": block_text,
                "image_path": "",
            })
            excerpts.append(block_text)
            if row["page"] is not None:
                used_pages.append(int(row["page"]))

    if not excerpts:
        for page in _page_numbers(finding.get("page")):
            span = spans.get(page)
            if not span:
                continue
            remaining = max_chars - sum(len(item) for item in excerpts)
            if remaining <= 0:
                break
            excerpts.append(
                _bounded_md_excerpt(text[span[0]:span[1]], remaining)
            )
            used_pages.append(page)

    if not excerpts:
        needles = _text_evidence(finding)
        for key in ("sheet", "norm", "norm_ref", "normative_ref"):
            value = finding.get(key)
            if isinstance(value, str) and value.strip():
                needles.append(value.strip())
        lowered = text.casefold()
        for needle in needles:
            token = str(needle or "").strip()[:160]
            if len(token) < 8:
                continue
            position = lowered.find(token.casefold())
            if position < 0:
                continue
            start = max(0, position - 1800)
            excerpts.append(text[start:start + max_chars])
            break

    excerpt = "\n\n--- DOCUMENT BLOCK ---\n\n".join(excerpts)[:max_chars]
    return (
        excerpt,
        str(md_path),
        list(dict.fromkeys(used_pages)),
        exact_rows,
    )


def _norm_paragraph_refs(text: str) -> list[str]:
    """Extract explicit clause/article locators without inventing a pairing."""
    refs: list[str] = []
    patterns = (
        (
            r"(?<![А-Яа-яЁё])п(?:п|ункт(?:ы|а|ов)?)?\.?\s*"
            r"([0-9][0-9.\s,;\-–—]*[0-9])"
        ),
        (
            r"(?<![А-Яа-яЁё])ст(?:атья|атьи|атей)?\.?\s*"
            r"([0-9][0-9.\s,;\-–—]*[0-9]|[0-9])"
        ),
    )
    for pattern in patterns:
        for match in re.finditer(pattern, str(text or ""), re.IGNORECASE):
            refs.extend(re.findall(r"\d+(?:\.\d+)*", match.group(1)))
    return list(dict.fromkeys(refs))[:16]



def _expanded_locator_values(raw: str) -> list[str]:
    values = re.findall(r"\d+(?:\.\d+)*", str(raw or ""))
    if len(values) == 2 and re.search(r"[-–—]", str(raw or "")):
        first_parts = values[0].split(".")
        last_parts = values[1].split(".")
        if (
            len(first_parts) == len(last_parts)
            and first_parts[:-1] == last_parts[:-1]
        ):
            first, last = int(first_parts[-1]), int(last_parts[-1])
            if 0 <= last - first <= 20:
                prefix = ".".join(first_parts[:-1])
                values = [
                    f"{prefix}.{value}" if prefix else str(value)
                    for value in range(first, last + 1)
                ]
    return list(dict.fromkeys(values))[:24]

def _additional_norm_codes(text: str) -> list[str]:
    return list(dict.fromkeys(
        re.sub(r"\s+", " ", match.group(0)).strip(" .;,")
        for match in _ADDITIONAL_NORM_CODE_RE.finditer(str(text or ""))
    ))[:8]


def _norm_status_with_alias(code: str, get_norm_status) -> dict:
    status = get_norm_status(code)
    if status.get("found"):
        return status
    federal = re.fullmatch(
        r"(?:ФЗ\s*[-№]?\s*(\d+)|(\d+)\s*-?\s*ФЗ)",
        str(code or "").strip(),
        re.IGNORECASE,
    )
    if not federal:
        return status
    number = federal.group(1) or federal.group(2)
    for alias in (f"{number}-ФЗ", f"ФЗ {number}-ФЗ"):
        candidate = get_norm_status(alias)
        if candidate.get("found"):
            candidate = dict(candidate)
            candidate["query_alias"] = code
            candidate["resolved_alias"] = alias
            return candidate
    return status



def _norm_locators(text: str) -> list[dict]:
    locators: list[dict] = []
    for match in _NORM_LOCATOR_RE.finditer(str(text or "")):
        kind_text = match.group("kind").casefold()
        kind = (
            "table"
            if kind_text.startswith("табл")
            else "article"
            if kind_text.startswith("ст")
            else "paragraph"
        )
        for value in _expanded_locator_values(match.group("values")):
            locators.append({
                "kind": kind,
                "value": value,
                "start": match.start(),
                "end": match.end(),
            })
    return locators


def _norm_reference_map(text: str, codes: Sequence[str]) -> dict[str, list[dict]]:
    """Bind every locator to the nearest cited norm instead of cross-pairing."""
    try:
        from norms._core import NORM_REGEX
    except Exception:
        return {}

    def key(value: str) -> str:
        return re.sub(r"\s+", " ", str(value or "")).strip(" .;,").casefold()

    canonical_by_key = {key(code): code for code in codes if key(code)}
    citations: list[dict] = []
    for match in NORM_REGEX.finditer(str(text or "")):
        matched = str(match.group(0) or "").strip()
        code = canonical_by_key.get(key(matched), matched)
        citations.append({
            "code": code,
            "start": match.start(),
            "end": match.end(),
        })
    occupied = {(row["start"], row["end"]) for row in citations}
    for code in codes:
        for match in re.finditer(re.escape(str(code)), str(text or ""), re.IGNORECASE):
            span = (match.start(), match.end())
            if span in occupied:
                continue
            occupied.add(span)
            citations.append({
                "code": code,
                "start": match.start(),
                "end": match.end(),
            })
    if not citations:
        return {}

    mapping: dict[str, list[dict]] = {code: [] for code in codes}
    for locator in _norm_locators(text):
        def distance(citation: dict) -> int:
            if locator["end"] <= citation["start"]:
                return citation["start"] - locator["end"]
            if locator["start"] >= citation["end"]:
                return locator["start"] - citation["end"]
            return 0

        def same_segment(citation: dict) -> bool:
            left = min(locator["end"], citation["end"])
            right = max(locator["start"], citation["start"])
            gap = str(text or "")[left:right]
            sentence_gap = re.sub(
                r"(?<![А-Яа-яЁё])(?:п|пп|ст|табл)\.\s*",
                "",
                gap,
                flags=re.IGNORECASE,
            )
            sentence_boundary = re.search(
                r"(?<!\d)[.!?](?=\s|$)",
                sentence_gap,
            )
            return "\n\n" not in gap and ";" not in gap and not sentence_boundary

        eligible = [citation for citation in citations if same_segment(citation)]
        if not eligible:
            continue
        citation = min(eligible, key=distance)
        # Do not bind free-prose numbering to a distant normative citation.
        if distance(citation) > 160:
            continue
        code = citation["code"]
        if code not in mapping:
            mapping[code] = []
        row = {"kind": locator["kind"], "value": locator["value"]}
        if row not in mapping[code]:
            mapping[code].append(row)

    if len(codes) == 1 and not mapping.get(codes[0]):
        mapping[codes[0]] = [
            {"kind": "paragraph", "value": value}
            for value in _norm_paragraph_refs(text)
        ]
    return mapping


def _literal_norm_table(
    code: str,
    table: str,
    status: dict,
    *,
    max_lines: int = 120,
) -> dict:
    """Extract a cited table literally from the authoritative local vault."""
    file_name = str(status.get("file") or "").strip()
    if status.get("source") != "vault" or not file_name:
        return {}
    vault = (ROOT_DIR / "norms" / "vault").resolve()
    try:
        path = (vault / file_name).resolve(strict=True)
    except (OSError, RuntimeError):
        return {}
    if not path.is_file() or not path.is_relative_to(vault):
        return {}
    try:
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError:
        return {}

    table_word = r"(?:таблица|табл\.|т\s*а\s*б\s*л\s*и\s*ц\s*а)"
    heading = re.compile(
        rf"^\s*(?:#{{1,6}}\s*)?(?:[*_]{{0,2}})?"
        rf"{table_word}\s*{re.escape(str(table))}\b",
        re.IGNORECASE,
    )
    start = next((index for index, line in enumerate(lines) if heading.search(line)), None)
    if start is None:
        return {}
    selected: list[str] = []
    next_table = re.compile(
        rf"^\s*(?:#{{1,6}}\s*)?(?:[*_]{{0,2}})?{table_word}\s*\d",
        re.IGNORECASE,
    )
    for line in lines[start:start + max(1, int(max_lines))]:
        if selected and next_table.search(line):
            break
        selected.append(line)
    text = _bounded_md_excerpt("\n".join(selected).strip(), 12000)
    if not text:
        return {}
    return _compact_value({
        "code": status.get("matched_code") or code,
        "locator_kind": "table",
        "table": str(table),
        "text": text,
        "file": path.name,
        "line": start + 1,
        "status": status.get("status"),
        "doc_status": status.get("doc_status"),
        "edition_status": status.get("edition_status"),
        "authoritative": status.get("authoritative"),
        "replacement_doc": status.get("replacement_doc"),
        "truncated": len(selected) >= max(1, int(max_lines)),
    })



def _literal_norm_article(
    code: str,
    article: str,
    status: dict,
    *,
    max_lines: int = 120,
) -> dict:
    """Extract a cited legal article literally from the local vault."""
    file_name = str(status.get("file") or "").strip()
    if status.get("source") != "vault" or not file_name:
        return {}
    vault = (ROOT_DIR / "norms" / "vault").resolve()
    try:
        path = (vault / file_name).resolve(strict=True)
    except (OSError, RuntimeError):
        return {}
    if not path.is_file() or not path.is_relative_to(vault):
        return {}
    try:
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError:
        return {}

    heading = re.compile(
        rf"^\s*(?:#{{1,6}}\s*)?(?:[*_]{{0,2}})?"
        rf"статья\s*{re.escape(str(article))}\b",
        re.IGNORECASE,
    )
    start = next((index for index, line in enumerate(lines) if heading.search(line)), None)
    if start is None:
        return {}
    selected: list[str] = []
    next_article = re.compile(
        r"^\s*(?:#{1,6}\s*)?(?:[*_]{0,2})?статья\s*\d",
        re.IGNORECASE,
    )
    for line in lines[start:start + max(1, int(max_lines))]:
        if selected and next_article.search(line):
            break
        selected.append(line)
    text = _bounded_md_excerpt("\n".join(selected).strip(), 12000)
    if not text:
        return {}
    return _compact_value({
        "code": status.get("matched_code") or code,
        "locator_kind": "article",
        "article": str(article),
        "text": text,
        "file": path.name,
        "line": start + 1,
        "status": status.get("status"),
        "doc_status": status.get("doc_status"),
        "edition_status": status.get("edition_status"),
        "authoritative": status.get("authoritative"),
        "replacement_doc": status.get("replacement_doc"),
        "truncated": len(selected) >= max(1, int(max_lines)),
    })
def _authoritative_norm_context(
    finding: dict,
    *,
    semantic_query: str = "",
) -> dict:
    """Collect literal local clauses, statuses and bounded semantic candidates."""
    norm_text = "\n\n".join(
        str(finding.get(key) or "")
        for key in ("norm", "norm_ref", "normative_ref")
    ).strip()
    if not norm_text and not str(semantic_query or "").strip():
        return {}
    try:
        norms_tools = str(ROOT_DIR / "norms" / "tools")
        if norms_tools not in sys.path:
            sys.path.insert(0, norms_tools)
        from norms._core import extract_norms_from_text
        from norms_api import get_norm_status, get_paragraph, semantic_search

        extracted_codes = list(dict.fromkeys([
            *extract_norms_from_text(norm_text),
            *_additional_norm_codes(norm_text),
        ]))[:8]
        codes: list[str] = []
        status_by_code: dict[str, dict] = {}
        for raw_code in extracted_codes:
            status = _norm_status_with_alias(raw_code, get_norm_status)
            code = str(raw_code or "").strip(" .;,")
            if not code or code in status_by_code:
                continue
            codes.append(code)
            status_by_code[code] = status
            if len(codes) >= 5:
                break
        raw = {
            "kind": "norm_status_bundle" if codes else "none",
            "decision_hint": "neutral",
            "confidence": 0.8 if codes else 0.0,
            "matched_code": codes[0] if codes else None,
            "status": "unknown",
            "flags": [],
            "suggestions": {},
            "reason": (
                "Проверены локальные статусы, буквальные пункты и таблицы; "
                "локаторы связаны с ближайшей ссылкой на норматив."
            ),
        }
        reference_map = _norm_reference_map(norm_text, codes)
        statuses: list[dict] = []
        clauses: list[dict] = []
        literal_tables: list[dict] = []
        unresolved_locators: list[dict] = []
        confirmed_absent_locators: list[dict] = []
        unavailable_norm_locators: list[dict] = []
        for code in codes:
            status = status_by_code[code]
            statuses.append(_compact_value({
                key: status.get(key)
                for key in (
                    "query", "found", "matched_code", "status", "doc_status",
                    "edition_status", "authoritative", "resolution_reason",
                    "replacement_doc", "current_version", "title", "file",
                    "last_verified", "source",
                )
            }))
            for locator in reference_map.get(code) or []:
                value = str(locator.get("value") or "")
                kind = str(locator.get("kind") or "paragraph")
                if status.get("source") != "vault" or not status.get("file"):
                    unavailable_norm_locators.append({
                        "code": code,
                        "kind": kind,
                        "value": value,
                        "reason": "norm_not_available_in_local_authoritative_corpus",
                    })
                    continue
                if kind == "article":
                    article = _literal_norm_article(code, value, status)
                    if article:
                        clauses.append(article)
                    else:
                        unresolved_locators.append({
                            "code": code,
                            "kind": kind,
                            "value": value,
                        })
                    continue
                if kind == "table":
                    table = _literal_norm_table(code, value, status)
                    if table:
                        literal_tables.append(table)
                    else:
                        unresolved_locators.append({
                            "code": code,
                            "kind": kind,
                            "value": value,
                        })
                    continue
                lookup_code = str(status.get("matched_code") or code)
                clause = get_paragraph(lookup_code, value, max_lines=40)
                if not clause.get("found") or not str(clause.get("text") or "").strip():
                    unresolved_locators.append({
                        "code": code,
                        "kind": kind,
                        "value": value,
                    })
                    continue
                clauses.append(_compact_value({
                    "code": clause.get("matched_code") or code,
                    "locator_kind": kind,
                    "paragraph": value,
                    "text": str(clause.get("text") or "")[:12000],
                    "file": clause.get("file"),
                    "line": clause.get("line"),
                    "status": clause.get("status"),
                    "doc_status": clause.get("doc_status"),
                    "edition_status": clause.get("edition_status"),
                    "authoritative": clause.get("authoritative"),
                    "replacement_doc": clause.get("replacement_doc"),
                    "truncated": clause.get("truncated"),
                }))

        # Combined finding/reason/audit text can place a locator closer to the
        # wrong citation. Retry an unresolved literal locator against the other
        # norms that are already cited, but accept reassociation only when one
        # and only one document contains the exact requested material.
        still_unresolved: list[dict] = []
        for unresolved in unresolved_locators:
            exact_hits: list[dict] = []
            for candidate_code in codes:
                if candidate_code == unresolved.get("code"):
                    continue
                candidate_status = status_by_code[candidate_code]
                if (
                    candidate_status.get("source") != "vault"
                    or not candidate_status.get("file")
                ):
                    continue
                value = str(unresolved.get("value") or "")
                if unresolved.get("kind") == "article":
                    hit = _literal_norm_article(candidate_code, value, candidate_status)
                elif unresolved.get("kind") == "table":
                    hit = _literal_norm_table(candidate_code, value, candidate_status)
                else:
                    lookup_code = str(candidate_status.get("matched_code") or candidate_code)
                    clause = get_paragraph(lookup_code, value, max_lines=40)
                    hit = {}
                    if clause.get("found") and str(clause.get("text") or "").strip():
                        hit = _compact_value({
                            "code": clause.get("matched_code") or candidate_code,
                            "locator_kind": unresolved.get("kind") or "paragraph",
                            "paragraph": value,
                            "text": str(clause.get("text") or "")[:12000],
                            "file": clause.get("file"),
                            "line": clause.get("line"),
                            "status": clause.get("status"),
                            "doc_status": clause.get("doc_status"),
                            "edition_status": clause.get("edition_status"),
                            "authoritative": clause.get("authoritative"),
                            "replacement_doc": clause.get("replacement_doc"),
                            "truncated": clause.get("truncated"),
                        })
                if hit:
                    exact_hits.append(hit)
            if len(exact_hits) == 1:
                hit = dict(exact_hits[0])
                hit["association"] = "fallback_unique_exact_match"
                hit["originally_assigned_code"] = unresolved.get("code")
                if unresolved.get("kind") == "table":
                    literal_tables.append(hit)
                else:
                    clauses.append(hit)
                continue
            unresolved = dict(unresolved)
            if exact_hits:
                unresolved["fallback_exact_codes"] = list(dict.fromkeys(
                    str(hit.get("code") or "") for hit in exact_hits
                    if str(hit.get("code") or "")
                ))
            still_unresolved.append(unresolved)
        for unresolved in still_unresolved:
            status = status_by_code.get(str(unresolved.get("code") or ""), {})
            row = dict(unresolved)
            row.update(_compact_value({
                "matched_code": status.get("matched_code"),
                "file": status.get("file"),
                "authoritative": status.get("authoritative"),
            }))
            if status.get("source") == "vault" and status.get("file"):
                row["reason"] = "exact_locator_not_found_in_complete_local_copy"
                row["search_scope"] = "entire_local_norm_document"
                confirmed_absent_locators.append(row)
            else:
                row["reason"] = "norm_not_available_in_local_authoritative_corpus"
                unavailable_norm_locators.append(row)
        unresolved_locators = []

        semantic_hits: list[dict] = []
        semantic_seen: set[tuple[str, str, str]] = set()
        query = re.sub(r"\s+", " ", str(semantic_query or "")).strip()[:3000]
        search_scopes: list[Optional[str]] = codes[:3] if codes else [None]
        if query:
            for code_filter in search_scopes:
                try:
                    hits = semantic_search(query, top=4, code_filter=code_filter)
                except TypeError:
                    hits = semantic_search(query, top=5)
                for hit in hits:
                    text = str(hit.get("text") or "").strip()
                    hit_code = str(hit.get("code") or code_filter or "").strip()
                    if not hit_code or not text:
                        continue
                    status = get_norm_status(hit_code)
                    matched_code = str(status.get("matched_code") or hit_code)
                    marker = (
                        matched_code.casefold(),
                        str(hit.get("paragraph") or ""),
                        text[:240].casefold(),
                    )
                    if marker in semantic_seen:
                        continue
                    semantic_seen.add(marker)
                    semantic_hits.append(_compact_value({
                        "code": matched_code,
                        "paragraph": hit.get("paragraph"),
                        "text": text[:7000],
                        "file": hit.get("file"),
                        "line": hit.get("line"),
                        "semantic_score": hit.get("score"),
                        "dense_score": hit.get("dense_score"),
                        "status": status.get("status"),
                        "doc_status": status.get("doc_status"),
                        "edition_status": status.get("edition_status"),
                        "status_authoritative": status.get("authoritative"),
                        "status_resolution": status.get("resolution_reason"),
                        "role": "candidate_clause_from_local_norms_search",
                    }))
                    if len(semantic_hits) >= 12:
                        break
                if len(semantic_hits) >= 12:
                    break

        raw.update({
            "kind": (
                "norm_authoritative_bundle"
                if clauses or literal_tables or semantic_hits
                else raw.get("kind") or "none"
            ),
            "codes_checked": codes or raw.get("codes_checked") or [],
            "statuses": statuses,
            "requested_locators": reference_map,
            "clauses": clauses,
            "literal_tables": literal_tables,
            "unresolved_locators": unresolved_locators,
            "confirmed_absent_locators": confirmed_absent_locators,
            "unavailable_norm_locators": unavailable_norm_locators,
            "semantic_candidates": semantic_hits,
            "semantic_query": query,
            "evidence_policy": (
                "Exact clauses and tables are literal extracts from the local "
                "Norms vault; confirmed_absent means an exhaustive exact-locator "
                "search in that local copy; semantic candidates are not conclusions."
            ),
        })
        return _compact_value(raw)
    except Exception as exc:  # fail-soft: absence must become explicit uncertainty
        return {
            "kind": "unavailable",
            "status": "unknown",
            "reason": f"Локальная проверка нормы недоступна: {type(exc).__name__}",
        }


def _safe_norm_context(finding: dict) -> dict:
    norm_text = " ".join(
        str(finding.get(key) or "")
        for key in ("norm", "norm_ref", "normative_ref")
    ).strip()
    if not norm_text:
        return {}
    return _authoritative_norm_context(finding)


def _deep_norm_context(case: dict, first_result: dict) -> dict:
    """Repeat local norm lookup and ask the local Norms search for candidates."""
    finding = dict(case.get("finding") or {})
    values = [
        finding.get("norm"),
        finding.get("norm_ref"),
        finding.get("normative_ref"),
        finding.get("problem"),
        finding.get("description"),
        case.get("expert_reason"),
        *(first_result.get("missing_context") or []),
        first_result.get("norm_assessment"),
    ]
    combined = "\n\n".join(
        str(value or "").strip()
        for value in values
        if str(value or "").strip()
    )
    if not combined:
        return {}
    finding["norm"] = combined[:12000]
    semantic_query = " ".join(
        str(value or "").strip()
        for value in (
            finding.get("problem"),
            finding.get("description"),
            case.get("expert_reason"),
            *(first_result.get("missing_context") or []),
        )
        if str(value or "").strip()
    )
    signal = _authoritative_norm_context(
        finding,
        semantic_query=semantic_query,
    )
    if signal.get("kind") in {
        None,
        "",
        "none",
        "unavailable",
        "norm_not_indexed",
        "norm_unsupported",
    }:
        return {}
    has_exact_text = bool(
        signal.get("clauses")
        or signal.get("literal_tables")
        or signal.get("confirmed_absent_locators")
        or signal.get("unavailable_norm_locators")
        or signal.get("semantic_candidates")
        or (signal.get("suggestions") or {}).get("paragraph_text")
    )
    if not has_exact_text:
        return {}
    signal["recovery_query"] = combined[:2000]
    return signal


def build_case_context(
    output_dir: Optional[Path],
    finding: dict,
    *,
    project_id: str,
    section: str,
    max_images: int = 3,
) -> dict:
    """Collect ranked PNGs plus text/OCR context without calling an LLM."""
    max_images = max(0, int(max_images))
    images: list[dict] = []
    all_refs = _block_refs(finding)
    finding_text_parts = _text_evidence(finding)
    text_parts = list(finding_text_parts)
    (
        document_excerpt,
        document_md_path,
        document_pages,
        exact_text_blocks,
    ) = _document_md_context(
        output_dir,
        finding,
    )
    blocks: list[dict] = list(exact_text_blocks)
    if document_excerpt:
        text_parts.append(document_excerpt)
    md_path = document_md_path
    graphic_ids: list[str] = []
    text_ids: list[str] = [
        row["block_id"] for row in exact_text_blocks
    ]
    context_error = ""

    ctx = None
    if output_dir is not None and output_dir.is_dir():
        try:
            from experiments.evidence_agent_v2.context import load_context_from_dir

            ctx = load_context_from_dir(
                output_dir,
                finding,
                project_id=project_id,
                section=section,
                max_blocks=max(max_images, 3),
            )
        except Exception as exc:
            context_error = f"{type(exc).__name__}: {exc}"[:500]

    if ctx is not None:
        graphic_ids = [str(value) for value in (ctx.graphic_block_ids or [])]
        text_ids = list(dict.fromkeys(
            text_ids
            + [str(value) for value in (ctx.text_block_ids or []) if value]
        ))
        if ctx.md_excerpt and not document_excerpt:
            text_parts.append(str(ctx.md_excerpt))
            finding_text_parts.append(str(ctx.md_excerpt))
        if not md_path and ctx.md_path:
            md_path = str(ctx.md_path.resolve())
        existing_block_ids = {row["block_id"] for row in blocks}
        for block in ctx.blocks or []:
            block_id = _canonical_block_id(block.block_id)
            png = (
                block.png_path.resolve()
                if block.png_path and block.png_path.is_file()
                else None
            )
            block_row = {
                "block_id": block_id,
                "page": block.page,
                "ocr_label": str(block.ocr_label or "")[:1000],
                "ocr_or_description": str(block.gemma_text or "")[:3500],
                "image_path": str(png) if png else "",
            }
            if block_id not in existing_block_ids:
                blocks.append(block_row)
                existing_block_ids.add(block_id)
            if png and len(images) < max_images:
                images.append({
                    "path": str(png),
                    "block_id": block_id,
                    "page": block.page,
                })

    # Context helper can miss a direct source_block_id after re-numbering.  Try
    # its deterministic block resolver before giving up on source-linked PNGs.
    existing_image_ids = {row["block_id"] for row in images}
    if output_dir is not None and len(images) < max_images:
        try:
            from experiments.evidence_agent_v2.context import _find_block_png, _gemma_text_for_block

            for block_id in all_refs:
                if len(images) >= max_images:
                    break
                if block_id in existing_image_ids:
                    continue
                png = _find_block_png(output_dir, block_id)
                if not png or not png.is_file():
                    continue
                png = png.resolve()
                text = _gemma_text_for_block(output_dir, block_id)
                blocks.append({
                    "block_id": block_id,
                    "page": None,
                    "ocr_label": "",
                    "ocr_or_description": str(text or "")[:3500],
                    "image_path": str(png),
                })
                images.append({"path": str(png), "block_id": block_id, "page": None})
                existing_image_ids.add(block_id)
        except Exception:
            pass

    unique_text: list[str] = []
    seen_text: set[str] = set()
    for part in text_parts:
        part = str(part or "").strip()
        if not part:
            continue
        marker = part[:240]
        if marker not in seen_text:
            seen_text.add(marker)
            unique_text.append(part)
    text_excerpt = "\n\n---\n\n".join(unique_text)[:12000]
    finding_evidence_text = "\n\n---\n\n".join(finding_text_parts)[:12000]
    document_text_excerpt = str(document_excerpt or "")[:12000]
    norm_context = _safe_norm_context(finding)

    explicit_graphic_refs = {
        str(item.get("block_id") or item.get("id") or "")
        .strip()
        .removeprefix("block_")
        for item in (finding.get("evidence") or [])
        if isinstance(item, dict)
        and str(item.get("type") or "").lower() in {"image", "graphic"}
    }
    expected_graphic_ids = set(graphic_ids) | {
        value for value in explicit_graphic_refs if value
    }
    attached_graphic_ids = {row["block_id"] for row in images}

    if images and text_excerpt:
        route = "mixed"
    elif images:
        route = "graphic"
    elif text_excerpt:
        route = "text"
    elif norm_context:
        route = "norm"
    else:
        route = "missing"

    return {
        "route": route,
        "images": images,
        "blocks": blocks[: max(8, max_images * 2)],
        "text_excerpt": text_excerpt,
        "document_text_path": md_path,
        "finding_evidence_text": finding_evidence_text,
        "document_text_excerpt": document_text_excerpt,
        "document_pages_loaded": document_pages,
        "norm_context": norm_context,
        "graphic_block_ids": graphic_ids,
        "text_block_ids": text_ids,
        "source_block_ids": all_refs,
        "source_block_count": len(all_refs),
        "images_truncated": bool(expected_graphic_ids - attached_graphic_ids),
        "context_error": context_error,
    }


def _retrieval_query(case: dict, first_result: dict) -> str:
    finding = case.get("finding") or {}
    parts: list[str] = []
    for value in first_result.get("missing_context") or []:
        if str(value or "").strip():
            parts.append(str(value).strip())
    for value in (
        first_result.get("reason_assessment"),
        first_result.get("finding_assessment"),
        case.get("expert_reason"),
        finding.get("problem"),
        finding.get("description"),
        finding.get("sheet"),
        finding.get("norm"),
        finding.get("norm_ref"),
        finding.get("normative_ref"),
    ):
        if str(value or "").strip():
            parts.append(str(value).strip())
    return "\n".join(dict.fromkeys(parts))[:12000]


def _retrieval_stem(word: str) -> str:
    normalized = str(word or "").casefold().replace("ё", "е")
    if len(normalized) <= 4:
        return normalized
    for suffix in _RETRIEVAL_RU_SUFFIXES:
        if normalized.endswith(suffix) and len(normalized) - len(suffix) >= 4:
            return normalized[:-len(suffix)]
    return normalized


def _retrieval_terms(value: Any) -> list[str]:
    terms: list[str] = []
    for raw in re.findall(r"[A-Za-zА-Яа-яЁё0-9]+", str(value or "").casefold()):
        if len(raw) < 3 or raw in _RETRIEVAL_STOPWORDS:
            continue
        terms.append(_retrieval_stem(raw))
    return terms


def _requested_sheet_pages(request: str, graph: dict) -> list[int]:
    """Map explicit single, listed or ranged sheet numbers to PDF pages."""
    sheet_numbers: list[str] = []
    pattern = re.compile(
        r"лист(?:ах|ами|ы|а|е|у|ом)?\s*[№#]?\s*"
        r"(\d+(?:\s*(?:[-–—,;]|\bи\b)\s*\d+)*)",
        re.IGNORECASE,
    )
    for match in pattern.finditer(str(request or "")):
        sheet_numbers.extend(_expanded_locator_values(match.group(1)))
    sheet_numbers = list(dict.fromkeys(sheet_numbers))[:24]
    if not sheet_numbers:
        return []
    mapped: list[int] = []
    wanted = set(sheet_numbers)
    for graph_page in graph.get("pages") or []:
        if not isinstance(graph_page, dict):
            continue
        actual_sheet = str(
            graph_page.get("sheet_no_normalized")
            or graph_page.get("sheet_no_raw")
            or ""
        ).strip()
        if actual_sheet not in wanted:
            continue
        try:
            page_number = int(
                graph_page.get("page")
                or graph_page.get("page_number")
                or 0
            )
        except (TypeError, ValueError):
            continue
        if page_number > 0:
            mapped.append(page_number)
    return list(dict.fromkeys(mapped))


def _minimum_ordered_span(tokens: Sequence[str], gram: Sequence[str]) -> int:
    positions = [[index for index, token in enumerate(tokens) if token == wanted] for wanted in gram]
    if any(not values for values in positions):
        return 10**9
    best = 10**9
    for indexes in itertools.product(*positions):
        if list(indexes) != sorted(indexes):
            continue
        best = min(best, indexes[-1] - indexes[0] + 1)
    return best


def _semantic_target_candidates(
    catalog: Sequence[dict],
    requests: Sequence[str],
    *,
    graph: dict,
    target_pages: Sequence[int],
    limit: int,
) -> list[dict]:
    """Rank missing-context targets by rare terms and short ordered phrases.

    The score intentionally uses each missing-context request separately.  A
    long finding/expert narrative otherwise lets repeated page boilerplate
    outrank the exact requested drawing (for example, a furniture plan).
    """
    if limit <= 0:
        return []
    usable: list[dict] = []
    document_terms: list[list[str]] = []
    for row in catalog:
        label = str(row.get("label") or row.get("searchable") or "").strip()
        lowered = label.casefold()
        if not label or (
            (lowered.startswith("{") and '"document_code"' in lowered)
            or any(marker in lowered for marker in (
                "основная надпись", "титульный лист", "пустой фрагмент",
                "пустой участок", "не содержит графических",
            ))
        ):
            continue
        searchable = str(row.get("searchable") or label)[:3500]
        terms = _retrieval_terms(searchable)
        if not terms:
            continue
        usable.append(row)
        document_terms.append(terms)
    if not usable:
        return []

    document_frequency = Counter(
        term for terms in document_terms for term in set(terms)
    )
    document_count = len(usable)
    target_page_set = set(_page_numbers(list(target_pages)))
    ordered: list[dict] = []
    seen: set[str] = set()
    request_values = [str(value).strip() for value in requests if str(value).strip()]
    for request in request_values:
        query_terms = _retrieval_terms(request)
        if not query_terms:
            continue
        query_set = set(query_terms)
        requested_pages = set(_requested_sheet_pages(request, graph))
        triples = [
            (query_terms[first], query_terms[second], query_terms[third])
            for first, second, third in itertools.combinations(range(len(query_terms)), 3)
            if third - first <= 5
            and len({query_terms[first], query_terms[second], query_terms[third]}) == 3
        ]
        scored: list[tuple[float, dict]] = []
        for row, terms in zip(usable, document_terms):
            try:
                page = int(row.get("page") or 0)
            except (TypeError, ValueError):
                page = 0
            if requested_pages and page not in requested_pages:
                continue
            common = query_set & set(terms)
            if not common:
                continue
            score = sum(
                math.log((document_count + 1) / (document_frequency[term] + 1)) + 1.0
                for term in common
            )
            for gram in triples:
                span = _minimum_ordered_span(terms, gram)
                if span <= 4:
                    score += (5 - span) * 1.5
                    score += 0.5 * sum(
                        math.log((document_count + 1) / (document_frequency[term] + 1))
                        for term in gram
                    )
            profile = str(row.get("profile_id") or "").casefold()
            if ({"план", "расположен"} & query_set) and "plan" in profile:
                score += 2.0
            if page in target_page_set:
                score += 0.25
            scored.append((score, row))
        scored.sort(key=lambda item: (
            -item[0],
            int(item[1].get("page") or 0),
            str(item[1].get("block_id") or ""),
        ))
        added_for_request = 0
        per_request_limit = min(8, max(4, limit // max(1, len(request_values))))
        for score, row in scored:
            block_id = _canonical_block_id(row.get("block_id") or row.get("id"))
            if not block_id or block_id in seen:
                continue
            seen.add(block_id)
            ordered.append({**row, "retrieval_score": round(score, 3)})
            added_for_request += 1
            if len(ordered) >= limit:
                return ordered
            if added_for_request >= per_request_limit:
                break
    return ordered


def _load_retrieval_document_graph(output_dir: Path) -> tuple[dict, str]:
    candidates = [output_dir / "document_graph.json"]
    runs_dir = output_dir.parent / "runs"
    if runs_dir.is_dir():
        candidates.extend(
            run_dir / "document_graph.json"
            for run_dir in sorted(runs_dir.iterdir(), reverse=True)
            if run_dir.is_dir()
        )
    version_dir = output_dir.parent.parent if output_dir.name == "latest" else output_dir.parent
    candidates.append(version_dir / "_output" / "document_graph.json")
    for path in candidates:
        payload = _load_json(path)
        if isinstance(payload, dict) and payload.get("pages"):
            return payload, str(path.resolve())
    return {}, ""


def _version_dir_for_case(case: dict, output_dir: Path) -> Optional[Path]:
    raw = str(case.get("version_dir") or "").strip()
    if raw:
        path = Path(raw)
        if path.is_dir():
            return path.resolve()
    if output_dir.name == "latest" and output_dir.parent.name == "03_analysis":
        candidate = output_dir.parent.parent
        if candidate.is_dir():
            return candidate.resolve()
    return None


def _merge_catalog_row(target: dict, incoming: dict) -> None:
    for key, value in incoming.items():
        if value in (None, "", [], {}):
            continue
        if key == "searchable":
            if len(str(value)) > len(str(target.get(key) or "")):
                target[key] = value
        elif target.get(key) in (None, "", [], {}):
            target[key] = value


def _load_input_graphics_catalog(
    case: dict,
    output_dir: Path,
    graph: dict,
) -> tuple[list[dict], list[str]]:
    """Load same-version block metadata, including crop URLs, without network."""
    version_dir = _version_dir_for_case(case, output_dir)
    if version_dir is None:
        return [], []
    candidates = list(sorted((version_dir / "01_input").glob("*_blocks.json")))
    candidates.extend(sorted((version_dir / "01_input").glob("*_result.json")))
    for candidate in (
        version_dir / "01_input" / "result.json",
        version_dir / "02_work" / "result.json",
    ):
        if candidate.is_file():
            candidates.append(candidate)

    catalog: dict[str, dict] = {}
    used_paths: list[str] = []

    def visit(value: Any, inherited: dict) -> None:
        if isinstance(value, list):
            for item in value:
                visit(item, inherited)
            return
        if not isinstance(value, dict):
            return

        meta = dict(inherited)
        for key in ("page_width", "page_height", "page", "page_number", "page_label", "page_index"):
            if value.get(key) not in (None, ""):
                meta[key] = value.get(key)

        block_id = _canonical_block_id(value.get("block_id") or value.get("id"))
        block_type = str(value.get("block_type") or value.get("type") or "").casefold()
        crop_url = str(value.get("crop_url") or "").strip()
        if block_id and (
            crop_url
            or "image" in block_type
            or value.get("coords_px")
            or value.get("coords_norm")
        ):
            page = (
                value.get("page_label")
                or value.get("page")
                or value.get("page_number")
                or meta.get("page_label")
                or meta.get("page")
                or meta.get("page_number")
                or value.get("page_index")
                or meta.get("page_index")
            )
            try:
                page = int(page) if page not in (None, "") else None
                if page == 0:
                    page = 1
            except (TypeError, ValueError):
                page = None
            page_width = value.get("page_width") or meta.get("page_width")
            page_height = value.get("page_height") or meta.get("page_height")
            coords_px = value.get("coords_px") or value.get("bbox") or value.get("coords")
            coords_norm = value.get("coords_norm")
            if (
                not coords_px
                and isinstance(coords_norm, (list, tuple))
                and len(coords_norm) >= 4
                and page_width
                and page_height
            ):
                try:
                    coords_px = [
                        float(coords_norm[0]) * float(page_width),
                        float(coords_norm[1]) * float(page_height),
                        float(coords_norm[2]) * float(page_width),
                        float(coords_norm[3]) * float(page_height),
                    ]
                except (TypeError, ValueError):
                    coords_px = None
            text_parts = [
                str(value.get(key) or "").strip()
                for key in (
                    "ocr_text_normalized",
                    "ocr_raw",
                    "pdfplumber_text",
                    "markdown",
                    "text",
                    "description",
                    "summary",
                )
                if str(value.get(key) or "").strip()
            ]
            row = {
                "block_id": block_id,
                "id": block_id,
                "page": page,
                "block_type": block_type or "image",
                "crop_url": crop_url,
                "image_file": str(value.get("image_file") or ""),
                "coords_px": coords_px,
                "coords_norm": coords_norm,
                "page_width": page_width,
                "page_height": page_height,
                "searchable": "\n\n".join(dict.fromkeys(text_parts))[:12000],
            }
            current = catalog.setdefault(block_id, {"block_id": block_id, "id": block_id})
            _merge_catalog_row(current, row)

        for child in value.values():
            if isinstance(child, (dict, list)):
                visit(child, meta)

    seen_paths: set[Path] = set()
    for path in candidates:
        path = path.resolve()
        if path in seen_paths or not path.is_file():
            continue
        seen_paths.add(path)
        payload = _load_json(path)
        if payload is None:
            continue
        used_paths.append(str(path))
        visit(payload, {})

    for graph_page in graph.get("pages") or []:
        if not isinstance(graph_page, dict):
            continue
        page = graph_page.get("page") or graph_page.get("page_number")
        for key in ("image_blocks", "graphic_blocks"):
            for block in graph_page.get(key) or []:
                if not isinstance(block, dict):
                    continue
                block_id = _canonical_block_id(block.get("block_id") or block.get("id"))
                if not block_id:
                    continue
                searchable = str(
                    block.get("ocr_text_normalized")
                    or block.get("ocr_raw")
                    or block.get("text")
                    or block.get("description")
                    or ""
                )[:12000]
                row = {
                    "block_id": block_id,
                    "id": block_id,
                    "page": block.get("page") or page,
                    "block_type": "image",
                    "coords_px": block.get("coords_px") or block.get("bbox"),
                    "coords_norm": block.get("coords_norm"),
                    "page_width": graph_page.get("page_width"),
                    "page_height": graph_page.get("page_height"),
                    "searchable": searchable,
                    "label": searchable[:1000],
                }
                current = catalog.setdefault(block_id, {"block_id": block_id, "id": block_id})
                _merge_catalog_row(current, row)

    # Some *_result.json records contain a bbox but omit the page raster size.
    # Rehydrate those dimensions from the same-version document graph so the
    # bbox can be mapped back to the canonical PDF instead of being discarded.
    page_geometry: dict[int, tuple[Any, Any]] = {}
    for graph_page in graph.get("pages") or []:
        if not isinstance(graph_page, dict):
            continue
        try:
            page_number = int(
                graph_page.get("page")
                or graph_page.get("page_number")
                or int(graph_page.get("page_index") or 0) + 1
            )
        except (TypeError, ValueError):
            continue
        page_width = graph_page.get("page_width")
        page_height = graph_page.get("page_height")
        if page_width and page_height:
            page_geometry[page_number] = (page_width, page_height)
    for row in catalog.values():
        try:
            page_number = int(row.get("page") or 0)
        except (TypeError, ValueError):
            continue
        geometry = page_geometry.get(page_number)
        if not geometry:
            continue
        if not row.get("page_width"):
            row["page_width"] = geometry[0]
        if not row.get("page_height"):
            row["page_height"] = geometry[1]
        if not row.get("coords_px") and row.get("coords_norm"):
            try:
                coords = [float(value) for value in row["coords_norm"][:4]]
                row["coords_px"] = [
                    coords[0] * float(row["page_width"]),
                    coords[1] * float(row["page_height"]),
                    coords[2] * float(row["page_width"]),
                    coords[3] * float(row["page_height"]),
                ]
            except (TypeError, ValueError, KeyError):
                pass

    return list(catalog.values()), used_paths


def _validate_remote_crop_url(url: str, block_id: str) -> tuple[bool, str, str]:
    from urllib.parse import urlsplit

    try:
        parsed = urlsplit(str(url or ""))
        port = parsed.port
    except ValueError:
        return False, "invalid_url", ""
    host = str(parsed.hostname or "").lower()
    if (
        parsed.scheme.lower() != "https"
        or not host
        or parsed.username
        or parsed.password
        or port not in (None, 443)
        or parsed.query
        or parsed.fragment
        or "%" in parsed.path
        or ".." in parsed.path.split("/")
    ):
        return False, "unsafe_url", host
    if host == "vibe.cloud-ip.cc":
        ok = bool(re.fullmatch(r"/api/crops/[A-Za-z0-9_-]{8,128}", parsed.path))
        return ok, "" if ok else "unexpected_vibe_path", host
    if re.fullmatch(r"pub-[0-9a-f]{32}\.r2\.dev", host):
        expected = _canonical_block_id(Path(parsed.path).stem)
        ok = bool(
            re.fullmatch(
                r"/tree_docs/[0-9a-fA-F-]{36}/crops/[A-Za-z0-9_-]+\.pdf",
                parsed.path,
            )
            and expected == _canonical_block_id(block_id)
        )
        return ok, "" if ok else "unexpected_r2_path", host
    return False, "host_not_allowed", host


def _safe_remote_crop_get(
    url: str,
    block_id: str,
    *,
    budget: Optional[dict] = None,
) -> tuple[int, Optional[str], Optional[bytes]]:
    ok, reason, _host = _validate_remote_crop_url(url, block_id)
    if not ok:
        raise ValueError(reason)
    import httpx

    run_used = int((budget or {}).get("run_used") or 0)
    case_used = int((budget or {}).get("case_used") or 0)
    run_limit = int((budget or {}).get("run_limit") or _REMOTE_CROP_RUN_MAX_BYTES)
    case_limit = int((budget or {}).get("case_limit") or _REMOTE_CROP_CASE_MAX_BYTES)
    allowed_bytes = min(
        _REMOTE_CROP_MAX_BYTES,
        max(0, run_limit - run_used),
        max(0, case_limit - case_used),
    )
    if allowed_bytes <= 0:
        raise ValueError("remote_crop_budget_exhausted")
    disk_path = Path(str((budget or {}).get("disk_path") or "."))
    try:
        if shutil.disk_usage(disk_path).free < _REMOTE_CROP_MIN_FREE_BYTES + allowed_bytes:
            raise ValueError("remote_crop_insufficient_disk_space")
    except OSError as exc:
        raise ValueError("remote_crop_disk_check_failed") from exc

    body = bytearray()
    with httpx.Client(timeout=30, follow_redirects=False) as client:
        with client.stream("GET", url) as response:
            if 300 <= response.status_code < 400:
                raise ValueError("redirect_not_allowed")
            content_length = response.headers.get("content-length")
            try:
                declared_length = int(content_length) if content_length else 0
            except ValueError:
                declared_length = 0
            if declared_length > allowed_bytes:
                raise ValueError("crop_or_budget_too_large")
            for chunk in response.iter_bytes():
                body.extend(chunk)
                if len(body) > allowed_bytes:
                    raise ValueError("crop_or_budget_too_large")
            content_type = response.headers.get("content-type")
            status = response.status_code
    payload = bytes(body)
    if budget is not None:
        budget["run_used"] = run_used + len(payload)
        budget["case_used"] = case_used + len(payload)
    if status == 200 and b"%PDF-" not in payload[:1024]:
        raise ValueError("crop_is_not_pdf")
    return status, "application/pdf" if status == 200 else content_type, payload


def _cached_remote_crop_get(
    url: str,
    block_id: str,
    *,
    budget: Optional[dict] = None,
) -> tuple[int, Optional[str], Optional[bytes]]:
    """Reuse terminal missing responses within one retrieval assembly run.

    Public crop objects are immutable for this workflow. Re-fetching the same
    known-missing URL for many findings cannot improve evidence and can make a
    large local preflight take hours. Transient failures and successful PDFs
    are deliberately not retained in this in-memory negative cache.
    """
    cache = budget.setdefault("terminal_response_cache", {}) if budget is not None else {}
    cache_key = f"{_canonical_block_id(block_id)}\n{url}"
    cached = cache.get(cache_key)
    if cached is not None:
        return cached
    response = _safe_remote_crop_get(url, block_id, budget=budget)
    if response[0] in {404, 410}:
        cache[cache_key] = response
    return response


def _find_source_pdf(version_dir: Optional[Path]) -> Optional[Path]:
    if version_dir is None:
        return None
    candidates = [version_dir / "02_work" / "document.pdf"]
    candidates.extend(sorted((version_dir / "01_input").glob("*.pdf")))
    for path in candidates:
        if path.is_file():
            return path.resolve()
    return None


@lru_cache(maxsize=1)
def _archived_block_png_index() -> dict[str, tuple[str, ...]]:
    """Index exact block-id PNGs retained by local historical experiments."""
    root = ROOT_DIR / "experiments"
    if not root.is_dir():
        return {}
    rows: dict[str, list[str]] = defaultdict(list)
    for path in root.rglob("block_*.png"):
        if not path.is_file():
            continue
        block_id = _canonical_block_id(path.stem.removeprefix("block_"))
        if not block_id:
            continue
        rows[block_id].append(str(path.resolve()))
    return {
        block_id: tuple(sorted(dict.fromkeys(paths)))
        for block_id, paths in rows.items()
    }


@lru_cache(maxsize=2048)
def _verified_archived_block_png(
    block_id: str,
) -> tuple[Optional[str], dict]:
    """Return an archived crop only when every exact-id copy has one hash."""
    canonical = _canonical_block_id(block_id)
    candidates = _archived_block_png_index().get(canonical, ())
    receipt: dict[str, Any] = {
        "status": "not_found",
        "source": "verified_archive_exact_block_id",
        "candidate_count": len(candidates),
    }
    if not candidates:
        return None, receipt
    by_hash: dict[str, list[str]] = defaultdict(list)
    for path_text in candidates:
        path = Path(path_text)
        try:
            digest = hashlib.sha256(path.read_bytes()).hexdigest()
        except OSError:
            continue
        by_hash[digest].append(path_text)
    if len(by_hash) != 1:
        receipt.update({
            "status": "hash_conflict",
            "sha256_values": sorted(by_hash),
        })
        return None, receipt
    digest, paths = next(iter(by_hash.items()))
    receipt.update({
        "status": "ok",
        "sha256": digest,
        "verified_copy_count": len(paths),
    })
    return paths[0], receipt


def _materialize_retrieval_graphic(
    row: dict,
    *,
    output_dir: Path,
    asset_dir: Optional[Path],
    allow_remote_crops: bool,
    source_pdf_path: Optional[Path],
    remote_budget: Optional[dict] = None,
) -> tuple[Optional[dict], dict]:
    block_id = _canonical_block_id(row.get("block_id") or row.get("id"))
    receipt = {"block_id": block_id, "status": "not_found", "source": ""}
    image_path = str(row.get("image_path") or row.get("path") or "")
    if image_path and Path(image_path).is_file():
        result = dict(row)
        result["block_id"] = block_id
        result["image_path"] = str(Path(image_path).resolve())
        receipt.update({"status": "ok", "source": "local_image"})
        return result, receipt

    try:
        from experiments.evidence_agent_v2.context import _find_block_png

        png = _find_block_png(output_dir, block_id)
    except Exception:
        png = None
    if png and png.is_file():
        result = dict(row)
        result["block_id"] = block_id
        result["image_path"] = str(png.resolve())
        receipt.update({"status": "ok", "source": "local_image"})
        return result, receipt

    archived_path, archive_receipt = _verified_archived_block_png(block_id)
    if archived_path and Path(archived_path).is_file():
        result = dict(row)
        result["block_id"] = block_id
        result["image_path"] = archived_path
        receipt.update(archive_receipt)
        return result, receipt
    if archive_receipt.get("status") == "hash_conflict":
        receipt["archive_fallback"] = archive_receipt

    # coords_norm is already sufficient when the canonical PDF is local.  If
    # upstream omitted page_width/page_height, use the actual PDF page box as
    # the coordinate system and let the normal vector crop path continue.
    row = dict(row)
    if (
        source_pdf_path
        and row.get("page") not in (None, "")
        and row.get("coords_norm")
        and not (
            row.get("coords_px")
            and row.get("page_width")
            and row.get("page_height")
        )
    ):
        try:
            fitz = _import_fitz()

            with fitz.open(str(source_pdf_path)) as document:
                page_index = int(row["page"]) - 1
                if 0 <= page_index < document.page_count:
                    rect = document[page_index].rect
                    coords = [float(value) for value in row["coords_norm"][:4]]
                    row["page_width"] = float(rect.width)
                    row["page_height"] = float(rect.height)
                    row["coords_px"] = [
                        coords[0] * float(rect.width),
                        coords[1] * float(rect.height),
                        coords[2] * float(rect.width),
                        coords[3] * float(rect.height),
                    ]
        except (OSError, TypeError, ValueError, ImportError):
            pass

    crop_url = str(row.get("crop_url") or "")
    has_source_geometry = bool(
        source_pdf_path
        and (row.get("coords_px") or row.get("bbox") or row.get("coords"))
        and row.get("page") not in (None, "")
        and row.get("page_width")
        and row.get("page_height")
    )
    if crop_url:
        ok, reason, host = _validate_remote_crop_url(crop_url, block_id)
        receipt.update({
            "source": "remote_crop",
            "host": host,
            "url_sha256": hashlib.sha256(crop_url.encode("utf-8")).hexdigest(),
        })
        if not ok:
            if not has_source_geometry:
                receipt.update({"status": "rejected", "reason": reason})
                return None, receipt
            receipt.update({"reason": reason, "remote_status": "rejected"})
            crop_url = ""
    elif has_source_geometry:
        receipt["source"] = "source_pdf"
    else:
        return None, receipt
    if asset_dir is None:
        receipt.update({"status": "asset_dir_missing"})
        return None, receipt
    if not allow_remote_crops and not has_source_geometry:
        receipt.update({"status": "network_disabled"})
        return None, receipt

    try:
        _import_fitz()
        from backend.app.services.common.block_pdf_source import (
            render_block_pdf,
            resolve_block_pdf_source,
        )

        asset_dir.mkdir(parents=True, exist_ok=True)
        source = resolve_block_pdf_source(
            {
                **row,
                "id": block_id,
                "block_id": block_id,
                "page": row.get("page"),
                "crop_url": crop_url,
            },
            cache_dir=asset_dir / "block_pdfs",
            prefer_source_pdf=has_source_geometry,
            http_get=(
                (lambda url: _cached_remote_crop_get(url, block_id, budget=remote_budget))
                if remote_budget is not None
                else (lambda url: _safe_remote_crop_get(url, block_id))
            ),
            allow_download=bool(allow_remote_crops),
            source_pdf_path=source_pdf_path,
        )
        if not source.ok or source.pdf_path is None:
            receipt.update({
                "status": "unavailable",
                "http_status": source.crop_url_status,
                "error": source.error or "",
            })
            return None, receipt
        try:
            pdf_size = source.pdf_path.stat().st_size
            pdf_magic = source.pdf_path.read_bytes()[:1024]
        except OSError as exc:
            receipt.update({"status": "invalid_pdf_cache", "error": type(exc).__name__})
            return None, receipt
        if (
            pdf_size <= 0
            or pdf_size > _REMOTE_CROP_MAX_BYTES
            or b"%PDF-" not in pdf_magic
        ):
            receipt.update({
                "status": "invalid_pdf_cache",
                "bytes": pdf_size,
                "cache_hit": bool(source.cache_hit),
            })
            return None, receipt
        safe_id = re.sub(r"[^A-Za-z0-9_-]+", "_", block_id)[:80] or "block"
        rendered = render_block_pdf(
            source.pdf_path,
            long_side=2600,
            out_path=asset_dir / "rendered" / f"{safe_id}.png",
        )
        if not rendered.ok or rendered.png_path is None:
            receipt.update({"status": "render_failed", "error": rendered.error or ""})
            return None, receipt
        result = dict(row)
        result["block_id"] = block_id
        result["image_path"] = str(rendered.png_path.resolve())
        receipt.update({
            "status": "ok",
            "source": source.source,
            "cache_hit": bool(source.cache_hit),
            "width": rendered.width,
            "height": rendered.height,
        })
        return result, receipt
    except Exception as exc:
        receipt.update({"status": "error", "error": f"{type(exc).__name__}: {exc}"[:300]})
        return None, receipt


def _spatial_page_excerpt(graph: dict, pages: Sequence[int], *, max_chars: int = 10000) -> str:
    wanted = set(_page_numbers(list(pages)))
    chunks: list[str] = []
    for graph_page in graph.get("pages") or []:
        if not isinstance(graph_page, dict):
            continue
        page = graph_page.get("page") or graph_page.get("page_number")
        try:
            page = int(page)
        except (TypeError, ValueError):
            continue
        if page not in wanted:
            continue
        header = (
            f"Пространственный контекст PDF-страницы {page}: "
            f"лист {graph_page.get('sheet_no_raw') or graph_page.get('sheet_no_normalized') or 'не указан'}; "
            f"наименование: {graph_page.get('sheet_name') or 'не указано'}; "
            f"размер: {graph_page.get('page_width') or '?'}x{graph_page.get('page_height') or '?'}."
        )
        rows = [header]
        for key in ("image_blocks", "graphic_blocks", "text_blocks"):
            for block in graph_page.get(key) or []:
                if not isinstance(block, dict):
                    continue
                block_id = _canonical_block_id(block.get("block_id") or block.get("id"))
                if not block_id:
                    continue
                bbox = block.get("coords_norm") or block.get("coords_px") or block.get("bbox")
                text = str(
                    block.get("ocr_text_normalized")
                    or block.get("ocr_raw")
                    or block.get("text")
                    or ""
                )
                text = re.sub(r"\s+", " ", text).strip()[:700]
                rows.append(f"- блок {block_id}; координаты {bbox}; {text}")
        chunks.append("\n".join(rows))
    return _bounded_md_excerpt("\n\n".join(chunks), max_chars)


def _requested_full_page_target(
    query: str,
    graph: dict,
    preferred_pages: Sequence[int],
) -> tuple[Optional[int], str, dict]:
    receipt: dict[str, Any] = {"status": "not_requested"}
    full_request = re.search(
        r"пол(?:ный|ное|ная)\s+(?:читаем\w+\s+)?(?:изображение\s+)?"
        r"(?:pdf[- ]?)?(?:лист(?:а|е)?|страниц(?:а|ы|у))\s*[№#]?\s*(\d+)",
        query,
        re.IGNORECASE,
    )
    if not full_request and not re.search(r"штамп", query, re.IGNORECASE):
        return None, "", receipt
    requested_sheet = full_request.group(1) if full_request else ""
    if not requested_sheet:
        generic = re.search(r"лист(?:е|а|ы|у|ом)?\s*[№#]?\s*(\d+)", query, re.IGNORECASE)
        requested_sheet = generic.group(1) if generic else ""
    if not requested_sheet:
        receipt["status"] = "sheet_not_specified"
        return None, "", receipt

    candidate_pages: list[int] = []
    for graph_page in graph.get("pages") or []:
        if not isinstance(graph_page, dict):
            continue
        sheet_no = str(
            graph_page.get("sheet_no_normalized")
            or graph_page.get("sheet_no_raw")
            or ""
        ).strip()
        if sheet_no != requested_sheet:
            continue
        try:
            candidate_pages.append(int(graph_page.get("page") or graph_page.get("page_number")))
        except (TypeError, ValueError):
            continue
    candidate_pages = list(dict.fromkeys(candidate_pages))
    preferred = [page for page in _page_numbers(list(preferred_pages)) if page in candidate_pages]
    if len(preferred) == 1:
        page_number = preferred[0]
    elif len(candidate_pages) == 1:
        page_number = candidate_pages[0]
    elif not candidate_pages:
        receipt.update({"status": "sheet_not_mapped", "sheet": requested_sheet})
        return None, requested_sheet, receipt
    else:
        receipt.update({
            "status": "ambiguous_sheet_mapping",
            "sheet": requested_sheet,
            "candidate_pages": candidate_pages,
            "preferred_pages": _page_numbers(list(preferred_pages)),
        })
        return None, requested_sheet, receipt
    return page_number, requested_sheet, {
        "status": "target_resolved",
        "sheet": requested_sheet,
        "page": page_number,
    }


def _render_requested_full_page(
    *,
    query: str,
    graph: dict,
    source_pdf_path: Optional[Path],
    asset_dir: Optional[Path],
    preferred_pages: Sequence[int] = (),
) -> tuple[Optional[dict], dict]:
    page_number, requested_sheet, receipt = _requested_full_page_target(
        query,
        graph,
        preferred_pages,
    )
    if page_number is None:
        return None, receipt
    if asset_dir is None:
        receipt["status"] = "asset_dir_missing"
        return None, receipt
    if source_pdf_path is None:
        receipt["status"] = "source_pdf_missing"
        return None, receipt
    try:
        fitz = _import_fitz()

        with fitz.open(str(source_pdf_path)) as document:
            index = page_number - 1
            if index < 0 or index >= document.page_count:
                receipt["status"] = "page_out_of_range"
                return None, receipt
            page = document[index]
            scale = min(4.0, max(1.0, 1800 / max(page.rect.width, page.rect.height)))
            pixmap = page.get_pixmap(matrix=fitz.Matrix(scale, scale), alpha=False)
            output = asset_dir / "full_pages" / f"page_{page_number}.png"
            output.parent.mkdir(parents=True, exist_ok=True)
            pixmap.save(str(output))
        row = {
            "block_id": f"full_page_{page_number}",
            "page": page_number,
            "label": f"Полная PDF-страница {page_number}",
            "searchable": (
                f"Полная PDF-страница {page_number}; номер листа {requested_sheet}; "
                "сохранена для пространственной проверки."
            ),
            "image_path": str(output.resolve()),
        }
        receipt.update({
            "status": "ok",
            "width": pixmap.width,
            "height": pixmap.height,
        })
        return row, receipt
    except Exception as exc:
        receipt.update({"status": "error", "error": f"{type(exc).__name__}: {exc}"[:300]})
        return None, receipt


def _render_pdf_page(
    *,
    page_number: int,
    source_pdf_path: Optional[Path],
    asset_dir: Optional[Path],
) -> tuple[Optional[dict], dict]:
    """Render one selected page from the case's canonical source PDF."""
    receipt: dict[str, Any] = {"status": "not_attempted", "page": int(page_number)}
    if asset_dir is None:
        receipt["status"] = "asset_dir_missing"
        return None, receipt
    if source_pdf_path is None:
        receipt["status"] = "source_pdf_missing"
        return None, receipt
    try:
        fitz = _import_fitz()

        with fitz.open(str(source_pdf_path)) as document:
            index = int(page_number) - 1
            if index < 0 or index >= document.page_count:
                receipt["status"] = "page_out_of_range"
                return None, receipt
            page = document[index]
            scale = min(4.0, max(1.0, 1800 / max(page.rect.width, page.rect.height)))
            pixmap = page.get_pixmap(matrix=fitz.Matrix(scale, scale), alpha=False)
            output = asset_dir / "full_pages" / f"page_{page_number}.png"
            output.parent.mkdir(parents=True, exist_ok=True)
            pixmap.save(str(output))
        row = {
            "block_id": f"full_page_{page_number}",
            "page": int(page_number),
            "label": f"Полная PDF-страница {page_number}",
            "searchable": (
                f"Полная PDF-страница {page_number} канонического исходного PDF; "
                "добавлена автономным восстановлением контекста."
            ),
            "image_path": str(output.resolve()),
        }
        receipt.update({"status": "ok", "width": pixmap.width, "height": pixmap.height})
        return row, receipt
    except Exception as exc:
        receipt.update({"status": "error", "error": f"{type(exc).__name__}: {exc}"[:300]})
        return None, receipt


def _render_spatial_page_composite(
    *,
    page_number: int,
    sheet_number: str,
    graph: dict,
    catalog: Sequence[dict],
    output_dir: Path,
    asset_dir: Path,
    allow_remote_crops: bool,
    source_pdf_path: Optional[Path],
    remote_budget: Optional[dict],
    max_blocks: int = 12,
) -> tuple[Optional[dict], dict, list[dict]]:
    """Rebuild a page-layout preview from same-page crops when source PDF is absent."""
    receipt: dict[str, Any] = {
        "status": "composite_not_built",
        "page": page_number,
        "sheet": sheet_number,
    }
    graph_page = next((
        row for row in (graph.get("pages") or [])
        if isinstance(row, dict)
        and int(row.get("page") or row.get("page_number") or 0) == int(page_number)
    ), None)
    if not graph_page:
        receipt["status"] = "composite_page_missing"
        return None, receipt, []
    try:
        page_width = int(graph_page.get("page_width") or 0)
        page_height = int(graph_page.get("page_height") or 0)
    except (TypeError, ValueError):
        page_width = page_height = 0
    if page_width <= 0 or page_height <= 0:
        receipt["status"] = "composite_dimensions_missing"
        return None, receipt, []

    try:
        from PIL import Image, ImageDraw
    except ImportError:
        receipt["status"] = "pillow_missing"
        return None, receipt, []
    scale = 2600.0 / max(page_width, page_height)
    canvas_width = max(1, int(round(page_width * scale)))
    canvas_height = max(1, int(round(page_height * scale)))
    canvas = Image.new("RGB", (canvas_width, canvas_height), "white")
    draw = ImageDraw.Draw(canvas)

    def normalized_bbox(row: dict) -> Optional[tuple[int, int, int, int]]:
        coords = row.get("coords_norm")
        if isinstance(coords, (list, tuple)) and len(coords) >= 4:
            try:
                raw = [float(coords[index]) for index in range(4)]
                x0, y0, x1, y1 = (
                    raw[0] * canvas_width,
                    raw[1] * canvas_height,
                    raw[2] * canvas_width,
                    raw[3] * canvas_height,
                )
            except (TypeError, ValueError):
                return None
        else:
            coords = row.get("coords_px") or row.get("bbox") or row.get("coords")
            if not isinstance(coords, (list, tuple)) or len(coords) < 4:
                return None
            try:
                raw = [float(coords[index]) for index in range(4)]
                x0, y0, x1, y1 = (
                    raw[0] * scale,
                    raw[1] * scale,
                    raw[2] * scale,
                    raw[3] * scale,
                )
            except (TypeError, ValueError):
                return None
        left = max(0, min(canvas_width - 1, int(round(min(x0, x1)))))
        top = max(0, min(canvas_height - 1, int(round(min(y0, y1)))))
        right = max(left + 1, min(canvas_width, int(round(max(x0, x1)))))
        bottom = max(top + 1, min(canvas_height, int(round(max(y0, y1)))))
        return left, top, right, bottom

    page_rows = []
    for row in catalog:
        try:
            row_page = int(row.get("page") or 0)
        except (TypeError, ValueError):
            continue
        bbox = normalized_bbox(row)
        if row_page == int(page_number) and bbox is not None:
            page_rows.append((row, bbox))
    page_rows.sort(key=lambda item: (
        -((item[1][2] - item[1][0]) * (item[1][3] - item[1][1])),
        str(item[0].get("block_id") or ""),
    ))

    included: list[str] = []
    placeholders: list[str] = []
    material_receipts: list[dict] = []
    for row, bbox in page_rows[:max_blocks]:
        block_id = _canonical_block_id(row.get("block_id") or row.get("id"))
        if not block_id:
            continue
        materialized, material_receipt = _materialize_retrieval_graphic(
            {**row, "block_id": block_id},
            output_dir=output_dir,
            asset_dir=asset_dir,
            allow_remote_crops=allow_remote_crops,
            source_pdf_path=source_pdf_path,
            remote_budget=remote_budget,
        )
        material_receipts.append(material_receipt)
        left, top, right, bottom = bbox
        if materialized is not None:
            try:
                with Image.open(str(materialized.get("image_path") or "")) as source_image:
                    block_image = source_image.convert("RGB")
                    block_image.thumbnail((right - left, bottom - top), Image.Resampling.LANCZOS)
                    paste_x = left + max(0, (right - left - block_image.width) // 2)
                    paste_y = top + max(0, (bottom - top - block_image.height) // 2)
                    canvas.paste(block_image, (paste_x, paste_y))
                included.append(block_id)
            except (OSError, ValueError):
                materialized = None
        if materialized is None:
            draw.rectangle(bbox, outline="#888888", width=2)
            marker = f"{block_id} / {str(row.get('block_type') or 'BLOCK').upper()} / SHEET {sheet_number}"
            draw.text((left + 4, top + 4), marker[:120], fill="#333333")
            placeholders.append(block_id)
        else:
            draw.rectangle(bbox, outline="#666666", width=1)

    if len(included) < 2:
        receipt.update({
            "status": "composite_insufficient_blocks",
            "included_block_ids": included,
            "placeholder_block_ids": placeholders,
        })
        return None, receipt, material_receipts
    output = asset_dir / "full_pages" / f"page_{page_number}_composite.png"
    output.parent.mkdir(parents=True, exist_ok=True)
    canvas.save(output, format="PNG", optimize=True)
    row = {
        "block_id": f"full_page_{page_number}_composite",
        "page": page_number,
        "label": f"Пространственный композит PDF-страницы {page_number}",
        "searchable": (
            f"Пространственный композит листа {sheet_number}, PDF-страница {page_number}; "
            f"включены блоки: {', '.join(included)}; placeholders: {', '.join(placeholders)}."
        ),
        "image_path": str(output.resolve()),
    }
    receipt.update({
        "status": "composite_ok",
        "width": canvas_width,
        "height": canvas_height,
        "included_block_ids": included,
        "placeholder_block_ids": placeholders,
    })
    return row, receipt, material_receipts


def _freeze_context_images(context: dict, asset_dir: Path) -> list[dict]:
    """Copy every model-visible image under the child audit snapshot."""
    lexical_asset_dir = Path(os.path.abspath(asset_dir))
    asset_dir.mkdir(parents=True, exist_ok=True)
    resolved_asset_dir = asset_dir.resolve()
    if lexical_asset_dir != resolved_asset_dir:
        raise ValueError("context asset directory must not traverse symlinks")
    image_dir = resolved_asset_dir / "images"
    image_dir.mkdir(parents=True, exist_ok=True)
    if image_dir.resolve() != image_dir:
        raise ValueError("context image directory must not be a symlink")

    frozen: list[dict] = []
    kept_images: list[dict] = []
    path_map: dict[str, str] = {}
    for index, image in enumerate(context.get("images") or [], start=1):
        if not isinstance(image, dict):
            continue
        try:
            source = Path(str(image.get("path") or "")).resolve(strict=True)
        except (OSError, RuntimeError):
            continue
        if not source.is_file():
            continue
        block_id = _canonical_block_id(image.get("block_id"))
        safe_id = re.sub(r"[^A-Za-z0-9_-]+", "_", block_id)[:80] or f"image_{index}"
        suffix = source.suffix.lower() if source.suffix.lower() in {".png", ".jpg", ".jpeg", ".webp"} else ".png"
        destination = image_dir / f"{index:02d}_{safe_id}{suffix}"
        if destination.exists() and destination.is_symlink():
            raise ValueError("context image destination must not be a symlink")
        try:
            if source != destination.resolve(strict=False):
                try:
                    os.link(source, destination)
                except OSError:
                    shutil.copy2(source, destination)
            destination = destination.resolve(strict=True)
            if not destination.is_relative_to(resolved_asset_dir):
                raise ValueError("frozen image escaped context asset directory")
            digest = hashlib.sha256(destination.read_bytes()).hexdigest()
        except (OSError, RuntimeError):
            continue
        old_path = str(source)
        frozen_image = dict(image)
        frozen_image["path"] = str(destination)
        frozen_image["block_id"] = block_id
        kept_images.append(frozen_image)
        path_map[old_path] = str(destination)
        frozen.append({
            "block_id": block_id,
            "page": image.get("page"),
            "path": str(destination),
            "sha256": digest,
            "bytes": destination.stat().st_size,
        })
    context["images"] = kept_images
    for block in context.get("blocks") or []:
        if not isinstance(block, dict):
            continue
        try:
            old_path = str(Path(str(block.get("image_path") or "")).resolve(strict=True))
        except (OSError, RuntimeError):
            old_path = ""
        if old_path in path_map:
            block["image_path"] = path_map[old_path]
    return frozen


def _explicit_block_ids(value: str) -> list[str]:
    matches = re.findall(
        r"(?<![A-Za-z0-9_])(?:[A-Z0-9]{3,5}(?:-[A-Z0-9]{3,5}){2}|blk_[A-Za-z0-9_-]{2,80})(?![A-Za-z0-9_])",
        str(value or ""),
        flags=re.IGNORECASE,
    )
    return list(dict.fromkeys(_canonical_block_id(match) for match in matches))


def _retrieval_analysis_dir(output_dir: Path) -> Optional[Path]:
    """Resolve the enclosing ``03_analysis`` directory without leaving a version."""
    if output_dir.name == "latest" and output_dir.parent.name == "03_analysis":
        return output_dir.parent
    if output_dir.parent.name == "runs" and output_dir.parent.parent.name == "03_analysis":
        return output_dir.parent.parent
    if output_dir.name == "03_analysis":
        return output_dir
    return None


def _find_retrieval_vector_graph(
    output_dir: Path,
    block_id: str,
) -> tuple[dict, str]:
    """Load an explicitly named vector block from the same document version."""
    block_id = _canonical_block_id(block_id)
    if not (
        _MODERN_BLOCK_ID_RE.fullmatch(block_id)
        or _LEGACY_BLOCK_ID_RE.fullmatch(block_id)
    ):
        return {}, ""
    analysis_dir = _retrieval_analysis_dir(output_dir)
    if analysis_dir is None:
        return {}, ""

    candidates = [output_dir / "block_vector_graphs" / f"{block_id}.json"]
    latest_dir = analysis_dir / "latest"
    if latest_dir != output_dir:
        candidates.append(latest_dir / "block_vector_graphs" / f"{block_id}.json")
    runs_dir = analysis_dir / "runs"
    if runs_dir.is_dir():
        candidates.extend(
            run_dir / "block_vector_graphs" / f"{block_id}.json"
            for run_dir in sorted(runs_dir.iterdir(), reverse=True)
            if run_dir.is_dir()
        )
    candidates.append(
        analysis_dir.parent / "_output" / "block_vector_graphs" / f"{block_id}.json"
    )

    seen: set[Path] = set()
    for path in candidates:
        if path in seen:
            continue
        seen.add(path)
        payload = _load_json(path)
        if not isinstance(payload, dict):
            continue
        payload_block_id = _canonical_block_id(payload.get("block_id"))
        if payload_block_id and payload_block_id != block_id:
            continue
        if any(
            str(payload.get(key) or "").strip()
            for key in ("markdown", "user_text")
        ) or isinstance(payload.get("classification"), dict):
            return payload, str(path.resolve())
    return {}, ""


def _retrieval_vector_excerpt(payload: dict, block_id: str) -> str:
    if not isinstance(payload, dict) or not payload:
        return ""
    classification = payload.get("classification")
    if not isinstance(classification, dict):
        classification = {}
    page = payload.get("page")
    header_parts = [f"Векторное описание блока {block_id}"]
    if page not in (None, ""):
        header_parts.append(f"страница PDF {page}")
    if payload.get("profile_id"):
        header_parts.append(f"профиль {payload['profile_id']}")

    parts = ["; ".join(header_parts)]
    for key in ("block_title", "block_type", "short_description", "description"):
        value = str(classification.get(key) or "").strip()
        if value:
            parts.append(value)
    vector_text = str(payload.get("user_text") or payload.get("markdown") or "").strip()
    if vector_text:
        parts.append(vector_text)

    unique_parts: list[str] = []
    seen_parts: set[str] = set()
    for part in parts:
        normalized = re.sub(r"\s+", " ", part).strip().casefold()
        if not normalized or normalized in seen_parts:
            continue
        seen_parts.add(normalized)
        unique_parts.append(part)
    return _bounded_md_excerpt("\n\n".join(unique_parts), 5000)


def _collect_retrieval_graphics(case: dict) -> list[dict]:
    try:
        from backend.app.services.section_optimization_graphics_agent_service import (
            collect_graphics_catalog,
        )

        return collect_graphics_catalog(
            str(case.get("project_id") or case.get("document") or ""),
            str(case.get("version_id") or ""),
            object_id=str(case.get("object_id") or ""),
        )
    except Exception:
        return []


def _object_dir_for_version(version_dir: Optional[Path]) -> Optional[Path]:
    if version_dir is None:
        return None
    for parent in (version_dir, *version_dir.parents):
        if parent.parent.name == "objects":
            return parent
    return None


@lru_cache(maxsize=64)
def _related_document_graph_paths(object_dir_text: str) -> tuple[str, ...]:
    """Return one canonical graph per related document in the same object."""
    object_dir = Path(object_dir_text)
    selected: list[str] = []
    for document_dir in sorted(object_dir.glob("disciplines/*/documents/*")):
        if not document_dir.is_dir():
            continue
        metadata = _load_json(document_dir / "document.json")
        current_version = (
            str(metadata.get("current_version") or "")
            if isinstance(metadata, dict)
            else ""
        )
        version_dirs = [
            path for path in (document_dir / "versions").glob("v*") if path.is_dir()
        ]
        version_dirs.sort(
            key=lambda path: (path.name == current_version, path.name),
            reverse=True,
        )
        for version_dir in version_dirs:
            graph_path = version_dir / "03_analysis" / "latest" / "document_graph.json"
            if graph_path.is_file():
                selected.append(str(graph_path.resolve()))
                break
    return tuple(selected)



def _requested_related_disciplines(query: str) -> list[str]:
    requested: list[str] = []
    for pattern, disciplines in _RELATED_DISCIPLINE_CUES:
        if pattern.search(str(query or "")):
            requested.extend(disciplines)
    return list(dict.fromkeys(requested))

def _related_document_context(
    case: dict,
    query: str,
    *,
    version_dir: Optional[Path],
    max_documents: int = 12,
    max_chars: int = 24000,
) -> tuple[str, dict]:
    """Search bounded canonical snapshots of referenced documents in one object."""
    receipt: dict[str, Any] = {
        "scope": "same_object_related_documents_reference_only",
        "status": "not_attempted",
        "searched": 0,
        "selected": [],
    }
    object_dir = _object_dir_for_version(version_dir)
    if object_dir is None:
        receipt["status"] = "object_dir_missing"
        return "", receipt
    current_output = Path(str(case.get("output_dir") or "")).resolve()
    query_terms = set(_retrieval_terms(query))
    discipline = str(case.get("discipline") or "").upper()
    requested_disciplines = set(_requested_related_disciplines(query))
    receipt["requested_disciplines"] = sorted(requested_disciplines)
    cross_document_cue = bool(re.search(
        r"смежн|друг(?:ой|ого|ом)|раздел|том|комплект|спецификац|ведомост|"
        r"пояснительн|архитектур|конструктив|\bпз\b|\bпзу\b|\bиос\b|"
        r"\bппр\b|\bэом\b|экспертиз",
        query,
        re.IGNORECASE,
    ))
    ranked: list[tuple[float, str]] = []
    for path_text in _related_document_graph_paths(str(object_dir.resolve())):
        path = Path(path_text)
        if path.parent == current_output:
            continue
        try:
            documents_index = path.parts.index("documents")
            document_name = path.parts[documents_index + 1]
            candidate_discipline = path.parts[documents_index - 1]
        except (ValueError, IndexError):
            continue
        name_terms = set(_retrieval_terms(document_name))
        overlap = len(query_terms & name_terms)
        exact_name = document_name.casefold() in query.casefold()
        candidate_code = candidate_discipline.upper()
        score = overlap * 4.0 + (20.0 if exact_name else 0.0)
        if candidate_code in requested_disciplines:
            score += 12.0
        elif requested_disciplines and overlap == 0:
            continue
        if cross_document_cue and candidate_code == discipline and overlap:
            score += 1.0
        if score > 0:
            ranked.append((score, path_text))
    ranked.sort(key=lambda item: (-item[0], item[1]))
    if not ranked:
        receipt["status"] = "no_related_document_candidate"
        return "", receipt

    parts: list[str] = []
    remaining = max_chars
    for _score, path_text in ranked[:max_documents]:
        graph = _load_json(Path(path_text))
        if not isinstance(graph, dict) or not graph.get("pages"):
            continue
        receipt["searched"] += 1
        try:
            from backend.app.pipeline.stages.block_analysis.document_retrieval import (
                retrieve_document_context,
            )

            excerpt, hit_receipt = retrieve_document_context(
                graph,
                query,
                0,
                max_hits=4,
                max_chars=min(8000, remaining),
            )
        except Exception:
            continue
        if not excerpt or not int(hit_receipt.get("selected_hits") or 0):
            continue
        path = Path(path_text)
        try:
            documents_index = path.parts.index("documents")
            document_name = path.parts[documents_index + 1]
            candidate_discipline = path.parts[documents_index - 1]
            version_name = path.parts[documents_index + 3]
        except (ValueError, IndexError):
            document_name = path.parent.name
            candidate_discipline = ""
            version_name = ""
        header = (
            f"[СВЯЗАННЫЙ ДОКУМЕНТ: {document_name}; раздел {candidate_discipline}; "
            f"версия {version_name}; источник {path_text}]"
        )
        chunk = f"{header}\n{excerpt}"
        if len(chunk) > remaining:
            chunk = _bounded_md_excerpt(chunk, remaining)
        parts.append(chunk)
        remaining -= len(chunk)
        receipt["selected"].append({
            "source_id": (
                f"related:{candidate_discipline}:{document_name}:{version_name}"
            ),
            "object_id": str(case.get("object_id") or ""),
            "document": document_name,
            "discipline": candidate_discipline,
            "version": version_name,
            "version_relation": "current_snapshot",
            "graph_path": path_text,
            "pages": _page_numbers(hit_receipt.get("selected_pages") or []),
            "hits": int(hit_receipt.get("selected_hits") or 0),
            "excerpt": excerpt,
        })
        if remaining <= 1000:
            break
    receipt["status"] = "ok" if parts else "no_hits"
    return "\n\n--- СВЯЗАННЫЙ ДОКУМЕНТ ---\n\n".join(parts), receipt


def _graph_page_number(page: dict) -> int:
    try:
        raw = page.get("page")
        if raw is None:
            raw = int(page.get("page_index") or 0) + 1
        return int(raw)
    except (TypeError, ValueError):
        return 0


def _graph_page_searchable_text(page: dict, *, max_chars: int = 16000) -> str:
    parts = [
        str(page.get("sheet_name") or ""),
        str(page.get("sheet_no_raw") or page.get("sheet_no_normalized") or ""),
    ]
    for key in ("text_blocks", "image_blocks", "graphic_blocks"):
        for block in page.get(key) or []:
            if not isinstance(block, dict):
                continue
            for field in (
                "text", "ocr_text_normalized", "ocr_raw", "pdfplumber_text",
                "markdown", "description", "summary",
            ):
                value = str(block.get(field) or "").strip()
                if value:
                    parts.append(value)
    return _bounded_md_excerpt(
        "\n".join(dict.fromkeys(part for part in parts if part)),
        max_chars,
    )


def _tail_specification_context(
    graph: dict,
    query: str,
    *,
    max_tail_pages: int = 12,
    max_selected_pages: int = 3,
    max_chars: int = 18000,
) -> tuple[str, dict]:
    """Inspect the end of a drawing set whenever a schedule is requested."""
    receipt: dict[str, Any] = {
        "status": "not_requested",
        "scope": "tail_pages_of_same_version_pdf",
        "scanned_pages": [],
        "selected_pages": [],
        "fallback_last_pages": False,
    }
    if not _SPECIFICATION_REQUEST_RE.search(str(query or "")):
        return "", receipt
    pages = [
        page for page in (graph.get("pages") or [])
        if isinstance(page, dict) and _graph_page_number(page) > 0
    ]
    pages.sort(key=_graph_page_number)
    tail = pages[-max(1, int(max_tail_pages)):]
    receipt["scanned_pages"] = [_graph_page_number(page) for page in tail]
    query_terms = set(_retrieval_terms(query))
    ranked: list[tuple[float, int, str, dict]] = []
    for tail_index, page in enumerate(tail, start=1):
        page_number = _graph_page_number(page)
        text = _graph_page_searchable_text(page)
        lowered = text.casefold()
        keyword_hits = len(_SPECIFICATION_PAGE_RE.findall(text))
        overlap = len(query_terms & set(_retrieval_terms(text)))
        score = keyword_hits * 8.0 + min(overlap, 12) * 2.0 + tail_index / 100.0
        if keyword_hits or overlap >= 2:
            ranked.append((score, page_number, text, page))
    ranked.sort(key=lambda item: (-item[0], -item[1]))
    if not ranked:
        receipt["fallback_last_pages"] = True
        ranked = [
            (0.0, _graph_page_number(page), _graph_page_searchable_text(page), page)
            for page in tail[-2:]
        ]
    chosen = sorted(
        ranked[:max(1, int(max_selected_pages))],
        key=lambda item: item[1],
    )
    parts: list[str] = []
    remaining = max_chars
    for _score, page_number, text, page in chosen:
        sheet = str(
            page.get("sheet_no_normalized")
            or page.get("sheet_no_raw")
            or ""
        ).strip()
        name = str(page.get("sheet_name") or "").strip()
        header = (
            f"[КОНЕЦ ДОКУМЕНТА; стр. PDF {page_number}"
            + (f"; лист {sheet}" if sheet else "")
            + (f"; {name}" if name else "")
            + "]"
        )
        chunk = f"{header}\n{text or '(текстовый слой пуст; требуется изображение страницы)'}"
        chunk = _bounded_md_excerpt(chunk, remaining)
        parts.append(chunk)
        remaining -= len(chunk)
        if remaining <= 500:
            break
    receipt["selected_pages"] = [item[1] for item in chosen]
    receipt["status"] = "selected" if chosen else "document_has_no_pages"
    receipt["text_chars"] = sum(len(part) for part in parts)
    return "\n\n--- ПОСЛЕДНИЕ ЛИСТЫ / СПЕЦИФИКАЦИИ ---\n\n".join(parts), receipt


def _retrieval_input_hash(
    case: dict,
    *,
    contract_version: str = AUTO_RETRIEVAL_CONTRACT_VERSION,
) -> str:
    payload = {
        "source_input_hash": case.get("source_input_hash"),
        "retrieval_contract": contract_version,
        "expert_reason": case.get("expert_reason"),
        "expert_timestamp": case.get("expert_timestamp"),
        "decision_origin": case.get("decision_origin"),
        "source_quality": case.get("source_quality"),
        "finding": case.get("finding") or {},
        "previous_audit": case.get("previous_audit") or {},
        "context": case.get("context") or {},
    }
    return hashlib.sha256(
        json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
    ).hexdigest()


def enrich_case_for_retrieval(
    case: dict,
    first_result: dict,
    *,
    max_images: int = 6,
    asset_dir: Optional[Path] = None,
    allow_remote_crops: bool = False,
    remote_budget: Optional[dict] = None,
    recovery_mode: bool = False,
) -> tuple[dict, dict]:
    """Autonomously collect same-version context without mutating source data."""
    enriched = json.loads(json.dumps(case, ensure_ascii=False))
    context = enriched.setdefault("context", {})
    finding = case.get("finding") or {}
    query = _retrieval_query(case, first_result)
    missing_requests = [
        str(value).strip()
        for value in (first_result.get("missing_context") or [])
        if str(value or "").strip()
    ]
    if recovery_mode and not missing_requests:
        missing_requests = [
            str(value).strip()
            for value in (
                first_result.get("reason_assessment"),
                first_result.get("finding_assessment"),
                first_result.get("norm_assessment"),
                finding.get("problem"),
            )
            if str(value or "").strip()
        ]
        query = "\n".join(dict.fromkeys([query, *missing_requests]))
    output_dir_text = str(case.get("output_dir") or "").strip()
    output_dir = Path(output_dir_text) if output_dir_text else Path()

    graph: dict = {}
    graph_path = ""
    if output_dir_text and output_dir.is_dir():
        graph, graph_path = _load_retrieval_document_graph(output_dir)
    version_dir = _version_dir_for_case(case, output_dir)

    current_pages = _page_numbers(finding.get("page"))
    current_page = current_pages[0] if current_pages else 0
    retrieved_text = ""
    document_receipt: dict[str, Any]
    if graph:
        try:
            from backend.app.pipeline.stages.block_analysis.document_retrieval import (
                retrieve_document_context,
            )

            retrieved_text, document_receipt = retrieve_document_context(
                graph,
                query,
                current_page,
                max_hits=20 if recovery_mode else 8,
                max_chars=50000 if recovery_mode else 16000,
            )
        except Exception as exc:
            document_receipt = {
                "scope": "same_version_document_graph",
                "status": "error",
                "error": f"{type(exc).__name__}: {exc}"[:500],
                "selected_hits": 0,
                "selected_pages": [],
            }
    else:
        document_receipt = {
            "scope": "same_version_document_graph",
            "status": "graph_missing",
            "selected_hits": 0,
            "selected_pages": [],
        }
    document_receipt["graph_path"] = graph_path
    selected_hits = int(document_receipt.get("selected_hits") or 0)
    selected_pages = _page_numbers(document_receipt.get("selected_pages") or [])
    ranked_selected_pages = list(dict.fromkeys(
        int(value)
        for value in re.findall(r"\[стр\. PDF (\d+)", retrieved_text)
        if int(value) > 0
    )) or selected_pages

    related_text = ""
    related_receipt: dict[str, Any] = {"status": "not_attempted"}
    tail_spec_text = ""
    tail_spec_receipt: dict[str, Any] = {"status": "not_requested"}
    if recovery_mode:
        related_text, related_receipt = _related_document_context(
            case,
            query,
            version_dir=version_dir,
        )
        tail_spec_text, tail_spec_receipt = _tail_specification_context(
            graph,
            query,
        )

    spatial_excerpt = _spatial_page_excerpt(graph, current_pages)
    # Keep reference documents outside the primary document surface.  Page
    # numbers repeat across packages and must never validate a quote as if it
    # came from the audited volume.
    context["related_documents"] = [
        dict(row)
        for row in related_receipt.get("selected") or []
        if isinstance(row, dict) and str(row.get("excerpt") or "").strip()
    ]
    added_document_parts = [
        value
        for value in (
            retrieved_text,
            spatial_excerpt,
            tail_spec_text,
        )
        if str(value or "").strip()
    ]
    if added_document_parts:
        old_document_text = str(context.get("document_text_excerpt") or "").strip()
        combined = "\n\n--- ДОПОЛНИТЕЛЬНО НАЙДЕННЫЙ КОНТЕКСТ ---\n\n".join(
            value for value in (old_document_text, *added_document_parts) if value
        )
        context["document_text_excerpt"] = _bounded_md_excerpt(
            combined,
            70000 if recovery_mode else 24000,
        )
        if not context.get("document_text_path") and graph_path:
            context["document_text_path"] = graph_path
        context["document_pages_loaded"] = list(dict.fromkeys(
            _page_numbers(context.get("document_pages_loaded") or [])
            + selected_pages
            + current_pages
            + _page_numbers(tail_spec_receipt.get("selected_pages") or [])
        ))

    recovered_norm = _deep_norm_context(case, first_result) if recovery_mode else {}
    if recovered_norm:
        context["norm_context"] = recovered_norm

    max_images = max(0, int(max_images))
    all_source_refs = _block_refs(finding)
    exact_source_ids = [
        _canonical_block_id(value)
        for value in (finding.get("source_block_ids") or [])
        if _canonical_block_id(value)
    ]
    explicit_graphic_ids = [
        _canonical_block_id(item.get("block_id") or item.get("id"))
        for item in finding.get("evidence") or []
        if isinstance(item, dict)
        and str(item.get("type") or "").lower() in {"image", "graphic"}
    ]
    explicit_graphic_ids = [value for value in explicit_graphic_ids if value]
    source_page_by_id: dict[str, int] = {}
    for item in finding.get("evidence") or []:
        if not isinstance(item, dict):
            continue
        block_id = _canonical_block_id(item.get("block_id") or item.get("id"))
        pages = _page_numbers(item.get("page"))
        if block_id and pages:
            source_page_by_id[block_id] = pages[0]
    pinned_source_ids = list(dict.fromkeys(exact_source_ids + explicit_graphic_ids))
    supporting_ids = [value for value in all_source_refs if value not in set(pinned_source_ids)]
    needs_semantic_neighbors = bool(
        max_images > 0
        and any(not _explicit_block_ids(request) for request in missing_requests)
    )
    semantic_reserve = 3 if needs_semantic_neighbors else 0
    # A finding can legitimately cite more than twelve source graphics.  Deep
    # recovery keeps every pinned source block (up to a defensive ceiling) so
    # the new manifest is not marked truncated merely by the old fixed cap.
    image_ceiling = 32 if recovery_mode else 8
    effective_max_images = min(
        image_ceiling,
        max(max_images, min(image_ceiling, len(pinned_source_ids) + semantic_reserve)),
    )

    existing_images: list[dict] = []
    allowed_image_suffixes = {".png", ".jpg", ".jpeg", ".webp"}
    for raw_row in context.get("images") or []:
        if not isinstance(raw_row, dict):
            continue
        try:
            existing_path = Path(str(raw_row.get("path") or "")).resolve(strict=True)
        except (OSError, RuntimeError):
            continue
        if not existing_path.is_file() or existing_path.suffix.lower() not in allowed_image_suffixes:
            continue
        row = dict(raw_row)
        row["path"] = str(existing_path)
        row["block_id"] = _canonical_block_id(row.get("block_id"))
        existing_images.append(row)
    existing_image_ids = {
        row["block_id"] for row in existing_images if row.get("block_id")
    }

    # The legacy catalog resolver eagerly restores every missing crop.  Deep
    # recovery ranks the version metadata first and materializes only selected
    # blocks below, avoiding thousands of unrelated network requests.
    local_catalog = (
        _collect_retrieval_graphics(case)
        if effective_max_images and not recovery_mode
        else []
    )
    input_catalog, input_metadata_paths = (
        _load_input_graphics_catalog(case, output_dir, graph)
        if output_dir_text and output_dir.is_dir()
        else ([], [])
    )
    catalog_by_id: dict[str, dict] = {}
    for row in [*local_catalog, *input_catalog]:
        if not isinstance(row, dict):
            continue
        block_id = _canonical_block_id(row.get("block_id") or row.get("id"))
        if not block_id:
            continue
        current = catalog_by_id.setdefault(block_id, {"block_id": block_id, "id": block_id})
        normalized = dict(row)
        normalized["block_id"] = block_id
        normalized["id"] = block_id
        _merge_catalog_row(current, normalized)
    catalog = list(catalog_by_id.values())

    query_block_ids = _explicit_block_ids(query)
    direct_ids = list(dict.fromkeys(
        pinned_source_ids + query_block_ids + supporting_ids
    ))
    selected_graphics: list[dict] = []
    selected_graphic_ids: set[str] = set()
    ranked_priority_ids: list[str] = []
    materialization_receipts: list[dict] = []
    rich_rows: dict[str, dict] = {}
    failed_direct_pages: list[int] = []
    source_pdf_path = _find_source_pdf(version_dir)

    def add_rich_row(row: dict) -> None:
        block_id = _canonical_block_id(row.get("block_id"))
        if not block_id:
            return
        candidate = dict(row)
        candidate["block_id"] = block_id
        current = rich_rows.get(block_id)
        if current is None:
            rich_rows[block_id] = candidate
            return
        if len(str(candidate.get("ocr_or_description") or "")) > len(
            str(current.get("ocr_or_description") or "")
        ):
            rich_rows[block_id] = candidate

    def add_graphic(row: Any) -> None:
        occupied_ids = existing_image_ids | selected_graphic_ids
        if not isinstance(row, dict) or len(occupied_ids) >= effective_max_images:
            return
        block_id = _canonical_block_id(row.get("block_id") or row.get("id"))
        if not block_id or block_id in existing_image_ids or block_id in selected_graphic_ids:
            return
        remote_budget_available = bool(
            allow_remote_crops
            and (
                remote_budget is None
                or (
                    int(remote_budget.get("run_used") or 0)
                    < int(remote_budget.get("run_limit") or _REMOTE_CROP_RUN_MAX_BYTES)
                    and int(remote_budget.get("case_used") or 0)
                    < int(remote_budget.get("case_limit") or _REMOTE_CROP_CASE_MAX_BYTES)
                )
            )
        )
        materialized, material_receipt = _materialize_retrieval_graphic(
            {**row, "block_id": block_id},
            output_dir=output_dir,
            asset_dir=asset_dir,
            allow_remote_crops=remote_budget_available,
            source_pdf_path=source_pdf_path,
            remote_budget=remote_budget,
        )
        materialization_receipts.append(material_receipt)
        if materialized is None:
            return
        selected_graphics.append(materialized)
        selected_graphic_ids.add(block_id)

    for block_id in direct_ids:
        direct_row = catalog_by_id.get(block_id) or {
            "block_id": block_id,
            "id": block_id,
            "page": source_page_by_id.get(block_id),
            "label": f"Явно указанный блок {block_id}",
        }
        add_graphic(direct_row)
        if block_id not in (existing_image_ids | selected_graphic_ids):
            pages = _page_numbers(
                direct_row.get("page") or source_page_by_id.get(block_id)
            )
            failed_direct_pages.extend(pages)

        vector_payload: dict = {}
        vector_path = ""
        if output_dir_text and output_dir.is_dir():
            vector_payload, vector_path = _find_retrieval_vector_graph(output_dir, block_id)
        vector_excerpt = _retrieval_vector_excerpt(vector_payload, block_id)
        catalog_excerpt = str(
            direct_row.get("searchable")
            or direct_row.get("page_text")
            or direct_row.get("label")
            or ""
        ).strip()
        best_excerpt = (
            vector_excerpt
            if len(vector_excerpt) >= len(catalog_excerpt)
            else _bounded_md_excerpt(catalog_excerpt, 5000)
        )
        if best_excerpt:
            add_rich_row({
                "block_id": block_id,
                "page": vector_payload.get("page") or direct_row.get("page"),
                "ocr_label": (
                    f"Векторное описание блока {block_id}"
                    if vector_excerpt
                    else f"Описание исходного блока {block_id}"
                ),
                "ocr_or_description": best_excerpt,
                "vector_graph_path": vector_path,
                "image_path": "",
            })

    full_page_row: Optional[dict] = None
    full_page_receipt: dict = {"status": "not_attempted"}
    if len(existing_image_ids | selected_graphic_ids) < effective_max_images:
        full_page_row, full_page_receipt = _render_requested_full_page(
            query=query,
            graph=graph,
            source_pdf_path=source_pdf_path,
            asset_dir=asset_dir,
            preferred_pages=current_pages,
        )
        if (
            full_page_row is None
            and full_page_receipt.get("status") == "source_pdf_missing"
            and asset_dir is not None
        ):
            full_page_row, full_page_receipt, composite_receipts = (
                _render_spatial_page_composite(
                    page_number=int(full_page_receipt.get("page") or 0),
                    sheet_number=str(full_page_receipt.get("sheet") or ""),
                    graph=graph,
                    catalog=catalog,
                    output_dir=output_dir,
                    asset_dir=asset_dir,
                    allow_remote_crops=allow_remote_crops,
                    source_pdf_path=source_pdf_path,
                    remote_budget=remote_budget,
                )
            )
            materialization_receipts.extend(composite_receipts)
        if full_page_row is not None:
            add_graphic(full_page_row)
            add_rich_row({
                "block_id": full_page_row["block_id"],
                "page": full_page_row.get("page"),
                "ocr_label": full_page_row.get("label") or "",
                "ocr_or_description": full_page_row.get("searchable") or "",
                "vector_graph_path": "",
                "image_path": full_page_row.get("image_path") or "",
            })

    recovery_page_receipts: list[dict] = []
    recovery_full_pages: list[dict] = []
    tail_spec_pages = _page_numbers(tail_spec_receipt.get("selected_pages") or [])
    explicit_requested_pages = list(dict.fromkeys(
        page
        for request in missing_requests
        for page in _requested_sheet_pages(request, graph)
    ))
    page_overview_query = "\n".join(missing_requests).casefold()
    needs_page_overview = bool(
        failed_direct_pages
        or explicit_requested_pages
        or re.search(
            r"лист|страниц|штамп|располож|взаимн|план|схем|разрез|фасад|"
            r"полноразмер|целиком|нечита|спецификац|ведомост|экспликац|таблиц|"
            r"изображен(?:ие|ия).*(?:недоступ|отсутств|обрез)",
            page_overview_query,
        )
    )
    if (
        recovery_mode
        and needs_page_overview
        and len(existing_image_ids | selected_graphic_ids) < effective_max_images
    ):
        candidate_order = (
            explicit_requested_pages
            + failed_direct_pages
            + tail_spec_pages
            + current_pages
            + ranked_selected_pages
        )
        page_candidates = list(dict.fromkeys(candidate_order))[:8]
        for page_number in page_candidates:
            if len(existing_image_ids | selected_graphic_ids) >= effective_max_images:
                break
            block_id = f"full_page_{page_number}"
            if block_id in existing_image_ids or block_id in selected_graphic_ids:
                continue
            page_row, page_receipt = _render_pdf_page(
                page_number=page_number,
                source_pdf_path=source_pdf_path,
                asset_dir=asset_dir,
            )
            recovery_page_receipts.append(page_receipt)
            if (
                page_row is None
                and page_receipt.get("status") == "source_pdf_missing"
                and asset_dir is not None
            ):
                page_row, composite_receipt, composite_receipts = (
                    _render_spatial_page_composite(
                        page_number=page_number,
                        sheet_number="",
                        graph=graph,
                        catalog=catalog,
                        output_dir=output_dir,
                        asset_dir=asset_dir,
                        allow_remote_crops=allow_remote_crops,
                        source_pdf_path=source_pdf_path,
                        remote_budget=remote_budget,
                    )
                )
                recovery_page_receipts.append(composite_receipt)
                materialization_receipts.extend(composite_receipts)
            if page_row is None:
                continue
            add_graphic(page_row)
            recovery_full_pages.append(page_row)
            add_rich_row({
                "block_id": page_row["block_id"],
                "page": page_row.get("page"),
                "ocr_label": page_row.get("label") or "",
                "ocr_or_description": page_row.get("searchable") or "",
                "vector_graph_path": "",
                "image_path": page_row.get("image_path") or "",
            })

    target_pages = list(dict.fromkeys(current_pages + selected_pages))
    if (
        needs_semantic_neighbors
        and len(existing_image_ids | selected_graphic_ids) < effective_max_images
        and catalog
    ):
        try:
            from backend.app.services.section_optimization_graphics_agent_service import (
                rank_block_candidates,
            )

            same_page_catalog = [
                row for row in catalog if row.get("page") in set(current_pages)
            ]
            ranked: list[dict] = _semantic_target_candidates(
                catalog,
                missing_requests or [query],
                graph=graph,
                target_pages=target_pages,
                limit=max(effective_max_images * 4, 20),
            )
            if same_page_catalog:
                ranked.extend(rank_block_candidates(
                    same_page_catalog,
                    query,
                    target_pages=current_pages,
                    limit=max(effective_max_images * 2, 6),
                ))
            ranked.extend(rank_block_candidates(
                catalog,
                query,
                target_pages=target_pages,
                limit=max(effective_max_images * 5, 20),
            ))
            seen_ranked: set[str] = set()
            rank_attempts = 0
            fallback_text_rows = 0
            neighbor_images = 0
            max_neighbor_images = (
                min(
                    6 if recovery_mode else 2,
                    max(0, effective_max_images - len(existing_image_ids | selected_graphic_ids)),
                )
                if needs_semantic_neighbors
                else 0
            )
            max_rank_attempts = (
                max(effective_max_images, 12)
                if recovery_mode
                else max(effective_max_images * 5, 20)
            )
            for row in ranked:
                block_id = _canonical_block_id(row.get("block_id") or row.get("id"))
                if not block_id or block_id in seen_ranked:
                    continue
                seen_ranked.add(block_id)
                rank_attempts += 1
                was_selected = block_id in selected_graphic_ids
                add_graphic(row)
                if block_id in selected_graphic_ids and not was_selected:
                    neighbor_images += 1
                selected_for_model = (
                    block_id in selected_graphic_ids
                    or block_id in existing_image_ids
                )
                if selected_for_model and block_id not in ranked_priority_ids:
                    ranked_priority_ids.append(block_id)
                description = str(
                    row.get("searchable")
                    or row.get("page_text")
                    or row.get("label")
                    or ""
                ).strip()
                if description and (selected_for_model or fallback_text_rows < 4):
                    add_rich_row({
                        "block_id": block_id,
                        "page": row.get("page"),
                        "ocr_label": f"Описание соседнего блока {block_id}",
                        "ocr_or_description": _bounded_md_excerpt(description, 5000),
                        "vector_graph_path": "",
                        "image_path": "",
                    })
                    if not selected_for_model:
                        fallback_text_rows += 1
                if (
                    len(existing_image_ids | selected_graphic_ids) >= effective_max_images
                    or neighbor_images >= max_neighbor_images
                    or rank_attempts >= max_rank_attempts
                ):
                    break
        except Exception:
            pass

    new_images = [
        {
            "path": str(row.get("image_path") or row.get("path") or ""),
            "block_id": _canonical_block_id(row.get("block_id")),
            "page": row.get("page"),
        }
        for row in selected_graphics
        if str(row.get("image_path") or row.get("path") or "")
    ]
    images_by_id: dict[str, dict] = {}
    anonymous_images: list[dict] = []
    for row in [*existing_images, *new_images]:
        block_id = _canonical_block_id(row.get("block_id"))
        row["block_id"] = block_id
        if block_id:
            images_by_id.setdefault(block_id, row)
        else:
            anonymous_images.append(row)

    image_order = list(dict.fromkeys(
        pinned_source_ids
        + query_block_ids
        + [row.get("block_id") for row in existing_images if row.get("block_id")]
        + ranked_priority_ids
        + [row.get("block_id") for row in new_images if row.get("block_id")]
        + supporting_ids
    ))
    merged_images = [
        images_by_id[block_id] for block_id in image_order if block_id in images_by_id
    ]
    for row in anonymous_images:
        if len(merged_images) >= effective_max_images:
            break
        merged_images.append(row)
    context["images"] = merged_images[:effective_max_images]

    blocks = [
        dict(row) for row in (context.get("blocks") or []) if isinstance(row, dict)
    ]
    blocks_by_id: dict[str, dict] = {}
    original_block_order: list[str] = []

    def merge_block(row: dict) -> None:
        block_id = _canonical_block_id(row.get("block_id"))
        if not block_id:
            return
        candidate = dict(row)
        candidate["block_id"] = block_id
        current = blocks_by_id.get(block_id)
        if current is None:
            blocks_by_id[block_id] = candidate
            original_block_order.append(block_id)
            return
        old_description = str(current.get("ocr_or_description") or "")
        new_description = str(candidate.get("ocr_or_description") or "")
        if len(new_description) > len(old_description):
            current["ocr_or_description"] = new_description
            if candidate.get("ocr_label"):
                current["ocr_label"] = candidate.get("ocr_label")
        for key in ("page", "image_path", "vector_graph_path"):
            if candidate.get(key) not in (None, ""):
                current[key] = candidate.get(key)

    for row in blocks:
        merge_block(row)
    for row in rich_rows.values():
        merge_block(row)
    for row in selected_graphics:
        merge_block({
            "block_id": row.get("block_id"),
            "page": row.get("page"),
            "ocr_label": str(row.get("label") or "")[:1000],
            "ocr_or_description": str(
                row.get("searchable") or row.get("page_text") or row.get("label") or ""
            )[:5000],
            "image_path": str(row.get("image_path") or row.get("path") or ""),
        })

    final_block_order = list(dict.fromkeys(
        direct_ids
        + [row.get("block_id") for row in context.get("images") or [] if row.get("block_id")]
        + original_block_order
    ))
    block_ceiling = 32 if recovery_mode else 16
    block_limit = min(block_ceiling, max(12, len(direct_ids) + (12 if recovery_mode else 4)))
    context["blocks"] = [
        blocks_by_id[block_id]
        for block_id in final_block_order
        if block_id in blocks_by_id
    ][:block_limit]

    frozen_assets: list[dict] = []
    if asset_dir is not None:
        frozen_assets = _freeze_context_images(context, asset_dir)

    attached_graphic_ids = {
        _canonical_block_id(row.get("block_id"))
        for row in context.get("images") or []
        if isinstance(row, dict) and row.get("block_id")
    }
    existing_graphic_ids = [
        _canonical_block_id(value)
        for value in (context.get("graphic_block_ids") or [])
        if _canonical_block_id(value)
    ]
    context["graphic_block_ids"] = list(dict.fromkeys(
        existing_graphic_ids
        + [row["block_id"] for row in new_images]
    ))
    existing_text_ids = [
        _canonical_block_id(value)
        for value in (context.get("text_block_ids") or [])
        if _canonical_block_id(value)
    ]
    rich_block_ids = list(rich_rows)
    context["text_block_ids"] = list(dict.fromkeys(existing_text_ids + rich_block_ids))
    existing_source_ids = [
        _canonical_block_id(value)
        for value in (context.get("source_block_ids") or [])
        if _canonical_block_id(value)
    ]
    context["source_block_ids"] = list(dict.fromkeys(
        existing_source_ids + direct_ids + [row["block_id"] for row in new_images]
    ))
    context["source_block_count"] = len(context["source_block_ids"])
    expected_source_graphic_ids = list(dict.fromkeys(
        explicit_graphic_ids
        + [
            block_id
            for block_id in pinned_source_ids
            if block_id in catalog_by_id
            and (
                "image" in str(catalog_by_id[block_id].get("block_type") or "").casefold()
                or bool(catalog_by_id[block_id].get("crop_url"))
                or bool(catalog_by_id[block_id].get("image_path"))
            )
        ]
    ))
    missing_source_images = [
        block_id
        for block_id in expected_source_graphic_ids
        if block_id not in attached_graphic_ids
    ]
    capacity_exhausted = bool(
        effective_max_images
        and len(context.get("images") or []) >= effective_max_images
    )
    context["source_images_unavailable"] = missing_source_images
    context["images_truncated"] = bool(
        missing_source_images and capacity_exhausted
    )

    if context.get("images") and context.get("document_text_excerpt"):
        context["route"] = "mixed"
    elif context.get("images"):
        context["route"] = "graphic"
    elif context.get("document_text_excerpt") or rich_rows:
        context["route"] = "text"

    vector_rows = [
        row for row in rich_rows.values() if row.get("vector_graph_path")
    ]
    receipt = {
        "contract_version": (
            DEEP_RETRIEVAL_CONTRACT_VERSION
            if recovery_mode
            else AUTO_RETRIEVAL_CONTRACT_VERSION
        ),
        "scope": (
            "same_version_plus_same_object_reference_documents"
            if recovery_mode
            else "same_document_same_version_only"
        ),
        "query": query[:6000],
        "document": document_receipt,
        "tail_specifications": tail_spec_receipt,
        "related_documents": related_receipt,
        "norm_recovery": {
            "found": bool(recovered_norm),
            "kind": recovered_norm.get("kind") if recovered_norm else "",
            "matched_code": recovered_norm.get("matched_code") if recovered_norm else "",
        },
        "spatial_context": {
            "pages": current_pages,
            "included": bool(spatial_excerpt),
            "chars": len(spatial_excerpt),
        },
        "graphics": {
            "catalog_size": len(catalog),
            "input_metadata_paths": input_metadata_paths,
            "pinned_source_block_ids": pinned_source_ids,
            "explicit_block_ids": direct_ids,
            "selected_block_ids": [row["block_id"] for row in new_images],
            "final_block_ids": [
                row.get("block_id") for row in context.get("images") or []
            ],
            "selected_pages": _page_numbers([row.get("page") for row in new_images]),
            "missing_source_image_ids": missing_source_images,
            "capacity_truncated_source_ids": (
                missing_source_images if capacity_exhausted else []
            ),
            "unavailable_source_image_ids": (
                [] if capacity_exhausted else missing_source_images
            ),
        },
        "remote_source_crops": materialization_receipts,
        "vector_blocks": {
            "selected_block_ids": [
                _canonical_block_id(row.get("block_id")) for row in vector_rows
            ],
            "selected_pages": _page_numbers([row.get("page") for row in vector_rows]),
            "paths": [row.get("vector_graph_path") for row in vector_rows],
        },
        "full_page": full_page_receipt,
        "recovery_full_pages": recovery_page_receipts,
        "material_delta": {
            "document_hits": selected_hits,
            "tail_specification_pages": len(tail_spec_pages),
            "related_document_hits": sum(
                int(row.get("hits") or 0)
                for row in related_receipt.get("selected") or []
            ),
            "new_graphics": len(frozen_assets if asset_dir is not None else new_images),
            "vector_blocks": len(vector_rows),
            "full_page": bool(full_page_row or recovery_full_pages),
            "norm_context": bool(recovered_norm),
            "spatial_only": bool(
                spatial_excerpt
                and not selected_hits
                and not (frozen_assets if asset_dir is not None else new_images)
                and not vector_rows
                and not full_page_row
            ),
        },
        "found": bool(
            selected_hits
            or tail_spec_text
            or related_text
            or recovered_norm
            or (frozen_assets if asset_dir is not None else new_images)
            or vector_rows
            or full_page_row
            or recovery_full_pages
        ),
    }

    if asset_dir is not None:
        receipt["frozen_assets"] = frozen_assets

    context["retrieval_receipt"] = receipt
    enriched["source_input_hash"] = str(case.get("input_hash") or "")
    enriched["previous_audit"] = {
        key: _compact_value(first_result.get(key))
        for key in (
            "verdict",
            "confidence",
            "recommended_action",
            "binding_status",
            "factual_verdict",
            "report_value",
            "reason_quality",
            "decision_effect",
            "rejection_basis",
            "practical_impact",
            "impact_assessment",
            "source_alignment",
            "scope_context_status",
            "review_priority",
            "reason_assessment",
            "finding_assessment",
            "norm_assessment",
            "missing_context",
            "guard_adjustments",
        )
        if first_result.get(key) not in (None, "", [], {})
    }
    enriched["input_hash"] = _retrieval_input_hash(
        enriched,
        contract_version=(
            DEEP_RETRIEVAL_CONTRACT_VERSION
            if recovery_mode
            else AUTO_RETRIEVAL_CONTRACT_VERSION
        ),
    )
    return enriched, receipt


def prepare_retrieval_cases(
    source_cases: Sequence[dict],
    source_results: dict[str, dict],
    *,
    limit: int,
    only_case_ids: Optional[set[str]] = None,
    max_images_per_case: int = 6,
    asset_dir: Optional[Path] = None,
    allow_remote_crops: bool = False,
) -> tuple[list[dict], dict]:
    """Build a deterministic frozen second-pass manifest from current results."""
    if int(limit) <= 0:
        raise ValueError("retrieval pilot limit must be greater than zero")
    selected: list[dict] = []
    stats = Counter()
    remote_budget = {
        "run_used": 0,
        "run_limit": _REMOTE_CROP_RUN_MAX_BYTES,
        "case_used": 0,
        "case_limit": _REMOTE_CROP_CASE_MAX_BYTES,
        "disk_path": str(Path(asset_dir) if asset_dir is not None else Path(".")),
    }
    allowed_ids = {str(value) for value in (only_case_ids or set()) if value}
    for case in source_cases:
        case_id = str(case.get("case_id") or "")
        if allowed_ids and case_id not in allowed_ids:
            continue
        result = source_results.get(case_id)
        if not isinstance(result, dict):
            stats["missing_result"] += 1
            continue
        if str(result.get("input_hash") or "") != str(case.get("input_hash") or ""):
            stats["stale_result"] += 1
            continue
        if result.get("status") != "success":
            stats["non_success_result"] += 1
            continue
        if result.get("verdict") != "insufficient_evidence":
            stats["non_insufficient_result"] += 1
            continue
        if not result.get("missing_context"):
            stats["missing_context_empty"] += 1
            continue
        if case.get("source_quality") != "same_version_artifact":
            stats["unsafe_source_quality"] += 1
            continue
        if case.get("decision_origin") != "human":
            stats["non_human_origin"] += 1
            continue
        if result.get("binding_status") != "exact":
            stats["binding_not_exact"] += 1
            continue

        stats["attempted"] += 1
        remote_budget["case_used"] = 0
        enrich_kwargs: dict[str, Any] = {
            "max_images": max_images_per_case,
        }
        if asset_dir is not None:
            safe_case_id = re.sub(r"[^A-Za-z0-9_-]+", "_", case_id)[:100] or "case"
            case_asset_dir = Path(asset_dir) / safe_case_id
            enrich_kwargs["asset_dir"] = case_asset_dir
            remote_budget["disk_path"] = str(case_asset_dir)
        if allow_remote_crops:
            enrich_kwargs["allow_remote_crops"] = True
            enrich_kwargs["remote_budget"] = remote_budget
        enriched, receipt = enrich_case_for_retrieval(
            case,
            result,
            **enrich_kwargs,
        )
        if not receipt.get("found"):
            stats["not_found"] += 1
            continue
        selected.append(enriched)
        stats["found"] += 1
        if len(selected) >= int(limit):
            break
    stats["selected_cases"] = len(selected)
    stats["remote_crop_downloaded_bytes"] = int(remote_budget.get("run_used") or 0)
    return selected, dict(stats)


def prepare_recovery_cases(
    source_cases: Sequence[dict],
    source_results: dict[str, dict],
    *,
    limit: int = 0,
    only_case_ids: Optional[set[str]] = None,
    max_images_per_case: int = 12,
    asset_dir: Optional[Path] = None,
    allow_remote_crops: bool = False,
) -> tuple[list[dict], dict, list[dict]]:
    """Build a deep, classified retry set for every still-insufficient case."""
    selected: list[dict] = []
    classifications: list[dict] = []
    stats = Counter()
    remote_budget = {
        "run_used": 0,
        "run_limit": _REMOTE_CROP_RUN_MAX_BYTES,
        "case_used": 0,
        "case_limit": _REMOTE_CROP_CASE_MAX_BYTES,
        "disk_path": str(Path(asset_dir) if asset_dir is not None else Path(".")),
    }
    allowed_ids = {str(value) for value in (only_case_ids or set()) if value}
    for case in source_cases:
        case_id = str(case.get("case_id") or "")
        if allowed_ids and case_id not in allowed_ids:
            continue
        result = source_results.get(case_id)
        if not isinstance(result, dict):
            stats["missing_result"] += 1
            continue
        if str(result.get("input_hash") or "") != str(case.get("input_hash") or ""):
            stats["stale_result"] += 1
            continue
        if result.get("status") != "success":
            stats["non_success_result"] += 1
            continue
        if result.get("verdict") != "insufficient_evidence":
            stats["non_insufficient_result"] += 1
            continue

        row: dict[str, Any] = {
            "case_id": case_id,
            "object": case.get("object_name"),
            "discipline": case.get("discipline"),
            "document": case.get("document"),
            "version": case.get("version_id"),
            "finding_id": (case.get("finding") or {}).get("id"),
            "missing_context": result.get("missing_context") or [],
            "selected_for_model": False,
        }
        if case.get("source_quality") != "same_version_artifact":
            row.update({"category": "unsafe_source_quality", "reason": "Нет надёжной привязки finding к той же версии."})
            stats["unsafe_source_quality"] += 1
            classifications.append(row)
            continue
        if case.get("decision_origin") != "human":
            row.update({"category": "non_human_origin", "reason": "Решение не подтверждено как ручное решение эксперта."})
            stats["non_human_origin"] += 1
            classifications.append(row)
            continue
        if result.get("binding_status") != "exact":
            row.update({"category": "binding_not_exact", "reason": "Причина отказа не привязана однозначно к текущему замечанию."})
            stats["binding_not_exact"] += 1
            classifications.append(row)
            continue

        stats["attempted"] += 1
        remote_budget["case_used"] = 0
        safe_case_id = re.sub(r"[^A-Za-z0-9_-]+", "_", case_id)[:100] or "case"
        case_asset_dir = Path(asset_dir) / safe_case_id if asset_dir is not None else None
        if case_asset_dir is not None:
            remote_budget["disk_path"] = str(case_asset_dir)
        enriched, receipt = enrich_case_for_retrieval(
            case,
            result,
            max_images=max_images_per_case,
            asset_dir=case_asset_dir,
            allow_remote_crops=allow_remote_crops,
            remote_budget=remote_budget if allow_remote_crops else None,
            recovery_mode=True,
        )
        delta = receipt.get("material_delta") or {}
        sources: list[str] = []
        if int(delta.get("document_hits") or 0):
            sources.append("whole_document")
        if int(delta.get("tail_specification_pages") or 0):
            sources.append("tail_specifications")
        if int(delta.get("related_document_hits") or 0):
            sources.append("related_documents")
        if bool(delta.get("norm_context")):
            sources.append("norm_corpus")
        if bool(delta.get("full_page")):
            sources.append("full_pdf_pages")
        if int(delta.get("new_graphics") or 0) or int(delta.get("vector_blocks") or 0):
            sources.append("graphic_blocks")
        row["recovered_sources"] = sources
        row["retrieval_receipt"] = receipt
        if not receipt.get("found"):
            row.update({
                "category": "not_found_after_autonomous_search",
                "reason": "По полному документу, связанным документам и локальной базе норм нового проверяемого источника не найдено.",
            })
            stats["not_found"] += 1
            classifications.append(row)
            continue

        row.update({
            "category": "recovered_context",
            "reason": "Найден новый проверяемый контекст; кейс включён в повторный аудит.",
            "selected_for_model": True,
        })
        classifications.append(row)
        selected.append(enriched)
        stats["found"] += 1
        if int(limit or 0) > 0 and len(selected) >= int(limit):
            break

    stats["selected_cases"] = len(selected)
    stats["classified_insufficient_cases"] = len(classifications)
    stats["remote_crop_downloaded_bytes"] = int(remote_budget.get("run_used") or 0)
    return selected, dict(stats), classifications


def _object_metadata(review_path: Path, cache: dict[Path, dict]) -> dict:
    object_name = _part_after(review_path, "objects")
    if not object_name:
        return {}
    objects_index = review_path.parts.index("objects")
    object_dir = Path(*review_path.parts[: objects_index + 2])
    if object_dir not in cache:
        payload = _load_json(object_dir / "object.json")
        cache[object_dir] = payload if isinstance(payload, dict) else {}
    return cache[object_dir]


def _document_metadata(review_path: Path, cache: dict[Path, dict]) -> dict:
    version_dir = review_path.parent.parent
    try:
        versions_index = version_dir.parts.index("versions")
        document_dir = Path(*version_dir.parts[:versions_index])
    except ValueError:
        return {}
    if document_dir not in cache:
        payload = _load_json(document_dir / "document.json")
        cache[document_dir] = payload if isinstance(payload, dict) else {}
    return cache[document_dir]


def _case_id(review_path: Path, item_id: str, timestamp: str, reason: str) -> str:
    try:
        relative = review_path.resolve().relative_to(ROOT_DIR.resolve())
    except ValueError:
        relative = review_path.resolve()
    canonical = "\u241f".join((str(relative), item_id, timestamp, reason))
    digest = hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:16]
    day = re.sub(r"\D", "", timestamp[:10]) or "undated"
    return f"RF-{day}-{digest}"


def collect_rejected_cases(
    *,
    month: str,
    projects_v2_root: Path = DEFAULT_PROJECTS_V2_ROOT,
    timezone_name: str = "Europe/Moscow",
    include_carried_over: bool = False,
    include_optimizations: bool = False,
    max_images_per_case: int = 3,
    object_ids: Optional[set[str]] = None,
    disciplines: Optional[set[str]] = None,
    reviewers: Optional[set[str]] = None,
) -> tuple[list[dict], dict]:
    """Build version-correct case snapshots for a calendar month."""
    start, end = parse_month(month, timezone_name)
    root = Path(projects_v2_root)
    pattern = "objects/*/disciplines/*/documents/*/versions/*/04_review/expert_review.json"
    review_paths = sorted(root.glob(pattern))
    cases: list[dict] = []
    stats = Counter()
    object_meta_cache: dict[Path, dict] = {}
    document_meta_cache: dict[Path, dict] = {}
    reviewer_keys = {value.casefold() for value in (reviewers or set())}

    for review_path in review_paths:
        stats["review_files_scanned"] += 1
        payload = _load_json(review_path)
        if not isinstance(payload, (dict, list)):
            stats["invalid_review_files"] += 1
            continue
        project_id = str(payload.get("project_id") or "") if isinstance(payload, dict) else ""
        object_meta = _object_metadata(review_path, object_meta_cache)
        document_meta = _document_metadata(review_path, document_meta_cache)
        object_id = str(object_meta.get("object_id") or "")
        object_name = str(object_meta.get("display_name") or object_meta.get("legacy_name") or _part_after(review_path, "objects"))
        discipline = _part_after(review_path, "disciplines")
        document_folder = _part_after(review_path, "documents")
        document_code = str(document_meta.get("document_code") or document_meta.get("legacy_project_name") or document_folder)
        version_id = _part_after(review_path, "versions")
        version_dir = review_path.parent.parent

        if object_ids and object_id not in object_ids:
            continue
        if disciplines and discipline.upper() not in disciplines:
            continue

        for decision in _decision_items(payload):
            if str(decision.get("decision") or decision.get("expert_decision") or "").lower() != "rejected":
                continue
            stats["rejected_decisions_seen"] += 1
            reviewer = str(
                decision.get("reviewer")
                or (payload.get("reviewer") if isinstance(payload, dict) else "")
                or ""
            ).strip()
            if reviewer_keys and reviewer.casefold() not in reviewer_keys:
                stats["excluded_reviewer"] += 1
                continue
            item_type = str(decision.get("item_type") or "finding").lower()
            if item_type == "optimization" and not include_optimizations:
                stats["excluded_optimizations"] += 1
                continue
            if item_type not in {"finding", "optimization"}:
                stats["excluded_unknown_item_type"] += 1
                continue
            carried_over = bool(decision.get("carried_over"))
            if carried_over and not include_carried_over:
                stats["excluded_carried_over"] += 1
                continue

            raw_timestamp = str(decision.get("timestamp") or decision.get("expert_date") or "")
            timestamp_source = "decision.timestamp"
            parsed_timestamp = parse_timestamp(raw_timestamp)
            if parsed_timestamp is None:
                stats["excluded_missing_timestamp"] += 1
                continue
            if not (start <= parsed_timestamp < end):
                stats["outside_period"] += 1
                continue

            item_id = str(decision.get("item_id") or "").strip()
            if not item_id:
                stats["excluded_missing_item_id"] += 1
                continue
            reason = str(decision.get("rejection_reason") or decision.get("expert_reason") or "").strip()
            suspected_transfer = bool(_TRANSFER_REASON_RE.search(reason))
            if carried_over:
                decision_origin = "carried_over"
            elif suspected_transfer:
                decision_origin = "suspected_carryover"
                stats["suspected_carryover_reason"] += 1
            else:
                decision_origin = "human"
            source_item, source_path, output_dir, source_quality = load_exact_source_item(version_dir, item_id, item_type)
            if source_item is None:
                stats["source_item_missing"] += 1
                source_item = {
                    "id": item_id,
                    "problem": str(decision.get("problem") or decision.get("summary") or ""),
                    "description": str(decision.get("description") or ""),
                    "norm": str(decision.get("norm") or ""),
                }
                finding_source = "review_decision_fallback"
            else:
                stats["source_item_found"] += 1
                finding_source = source_quality
                stats[f"source_quality_{finding_source}"] += 1

            source_item.setdefault("id", item_id)
            context = build_case_context(
                output_dir,
                source_item,
                project_id=project_id or document_code,
                section=discipline,
                max_images=max_images_per_case,
            )
            if not reason:
                stats["missing_expert_reason"] += 1
            stats[f"route_{context['route']}"] += 1

            timestamp_iso = parsed_timestamp.isoformat()
            timestamp_local_iso = parsed_timestamp.astimezone(start.tzinfo).isoformat()
            case = {
                "case_id": _case_id(review_path, item_id, timestamp_iso, reason),
                "period": month,
                "object_id": object_id,
                "object_name": object_name,
                "discipline": discipline,
                "document": document_code,
                "document_folder": document_folder,
                "project_id": project_id or document_code,
                "version_id": version_id,
                "item_id": item_id,
                "item_type": item_type,
                "expert_reason": reason,
                "expert_reviewer": reviewer,
                "expert_timestamp": timestamp_iso,
                "expert_timestamp_local": timestamp_local_iso,
                "decision_origin": decision_origin,
                "timestamp_source": timestamp_source,
                "carried_over": carried_over,
                "review_path": str(review_path.resolve()),
                "version_dir": str(version_dir.resolve()),
                "output_dir": str(output_dir.resolve()) if output_dir else "",
                "source_item_path": str(source_path.resolve()) if source_path else "",
                "source_quality": finding_source,
                "finding": compact_finding(source_item),
                "context": context,
            }
            case["input_hash"] = hashlib.sha256(
                json.dumps(
                    {
                        "expert_reason": case["expert_reason"],
                        "expert_timestamp": case["expert_timestamp"],
                        "decision_origin": case["decision_origin"],
                        "source_quality": case["source_quality"],
                        "audit_contract": AUDIT_CONTRACT_VERSION,
                        "finding": case["finding"],
                        "context": case["context"],
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                ).encode("utf-8")
            ).hexdigest()
            cases.append(case)

    cases.sort(
        key=lambda case: (
            case["expert_timestamp"],
            case["object_id"],
            case["discipline"],
            case["document"],
            case["version_id"],
            case["item_id"],
        )
    )
    stats["selected_cases"] = len(cases)
    inventory = {
        "schema_version": 3,
        "audit_contract_version": AUDIT_CONTRACT_VERSION,
        "generated_at": utc_now_iso(),
        "period": month,
        "timezone": timezone_name,
        "interval": {
            "from": start.isoformat(),
            "to_exclusive": end.isoformat(),
            "from_utc": start.astimezone(timezone.utc).isoformat(),
            "to_exclusive_utc": end.astimezone(timezone.utc).isoformat(),
        },
        "source": "projects_v2/**/04_review/expert_review.json",
        "timestamp_semantics": (
            "timestamp is the last batch-save time currently stored on the decision; "
            "it may not be the first time the expert made the decision"
        ),
        "filters": {
            "explicit_carried_over_excluded": not include_carried_over,
            "include_optimizations": include_optimizations,
            "object_ids": sorted(object_ids or []),
            "disciplines": sorted(disciplines or []),
            "reviewers": sorted(reviewers or []),
        },
        "counts": dict(stats),
        "by_object": dict(Counter(case["object_name"] for case in cases)),
        "by_discipline": dict(Counter(case["discipline"] for case in cases)),
        "by_reviewer": dict(Counter(case["expert_reviewer"] for case in cases)),
        "by_day": dict(Counter(case["expert_timestamp_local"][:10] for case in cases)),
        "by_route": dict(Counter(case["context"]["route"] for case in cases)),
    }
    return cases, inventory


def write_manifest(output_dir: Path, cases: Sequence[dict], inventory: dict) -> tuple[Path, Path]:
    output_dir = Path(output_dir)
    manifest_path = output_dir / "manifest.jsonl"
    inventory_path = output_dir / "inventory.json"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_manifest = manifest_path.with_name(f".{manifest_path.name}.{os.getpid()}.tmp")
    with tmp_manifest.open("w", encoding="utf-8") as handle:
        for case in cases:
            handle.write(json.dumps(case, ensure_ascii=False) + "\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(tmp_manifest, manifest_path)
    _atomic_write_json(inventory_path, inventory)
    return manifest_path, inventory_path


def load_manifest(path: Path) -> list[dict]:
    cases: list[dict] = []
    with Path(path).open(encoding="utf-8") as handle:
        for line_no, raw in enumerate(handle, start=1):
            line = raw.strip()
            if not line:
                continue
            try:
                item = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(f"invalid manifest JSONL at line {line_no}: {exc}") from exc
            if isinstance(item, dict) and item.get("case_id"):
                cases.append(item)
    return cases


def append_result(path: Path, result: dict) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(result, ensure_ascii=False) + "\n")
        handle.flush()
        os.fsync(handle.fileno())


def load_latest_results(path: Path) -> tuple[dict[str, dict], int]:
    latest: dict[str, dict] = {}
    malformed = 0
    path = Path(path)
    if not path.is_file():
        return latest, malformed
    with path.open(encoding="utf-8", errors="replace") as handle:
        for raw in handle:
            line = raw.strip()
            if not line:
                continue
            try:
                item = json.loads(line)
            except json.JSONDecodeError:
                malformed += 1
                continue
            if isinstance(item, dict) and item.get("case_id"):
                latest[str(item["case_id"])] = item
    return latest, malformed


def _image_paths(case: dict) -> list[str]:
    return [
        str(row.get("path") or "")
        for row in (case.get("context") or {}).get("images") or []
        if row.get("path")
    ]


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()

def align_batch_images(cases: Sequence[dict], *, max_total_images: int = 0) -> tuple[list[str], dict[str, list[dict]]]:
    """Mirror runner image filtering/dedup and assign stable 1-based indexes."""
    allowed_suffixes = {".png", ".jpg", ".jpeg", ".webp", ".gif"}
    paths: list[str] = []
    index_by_path: dict[str, int] = {}
    sha256_by_path: dict[str, str] = {}
    aligned: dict[str, list[dict]] = {str(case["case_id"]): [] for case in cases}
    for case in cases:
        case_id = str(case["case_id"])
        for row in (case.get("context") or {}).get("images") or []:
            try:
                path = Path(str(row.get("path") or "")).expanduser().resolve(strict=True)
            except (OSError, RuntimeError):
                continue
            if not path.is_file() or path.suffix.lower() not in allowed_suffixes:
                continue
            path_text = str(path)
            image_index = index_by_path.get(path_text)
            if image_index is None:
                if max_total_images > 0 and len(paths) >= max_total_images:
                    continue
                paths.append(path_text)
                image_index = len(paths)
                index_by_path[path_text] = image_index
            block_id = str(row.get("block_id") or "").removeprefix("block_")
            asset_role = str(row.get("asset_role") or "")
            if not asset_role:
                if block_id.startswith("full_page_"):
                    asset_role = "full_page"
                elif "composite" in block_id.casefold():
                    asset_role = "spatial_composite"
                else:
                    asset_role = "crop"
            image_sha256 = str(row.get("sha256") or "")
            if not image_sha256:
                image_sha256 = sha256_by_path.get(path_text, "")
            if not image_sha256:
                image_sha256 = _file_sha256(path)
                sha256_by_path[path_text] = image_sha256
            aligned[case_id].append({
                "image_index": image_index,
                "path": path_text,
                "block_id": block_id,
                "page": row.get("page"),
                "source_id": str(row.get("source_id") or f"graphic:{block_id}"),
                "asset_role": asset_role,
                "sha256": image_sha256,
            })
    return paths, aligned



def plan_batches(
    cases: Sequence[dict],
    *,
    batch_size: int = 4,
    max_batch_images: int = 6,
) -> list[list[dict]]:
    """Greedily batch only within one exact document version and modality."""
    batch_size = max(1, int(batch_size))
    max_batch_images = max(1, int(max_batch_images))
    groups: dict[tuple, list[dict]] = defaultdict(list)
    for case in cases:
        has_images = bool(_image_paths(case))
        version_key = case.get("version_dir") or case.get("review_path")
        if not version_key:
            version_key = "|".join(str(case.get(field) or "") for field in (
                "object_id", "discipline", "document", "version_id",
            ))
        key = (str(version_key), has_images)
        groups[key].append(case)

    planned: list[list[dict]] = []
    for key in sorted(groups, key=lambda value: tuple(str(part) for part in value)):
        current: list[dict] = []
        current_images: set[str] = set()
        for case in groups[key]:
            case_images = set(_image_paths(case))
            would_overflow = len(current_images | case_images) > max_batch_images
            if current and (len(current) >= batch_size or would_overflow):
                planned.append(current)
                current = []
                current_images = set()
            current.append(case)
            current_images.update(case_images)
        if current:
            planned.append(current)
    return planned


def output_schema(case_ids: Sequence[str]) -> dict:
    evidence_schema = {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "source": {"type": "string", "enum": sorted(VALID_EVIDENCE_SOURCES)},
            "source_id": {"type": "string"},
            "image_index": {"type": "integer", "minimum": 0},
            "block_id": {"type": "string"},
            "locator": {"type": "string"},
            "quote": {"type": "string"},
            "implication": {"type": "string"},
            "observation_basis": {
                "type": "string",
                "enum": sorted(VALID_OBSERVATION_BASES),
            },
            "verification_state": {
                "type": "string",
                "enum": sorted(VALID_VERIFICATION_STATES),
            },
            "claim_type": {"type": "string", "enum": sorted(VALID_CLAIM_TYPES)},
            "absence_scope": {"type": "string", "enum": sorted(VALID_ABSENCE_SCOPES)},
        },
        "required": [
            "source",
            "source_id",
            "image_index",
            "block_id",
            "locator",
            "quote",
            "implication",
            "observation_basis",
            "verification_state",
            "claim_type",
            "absence_scope",
        ],
    }
    review_schema = {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "case_id": {"type": "string", "enum": list(case_ids)},
            "verdict": {"type": "string", "enum": sorted(VALID_VERDICTS)},
            "confidence": {"type": "string", "enum": ["high", "medium", "low"]},
            "binding_status": {"type": "string", "enum": sorted(VALID_BINDING_STATUSES)},
            "factual_verdict": {"type": "string", "enum": sorted(VALID_FACTUAL_VERDICTS)},
            "report_value": {"type": "string", "enum": sorted(VALID_REPORT_VALUES)},
            "reason_quality": {"type": "string", "enum": sorted(VALID_REASON_QUALITIES)},
            "decision_effect": {"type": "string", "enum": sorted(VALID_DECISION_EFFECTS)},
            "rejection_basis": {"type": "string", "enum": sorted(VALID_REJECTION_BASES)},
            "practical_impact": {"type": "string", "enum": sorted(VALID_PRACTICAL_IMPACTS)},
            "impact_assessment": {"type": "string"},
            "source_alignment": {"type": "string", "enum": sorted(VALID_SOURCE_ALIGNMENTS)},
            "scope_context_status": {
                "type": "string",
                "enum": sorted(VALID_SCOPE_CONTEXT_STATUSES),
            },
            "integrity_flags": {"type": "array", "items": {"type": "string"}},
            "reason_assessment": {"type": "string"},
            "finding_assessment": {"type": "string"},
            "norm_assessment": {"type": "string"},
            "decisive_evidence": {"type": "array", "items": evidence_schema},
            "reviewed_sources": {
                "type": "array",
                "items": {"type": "string", "enum": sorted(VALID_EVIDENCE_SOURCES)},
            },
            "missing_context": {"type": "array", "items": {"type": "string"}},
            "recommended_action": {"type": "string", "enum": sorted(VALID_ACTIONS)},
        },
        "required": [
            "case_id",
            "verdict",
            "confidence",
            "reason_assessment",
            "binding_status",
            "factual_verdict",
            "report_value",
            "reason_quality",
            "decision_effect",
            "rejection_basis",
            "practical_impact",
            "impact_assessment",
            "source_alignment",
            "scope_context_status",
            "integrity_flags",
            "finding_assessment",
            "norm_assessment",
            "decisive_evidence",
            "reviewed_sources",
            "missing_context",
            "recommended_action",
        ],
    }
    return {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "reviews": {
                "type": "array",
                "minItems": len(case_ids),
                "maxItems": len(case_ids),
                "items": review_schema,
            }
        },
        "required": ["reviews"],
    }


def _model_case_payload(case: dict, attached_images: Optional[list[dict]] = None) -> dict:
    context = case.get("context") or {}
    return {
        "case_id": case["case_id"],
        "document": {
            "object": case.get("object_name"),
            "discipline": case.get("discipline"),
            "code": case.get("document"),
            "version": case.get("version_id"),
        },
        "expert_decision": {
            "decision": "rejected",
            "origin": case.get("decision_origin") or "unknown",
            "reason": case.get("expert_reason") or "(причина не заполнена)",
            "timestamp": case.get("expert_timestamp"),
        },
        "finding": case.get("finding") or {},
        "previous_audit": case.get("previous_audit") or {},
        "context": {
            "route": context.get("route"),
            "finding_evidence_text": context.get("finding_evidence_text") or "",
            "document_text": {
                "source_id": "primary",
                "path": context.get("document_text_path") or "",
                "pages": context.get("document_pages_loaded") or [],
                "excerpt": context.get("document_text_excerpt") or "",
            },
            "related_documents": [
                {
                    "source_id": row.get("source_id") or "",
                    "source_role": "related_reference",
                    "object_id": row.get("object_id") or case.get("object_id") or "",
                    "discipline": row.get("discipline") or "",
                    "document_code": row.get("document") or "",
                    "version_id": row.get("version") or "",
                    "version_relation": row.get("version_relation") or "unknown",
                    "pages": row.get("pages") or [],
                    "excerpt": row.get("excerpt") or "",
                }
                for row in context.get("related_documents") or []
                if isinstance(row, dict)
            ],
            "blocks": context.get("blocks") or [],
            "attached_images": attached_images if attached_images is not None else (context.get("images") or []),
            "norm_context": context.get("norm_context") or {},
            "retrieval": context.get("retrieval_receipt") or {},
            "limitations": {
                "source_quality": case.get("source_quality"),
                "images_truncated": bool(context.get("images_truncated")),
                "source_images_unavailable": (
                    context.get("source_images_unavailable") or []
                ),
                "source_block_count": context.get("source_block_count", 0),
                "context_error": context.get("context_error") or "",
            },
        },
    }


def build_messages(
    cases: Sequence[dict],
    prompt_path: Path = PROMPT_PATH,
    image_alignment: Optional[dict[str, list[dict]]] = None,
) -> list[dict]:
    prompt = Path(prompt_path).read_text(encoding="utf-8")
    if image_alignment is None:
        _, image_alignment = align_batch_images(cases)
    payload = {"cases": [_model_case_payload(case, image_alignment.get(str(case["case_id"]), [])) for case in cases]}
    return [
        {"role": "system", "content": prompt},
        {
            "role": "user",
            "content": (
                "Проведи независимый reason-first аудит всех кейсов ниже. "
                "image_index — 1-based номер в фактическом порядке приложенных изображений; используй его вместе с case_id и block_id.\n\n"
                + json.dumps(payload, ensure_ascii=False)
            ),
        },
    ]


def _clean_evidence(raw: Any) -> list[dict]:
    cleaned: list[dict] = []
    if not isinstance(raw, list):
        return cleaned
    for item in raw:
        if not isinstance(item, dict):
            continue
        source = str(item.get("source") or "")
        if source not in VALID_EVIDENCE_SOURCES:
            continue
        observation_basis = str(item.get("observation_basis") or "")
        if observation_basis not in VALID_OBSERVATION_BASES:
            observation_basis = "derived"
        verification_state = str(item.get("verification_state") or "")
        if verification_state not in VALID_VERIFICATION_STATES:
            verification_state = "unavailable"
        claim_type = str(item.get("claim_type") or "")
        if claim_type not in VALID_CLAIM_TYPES:
            claim_type = "other"
        absence_scope = str(item.get("absence_scope") or "")
        if absence_scope not in VALID_ABSENCE_SCOPES:
            absence_scope = "none"
        cleaned.append({
            "source": source,
            "source_id": str(item.get("source_id") or "")[:300],
            "image_index": _to_int(item.get("image_index")),
            "block_id": str(item.get("block_id") or "").removeprefix("block_")[:200],
            "locator": str(item.get("locator") or "")[:1000],
            "quote": str(item.get("quote") or "")[:3000],
            "implication": str(item.get("implication") or "")[:3000],
            "observation_basis": observation_basis,
            "verification_state": verification_state,
            "claim_type": claim_type,
            "absence_scope": absence_scope,
        })
    return cleaned

def _to_int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _normalized_text(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip().casefold()


def _quote_is_in(quote: str, surface: str) -> bool:
    needle = _normalized_text(quote)
    return len(needle) >= 3 and needle in _normalized_text(surface)


def _case_text_surfaces(case: dict) -> dict[str, str]:
    context = case.get("context") or {}
    blocks = context.get("blocks") or []
    block_text = "\n".join(
        " ".join(
            str(block.get(key) or "")
            for key in ("block_id", "ocr_label", "ocr_or_description")
        )
        for block in blocks
        if isinstance(block, dict)
    )
    finding_text = json.dumps(case.get("finding") or {}, ensure_ascii=False)
    document_text = str(context.get("document_text_excerpt") or "")
    # Compatibility with frozen v3/v7 recovery manifests: related excerpts
    # used to be appended to the primary surface.  Never let those chunks
    # validate a primary-document quote.
    marker_positions = [
        position
        for marker in (
            "[СВЯЗАННЫЙ ДОКУМЕНТ:",
            "--- СВЯЗАННЫЙ ДОКУМЕНТ ---",
        )
        if (position := document_text.find(marker)) >= 0
    ]
    if marker_positions:
        document_text = document_text[: min(marker_positions)].rstrip()
    return {
        "finding": finding_text,
        "expert_reason": str(case.get("expert_reason") or ""),
        "text_block": block_text,
        "document_text": document_text,
        "norm_context": json.dumps(context.get("norm_context") or {}, ensure_ascii=False),
    }


def _block_text_surfaces(case: dict) -> dict[str, str]:
    surfaces: dict[str, list[str]] = defaultdict(list)
    for block in (case.get("context") or {}).get("blocks") or []:
        if not isinstance(block, dict):
            continue
        block_id = str(block.get("block_id") or "").removeprefix("block_")
        if not block_id:
            continue
        surfaces[block_id].append(
            " ".join(
                str(block.get(key) or "")
                for key in ("ocr_label", "ocr_or_description")
            )
        )
    return {block_id: "\n".join(parts) for block_id, parts in surfaces.items()}


def _related_document_surface(
    case: dict,
    *,
    source_id: str,
    locator: str,
) -> tuple[str, str]:
    """Resolve one reference-document surface without crossing provenance."""
    rows = [
        row
        for row in (case.get("context") or {}).get("related_documents") or []
        if isinstance(row, dict) and str(row.get("source_id") or "") == source_id
    ]
    if len(rows) != 1:
        return "", "related_document: source_id is missing or ambiguous"
    row = rows[0]
    if str(row.get("object_id") or "") not in {"", str(case.get("object_id") or "")}:
        return "", "related_document: object_id does not match the case"
    locator_text = _normalized_text(locator)
    for value, label in (
        (row.get("document"), "document code"),
        (row.get("version"), "version"),
    ):
        token = _normalized_text(value)
        if token and token not in locator_text:
            return "", f"related_document: locator omits {label}"
    allowed_pages = set(_page_numbers(row.get("pages") or []))
    locator_pages = set(_locator_page_numbers(locator))
    if allowed_pages and (not locator_pages or not (allowed_pages & locator_pages)):
        return "", "related_document: locator does not match selected pages"
    return str(row.get("excerpt") or ""), ""


def _document_locator_is_valid(case: dict, locator: str) -> bool:
    context = case.get("context") or {}
    loaded_pages = set(_page_numbers(context.get("document_pages_loaded") or []))
    locator_pages = set(_page_numbers(locator))
    if loaded_pages:
        return bool(loaded_pages & locator_pages)
    path_name = Path(str(context.get("document_text_path") or "")).name
    if path_name:
        return _normalized_text(path_name) in _normalized_text(locator)
    return True


def _validate_case_evidence(
    case: dict,
    evidence: Sequence[dict],
    image_alignment: dict[str, list[dict]],
) -> tuple[list[dict], list[str]]:
    case_id = str(case["case_id"])
    aligned_rows = image_alignment.get(case_id, [])
    allowed_images = {
        (
            _to_int(row.get("image_index")),
            str(row.get("block_id") or "").removeprefix("block_"),
        )
        for row in aligned_rows
    }
    allowed_image_sources = {
        (
            _to_int(row.get("image_index")),
            str(row.get("block_id") or "").removeprefix("block_"),
        ): str(row.get("source_id") or "")
        for row in aligned_rows
    }
    context = case.get("context") or {}
    allowed_blocks = {
        str(value or "").removeprefix("block_")
        for value in (
            list(context.get("source_block_ids") or [])
            + list(context.get("graphic_block_ids") or [])
            + list(context.get("text_block_ids") or [])
        )
        if value
    }
    allowed_blocks.update(block_id for _, block_id in allowed_images if block_id)
    surfaces = _case_text_surfaces(case)
    block_surfaces = _block_text_surfaces(case)
    valid: list[dict] = []
    rejected: list[str] = []
    for item in evidence:
        source = str(item.get("source") or "")
        locator = str(item.get("locator") or "").strip()
        quote = str(item.get("quote") or "").strip()
        image_index = _to_int(item.get("image_index"))
        block_id = str(item.get("block_id") or "").removeprefix("block_")
        source_id = str(item.get("source_id") or "").strip()
        if not locator or not quote:
            rejected.append(f"{source}: empty locator or quote")
            continue
        if source == "graphic_block":
            if image_index <= 0 or not block_id or (image_index, block_id) not in allowed_images:
                rejected.append(
                    f"graphic_block: image_index/block_id not attached to {case_id}"
                )
                continue
            expected_source_id = allowed_image_sources.get((image_index, block_id), "")
            if not source_id or not expected_source_id or source_id != expected_source_id:
                rejected.append("graphic_block: source_id does not match attached image")
                continue
        else:
            if image_index != 0:
                rejected.append(f"{source}: non-graphic evidence has image_index")
                continue
            if source == "text_block":
                if not block_id or block_id not in allowed_blocks:
                    rejected.append(f"text_block: block_id not present in {case_id}")
                    continue
                surface = block_surfaces.get(block_id, "")
            elif source == "related_document":
                if block_id:
                    rejected.append("related_document: unexpected block_id")
                    continue
                surface, related_error = _related_document_surface(
                    case,
                    source_id=source_id,
                    locator=locator,
                )
                if related_error:
                    rejected.append(related_error)
                    continue
            else:
                if block_id:
                    rejected.append(f"{source}: unexpected block_id")
                    continue
                if source == "document_text" and source_id != "primary":
                    rejected.append("document_text: source_id must be primary")
                    continue
                if source == "document_text" and not _document_locator_is_valid(case, locator):
                    rejected.append("document_text: locator does not match loaded page/path")
                    continue
                surface = surfaces.get(source, "")
            if not _quote_is_in(quote, surface):
                rejected.append(f"{source}: quote not found in {case_id} context")
                continue
        valid.append(item)
    return valid, rejected



def _normalize_batch_output_legacy(cases: Sequence[dict], payload: Any) -> tuple[list[dict], list[str]]:
    """Validate model output and apply conservative evidence gates."""
    expected = {str(case["case_id"]): case for case in cases}
    raw_reviews = payload.get("reviews") if isinstance(payload, dict) else None
    if not isinstance(raw_reviews, list):
        return [], ["top-level reviews array missing"]
    by_id: dict[str, dict] = {}
    errors: list[str] = []
    for raw in raw_reviews:
        if not isinstance(raw, dict):
            errors.append("non-object review returned")
            continue
        case_id = str(raw.get("case_id") or "")
        if case_id not in expected:
            errors.append(f"unexpected case_id: {case_id or '<empty>'}")
            continue
        if case_id in by_id:
            errors.append(f"duplicate case_id: {case_id}")
            continue
        by_id[case_id] = raw

    normalized: list[dict] = []
    for case_id, case in expected.items():
        raw = by_id.get(case_id)
        if raw is None:
            errors.append(f"missing case_id: {case_id}")
            continue
        verdict = str(raw.get("verdict") or "")
        if verdict not in VALID_VERDICTS:
            verdict = "insufficient_evidence"
        evidence = _clean_evidence(raw.get("decisive_evidence"))
        reviewed_sources = [
            source for source in (raw.get("reviewed_sources") or [])
            if source in VALID_EVIDENCE_SOURCES
        ]
        adjustments: list[str] = []
        concrete = [
            item for item in evidence
            if item["source"] in CONCRETE_EVIDENCE_SOURCES
            and item["locator"].strip()
            and item["quote"].strip()
        ]
        if verdict in {"expert_may_be_wrong", "expert_correct"} and not concrete:
            adjustments.append(
                f"{verdict} downgraded: no concrete external evidence with locator and quote"
            )
            verdict = "insufficient_evidence"

        action = str(raw.get("recommended_action") or "")
        if verdict == "expert_correct":
            action = "keep_rejected"
        elif verdict == "expert_may_be_wrong":
            action = "manual_recheck"
        else:
            action = "collect_context"
        normalized.append({
            "case_id": case_id,
            "status": "success",
            "verdict": verdict,
            "raw_verdict": str(raw.get("verdict") or ""),
            "confidence": str(raw.get("confidence") or "low"),
            "reason_assessment": str(raw.get("reason_assessment") or "")[:6000],
            "finding_assessment": str(raw.get("finding_assessment") or "")[:6000],
            "norm_assessment": str(raw.get("norm_assessment") or "")[:4000],
            "decisive_evidence": evidence,
            "reviewed_sources": reviewed_sources,
            "missing_context": [str(value)[:2000] for value in (raw.get("missing_context") or [])],
            "recommended_action": action,
            "guard_adjustments": adjustments,
            "input_hash": case.get("input_hash"),
            "object_id": case.get("object_id"),
            "object_name": case.get("object_name"),
            "discipline": case.get("discipline"),
            "document": case.get("document"),
            "version_id": case.get("version_id"),
            "item_id": case.get("item_id"),
            "expert_timestamp": case.get("expert_timestamp"),
            "expert_reason": case.get("expert_reason"),
            "finding_problem": (case.get("finding") or {}).get("problem") or (case.get("finding") or {}).get("description") or "",
            "reviewed_at": utc_now_iso(),
        })
    return normalized, errors

def normalize_batch_output(
    cases: Sequence[dict],
    payload: Any,
    *,
    image_alignment: Optional[dict[str, list[dict]]] = None,
) -> tuple[list[dict], list[str]]:
    """Validate model output with binding, per-case quote, and image gates."""
    expected = {str(case["case_id"]): case for case in cases}
    raw_reviews = payload.get("reviews") if isinstance(payload, dict) else None
    if not isinstance(raw_reviews, list):
        return [], ["top-level reviews array missing"]
    if image_alignment is None:
        _, image_alignment = align_batch_images(cases)

    by_id: dict[str, dict] = {}
    errors: list[str] = []
    for raw in raw_reviews:
        if not isinstance(raw, dict):
            errors.append("non-object review returned")
            continue
        case_id = str(raw.get("case_id") or "")
        if case_id not in expected:
            errors.append(f"unexpected case_id: {case_id or '<empty>'}")
            continue
        if case_id in by_id:
            errors.append(f"duplicate case_id: {case_id}")
            continue
        by_id[case_id] = raw

    normalized: list[dict] = []
    for case_id, case in expected.items():
        raw = by_id.get(case_id)
        if raw is None:
            errors.append(f"missing case_id: {case_id}")
            continue

        verdict = str(raw.get("verdict") or "")
        if verdict not in VALID_VERDICTS:
            verdict = "insufficient_evidence"
        binding_status = str(raw.get("binding_status") or "")
        if binding_status not in VALID_BINDING_STATUSES:
            binding_status = "missing"
        if case.get("source_quality") != "same_version_artifact":
            binding_status = "missing"

        factual_verdict = str(raw.get("factual_verdict") or "")
        if factual_verdict not in VALID_FACTUAL_VERDICTS:
            factual_verdict = "unclear"
        report_value = str(raw.get("report_value") or "")
        if report_value not in VALID_REPORT_VALUES:
            report_value = "unclear"
        reason_quality = str(raw.get("reason_quality") or "")
        if reason_quality not in VALID_REASON_QUALITIES:
            reason_quality = "unsubstantiated"
        decision_effect = str(raw.get("decision_effect") or "")
        if decision_effect not in VALID_DECISION_EFFECTS:
            decision_effect = "unclear"
        rejection_basis = str(raw.get("rejection_basis") or "")
        if rejection_basis not in VALID_REJECTION_BASES:
            rejection_basis = "unknown"
        practical_impact = str(raw.get("practical_impact") or "")
        if practical_impact not in VALID_PRACTICAL_IMPACTS:
            practical_impact = "unclear"
        impact_assessment = str(raw.get("impact_assessment") or "")[:4000]
        source_alignment = str(raw.get("source_alignment") or "")
        if source_alignment not in VALID_SOURCE_ALIGNMENTS:
            source_alignment = "unreadable"
        scope_context_status = str(raw.get("scope_context_status") or "")
        if scope_context_status not in VALID_SCOPE_CONTEXT_STATUSES:
            scope_context_status = "missing"
        confidence = str(raw.get("confidence") or "low")
        if confidence not in {"high", "medium", "low"}:
            confidence = "low"

        integrity_flags = [
            str(value)[:300]
            for value in (raw.get("integrity_flags") or [])
            if str(value or "").strip()
        ]
        if case.get("source_quality") == "same_version_artifact_conflict":
            if "source_artifact_conflict" not in integrity_flags:
                integrity_flags.append("source_artifact_conflict")
        decision_origin = str(case.get("decision_origin") or "human")
        non_human_reason = decision_origin != "human"
        if non_human_reason:
            reason_quality = "unsubstantiated"
            if "service_or_carried_reason" not in integrity_flags:
                integrity_flags.append("service_or_carried_reason")
        if not str(case.get("expert_reason") or "").strip():
            reason_quality = "missing"
            binding_status = "missing"
            if "missing_expert_reason" not in integrity_flags:
                integrity_flags.append("missing_expert_reason")

        raw_evidence = _clean_evidence(raw.get("decisive_evidence"))
        evidence, rejected_evidence = _validate_case_evidence(
            case,
            raw_evidence,
            image_alignment,
        )
        reviewed_sources = [
            source
            for source in (raw.get("reviewed_sources") or [])
            if source in VALID_EVIDENCE_SOURCES
        ]
        missing_context = [
            str(value)[:2000]
            for value in (raw.get("missing_context") or [])
            if str(value or "").strip()
        ]
        adjustments = [f"evidence rejected: {reason}" for reason in rejected_evidence]
        concrete = [
            item
            for item in evidence
            if item["source"] in CONCRETE_EVIDENCE_SOURCES
        ]
        evidence_sources = {item["source"] for item in evidence}
        mismatch_proof = (
            binding_status == "conflict"
            and "reason_item_mismatch" in integrity_flags
            and {"finding", "expert_reason"} <= evidence_sources
        )

        missing_context_downgrade = False
        critical_context_gap = False
        if verdict == "expert_correct":
            if not concrete:
                adjustments.append(
                    "expert_correct downgraded: no case-validated external evidence"
                )
                verdict = "insufficient_evidence"
            elif binding_status != "exact":
                adjustments.append("expert_correct downgraded: finding binding is not exact")
                verdict = "insufficient_evidence"
            elif reason_quality not in {"substantiated", "partial"}:
                adjustments.append(
                    "expert_correct downgraded: reason quality is inconsistent"
                )
                verdict = "insufficient_evidence"
            elif report_value == "remove" and missing_context:
                adjustments.append(
                    "expert_correct/remove downgraded: "
                    "decision-critical context is missing"
                )
                verdict = "insufficient_evidence"
                report_value = "unclear"
                missing_context_downgrade = True
        elif verdict == "expert_may_be_wrong" and not (concrete or mismatch_proof):
            adjustments.append(
                "expert_may_be_wrong downgraded: no validated evidence or binding mismatch"
            )
            verdict = "insufficient_evidence"
        if binding_status == "missing":
            adjustments.append("verdict downgraded: exact finding binding is missing")
            verdict = "insufficient_evidence"
        if binding_status == "conflict" and not (
            verdict == "expert_may_be_wrong" and mismatch_proof
        ):
            adjustments.append("verdict downgraded: conflicting finding binding")
            verdict = "insufficient_evidence"
        if reason_quality == "missing":
            verdict = "insufficient_evidence"
        if non_human_reason:
            adjustments.append("verdict downgraded: reason is service/carryover text")
            verdict = "insufficient_evidence"

        unsafe_evidence = [
            item
            for item in concrete
            if item.get("verification_state") in {"conflict", "unavailable"}
            or (
                item.get("observation_basis") == "ocr"
                and item.get("verification_state") != "corroborated"
                and item.get("claim_type") in {
                    "text_token", "dimension", "geometry", "absence", "relation"
                }
            )
            or (
                item.get("claim_type") == "absence"
                and item.get("absence_scope") in {"crop", "page"}
            )
        ]
        related_by_id = {
            str(row.get("source_id") or ""): row
            for row in (case.get("context") or {}).get("related_documents") or []
            if isinstance(row, dict) and str(row.get("source_id") or "")
        }
        temporally_unbound_related = [
            item
            for item in concrete
            if item.get("source") == "related_document"
            and str(
                related_by_id.get(str(item.get("source_id") or ""), {}).get(
                    "version_relation"
                )
                or "unknown"
            )
            not in {"case_exact", "explicit_historical_match"}
        ]
        visual_source_gap = source_alignment in {
            "ocr_only_visual_claim",
            "raster_text_conflict",
            "unreadable",
        }
        if unsafe_evidence or visual_source_gap:
            reason = (
                "visual/OCR evidence is conflicting, unreadable, or not corroborated"
            )
            adjustments.append(f"verdict downgraded: {reason}")
            verdict = "insufficient_evidence"
            factual_verdict = "unclear"
            report_value = "unclear"
            confidence = "low"
            critical_context_gap = True
            request = (
                "Сверить спорное значение по исходному PDF: text layer, "
                "растровый crop/полный лист и OCR отдельно."
            )
            if request not in missing_context:
                missing_context.append(request)

        if temporally_unbound_related:
            adjustments.append(
                "verdict downgraded: related-document evidence is not bound to the decision-time version"
            )
            verdict = "insufficient_evidence"
            confidence = "low"
            critical_context_gap = True
            request = (
                "Подтвердить, что редакция связанного документа действовала "
                "на дату экспертного решения."
            )
            if request not in missing_context:
                missing_context.append(request)

        scope_context_gap = scope_context_status in {
            "missing",
            "version_uncertain",
            "conflict",
        }
        if scope_context_gap:
            adjustments.append(
                "verdict downgraded: decision-critical scope/linked-document context is not verified"
            )
            verdict = "insufficient_evidence"
            confidence = "low"
            critical_context_gap = True
            request = (
                "Проверить конкретный связанный документ, его код, раздел, "
                "версию и фрагмент, действовавший на дату решения."
            )
            if request not in missing_context:
                missing_context.append(request)

        if report_value == "remove" and practical_impact not in {"none", "low"}:
            adjustments.append(
                "report_value reset: remove requires demonstrated low or no practical impact"
            )
            report_value = "unclear"
            verdict = "insufficient_evidence"
            critical_context_gap = True

        if decision_effect == "unclear":
            adjustments.append("verdict downgraded: effect on the rejection decision is unclear")
            verdict = "insufficient_evidence"
            critical_context_gap = True
        elif decision_effect == "supports_rejection" and verdict == "expert_may_be_wrong":
            adjustments.append(
                "verdict downgraded: decision_effect supports rejection but verdict disputes it"
            )
            verdict = "insufficient_evidence"
            critical_context_gap = True
        elif decision_effect == "changes_rejection":
            if report_value == "remove":
                adjustments.append(
                    "verdict downgraded: changes_rejection conflicts with report_value=remove"
                )
                verdict = "insufficient_evidence"
                critical_context_gap = True
            elif verdict == "expert_correct":
                adjustments.append(
                    "verdict downgraded: decision_effect changes rejection but verdict keeps it"
                )
                verdict = "insufficient_evidence"
                critical_context_gap = True
        elif decision_effect == "reason_only":
            can_keep_rejected = (
                report_value == "remove"
                and binding_status == "exact"
                and reason_quality in {"substantiated", "partial"}
                and bool(concrete)
                and not missing_context
                and not critical_context_gap
                and not non_human_reason
            )
            if can_keep_rejected:
                if verdict != "expert_correct":
                    adjustments.append(
                        "verdict normalized: reason-only defect does not change a justified rejection"
                    )
                verdict = "expert_correct"
            elif verdict == "expert_may_be_wrong":
                adjustments.append(
                    "verdict downgraded: reason-only criticism does not prove the rejection wrong"
                )
                verdict = "insufficient_evidence"
                critical_context_gap = True

        raw_action = str(raw.get("recommended_action") or "")
        if binding_status == "conflict":
            action = "manual_recheck"
        elif case.get("source_quality") == "same_version_artifact_conflict":
            action = "manual_recheck"
        elif verdict == "expert_correct":
            action = "keep_rejected"
        elif verdict == "expert_may_be_wrong" or mismatch_proof:
            action = "manual_recheck"
        elif non_human_reason:
            action = "manual_recheck"
        elif missing_context_downgrade or critical_context_gap:
            action = "collect_context"
        elif raw_action == "manual_recheck":
            action = "manual_recheck"
        else:
            action = "collect_context"

        if action != "manual_recheck":
            review_priority = "none"
        elif (
            binding_status == "conflict"
            or case.get("source_quality") == "same_version_artifact_conflict"
            or non_human_reason
            or practical_impact == "high"
        ):
            review_priority = "high"
        elif report_value == "downgrade" or practical_impact in {"low", "none"}:
            review_priority = "low"
        else:
            review_priority = "medium"

        normalized.append({
            "case_id": case_id,
            "status": "success",
            "verdict": verdict,
            "raw_verdict": str(raw.get("verdict") or ""),
            "confidence": confidence,
            "binding_status": binding_status,
            "decision_origin": decision_origin,
            "factual_verdict": factual_verdict,
            "report_value": report_value,
            "reason_quality": reason_quality,
            "decision_effect": decision_effect,
            "rejection_basis": rejection_basis,
            "practical_impact": practical_impact,
            "impact_assessment": impact_assessment,
            "source_alignment": source_alignment,
            "scope_context_status": scope_context_status,
            "review_priority": review_priority,
            "integrity_flags": integrity_flags,
            "reason_assessment": str(raw.get("reason_assessment") or "")[:6000],
            "finding_assessment": str(raw.get("finding_assessment") or "")[:6000],
            "norm_assessment": str(raw.get("norm_assessment") or "")[:4000],
            "decisive_evidence": evidence,
            "reviewed_sources": reviewed_sources,
            "missing_context": missing_context,
            "recommended_action": action,
            "guard_adjustments": adjustments,
            "input_hash": case.get("input_hash"),
            "object_id": case.get("object_id"),
            "object_name": case.get("object_name"),
            "discipline": case.get("discipline"),
            "document": case.get("document"),
            "version_id": case.get("version_id"),
            "item_id": case.get("item_id"),
            "expert_timestamp": case.get("expert_timestamp"),
            "expert_reason": case.get("expert_reason"),
            "finding_problem": (
                (case.get("finding") or {}).get("problem")
                or (case.get("finding") or {}).get("description")
                or ""
            ),
            "reviewed_at": utc_now_iso(),
        })
    return normalized, errors



def _error_record(case: dict, error: str, *, model: str, call_index: int) -> dict:
    return {
        "case_id": case["case_id"],
        "status": "error",
        "error": str(error)[:8000],
        "model": model,
        "call_index": call_index,
        "input_hash": case.get("input_hash"),
        "object_id": case.get("object_id"),
        "object_name": case.get("object_name"),
        "discipline": case.get("discipline"),
        "document": case.get("document"),
        "version_id": case.get("version_id"),
        "item_id": case.get("item_id"),
        "reviewed_at": utc_now_iso(),
    }


def _looks_like_subscription_limit(text: str) -> bool:
    lowered = str(text or "").lower()
    markers = (
        "usage limit",
        "rate limit",
        "too many requests",
        "quota",
        "лимит",
        "try again later",
    )
    return any(marker in lowered for marker in markers)


async def run_codex_audit(
    cases: Sequence[dict],
    *,
    results_path: Path,
    model: str = "codex/gpt-5.6-sol",
    reasoning_effort: str = "high",
    timeout: int = 600,
    batch_size: int = 4,
    max_batch_images: int = 6,
    limit: int = 0,
    max_calls: int = 0,
    delay_seconds: float = 0.0,
    only_case_ids: Optional[set[str]] = None,
    runner: Optional[Callable[..., Any]] = None,
    progress: Optional[Callable[[str], None]] = None,
) -> dict:
    """Run pending cases through subscription Codex, checkpointing each batch."""
    latest, malformed_lines = load_latest_results(results_path)
    pending = [
        case for case in cases
        if not (
            latest.get(str(case["case_id"]), {}).get("status") == "success"
            and latest.get(str(case["case_id"]), {}).get("input_hash")
            == case.get("input_hash")
        )
        and (not only_case_ids or str(case["case_id"]) in only_case_ids)
    ]
    if limit > 0:
        pending = pending[:limit]
    batches = plan_batches(
        pending,
        batch_size=batch_size,
        max_batch_images=max_batch_images,
    )
    if max_calls > 0:
        batches = batches[:max_calls]

    if runner is None:
        from backend.app.services.llm.codex_runner import run_codex_json_messages

        runner = run_codex_json_messages

    counters = Counter()
    counters["selected_pending"] = len(pending)
    counters["planned_calls"] = len(batches)
    counters["malformed_existing_result_lines"] = malformed_lines
    halted_reason = ""

    for call_index, batch in enumerate(batches, start=1):
        image_paths, image_alignment = align_batch_images(batch, max_total_images=max_batch_images)
        if progress:
            progress(
                f"Codex {call_index}/{len(batches)}: {len(batch)} кейс(а/ов), "
                f"{len(image_paths)} изображений"
            )
        try:
            result = await runner(
                build_messages(batch, image_alignment=image_alignment),
                timeout=timeout,
                stage="rejected_finding_expert_audit",
                project_id=str(batch[0].get("project_id") or batch[0].get("document") or ""),
                model=model,
                image_paths=image_paths,
                reasoning_effort=reasoning_effort,
                output_schema=output_schema([str(case["case_id"]) for case in batch]),
                allowed_tools="",
            )
            error_text = " ".join(
                str(value or "")
                for value in (
                    getattr(result, "error_message", ""),
                    getattr(result, "text", ""),
                )
            )
            if getattr(result, "is_error", False) or not isinstance(getattr(result, "json_data", None), dict):
                message = error_text or "Codex returned no valid JSON"
                for case in batch:
                    append_result(results_path, _error_record(case, message, model=model, call_index=call_index))
                    counters["errors"] += 1
                if _looks_like_subscription_limit(message):
                    halted_reason = "subscription_or_rate_limit"
                    break
                continue

            normalized, validation_errors = normalize_batch_output(
                batch,
                result.json_data,
                image_alignment=image_alignment,
            )
            if validation_errors:
                message = "; ".join(validation_errors)
                for case in batch:
                    append_result(results_path, _error_record(case, message, model=model, call_index=call_index))
                    counters["errors"] += 1
                continue

            model_meta = {
                "model": getattr(result, "model", model),
                "reasoning_effort": reasoning_effort,
                "cost_source": getattr(result, "cost_source", "subscription"),
                "input_tokens": int(getattr(result, "input_tokens", 0) or 0),
                "output_tokens": int(getattr(result, "output_tokens", 0) or 0),
                "cached_tokens": int(getattr(result, "cached_tokens", 0) or 0),
                "reasoning_tokens": int(getattr(result, "reasoning_tokens", 0) or 0),
                "duration_ms": int(getattr(result, "duration_ms", 0) or 0),
                "response_id": str(getattr(result, "response_id", "") or ""),
                "call_index": call_index,
            }
            for row in normalized:
                row.update(model_meta)
                append_result(results_path, row)
                counters["completed"] += 1
                counters[f"verdict_{row['verdict']}"] += 1
            counters["calls_completed"] += 1
        except Exception as exc:  # checkpoint error and keep later cases resumable
            message = f"{type(exc).__name__}: {exc}"
            for case in batch:
                append_result(results_path, _error_record(case, message, model=model, call_index=call_index))
                counters["errors"] += 1
            if _looks_like_subscription_limit(message):
                halted_reason = "subscription_or_rate_limit"
                break

        if delay_seconds > 0 and call_index < len(batches):
            await asyncio.sleep(min(float(delay_seconds), 60.0))

    return {
        "counts": dict(counters),
        "halted_reason": halted_reason,
        "results_path": str(Path(results_path).resolve()),
    }


def _md(value: Any, limit: int = 260) -> str:
    text = re.sub(r"\s+", " ", str(value or "")).strip()
    return text[:limit].replace("|", "\\|")


def generate_report(
    cases: Sequence[dict],
    *,
    output_dir: Path,
    results_path: Optional[Path] = None,
) -> dict:
    output_dir = Path(output_dir)
    results_path = Path(results_path or output_dir / "results.jsonl")
    inventory = _load_json(output_dir / "inventory.json") or {}
    filters = inventory.get("filters") if isinstance(inventory, dict) else {}
    latest, malformed = load_latest_results(results_path)
    case_by_id = {str(case["case_id"]): case for case in cases}
    stale_result_ids = [
        case_id
        for case_id, case in case_by_id.items()
        if case_id in latest
        and latest[case_id].get("input_hash") != case.get("input_hash")
    ]
    current = {
        case_id: latest[case_id]
        for case_id in case_by_id
        if case_id in latest and case_id not in stale_result_ids
    }
    successful = [row for row in current.values() if row.get("status") == "success"]
    errors = [row for row in current.values() if row.get("status") == "error"]
    pending_ids = [case_id for case_id in case_by_id if current.get(case_id, {}).get("status") != "success"]
    candidates = [
        row for row in successful
        if row.get("recommended_action") == "manual_recheck"
    ]
    candidates.sort(
        key=lambda row: (
            {"high": 0, "medium": 1, "low": 2, "none": 3}.get(
                str(row.get("review_priority")), 4
            ),
            {"high": 0, "medium": 1, "low": 2}.get(str(row.get("confidence")), 3),
            str(row.get("object_name") or ""),
            str(row.get("document") or ""),
            str(row.get("item_id") or ""),
        )
    )
    verdicts = Counter(str(row.get("verdict") or "") for row in successful)
    review_priorities = Counter(
        str(row.get("review_priority") or "none") for row in candidates
    )
    summary = {
        "schema_version": 2,
        "generated_at": utc_now_iso(),
        "period": inventory.get("period") or (cases[0].get("period") if cases else ""),
        "filters": filters or {},
        "selected_cases": len(cases),
        "completed": len(successful),
        "remaining": len(pending_ids),
        "latest_errors": len(errors),
        "malformed_result_lines_ignored": malformed,
        "stale_results_ignored": len(stale_result_ids),
        "verdicts": dict(verdicts),
        "manual_recheck_priorities": dict(review_priorities),
        "manual_recheck_candidates": len(candidates),
        "completion_pct": round(100 * len(successful) / len(cases), 2) if cases else 100.0,
        "paths": {
            "manifest": str((output_dir / "manifest.jsonl").resolve()),
            "results_log": str(results_path.resolve()),
            "candidates": str((output_dir / "candidates.json").resolve()),
            "csv": str((output_dir / "results.csv").resolve()),
            "report": str((output_dir / "report.md").resolve()),
        },
    }
    _atomic_write_json(output_dir / "summary.json", summary)
    _atomic_write_json(output_dir / "candidates.json", {"summary": summary, "candidates": candidates})

    csv_path = output_dir / "results.csv"
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_csv = csv_path.with_name(f".{csv_path.name}.{os.getpid()}.tmp")
    with tmp_csv.open("w", encoding="utf-8-sig", newline="") as handle:
        fields = [
            "case_id", "status", "verdict", "confidence", "recommended_action", "review_priority",
            "object_name", "discipline", "document", "version_id", "item_id",
            "decision_origin", "binding_status", "factual_verdict", "report_value",
            "reason_quality", "decision_effect", "rejection_basis", "practical_impact",
            "impact_assessment", "source_alignment", "scope_context_status", "integrity_flags",
            "expert_timestamp", "finding_problem", "expert_reason", "reason_assessment",
            "finding_assessment", "norm_assessment", "decisive_evidence", "missing_context", "error",
        ]
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for case in cases:
            row = current.get(str(case["case_id"]), {})
            writer.writerow({
                "case_id": case["case_id"],
                "status": row.get("status", "pending"),
                "verdict": row.get("verdict", ""),
                "confidence": row.get("confidence", ""),
                "recommended_action": row.get("recommended_action", ""),
                "review_priority": row.get("review_priority", ""),
                "object_name": case.get("object_name", ""),
                "discipline": case.get("discipline", ""),
                "decision_origin": row.get("decision_origin") or case.get("decision_origin", ""),
                "binding_status": row.get("binding_status", ""),
                "factual_verdict": row.get("factual_verdict", ""),
                "report_value": row.get("report_value", ""),
                "reason_quality": row.get("reason_quality", ""),
                "decision_effect": row.get("decision_effect", ""),
                "rejection_basis": row.get("rejection_basis", ""),
                "practical_impact": row.get("practical_impact", ""),
                "impact_assessment": row.get("impact_assessment", ""),
                "source_alignment": row.get("source_alignment", ""),
                "scope_context_status": row.get("scope_context_status", ""),
                "integrity_flags": json.dumps(row.get("integrity_flags") or [], ensure_ascii=False),
                "document": case.get("document", ""),
                "version_id": case.get("version_id", ""),
                "item_id": case.get("item_id", ""),
                "expert_timestamp": case.get("expert_timestamp", ""),
                "finding_problem": row.get("finding_problem") or (case.get("finding") or {}).get("problem", ""),
                "expert_reason": case.get("expert_reason", ""),
                "reason_assessment": row.get("reason_assessment", ""),
                "finding_assessment": row.get("finding_assessment", ""),
                "norm_assessment": row.get("norm_assessment", ""),
                "decisive_evidence": json.dumps(row.get("decisive_evidence") or [], ensure_ascii=False),
                "missing_context": json.dumps(row.get("missing_context") or [], ensure_ascii=False),
                "error": row.get("error", ""),
            })
    os.replace(tmp_csv, csv_path)

    include_optimizations = bool((filters or {}).get("include_optimizations"))
    carryover_excluded = bool((filters or {}).get("explicit_carried_over_excluded", True))
    item_scope = "findings и optimizations" if include_optimizations else "findings"
    carryover_scope = "явный carryover исключён" if carryover_excluded else "включая carryover"

    lines = [
        f"# Аудит отклонённых замечаний — {summary['period'] or 'период не указан'}",
        "",
        f"- Выбрано отклонённых {item_scope} ({carryover_scope}): **{summary['selected_cases']}**",
        f"- Проверено Codex: **{summary['completed']}** ({summary['completion_pct']}%)",
        f"- Осталось/нужно повторить: **{summary['remaining']}**",
        f"- Кандидатов на ручную перепроверку: **{summary['manual_recheck_candidates']}**",
        f"- Приоритеты ручной перепроверки: `{json.dumps(summary['manual_recheck_priorities'], ensure_ascii=False)}`",
        f"- Вердикты: `{json.dumps(summary['verdicts'], ensure_ascii=False)}`",
        "",
        "> Дата решения в исходных данных — время последнего пакетного сохранения. Она может не совпадать с датой первоначального решения эксперта.",
        "",
        "Агент не меняет исходные статусы. `expert_may_be_wrong` и ошибки привязки — поводы для ручной перепроверки, а не автоматическая отмена решения.",
        "",
        "## Эксперт мог ошибиться — кандидаты",
        "",
    ]
    if not candidates:
        lines.append("Пока кандидатов нет или прогон ещё не начат.")
    else:
        lines.extend([
            "| Приоритет | Увер. | Объект / документ | ID | Замечание | Почему перепроверить |",
            "|---|---|---|---|---|---|",
        ])
        for row in candidates:
            lines.append(
                f"| {_md(row.get('review_priority'), 20)} | "
                f"{_md(row.get('confidence'), 20)} | "
                f"{_md(row.get('object_name'), 80)} / {_md(row.get('document'), 80)} | "
                f"{_md(row.get('item_id'), 30)} | {_md(row.get('finding_problem'))} | "
                f"{_md(row.get('reason_assessment'))} |"
            )
    lines.extend([
        "",
        "## Файлы",
        "",
        "- `inventory.json` — полнота и состав июльской выборки.",
        "- `manifest.jsonl` — замороженный вход каждого кейса.",
        "- `results.jsonl` — append-only checkpoint; повторный запуск продолжает с незавершённых кейсов.",
        "- `results.csv` — текущий результат по всем кейсам.",
        "- `candidates.json` — только кандидаты на ручную перепроверку.",
    ])
    _atomic_write_text(output_dir / "report.md", "\n".join(lines) + "\n")
    return summary


__all__ = [
    "AUDIT_CONTRACT_VERSION",
    "AUTO_RETRIEVAL_CONTRACT_VERSION",
    "DEEP_RETRIEVAL_CONTRACT_VERSION",
    "DEFAULT_PROJECTS_V2_ROOT",
    "DEFAULT_REPORT_ROOT",
    "PROMPT_PATH",
    "align_batch_images",
    "build_case_context",
    "build_messages",
    "collect_rejected_cases",
    "enrich_case_for_retrieval",
    "generate_report",
    "load_exact_source_item",
    "load_latest_results",
    "load_manifest",
    "normalize_batch_output",
    "output_schema",
    "parse_month",
    "parse_timestamp",
    "plan_batches",
    "prepare_retrieval_cases",
    "run_codex_audit",
    "write_manifest",
]
