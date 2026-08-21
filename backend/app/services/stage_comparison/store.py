"""Persistence for PDF pairs, text-only sheet suggestions and user links."""
from __future__ import annotations

import gzip
import hashlib
import json
import math
import os
import re
import threading
import uuid
from collections import OrderedDict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from . import paths as paths_mod
from . import scanner as scanner_mod
from . import document_matching
from . import sheet_matching
from . import text_comparison


SHELL_KIND = "stage_comparison_shell"
SHELL_VERSION = 1
_lock = threading.RLock()


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _new_id(prefix: str = "", length: int = 16) -> str:
    return f"{prefix}{uuid.uuid4().hex[:length]}"


def _atomic_write_json(path: Path, payload: dict | list) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(temporary, path)


def _read_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        return None


def _load_session_meta(session_id: str) -> dict | None:
    payload = _read_json(paths_mod.session_json_path(session_id))
    if not isinstance(payload, dict) or payload.get("kind") != SHELL_KIND:
        return None
    if payload.get("schema_version") != SHELL_VERSION:
        return None
    return payload


def _load_pair(session_id: str, pair_id: str) -> dict | None:
    payload = _read_json(paths_mod.pair_json_path(session_id, pair_id))
    if not isinstance(payload, dict) or payload.get("kind") != "selected_pdf_pair":
        return None
    return {
        "id": payload.get("id"),
        "created_at": payload.get("created_at"),
        "left": payload.get("left"),
        "right": payload.get("right"),
    }


def _pair_ids(session_id: str) -> list[str]:
    meta = _load_session_meta(session_id)
    if meta is None:
        return []
    root = paths_mod.pairs_root(session_id)
    available = {path.name for path in root.iterdir() if path.is_dir()} if root.is_dir() else set()
    ordered = [pair_id for pair_id in meta.get("pair_order", []) if pair_id in available]
    ordered.extend(sorted(available - set(ordered)))
    return ordered


def _pair_with_persisted_status(session_id: str, pair: dict) -> dict:
    """Expose durable comparison state in the lightweight session payload."""
    pair_id = str(pair.get("id") or "")
    suggestions = _read_json(paths_mod.sheet_match_suggestions_path(session_id, pair_id))
    return {
        **pair,
        "sheet_matching_ready": bool(
            isinstance(suggestions, dict) and suggestions.get("version") == 2
        ),
    }


def _session_payload(meta: dict) -> dict:
    session_id = str(meta["id"])
    pairs = [
        _pair_with_persisted_status(session_id, pair)
        for pair_id in _pair_ids(session_id)
        if (pair := _load_pair(session_id, pair_id))
    ]
    return {
        "id": session_id,
        "kind": SHELL_KIND,
        "schema_version": SHELL_VERSION,
        "created_at": meta.get("created_at"),
        "stage_a_path": meta.get("stage_a_path"),
        "stage_b_path": meta.get("stage_b_path"),
        "documents": meta.get("documents") or {"stage_1": [], "stage_2": []},
        "document_pairing": meta.get("document_pairing"),
        "warnings": meta.get("warnings") or [],
        "pairs": pairs,
    }


def get_session(session_id: str) -> dict | None:
    with _lock:
        meta = _load_session_meta(session_id)
        return _session_payload(meta) if meta else None


def _read_index() -> dict:
    payload = _read_json(paths_mod.index_json_path())
    if isinstance(payload, dict) and isinstance(payload.get("sessions"), list):
        return payload
    return {"sessions": []}


def _save_index_entry(meta: dict) -> None:
    index = _read_index()
    entries = [entry for entry in index["sessions"] if entry.get("id") != meta["id"]]
    entries.insert(0, {
        "id": meta["id"],
        "kind": SHELL_KIND,
        "schema_version": SHELL_VERSION,
        "created_at": meta["created_at"],
        "stage_a_path": meta["stage_a_path"],
        "stage_b_path": meta["stage_b_path"],
        "source_signature": meta["source_signature"],
    })
    _atomic_write_json(paths_mod.index_json_path(), {"sessions": entries})


def list_sessions() -> list[dict]:
    items: list[dict] = []
    for entry in _read_index()["sessions"]:
        session_id = str(entry.get("id") or "")
        meta = _load_session_meta(session_id) if session_id else None
        if meta is None:
            continue
        items.append({
            "id": session_id,
            "created_at": meta.get("created_at"),
            "stage_a_path": meta.get("stage_a_path"),
            "stage_b_path": meta.get("stage_b_path"),
            "source_signature": meta.get("source_signature"),
            "stage_1_pdf_count": len((meta.get("documents") or {}).get("stage_1", [])),
            "stage_2_pdf_count": len((meta.get("documents") or {}).get("stage_2", [])),
            "pairs_total": len(_pair_ids(session_id)),
        })
    return items


def _source_signature(stage_a_path: str, stage_b_path: str, left: list[dict], right: list[dict]) -> str:
    compact = {
        "stage_a_path": str(Path(stage_a_path).expanduser().resolve()),
        "stage_b_path": str(Path(stage_b_path).expanduser().resolve()),
        "stage_1": [
            (item["pdf_path"], item.get("html_path"), item.get("version_id")) for item in left
        ],
        "stage_2": [
            (item["pdf_path"], item.get("html_path"), item.get("version_id")) for item in right
        ],
    }
    raw = json.dumps(compact, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def create_session(stage_a_path: str, stage_b_path: str) -> tuple[dict, list[str]]:
    left_entries, left_warnings = scanner_mod.scan_stage_folder(stage_a_path)
    right_entries, right_warnings = scanner_mod.scan_stage_folder(stage_b_path)
    left = [entry.to_dict() for entry in left_entries]
    right = [entry.to_dict() for entry in right_entries]
    warnings = [*left_warnings, *right_warnings]
    signature = _source_signature(stage_a_path, stage_b_path, left, right)

    with _lock:
        for entry in _read_index()["sessions"]:
            if entry.get("kind") == SHELL_KIND and entry.get("source_signature") == signature:
                existing = get_session(str(entry.get("id") or ""))
                if existing is not None:
                    return existing, list(existing.get("warnings") or [])

        session_id = _new_id()
        meta = {
            "id": session_id,
            "kind": SHELL_KIND,
            "schema_version": SHELL_VERSION,
            "created_at": _utc_now(),
            "stage_a_path": str(Path(stage_a_path).expanduser().resolve()),
            "stage_b_path": str(Path(stage_b_path).expanduser().resolve()),
            "source_signature": signature,
            "documents": {"stage_1": left, "stage_2": right},
            "warnings": warnings,
            "pair_order": [],
        }
        _atomic_write_json(paths_mod.session_json_path(session_id), meta)
        _save_index_entry(meta)
        return _session_payload(meta), warnings


def _document(meta: dict, stage_name: str, pdf_path: str) -> dict | None:
    for item in (meta.get("documents") or {}).get(stage_name, []):
        if item.get("pdf_path") == pdf_path:
            return dict(item)
    return None


def create_pair(session_id: str, left_pdf: str, right_pdf: str) -> dict:
    with _lock:
        meta = _load_session_meta(session_id)
        if meta is None:
            raise KeyError("session_not_found")
        left = _document(meta, "stage_1", left_pdf)
        right = _document(meta, "stage_2", right_pdf)
        if left is None or right is None:
            raise ValueError("pdf_must_belong_to_session_sources")

        for pair_id in _pair_ids(session_id):
            pair = _load_pair(session_id, pair_id)
            if pair and (pair.get("left") or {}).get("pdf_path") == left_pdf \
                    and (pair.get("right") or {}).get("pdf_path") == right_pdf:
                return get_pair_view(session_id, pair_id) or pair

        pair_id = _new_id("p", 10)
        pair = {
            "id": pair_id,
            "kind": "selected_pdf_pair",
            "schema_version": 1,
            "created_at": _utc_now(),
            "left": left,
            "right": right,
        }
        _atomic_write_json(paths_mod.pair_json_path(session_id, pair_id), pair)
        meta["pair_order"] = [*meta.get("pair_order", []), pair_id]
        _atomic_write_json(paths_mod.session_json_path(session_id), meta)
        return get_pair_view(session_id, pair_id) or pair


def save_document_pairing(
    session_id: str,
    left_order: list[str | None],
    right_order: list[str | None],
    confirmed_pairs: list[dict],
) -> dict:
    """Persist the user-arranged document rows independently of browser state."""
    with _lock:
        meta = _load_session_meta(session_id)
        if meta is None:
            raise KeyError("session_not_found")
        documents = meta.get("documents") or {}
        available_left = {
            str(item.get("pdf_path")) for item in documents.get("stage_1") or []
            if item.get("pdf_path")
        }
        available_right = {
            str(item.get("pdf_path")) for item in documents.get("stage_2") or []
            if item.get("pdf_path")
        }
        normalized_left = [str(path) if path else None for path in left_order]
        normalized_right = [str(path) if path else None for path in right_order]
        if len(normalized_left) != len(normalized_right):
            raise ValueError("document_orders_must_have_equal_length")

        # Строка, пустая с ОБЕИХ сторон, не выражает ничего и в интерфейсе
        # выглядит мусорной «парой» с двумя приглашениями перетащить документ.
        # Нормализуем на записи, чтобы такие строки нельзя было сохранить в
        # принципе — независимо от того, какой клиент прислал заказ.
        rows = [
            (left, right)
            for left, right in zip(normalized_left, normalized_right)
            if left or right
        ]
        normalized_left = [left for left, _right in rows]
        normalized_right = [right for _left, right in rows]

        def validate_order(order: list[str | None], available: set[str], side: str) -> None:
            selected = [path for path in order if path]
            if len(selected) != len(set(selected)):
                raise ValueError(f"duplicate_document_in_{side}_order")
            if set(selected) != available:
                raise ValueError(f"{side}_order_must_contain_all_session_documents")

        validate_order(normalized_left, available_left, "left")
        validate_order(normalized_right, available_right, "right")
        left_positions = {path: index for index, path in enumerate(normalized_left) if path}
        right_positions = {path: index for index, path in enumerate(normalized_right) if path}
        normalized_pairs: list[dict[str, str]] = []
        seen_left: set[str] = set()
        seen_right: set[str] = set()
        for raw in confirmed_pairs:
            if not isinstance(raw, dict):
                raise ValueError("confirmed_pair_must_be_object")
            left_pdf = str(raw.get("left_pdf") or "")
            right_pdf = str(raw.get("right_pdf") or "")
            if left_pdf not in available_left or right_pdf not in available_right:
                raise ValueError("confirmed_pair_document_not_in_session")
            if left_positions[left_pdf] != right_positions[right_pdf]:
                raise ValueError("confirmed_pair_documents_must_share_row")
            if left_pdf in seen_left or right_pdf in seen_right:
                raise ValueError("confirmed_pair_document_reused")
            seen_left.add(left_pdf)
            seen_right.add(right_pdf)
            normalized_pairs.append({"left_pdf": left_pdf, "right_pdf": right_pdf})

        pairing = {
            "version": 1,
            "left_order": normalized_left,
            "right_order": normalized_right,
            "confirmed_pairs": normalized_pairs,
            "updated_at": _utc_now(),
        }
        meta["document_pairing"] = pairing
        _atomic_write_json(paths_mod.session_json_path(session_id), meta)
        return pairing


def suggest_document_pairing(session_id: str) -> dict:
    """Build a disposable approximate filename pairing without saving it."""
    with _lock:
        meta = _load_session_meta(session_id)
        if meta is None:
            raise KeyError("session_not_found")
        documents = meta.get("documents") or {}
        return document_matching.suggest_document_pairing(
            list(documents.get("stage_1") or []),
            list(documents.get("stage_2") or []),
        )


def _import_fitz():
    try:
        import fitz
        return fitz
    except ImportError as exc:  # pragma: no cover - deployment dependency
        raise RuntimeError("PyMuPDF not installed: pip install PyMuPDF") from exc


def _page_count(pdf_path: str) -> int:
    fitz = _import_fitz()
    with fitz.open(pdf_path) as document:
        return int(document.page_count)


def get_pair_view(session_id: str, pair_id: str) -> dict | None:
    if _load_session_meta(session_id) is None:
        return None
    pair = _load_pair(session_id, pair_id)
    if pair is None:
        return None
    return {
        "session_id": session_id,
        "pair": pair,
        "left_page_count": _page_count((pair["left"] or {})["pdf_path"]),
        "right_page_count": _page_count((pair["right"] or {})["pdf_path"]),
        "sheet_matching": get_sheet_matching_state(session_id, pair_id),
        "text_comparison": get_text_comparison_state(session_id, pair_id),
    }


def _empty_sheet_links(pair_id: str) -> dict:
    return {
        "version": 1,
        "pair_id": pair_id,
        "links": [],
        "unlinked_left_pages": [],
        "updated_at": None,
    }


def _load_sheet_suggestions(session_id: str, pair_id: str) -> dict | None:
    payload = _read_json(paths_mod.sheet_match_suggestions_path(session_id, pair_id))
    # Version 1 was produced by the removed Markdown feature matcher.
    # It must never be exposed as a fallback for the HTML-index matcher.
    if not isinstance(payload, dict) or payload.get("version") != 2:
        return None
    return payload


def _load_sheet_links(session_id: str, pair_id: str) -> dict:
    payload = _read_json(paths_mod.sheet_links_path(session_id, pair_id))
    if not isinstance(payload, dict) or payload.get("version") != 1:
        return _empty_sheet_links(pair_id)
    if not isinstance(payload.get("links"), list):
        return _empty_sheet_links(pair_id)
    payload.setdefault("unlinked_left_pages", [])
    return payload


def _matching_summary(suggestions: dict | None, links: dict) -> dict:
    left_sheet_index = (suggestions or {}).get("left_sheet_index") or []
    right_sheet_index = (suggestions or {}).get("right_sheet_index") or []
    linked_left: set[int] = set()
    linked_right: set[int] = set()
    manual_links = 0
    high = 0
    review = 0
    for link in links.get("links") or []:
        linked_left.update(int(page) for page in link.get("left_pages") or [])
        linked_right.update(int(page) for page in link.get("right_pages") or [])
        if link.get("source") == "manual":
            manual_links += 1
        elif link.get("confidence") == "high":
            high += len(link.get("left_pages") or [])
        else:
            review += len(link.get("left_pages") or [])
    explicitly_unlinked = {int(page) for page in links.get("unlinked_left_pages") or []}
    effective_left = set(linked_left)
    effective_right = set(linked_right)
    for suggestion in (suggestions or {}).get("suggestions") or []:
        left_page = int(suggestion["left_page"])
        if left_page in linked_left or left_page in explicitly_unlinked:
            continue
        right_pages = [
            int(page) for page in suggestion.get("primary_right_pages") or []
        ]
        if not right_pages and suggestion.get("primary_right_page") is not None:
            right_pages = [int(suggestion["primary_right_page"])]
        if not right_pages:
            continue
        effective_left.add(left_page)
        effective_right.update(right_pages)
        if suggestion.get("confidence") == "high":
            high += 1
        else:
            review += 1
    all_left = {int(item["pdf_page"]) for item in left_sheet_index}
    all_right = {int(item["pdf_page"]) for item in right_sheet_index}
    return {
        "auto_high": high,
        "needs_review": review,
        "manual_links": manual_links,
        "unmatched_left": len(all_left - effective_left),
        "unmatched_right": len(all_right - effective_right),
        "unmatched_left_pages": sorted(all_left - effective_left),
        "unmatched_right_pages": sorted(all_right - effective_right),
    }


def get_sheet_matching_state(session_id: str, pair_id: str) -> dict:
    if _load_session_meta(session_id) is None or _load_pair(session_id, pair_id) is None:
        raise KeyError("pair_not_found")
    suggestions = _load_sheet_suggestions(session_id, pair_id)
    links = _load_sheet_links(session_id, pair_id)
    return {
        "suggestions": suggestions,
        "links": links,
        "summary": _matching_summary(suggestions, links),
    }


def run_sheet_matching(session_id: str, pair_id: str) -> dict:
    """Rebuild disposable suggestions without changing the user's link file."""
    with _lock:
        pair = _load_pair(session_id, pair_id) if _load_session_meta(session_id) else None
        if pair is None:
            raise KeyError("pair_not_found")
        indexes: dict[str, list[dict]] = {}
        unavailable_sides: list[str] = []
        for side in ("left", "right"):
            document = pair.get(side) or {}
            pdf_path = Path(str(document.get("pdf_path") or ""))
            html_path = Path(str(document.get("html_path") or ""))
            if not html_path.is_file():
                # Existing sessions predate html_path. The canonical import location
                # is deterministic and does not invoke the old Markdown matcher.
                html_path = pdf_path.parent / "ocr.html"
            extracted: list[dict] = []
            if html_path.is_file():
                try:
                    extracted = sheet_matching.extract_sheet_index_from_results_html(
                        html_path.read_text(encoding="utf-8")
                    )
                except (OSError, UnicodeDecodeError):
                    extracted = []
            if not extracted:
                unavailable_sides.append(side)
            page_count = _page_count(str(pdf_path))
            by_page = {int(item["pdf_page"]): item for item in extracted}
            placeholders = sheet_matching.placeholder_sheet_index(page_count)
            index = [
                dict(by_page.get(page, placeholder))
                for page, placeholder in enumerate(placeholders, 1)
            ]
            md_path = Path(str(document.get("md_path") or ""))
            if md_path.is_file():
                try:
                    semantics = sheet_matching.extract_page_semantics_from_markdown(
                        md_path.read_text(encoding="utf-8")
                    )
                except (OSError, UnicodeDecodeError):
                    semantics = {}
                for record in index:
                    semantic_text = semantics.get(int(record["pdf_page"]))
                    if semantic_text:
                        record["_semantic_text"] = semantic_text
            indexes[side] = index

        result = sheet_matching.match_sheet_indexes(indexes["left"], indexes["right"])
        if unavailable_sides:
            result["status"] = "sheet_index_unavailable"
            result["unavailable_sides"] = unavailable_sides
        payload = {
            "version": 2,
            "pair_id": pair_id,
            "generated_at": _utc_now(),
            **result,
        }
        _atomic_write_json(paths_mod.sheet_match_suggestions_path(session_id, pair_id), payload)
        return get_sheet_matching_state(session_id, pair_id)


def save_sheet_links(
    session_id: str,
    pair_id: str,
    links: list[dict],
    unlinked_left_pages: list[int] | None = None,
) -> dict:
    """Replace the explicit user decision while permitting many-to-many links."""
    with _lock:
        pair = _load_pair(session_id, pair_id) if _load_session_meta(session_id) else None
        if pair is None:
            raise KeyError("pair_not_found")
        left_count = _page_count(str((pair.get("left") or {})["pdf_path"]))
        right_count = _page_count(str((pair.get("right") or {})["pdf_path"]))
        normalized = []
        for raw in links:
            if not isinstance(raw, dict):
                raise ValueError("link_must_be_object")
            left_pages = sorted({int(page) for page in raw.get("left_pages") or []})
            right_pages = sorted({int(page) for page in raw.get("right_pages") or []})
            if not left_pages or not right_pages:
                raise ValueError("link_pages_must_be_non_empty_arrays")
            if left_pages[0] < 1 or left_pages[-1] > left_count:
                raise ValueError("left_page_out_of_range")
            if right_pages[0] < 1 or right_pages[-1] > right_count:
                raise ValueError("right_page_out_of_range")
            source = str(raw.get("source") or "manual")
            if source not in {"auto", "manual"}:
                raise ValueError("invalid_link_source")
            confidence = str(raw.get("confidence") or ("manual" if source == "manual" else "medium"))
            reason = [str(item) for item in raw.get("reason") or [] if str(item)]
            normalized.append({
                "id": str(raw.get("id") or _new_id("link_", 12)),
                "left_pages": left_pages,
                "right_pages": right_pages,
                "source": source,
                "confidence": confidence,
                "reason": reason,
            })
        unlinked = sorted({int(page) for page in unlinked_left_pages or []})
        if unlinked and (unlinked[0] < 1 or unlinked[-1] > left_count):
            raise ValueError("unlinked_left_page_out_of_range")
        linked_left = {page for link in normalized for page in link["left_pages"]}
        payload = {
            "version": 1,
            "pair_id": pair_id,
            "links": normalized,
            "unlinked_left_pages": [page for page in unlinked if page not in linked_left],
            "updated_at": _utc_now(),
        }
        _atomic_write_json(paths_mod.sheet_links_path(session_id, pair_id), payload)
        return get_sheet_matching_state(session_id, pair_id)


def _text_source_signature(pair: dict, links: dict) -> str:
    """Fingerprint all read-only inputs that influence Stage 2."""
    source: dict[str, Any] = {
        "algorithm": "deterministic_exact_text_v1_4",
        "links": [
            {
                "id": link.get("id"),
                "left_pages": sorted(int(page) for page in link.get("left_pages") or []),
                "right_pages": sorted(int(page) for page in link.get("right_pages") or []),
            }
            for link in links.get("links") or []
        ],
        "documents": {},
    }
    for side in ("left", "right"):
        document = pair.get(side) or {}
        entries = {}
        for kind in ("pdf_path", "md_path"):
            path = Path(str(document.get(kind) or ""))
            try:
                stat = path.stat()
                entries[kind] = [str(path.resolve()), stat.st_size, stat.st_mtime_ns]
            except OSError:
                entries[kind] = [str(path), None, None]
        source["documents"][side] = entries
    encoded = json.dumps(source, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _sheet_labels(index: list[dict]) -> dict[int, str]:
    labels: dict[int, str] = {}
    for item in index:
        page = int(item["pdf_page"])
        sheet = str(item.get("sheet_number") or "").strip()
        title = re.sub(r"\s+", " ", str(item.get("title") or "")).strip().rstrip(".")
        if sheet:
            labels[page] = f"Лист {sheet}" + (f" — {title}" if title else "")
        else:
            labels[page] = f"Страница {page}"
    return labels


def get_text_comparison_state(session_id: str, pair_id: str) -> dict | None:
    pair = _load_pair(session_id, pair_id) if _load_session_meta(session_id) else None
    if pair is None:
        raise KeyError("pair_not_found")
    payload = _read_json(paths_mod.text_comparison_path(session_id, pair_id))
    if not isinstance(payload, dict) or payload.get("version") != 1:
        return None
    current_signature = _text_source_signature(pair, _load_sheet_links(session_id, pair_id))
    return text_comparison.public_view(
        payload, stale=payload.get("source_signature") != current_signature
    )


def run_text_comparison(session_id: str, pair_id: str) -> dict:
    """Build the deterministic text overlay without mutating PDFs or links."""
    with _lock:
        pair = _load_pair(session_id, pair_id) if _load_session_meta(session_id) else None
        if pair is None:
            raise KeyError("pair_not_found")
        links_payload = _load_sheet_links(session_id, pair_id)
        links = list(links_payload.get("links") or [])
        if not links:
            raise ValueError("accepted_sheet_links_required")
        signature = _text_source_signature(pair, links_payload)
        existing = _read_json(paths_mod.text_comparison_path(session_id, pair_id))
        # Identical inputs return the byte-for-byte same deterministic result.
        if isinstance(existing, dict) and existing.get("version") == 1 and existing.get("source_signature") == signature:
            return text_comparison.public_view(existing, stale=False) or {}

        suggestions = _load_sheet_suggestions(session_id, pair_id) or {}
        indexes: dict[str, list[dict]] = {}
        fragments: dict[str, list[dict]] = {}
        fitz = _import_fitz()
        for side, stage in (("left", "stage_1"), ("right", "stage_2")):
            document = pair.get(side) or {}
            pdf_path = Path(str(document.get("pdf_path") or ""))
            markdown_path = Path(str(document.get("md_path") or ""))
            if not markdown_path.is_file():
                markdown_path = pdf_path.parent / "document.md"
            if not pdf_path.is_file():
                raise FileNotFoundError(pdf_path)
            if not markdown_path.is_file():
                raise FileNotFoundError(markdown_path)
            page_count = _page_count(str(pdf_path))
            index = list(suggestions.get(f"{side}_sheet_index") or [])
            if not index:
                index = sheet_matching.placeholder_sheet_index(page_count)
            indexes[side] = index
            fragments[side] = text_comparison.extract_document_fragments(
                stage=stage,
                markdown_path=markdown_path,
                pdf_path=pdf_path,
                sheet_index=index,
                fitz=fitz,
            )

        comparison = text_comparison.compare_fragments(
            fragments["left"], fragments["right"], links,
            left_page_count=_page_count(str((pair.get("left") or {})["pdf_path"])),
            right_page_count=_page_count(str((pair.get("right") or {})["pdf_path"])),
        )
        labels = {
            "left": _sheet_labels(indexes["left"]),
            "right": _sheet_labels(indexes["right"]),
        }
        metrics, hints, summary = text_comparison.build_metrics_and_hints(
            comparison,
            fragments["left"],
            fragments["right"],
            links,
            labels["left"],
            labels["right"],
        )
        payload = {
            "version": 1,
            "pair_id": pair_id,
            "algorithm": "deterministic_exact_text_v1_4",
            "generated_at": _utc_now(),
            "source_signature": signature,
            "fragments": fragments,
            "matches": comparison["matches"],
            "remaining": comparison["remaining"],
            "remaining_status": "remaining_for_comparison",
            "overlays": text_comparison.build_overlays(comparison["matches"], labels),
            "link_metrics": metrics,
            "sheet_link_hints": hints,
            "summary": summary,
            "constraints": {
                "llm": False,
                "vision": False,
                "ocr_rerun": False,
                "vector_graphics_comparison": False,
                "pdf_modified": False,
                "sheet_links_modified_automatically": False,
            },
        }
        _atomic_write_json(paths_mod.text_comparison_path(session_id, pair_id), payload)
        return text_comparison.public_view(payload, stale=False) or {}


# MuPDF нумерует clip- и font-идентификаторы заново на каждой странице
# (clip_1, font_386_0, ...). Обе страницы пары вставляются в ОДИН документ
# фронтенда, поэтому одинаковые id столкнутся: `url(#clip_1)` правой панели
# разрешится в первый по документу элемент, то есть в clip-path ЛЕВОЙ. Префикс
# по стороне разводит пространства имён ещё на сервере — один проход по строке,
# результат кэшируется.
_SVG_ID_ANCHOR_RE = re.compile(r'(\bid="|url\(#|\bhref="#)')

# Просмотрщик вставляет страницу инлайновым SVG, а не через <img>, — значит,
# скрипт внутри SVG выполнился бы в контексте портала (в <img> он был инертен).
# Писатель SVG в MuPDF таких узлов не порождает, но PDF приносит пользователь,
# поэтому активные узлы снимаем до отдачи. Проверки-подстроки дешёвые: регулярки
# запускаются, только если подозрительный фрагмент реально есть.
_SVG_ACTIVE_RE = re.compile(
    r"<\s*(script|foreignObject|iframe|object|embed)\b[\s\S]*?<\s*/\s*\1\s*>"
    r"|<\s*(script|foreignObject|iframe|object|embed)\b[^>]*/\s*>",
    re.IGNORECASE,
)
_SVG_HANDLER_RE = re.compile(r"""\son[a-zA-Z]+\s*=\s*(?:"[^"]*"|'[^']*'|[^\s>]+)""")
_SVG_JS_URL_RE = re.compile(r'(?i)(href|xlink:href)\s*=\s*"\s*javascript:[^"]*"')


def _harden_svg(svg: str) -> str:
    lowered = svg.lower()
    if any(token in lowered for token in ("<script", "<foreignobject", "<iframe", "<object", "<embed")):
        svg = _SVG_ACTIVE_RE.sub("", svg)
    if " on" in lowered:
        svg = _SVG_HANDLER_RE.sub("", svg)
    if "javascript:" in lowered:
        svg = _SVG_JS_URL_RE.sub(r'\1="#"', svg)
    return svg

# Кэш готовых страниц: ключ включает mtime/размер PDF, поэтому перезалитый
# документ инвалидирует запись сам. Храним ТОЛЬКО gzip (≈0,5 МБ против ≈6 МБ
# исходника) — при листании туда-сюда повторный рендер не нужен.
_SVG_CACHE_LIMIT = 12
_SVG_GZIP_LEVEL = 6
_svg_cache: "OrderedDict[str, dict]" = OrderedDict()
_svg_cache_lock = threading.Lock()


def _svg_id_prefix(side: str) -> str:
    return "scl_" if side == "left" else "scr_"


def _namespace_svg_ids(svg: str, prefix: str) -> str:
    return _SVG_ID_ANCHOR_RE.sub(lambda match: match.group(1) + prefix, svg)


def _resolve_pair_pdf(session_id: str, pair_id: str, side: str) -> Path:
    if side not in {"left", "right"}:
        raise ValueError("side must be 'left' or 'right'")
    pair = _load_pair(session_id, pair_id) if _load_session_meta(session_id) else None
    if pair is None:
        raise KeyError("pair_not_found")
    pdf_path = Path(str((pair.get(side) or {}).get("pdf_path") or ""))
    if not pdf_path.is_file():
        raise FileNotFoundError(f"pdf_not_found:{pdf_path}")
    return pdf_path


def render_pdf_page_svg(session_id: str, pair_id: str, side: str, page: int) -> bytes:
    if page < 1:
        raise ValueError("page must be >= 1")
    pdf_path = _resolve_pair_pdf(session_id, pair_id, side)
    fitz = _import_fitz()
    with fitz.open(str(pdf_path)) as document:
        if page > document.page_count:
            raise ValueError(f"page_out_of_range:{page}>doc:{document.page_count}")
        svg = document[page - 1].get_svg_image(text_as_path=True)
    return _harden_svg(_namespace_svg_ids(svg, _svg_id_prefix(side))).encode("utf-8")


# Полоса миниатюр листает десятки страниц, а насыщенный лист A1 рисуется
# ~120 мс (лёгкий титульный — 3 мс): цена в отрисовке содержимого, а не в
# размере растра, поэтому ширина на стоимость почти не влияет. Держим готовые
# PNG в памяти — 512 штук это ~6 МБ, зато повторное открытие панели бесплатно.
_THUMB_CACHE_LIMIT = 512
_THUMB_MIN_WIDTH = 64
_THUMB_MAX_WIDTH = 400
_thumb_cache: "OrderedDict[str, dict]" = OrderedDict()
_thumb_cache_lock = threading.Lock()

# Основной просмотрщик не должен превращать насыщенный CAD-лист в десятки
# тысяч DOM-узлов. Полный лист сначала показываем как умеренный preview, а при
# увеличении дорисовываем только видимые фрагменты сетки. Кэш общий для preview
# и тайлов и ограничен байтами: PNG разных дисциплин отличается по размеру на
# порядки, поэтому лимит количества элементов не защищал бы память процесса.
_PAGE_PREVIEW_MIN_WIDTH = 640
_PAGE_PREVIEW_MAX_WIDTH = 2400
_PAGE_TILE_SIZE = 512
_PAGE_TILE_MAX_LEVEL = 6
_PAGE_RASTER_CACHE_MAX_BYTES = 128 * 1024 * 1024
_PAGE_DISPLAY_CACHE_LIMIT = 2
_page_raster_cache: "OrderedDict[str, dict]" = OrderedDict()
_page_raster_cache_bytes = 0
_page_raster_cache_lock = threading.Lock()
_page_display_cache: "OrderedDict[str, dict]" = OrderedDict()
_page_display_cache_lock = threading.RLock()
_page_display_build_locks: dict[str, threading.Lock] = {}
_page_raster_render_slots = threading.Semaphore(2)

# Поиск работает по встроенному текстовому слою PDF. Извлекать текст заново
# при каждом запросе дорого (в паре бывает по несколько десятков A1-листов),
# поэтому держим нормализованный текст последних документов. Подпись в ключе
# автоматически инвалидирует запись после замены PDF.
_PDF_TEXT_CACHE_LIMIT = 4
_pdf_text_cache: "OrderedDict[str, list[str]]" = OrderedDict()
_pdf_text_cache_lock = threading.Lock()


def _pdf_signature(pdf_path: Path) -> str:
    try:
        stat = pdf_path.stat()
        return f"{stat.st_mtime_ns}:{stat.st_size}"
    except OSError:
        return "nostat"


def _normalize_pdf_search_text(value: str) -> str:
    """Case-insensitive search text with PDF line breaks treated as spaces."""
    return " ".join(str(value or "").casefold().split())


def _pdf_page_search_texts(pdf_path: Path) -> list[str]:
    signature = _pdf_signature(pdf_path)
    key = f"{pdf_path}|{signature}"
    with _pdf_text_cache_lock:
        cached = _pdf_text_cache.get(key)
        if cached is not None:
            _pdf_text_cache.move_to_end(key)
            return cached

    fitz = _import_fitz()
    with fitz.open(str(pdf_path)) as document:
        texts = [
            _normalize_pdf_search_text(page.get_text("text", sort=True))
            for page in document
        ]

    with _pdf_text_cache_lock:
        _pdf_text_cache[key] = texts
        _pdf_text_cache.move_to_end(key)
        while len(_pdf_text_cache) > _PDF_TEXT_CACHE_LIMIT:
            _pdf_text_cache.popitem(last=False)
    return texts


def _pdf_page_search_highlights(page, normalized_query: str) -> list[dict[str, float]]:
    """Return normalized rectangles for every word touched by a match.

    The viewer renders a raster page at several resolutions, so PDF points
    would tie the response to one particular preview size.  Fractions of the
    rotated page rectangle remain valid for previews, tiles and continuous
    mode alike.  Matching the normalized word stream also keeps Cyrillic
    case-insensitivity identical to the page-level search above.
    """
    words: list[dict] = []
    text_parts: list[str] = []
    cursor = 0
    for raw_word in page.get_text("words", sort=True):
        word_text = _normalize_pdf_search_text(raw_word[4])
        if not word_text:
            continue
        if text_parts:
            cursor += 1
        start = cursor
        cursor += len(word_text)
        text_parts.append(word_text)
        words.append({
            "start": start,
            "end": cursor,
            "rect": tuple(float(value) for value in raw_word[:4]),
        })

    normalized_text = " ".join(text_parts)
    if not normalized_text or normalized_query not in normalized_text:
        return []

    fitz = _import_fitz()
    page_rect = page.rect
    if not page_rect.width or not page_rect.height:
        return []
    rotation_matrix = page.rotation_matrix
    highlights: list[dict[str, float]] = []
    match_start = 0
    match_index = 0
    while True:
        match_start = normalized_text.find(normalized_query, match_start)
        if match_start < 0:
            break
        match_end = match_start + len(normalized_query)
        for word in words:
            if word["end"] <= match_start or word["start"] >= match_end:
                continue
            rect = fitz.Rect(word["rect"])
            if page.rotation:
                rect = rect * rotation_matrix
            x = max(0.0, min(1.0, (rect.x0 - page_rect.x0) / page_rect.width))
            y = max(0.0, min(1.0, (rect.y0 - page_rect.y0) / page_rect.height))
            right = max(x, min(1.0, (rect.x1 - page_rect.x0) / page_rect.width))
            bottom = max(y, min(1.0, (rect.y1 - page_rect.y0) / page_rect.height))
            if right > x and bottom > y:
                highlights.append({
                    "match_index": match_index,
                    "x": round(x, 6),
                    "y": round(y, 6),
                    "width": round(right - x, 6),
                    "height": round(bottom - y, 6),
                })
        # Match ``str.count`` semantics used by the existing result counter:
        # occurrences do not overlap.
        match_start = match_end
        match_index += 1
    return highlights


def pdf_text_search_payload(
    session_id: str,
    pair_id: str,
    side: str,
    query: str,
) -> dict:
    """Find PDF pages containing ``query`` in one side of the viewer pair."""
    normalized_query = _normalize_pdf_search_text(query)
    if not normalized_query:
        raise ValueError("search_query_must_not_be_empty")

    pdf_path = _resolve_pair_pdf(session_id, pair_id, side)
    page_texts = _pdf_page_search_texts(pdf_path)
    matches: list[dict] = [
        {"page": index + 1, "matches": text.count(normalized_query)}
        for index, text in enumerate(page_texts)
        if normalized_query in text
    ]
    if matches:
        fitz = _import_fitz()
        with fitz.open(str(pdf_path)) as document:
            for match in matches:
                match["highlights"] = _pdf_page_search_highlights(
                    document[int(match["page"]) - 1], normalized_query
                )
    return {
        "query": str(query).strip(),
        "pages": matches,
        "matched_pages": len(matches),
        "total_matches": sum(item["matches"] for item in matches),
        "page_count": len(page_texts),
        "has_text_layer": any(page_texts),
    }


def _page_raster_cache_get(key: str) -> dict | None:
    with _page_raster_cache_lock:
        entry = _page_raster_cache.get(key)
        if entry is not None:
            _page_raster_cache.move_to_end(key)
        return entry


def _page_raster_cache_put(key: str, entry: dict) -> None:
    global _page_raster_cache_bytes
    size = len(entry["body"])
    with _page_raster_cache_lock:
        previous = _page_raster_cache.pop(key, None)
        if previous is not None:
            _page_raster_cache_bytes -= len(previous["body"])
        _page_raster_cache[key] = entry
        _page_raster_cache_bytes += size
        while _page_raster_cache and _page_raster_cache_bytes > _PAGE_RASTER_CACHE_MAX_BYTES:
            _, evicted = _page_raster_cache.popitem(last=False)
            _page_raster_cache_bytes -= len(evicted["body"])


def _close_page_display_context(entry: dict) -> None:
    try:
        entry["document"].close()
    except Exception:  # noqa: BLE001 - очистка кэша не должна ронять запрос
        pass


def _evict_page_display_contexts_locked() -> list[dict]:
    evicted: list[dict] = []
    while len(_page_display_cache) > _PAGE_DISPLAY_CACHE_LIMIT:
        victim_key = next(
            (key for key, value in _page_display_cache.items() if value["users"] == 0),
            None,
        )
        if victim_key is None:
            break
        evicted.append(_page_display_cache.pop(victim_key))
    return evicted


def _acquire_page_display_context(pdf_path: Path, signature: str, page: int) -> dict:
    """Один раз разобрать PDF page display list и переиспользовать в пачке тайлов."""
    key = f"{pdf_path}|{signature}|{page}"
    with _page_display_cache_lock:
        entry = _page_display_cache.get(key)
        if entry is not None:
            entry["users"] += 1
            _page_display_cache.move_to_end(key)
            return entry
        build_lock = _page_display_build_locks.setdefault(key, threading.Lock())

    # По одному build-lock на PDF-страницу: разные стороны строятся параллельно,
    # но пачка запросов одного тяжёлого A1 создаёт только одну display list.
    with build_lock:
        with _page_display_cache_lock:
            entry = _page_display_cache.get(key)
            if entry is not None:
                entry["users"] += 1
                _page_display_cache.move_to_end(key)
                return entry
        fitz = _import_fitz()
        document = fitz.open(str(pdf_path))
        try:
            if page > document.page_count:
                raise ValueError(f"page_out_of_range:{page}>doc:{document.page_count}")
            rendered = document[page - 1]
            entry = {
                "key": key,
                "document": document,
                "display_list": rendered.get_displaylist(annots=1),
                "rect": rendered.rect,
                "users": 1,
                "render_lock": threading.Lock(),
            }
        except Exception:
            document.close()
            with _page_display_cache_lock:
                if _page_display_build_locks.get(key) is build_lock:
                    _page_display_build_locks.pop(key, None)
            raise
        with _page_display_cache_lock:
            _page_display_cache[key] = entry
            _page_display_cache.move_to_end(key)
            if _page_display_build_locks.get(key) is build_lock:
                _page_display_build_locks.pop(key, None)
            evicted = _evict_page_display_contexts_locked()
    for stale in evicted:
        _close_page_display_context(stale)
    return entry


def _release_page_display_context(entry: dict) -> None:
    with _page_display_cache_lock:
        entry["users"] = max(0, entry["users"] - 1)
        evicted = _evict_page_display_contexts_locked()
    for stale in evicted:
        _close_page_display_context(stale)


def page_info_payload(session_id: str, pair_id: str, side: str, page: int) -> dict:
    """Размер повёрнутой PDF-страницы и подпись версии для raster viewer."""
    if page < 1:
        raise ValueError("page must be >= 1")
    pdf_path = _resolve_pair_pdf(session_id, pair_id, side)
    fitz = _import_fitz()
    with fitz.open(str(pdf_path)) as document:
        if page > document.page_count:
            raise ValueError(f"page_out_of_range:{page}>doc:{document.page_count}")
        rect = document[page - 1].rect
        width, height = float(rect.width), float(rect.height)
    return {
        "page": page,
        "width": width,
        "height": height,
        "signature": _pdf_signature(pdf_path),
        "tile_size": _PAGE_TILE_SIZE,
        "max_level": _PAGE_TILE_MAX_LEVEL,
    }


def page_preview_payload(
    session_id: str,
    pair_id: str,
    side: str,
    page: int,
    width: int = 1400,
) -> dict:
    """Умеренный полноформатный PNG: мгновенная подложка для листа."""
    if page < 1:
        raise ValueError("page must be >= 1")
    width = max(_PAGE_PREVIEW_MIN_WIDTH, min(_PAGE_PREVIEW_MAX_WIDTH, int(width)))
    pdf_path = _resolve_pair_pdf(session_id, pair_id, side)
    signature = _pdf_signature(pdf_path)
    key = f"preview|{pdf_path}|{signature}|{page}|{width}"
    entry = _page_raster_cache_get(key)
    if entry is not None:
        return entry

    fitz = _import_fitz()
    with _page_raster_render_slots:
        context = _acquire_page_display_context(pdf_path, signature, page)
        try:
            rect = context["rect"]
            scale = width / rect.width if rect.width else 1
            with context["render_lock"]:
                pixmap = context["display_list"].get_pixmap(
                    matrix=fitz.Matrix(scale, scale), colorspace=fitz.csRGB, alpha=False
                )
            body = pixmap.tobytes("png")
        finally:
            _release_page_display_context(context)
    entry = {"body": body, "etag": '"' + hashlib.sha1(body).hexdigest()[:24] + '"'}
    _page_raster_cache_put(key, entry)
    return entry


def page_tile_payload(
    session_id: str,
    pair_id: str,
    side: str,
    page: int,
    level: int,
    tile_x: int,
    tile_y: int,
) -> dict:
    """PNG одного 512px-тайла; level N означает 2**N пикселей на PDF-point."""
    if page < 1:
        raise ValueError("page must be >= 1")
    if level < 0 or level > _PAGE_TILE_MAX_LEVEL:
        raise ValueError(f"tile_level_out_of_range:{level}")
    if tile_x < 0 or tile_y < 0:
        raise ValueError("tile_coordinates_must_be_non_negative")

    pdf_path = _resolve_pair_pdf(session_id, pair_id, side)
    signature = _pdf_signature(pdf_path)
    key = f"tile|{pdf_path}|{signature}|{page}|{level}|{tile_x}|{tile_y}"
    entry = _page_raster_cache_get(key)
    if entry is not None:
        return entry

    fitz = _import_fitz()
    scale = 2 ** level
    span = _PAGE_TILE_SIZE / scale
    with _page_raster_render_slots:
        context = _acquire_page_display_context(pdf_path, signature, page)
        try:
            rect = context["rect"]
            columns = max(1, math.ceil(rect.width / span))
            rows = max(1, math.ceil(rect.height / span))
            if tile_x >= columns or tile_y >= rows:
                raise ValueError(
                    f"tile_out_of_range:{tile_x},{tile_y}>grid:{columns},{rows}"
                )
            clip = fitz.Rect(
                rect.x0 + tile_x * span,
                rect.y0 + tile_y * span,
                min(rect.x1, rect.x0 + (tile_x + 1) * span),
                min(rect.y1, rect.y0 + (tile_y + 1) * span),
            )
            with context["render_lock"]:
                pixmap = context["display_list"].get_pixmap(
                    matrix=fitz.Matrix(scale, scale),
                    clip=clip,
                    colorspace=fitz.csRGB,
                    alpha=False,
                )
            body = pixmap.tobytes("png")
        finally:
            _release_page_display_context(context)
    entry = {"body": body, "etag": '"' + hashlib.sha1(body).hexdigest()[:24] + '"'}
    _page_raster_cache_put(key, entry)
    return entry


def page_thumbnail_payload(
    session_id: str,
    pair_id: str,
    side: str,
    page: int,
    width: int = 160,
) -> dict:
    """Растровая миниатюра страницы для панели навигации по листам."""
    if page < 1:
        raise ValueError("page must be >= 1")
    width = max(_THUMB_MIN_WIDTH, min(_THUMB_MAX_WIDTH, int(width)))
    pdf_path = _resolve_pair_pdf(session_id, pair_id, side)
    try:
        stat = pdf_path.stat()
        signature = f"{stat.st_mtime_ns}:{stat.st_size}"
    except OSError:
        signature = "nostat"
    key = f"{pdf_path}|{signature}|{page}|{width}"

    with _thumb_cache_lock:
        entry = _thumb_cache.get(key)
        if entry is not None:
            _thumb_cache.move_to_end(key)

    if entry is None:
        fitz = _import_fitz()
        with fitz.open(str(pdf_path)) as document:
            if page > document.page_count:
                raise ValueError(f"page_out_of_range:{page}>doc:{document.page_count}")
            rendered = document[page - 1]
            scale = width / rendered.rect.width if rendered.rect.width else 1
            pixmap = rendered.get_pixmap(
                matrix=fitz.Matrix(scale, scale), colorspace=fitz.csRGB, alpha=False
            )
            body = pixmap.tobytes("png")
        entry = {"body": body, "etag": '"' + hashlib.sha1(body).hexdigest()[:24] + '"'}
        with _thumb_cache_lock:
            _thumb_cache[key] = entry
            _thumb_cache.move_to_end(key)
            while len(_thumb_cache) > _THUMB_CACHE_LIMIT:
                _thumb_cache.popitem(last=False)

    return entry


def page_svg_payload(
    session_id: str,
    pair_id: str,
    side: str,
    page: int,
    accept_gzip: bool = True,
) -> dict:
    """Векторная страница для просмотрщика: gzip-тело + ETag.

    Сжатие делаем здесь (уровень 6, в threadpool роутера), а не в общем
    GZipMiddleware: тот жмёт уровнем 9 прямо в event loop, а лист формата A1
    после `text_as_path` весит ~6 МБ — это ~113 мс блокировки цикла на каждую
    открытую страницу против ~45 мс вне его.
    """
    if page < 1:
        raise ValueError("page must be >= 1")
    pdf_path = _resolve_pair_pdf(session_id, pair_id, side)
    try:
        stat = pdf_path.stat()
        signature = f"{stat.st_mtime_ns}:{stat.st_size}"
    except OSError:
        signature = "nostat"
    key = f"{pdf_path}|{signature}|{side}|{page}"

    with _svg_cache_lock:
        entry = _svg_cache.get(key)
        if entry is not None:
            _svg_cache.move_to_end(key)

    if entry is None:
        raw = render_pdf_page_svg(session_id, pair_id, side, page)
        entry = {
            "gzip": gzip.compress(raw, _SVG_GZIP_LEVEL),
            "etag": '"' + hashlib.sha1(raw).hexdigest()[:24] + '"',
        }
        with _svg_cache_lock:
            _svg_cache[key] = entry
            _svg_cache.move_to_end(key)
            while len(_svg_cache) > _SVG_CACHE_LIMIT:
                _svg_cache.popitem(last=False)

    if accept_gzip:
        return {"body": entry["gzip"], "encoding": "gzip", "etag": entry["etag"]}
    return {"body": gzip.decompress(entry["gzip"]), "encoding": None, "etag": entry["etag"]}


def _parse_allowlist() -> list[Path]:
    raw = os.environ.get("AUDIT_STAGE_COMPARISON_ROOTS", "").strip()
    if not raw:
        return []
    parts = [part.strip() for part in raw.split(";") if part.strip()]
    if len(parts) == 1 and os.pathsep != ";":
        parts = [part.strip() for part in raw.split(os.pathsep) if part.strip()]
    return [Path(part).expanduser().resolve() for part in parts]


def assert_path_in_allowlist(path: str) -> None:
    candidate = Path(path).expanduser().resolve()
    try:
        from backend.app.core.config import DATA_DIR
        v2_root = Path(os.environ.get("AUDIT_PROJECTS_V2_DIR") or (Path(DATA_DIR) / "projects_v2")).resolve()
        relative = candidate.relative_to((v2_root / "objects").resolve())
        if len(relative.parts) >= 3 and relative.parts[1:3] in {
            ("comparison", "stage_1"), ("comparison", "stage_2"),
        }:
            return
    except ValueError:
        pass
    allowlist = _parse_allowlist()
    if not allowlist:
        return
    if any(candidate == root or root in candidate.parents for root in allowlist):
        return
    raise PermissionError(f"path_outside_allowlist:{candidate}")


__all__ = [
    "SHELL_KIND",
    "create_session",
    "list_sessions",
    "get_session",
    "create_pair",
    "save_document_pairing",
    "suggest_document_pairing",
    "get_pair_view",
    "get_sheet_matching_state",
    "run_sheet_matching",
    "save_sheet_links",
    "get_text_comparison_state",
    "run_text_comparison",
    "render_pdf_page_svg",
    "page_svg_payload",
    "page_thumbnail_payload",
    "page_info_payload",
    "page_preview_payload",
    "page_tile_payload",
    "pdf_text_search_payload",
    "assert_path_in_allowlist",
]
