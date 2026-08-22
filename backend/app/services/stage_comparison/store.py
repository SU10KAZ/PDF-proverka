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
from . import sheet_link_repair
from . import text_comparison
from . import text_differences
from . import text_ai_reviewer
from . import project_change_summary
from backend.app.services.llm.codex_runner import run_codex_json_messages


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
        "sheet_link_repairs": get_sheet_link_repairs_state(session_id, pair_id),
        "text_comparison": get_text_comparison_state(session_id, pair_id),
        "text_differences": get_text_differences_state(session_id, pair_id),
        "text_ai_review": get_text_ai_review_state(session_id, pair_id),
        "text_final_comparison": get_text_final_comparison_state(session_id, pair_id),
        "project_change_summary": get_project_change_summary_state(session_id, pair_id),
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


def _load_sheet_link_repairs(session_id: str, pair_id: str) -> dict:
    payload = _read_json(paths_mod.sheet_link_repairs_path(session_id, pair_id))
    if (
        not isinstance(payload, dict)
        or payload.get("version") != sheet_link_repair.VERSION
        or payload.get("kind") != sheet_link_repair.KIND
        or not isinstance(payload.get("repairs"), list)
    ):
        return sheet_link_repair.empty_artifact(pair_id)
    return payload


def get_sheet_link_repairs_state(session_id: str, pair_id: str) -> dict:
    if _load_session_meta(session_id) is None or _load_pair(session_id, pair_id) is None:
        raise KeyError("pair_not_found")
    return sheet_link_repair.public_view(_load_sheet_link_repairs(session_id, pair_id))


def _supersede_active_sheet_link_repairs(session_id: str, pair_id: str) -> None:
    artifact = _load_sheet_link_repairs(session_id, pair_id)
    changed = False
    for repair in artifact.get("repairs") or []:
        if repair.get("status") == "applied":
            repair["status"] = "superseded"
            repair["superseded_at"] = _utc_now()
            changed = True
    if changed:
        artifact["updated_at"] = _utc_now()
        _atomic_write_json(paths_mod.sheet_link_repairs_path(session_id, pair_id), artifact)


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
            if source not in {"auto", "manual", "auto_repair"}:
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
        _supersede_active_sheet_link_repairs(session_id, pair_id)
        return get_sheet_matching_state(session_id, pair_id)


def _text_source_signature(pair: dict, links: dict) -> str:
    """Fingerprint all read-only inputs that influence Stage 2."""
    source: dict[str, Any] = {
        "algorithm": "deterministic_exact_text_v1_13",
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
        document_paths = {
            "pdf_path": Path(str(document.get("pdf_path") or "")),
            "md_path": Path(str(document.get("md_path") or "")),
        }
        document_paths["blocks_path"] = document_paths["md_path"].parent / "blocks.json"
        for kind, path in document_paths.items():
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


def get_text_exclusions_state(session_id: str, pair_id: str) -> dict | None:
    pair = _load_pair(session_id, pair_id) if _load_session_meta(session_id) else None
    if pair is None:
        raise KeyError("pair_not_found")
    payload = _read_json(paths_mod.text_exclusions_path(session_id, pair_id))
    if not isinstance(payload, dict):
        return None
    current_signature = _text_source_signature(pair, _load_sheet_links(session_id, pair_id))
    return text_comparison.public_exclusion_view(
        payload, stale=payload.get("source_signature") != current_signature
    )


def require_text_exclusions_for_downstream(session_id: str, pair_id: str) -> dict:
    """Mandatory gate for every later document-comparison stage."""
    payload = get_text_exclusions_state(session_id, pair_id)
    if not payload:
        raise ValueError("text_exclusions_required")
    if payload.get("stale"):
        raise ValueError("text_exclusions_stale")
    if not payload.get("valid"):
        raise ValueError("text_exclusions_invalid")
    return payload


def get_text_differences_state(session_id: str, pair_id: str) -> dict | None:
    pair = _load_pair(session_id, pair_id) if _load_session_meta(session_id) else None
    if pair is None:
        raise KeyError("pair_not_found")
    payload = _read_json(paths_mod.text_differences_path(session_id, pair_id))
    if not isinstance(payload, dict):
        return None
    exclusions = get_text_exclusions_state(session_id, pair_id)
    stale = (
        not exclusions
        or bool(exclusions.get("stale"))
        or not bool(exclusions.get("valid"))
        or payload.get("source_signature")
        != text_differences.source_signature(exclusions or {})
    )
    return text_differences.public_view(payload, stale=stale)


def run_text_differences(session_id: str, pair_id: str) -> dict:
    """Build one factual-difference record per accepted sheet group."""
    with _lock:
        pair = _load_pair(session_id, pair_id) if _load_session_meta(session_id) else None
        if pair is None:
            raise KeyError("pair_not_found")
        exclusions = require_text_exclusions_for_downstream(session_id, pair_id)
        comparison = _read_json(paths_mod.text_comparison_path(session_id, pair_id))
        if (
            not isinstance(comparison, dict)
            or comparison.get("version") != 1
            or comparison.get("source_signature") != exclusions.get("source_signature")
        ):
            raise ValueError("text_comparison_required")
        expected_signature = text_differences.source_signature(exclusions)
        existing = _read_json(paths_mod.text_differences_path(session_id, pair_id))
        if (
            isinstance(existing, dict)
            and existing.get("version") == text_differences.VERSION
            and existing.get("source_signature") == expected_signature
        ):
            return text_differences.public_view(existing, stale=False) or {}

        links_payload = _load_sheet_links(session_id, pair_id)
        links = list(links_payload.get("links") or [])
        if not links:
            raise ValueError("accepted_sheet_links_required")
        suggestions = _load_sheet_suggestions(session_id, pair_id) or {}
        labels = {
            side: _sheet_labels(list(suggestions.get(f"{side}_sheet_index") or []))
            for side in ("left", "right")
        }
        payload = text_differences.build_text_differences(
            pair_id=pair_id,
            generated_at=_utc_now(),
            exclusions=exclusions,
            comparison=comparison,
            links=links,
            labels=labels,
        )
        _atomic_write_json(paths_mod.text_differences_path(session_id, pair_id), payload)
        return text_differences.public_view(payload, stale=False) or {}


def _current_text_ai_signature(
    session_id: str, pair_id: str,
) -> tuple[dict[str, Any] | None, dict[str, Any] | None, str | None]:
    comparison = _read_json(paths_mod.text_comparison_path(session_id, pair_id))
    differences = _read_json(paths_mod.text_differences_path(session_id, pair_id))
    if not isinstance(comparison, dict) or not isinstance(differences, dict):
        return comparison, differences, None
    return comparison, differences, text_ai_reviewer.source_signature(
        comparison, differences,
        model=text_ai_reviewer.PRODUCTION_MODEL,
        reasoning_effort=text_ai_reviewer.PRODUCTION_REASONING_EFFORT,
    )


def get_text_ai_review_state(session_id: str, pair_id: str) -> dict | None:
    pair = _load_pair(session_id, pair_id) if _load_session_meta(session_id) else None
    if pair is None:
        raise KeyError("pair_not_found")
    payload = _read_json(paths_mod.text_ai_review_path(session_id, pair_id))
    _comparison, _differences, expected = _current_text_ai_signature(session_id, pair_id)
    stale = (
        expected is None or payload.get("source_signature") != expected
        if isinstance(payload, dict) else True
    )
    return text_ai_reviewer.public_review_view(payload, stale=stale)


def get_text_final_comparison_state(session_id: str, pair_id: str) -> dict | None:
    pair = _load_pair(session_id, pair_id) if _load_session_meta(session_id) else None
    if pair is None:
        raise KeyError("pair_not_found")
    payload = _read_json(paths_mod.text_final_comparison_path(session_id, pair_id))
    _comparison, _differences, expected = _current_text_ai_signature(session_id, pair_id)
    stale = (
        expected is None or payload.get("source_signature") != expected
        if isinstance(payload, dict) else True
    )
    return text_ai_reviewer.public_final_view(payload, stale=stale)


def _current_project_change_signature(
    session_id: str, pair_id: str,
) -> tuple[dict[str, Any] | None, list[dict[str, Any]], str | None]:
    final_comparison = _read_json(paths_mod.text_final_comparison_path(session_id, pair_id))
    _comparison, _differences, current_stage4_signature = _current_text_ai_signature(
        session_id, pair_id
    )
    if (
        not isinstance(final_comparison, dict)
        or final_comparison.get("version") != text_ai_reviewer.VERSION
        or final_comparison.get("kind") != text_ai_reviewer.FINAL_KIND
        or current_stage4_signature is None
        or final_comparison.get("source_signature") != current_stage4_signature
    ):
        return final_comparison, [], None
    source_groups = project_change_summary.build_source_groups(final_comparison)
    return final_comparison, source_groups, project_change_summary.source_signature(
        final_comparison, source_groups
    )


def get_project_change_summary_state(session_id: str, pair_id: str) -> dict | None:
    pair = _load_pair(session_id, pair_id) if _load_session_meta(session_id) else None
    if pair is None:
        raise KeyError("pair_not_found")
    payload = _read_json(paths_mod.project_change_summary_path(session_id, pair_id))
    _final, _groups, expected = _current_project_change_signature(session_id, pair_id)
    stale = (
        expected is None or payload.get("source_signature") != expected
        if isinstance(payload, dict) else True
    )
    return project_change_summary.public_view(payload, stale=stale)


def _apply_sheet_link_repair(
    session_id: str, pair_id: str, source_groups: list[dict[str, Any]],
) -> dict[str, Any] | None:
    """Persist at most one atomic high-confidence plan for this Stage 5 run."""
    problem_group_ids = {
        str(group.get("group_id") or "") for group in source_groups
        if (group.get("pair_precheck") or {}).get("status")
        == project_change_summary.PAIR_REVIEW_REQUIRED
    }
    if not problem_group_ids:
        return None
    links = _load_sheet_links(session_id, pair_id)
    suggestions = _load_sheet_suggestions(session_id, pair_id) or {}
    plan = sheet_link_repair.plan_repairs(links, suggestions, problem_group_ids)
    if plan is None:
        return None
    artifact = _load_sheet_link_repairs(session_id, pair_id)
    # Undo is a durable human decision.  The exact same source state must not
    # silently reapply itself on the next Stage 5 button press.
    if any(
        item.get("source_signature") == plan["source_signature"]
        and item.get("status") == "undone"
        for item in artifact.get("repairs") or []
    ):
        return None
    now = _utc_now()
    after_snapshot = dict(plan["after_snapshot"])
    after_snapshot["updated_at"] = now
    repair = {
        "id": _new_id("slr_", 12), "status": "applied",
        "created_at": now, "undone_at": None, "superseded_at": None,
        "source_signature": plan["source_signature"], "confidence": "high",
        "reason": "stage5_sheet_purpose_conflict_with_unique_title_repair",
        "before_links": plan["before_links"], "after_links": plan["after_links"],
        "changes": plan["changes"],
        "before_snapshot": plan["before_snapshot"], "after_snapshot": after_snapshot,
        "dependent_artifacts_recomputed": False,
    }
    artifact.setdefault("repairs", []).append(repair)
    artifact["updated_at"] = now
    _atomic_write_json(paths_mod.sheet_links_path(session_id, pair_id), after_snapshot)
    _atomic_write_json(paths_mod.sheet_link_repairs_path(session_id, pair_id), artifact)
    return repair


def _mark_sheet_link_repair_recomputed(
    session_id: str, pair_id: str, repair_id: str,
) -> None:
    with _lock:
        artifact = _load_sheet_link_repairs(session_id, pair_id)
        for repair in artifact.get("repairs") or []:
            if repair.get("id") == repair_id:
                repair["dependent_artifacts_recomputed"] = True
                repair["recomputed_at"] = _utc_now()
                break
        artifact["updated_at"] = _utc_now()
        _atomic_write_json(paths_mod.sheet_link_repairs_path(session_id, pair_id), artifact)


async def _recompute_after_sheet_link_change(
    session_id: str, pair_id: str,
) -> dict[str, Any]:
    run_text_comparison(session_id, pair_id)
    run_text_differences(session_id, pair_id)
    await run_text_ai_review(session_id, pair_id)
    return await run_project_change_summary(
        session_id, pair_id, _allow_sheet_link_repair=False,
    )


async def undo_sheet_link_repair(
    session_id: str, pair_id: str, repair_id: str,
) -> dict[str, Any]:
    """Restore the latest safe snapshot, then rebuild Stages 2 through 5."""
    with _lock:
        if _load_session_meta(session_id) is None or _load_pair(session_id, pair_id) is None:
            raise KeyError("pair_not_found")
        artifact = _load_sheet_link_repairs(session_id, pair_id)
        active = [item for item in artifact.get("repairs") or [] if item.get("status") == "applied"]
        if not active or str(active[-1].get("id")) != str(repair_id):
            raise ValueError("sheet_link_repair_not_active")
        repair = active[-1]
        current = _load_sheet_links(session_id, pair_id)
        expected = dict(repair.get("after_snapshot") or {})
        current_compare = {key: value for key, value in current.items() if key != "updated_at"}
        expected_compare = {key: value for key, value in expected.items() if key != "updated_at"}
        if current_compare != expected_compare:
            raise ValueError("sheet_links_changed_after_repair")
        restored = dict(repair.get("before_snapshot") or {})
        restored["updated_at"] = _utc_now()
        repair["status"] = "undone"
        repair["undone_at"] = _utc_now()
        repair["dependent_artifacts_recomputed"] = False
        artifact["updated_at"] = _utc_now()
        _atomic_write_json(paths_mod.sheet_links_path(session_id, pair_id), restored)
        _atomic_write_json(paths_mod.sheet_link_repairs_path(session_id, pair_id), artifact)
    await _recompute_after_sheet_link_change(session_id, pair_id)
    _mark_sheet_link_repair_recomputed(session_id, pair_id, repair_id)
    return get_pair_view(session_id, pair_id) or {}


async def run_project_change_summary(
    session_id: str, pair_id: str, *, _allow_sheet_link_repair: bool = True,
) -> dict:
    """Classify and aggregate immutable Stage 4 evidence into engineering facts."""
    with _lock:
        pair = _load_pair(session_id, pair_id) if _load_session_meta(session_id) else None
        if pair is None:
            raise KeyError("pair_not_found")
        final_comparison, source_groups, signature = _current_project_change_signature(
            session_id, pair_id
        )
        if not isinstance(final_comparison, dict) or signature is None:
            raise ValueError("text_final_comparison_required")
        repair = (
            _apply_sheet_link_repair(session_id, pair_id, source_groups)
            if _allow_sheet_link_repair else None
        )
        if repair is not None:
            repair_id = str(repair["id"])
        else:
            repair_id = ""
        existing = _read_json(paths_mod.project_change_summary_path(session_id, pair_id))
        if (
            not repair_id
            and isinstance(existing, dict)
            and existing.get("version") == project_change_summary.VERSION
            and existing.get("source_signature") == signature
            and existing.get("status") == "completed"
            and (existing.get("constraints") or {}).get("fallback_policy") == "review_only_v1"
        ):
            return project_change_summary.public_view(existing, stale=False) or {}
        reusable = {
            str(group.get("group_id") or ""): group
            for group in (existing.get("sheet_groups") or [])
            if (
                isinstance(existing, dict)
                and existing.get("source_signature") == signature
                and isinstance(group, dict)
                and group.get("aggregation_status") in {
                    "ai_aggregated", "pair_review_required",
                }
            )
        } if isinstance(existing, dict) else {}

    if repair_id:
        result = await _recompute_after_sheet_link_change(session_id, pair_id)
        _mark_sheet_link_repair_recomputed(session_id, pair_id, repair_id)
        return {
            **result,
            "sheet_link_repair_applied": True,
            "sheet_link_repair_id": repair_id,
        }

    results: list[dict[str, Any]] = []
    fresh_model_calls = 0
    provider_unavailable = ""
    for source_group in source_groups:
        cached = reusable.get(source_group["group_id"])
        if cached and cached.get("source_group_sha256") == source_group["source_group_sha256"]:
            results.append(cached)
            continue
        if source_group["pair_precheck"]["status"] == project_change_summary.PAIR_REVIEW_REQUIRED:
            results.append(project_change_summary.build_wrong_pair_summary(source_group))
            continue
        error = provider_unavailable
        usage = {"input_tokens": 0, "output_tokens": 0, "cached_tokens": 0, "duration_ms": 0}
        normalized: list[dict[str, Any]] = []
        if not error:
            result = None
            try:
                result = await run_codex_json_messages(
                    [{"role": "user", "content": project_change_summary.prompt_for_groups([source_group])}],
                    timeout=240,
                    stage="stage_comparison_project_change_summary",
                    project_id=f"{session_id}:{pair_id}:{source_group['group_id']}",
                    model=project_change_summary.PRODUCTION_MODEL,
                    reasoning_effort=project_change_summary.PRODUCTION_REASONING_EFFORT,
                    output_schema=project_change_summary.RESPONSE_SCHEMA,
                    allowed_tools="",
                )
            except Exception as exc:  # noqa: BLE001 - persisted conservative fallback
                error = f"provider_exception:{type(exc).__name__}:{exc}"
            fresh_model_calls += 1
            if result is None and not error:
                error = "ai_provider_failure:no_result"
            if result is not None:
                usage = {
                    "input_tokens": result.input_tokens,
                    "output_tokens": result.output_tokens,
                    "cached_tokens": result.cached_tokens,
                    "duration_ms": result.duration_ms,
                }
                if result.is_error or result.json_data is None:
                    error = result.error_message or "ai_provider_failure"
                else:
                    try:
                        normalized = project_change_summary.validate_response(
                            result.json_data, [source_group], recover_single_group_id=True,
                        )[0]
                    except project_change_summary.SummaryValidationError as exc:
                        error = f"validation_failed:{exc}"
            failure_text = f"{error}\n{getattr(result, 'text', '') if result else ''}".lower()
            if error and any(marker in failure_text for marker in (
                "cli_not_found", "not authenticated", "usage limit", "quota",
                "unauthorized", "invalid api key",
            )):
                provider_unavailable = error
        if error:
            normalized = project_change_summary.deterministic_fallback_items(source_group)
        results.append(project_change_summary.build_group_summary(
            source_group, normalized,
            aggregation_status="deterministic_fallback" if error else "ai_aggregated",
            error=error or None, usage=usage,
        ))

    artifact = project_change_summary.build_artifact(
        pair_id=pair_id, generated_at=_utc_now(), source_signature_value=signature,
        sheet_groups=results, fresh_model_calls=fresh_model_calls,
    )
    with _lock:
        _latest_final, _latest_groups, latest_signature = _current_project_change_signature(
            session_id, pair_id
        )
        if latest_signature != signature:
            raise ValueError("text_final_comparison_changed_during_summary")
        _atomic_write_json(paths_mod.project_change_summary_path(session_id, pair_id), artifact)
    return project_change_summary.public_view(artifact, stale=False) or {}


async def run_text_ai_review(session_id: str, pair_id: str) -> dict:
    """Review every accepted text group once with the benchmark-selected model."""
    with _lock:
        pair = _load_pair(session_id, pair_id) if _load_session_meta(session_id) else None
        if pair is None:
            raise KeyError("pair_not_found")
        exclusions = require_text_exclusions_for_downstream(session_id, pair_id)
        comparison, differences, signature = _current_text_ai_signature(session_id, pair_id)
        if not isinstance(comparison, dict) or comparison.get("version") != 1:
            raise ValueError("text_comparison_required")
        if (
            not isinstance(differences, dict)
            or differences.get("version") != text_differences.VERSION
            or differences.get("source_signature") != text_differences.source_signature(exclusions)
        ):
            raise ValueError("text_differences_required")
        assert signature is not None
        existing_review = _read_json(paths_mod.text_ai_review_path(session_id, pair_id))
        existing_final = _read_json(paths_mod.text_final_comparison_path(session_id, pair_id))
        if (
            isinstance(existing_review, dict)
            and existing_review.get("version") == text_ai_reviewer.VERSION
            and existing_review.get("source_signature") == signature
            and existing_review.get("status") == "completed"
            and isinstance(existing_final, dict)
            and existing_final.get("version") == text_ai_reviewer.VERSION
            and existing_final.get("source_signature") == signature
        ):
            return {
                "text_ai_review": text_ai_reviewer.public_review_view(existing_review, stale=False),
                "text_final_comparison": text_ai_reviewer.public_final_view(existing_final, stale=False),
            }
        reusable_completed = {
            str(group.get("id") or ""): group
            for group in (existing_review.get("sheet_groups") or [])
            if (
                isinstance(existing_review, dict)
                and existing_review.get("source_signature") == signature
                and isinstance(group, dict) and group.get("status") == "completed"
            )
        } if isinstance(existing_review, dict) else {}
        links = list(_load_sheet_links(session_id, pair_id).get("links") or [])
        if not links:
            raise ValueError("accepted_sheet_links_required")
        suggestions = _load_sheet_suggestions(session_id, pair_id) or {}
        labels = {
            side: _sheet_labels(list(suggestions.get(f"{side}_sheet_index") or []))
            for side in ("left", "right")
        }
        review_groups = text_ai_reviewer.build_review_groups(
            comparison=comparison, links=links, labels=labels,
        )

    group_results = []
    total_input = total_output = total_cached = total_duration = 0
    represented_model_calls = fresh_model_calls = 0
    chunks_total = 0
    provider_unavailable = ""
    for source_group in review_groups:
        reusable = reusable_completed.get(source_group["group_id"])
        if (
            reusable
            and reusable.get("source_group_sha256") == source_group["source_group_sha256"]
        ):
            group_results.append(reusable)
            reuse_usage = reusable.get("usage") or {}
            total_input += int(reuse_usage.get("input_tokens") or 0)
            total_output += int(reuse_usage.get("output_tokens") or 0)
            total_cached += int(reuse_usage.get("cached_tokens") or 0)
            total_duration += int(reuse_usage.get("duration_ms") or 0)
            represented_model_calls += max(1, len(reusable.get("chunks") or []))
            chunks_total += max(1, len(reusable.get("chunks") or []))
            continue
        error = provider_unavailable
        normalized: list[dict[str, Any]] = []
        model_reported = ""
        usage = {"input_tokens": 0, "output_tokens": 0, "cached_tokens": 0, "duration_ms": 0}
        chunk_results = []
        chunks = text_ai_reviewer.chunk_review_group(source_group)
        chunks_total += len(chunks)
        for chunk in chunks:
            if error:
                break
            chunk_error = ""
            chunk_usage = {
                "input_tokens": 0, "output_tokens": 0,
                "cached_tokens": 0, "duration_ms": 0,
            }
            try:
                result = await run_codex_json_messages(
                    [{"role": "user", "content": text_ai_reviewer.prompt_for_groups([chunk])}],
                    timeout=240,
                    stage="stage_comparison_text_ai_review",
                    project_id=f"{session_id}:{pair_id}:{chunk['group_id']}",
                    model=text_ai_reviewer.PRODUCTION_MODEL,
                    reasoning_effort=text_ai_reviewer.PRODUCTION_REASONING_EFFORT,
                    output_schema=text_ai_reviewer.RESPONSE_SCHEMA,
                    allowed_tools="",
                )
            except Exception as exc:  # noqa: BLE001 - persisted closed failure
                chunk_error = f"provider_exception:{type(exc).__name__}:{exc}"
                result = None
            fresh_model_calls += 1
            if result is None and not chunk_error:
                chunk_error = "ai_provider_failure:no_result"
            if result is not None:
                model_reported = result.model or model_reported
                chunk_usage = {
                    "input_tokens": result.input_tokens,
                    "output_tokens": result.output_tokens,
                    "cached_tokens": result.cached_tokens,
                    "duration_ms": result.duration_ms,
                }
                if result.is_error or result.json_data is None:
                    chunk_error = result.error_message or "ai_provider_failure"
                else:
                    try:
                        chunk_decisions = text_ai_reviewer.validate_response(
                            result.json_data, [chunk], safe_same_moved=True,
                        )[0]["decisions"]
                    except text_ai_reviewer.ReviewValidationError as exc:
                        chunk_error = f"validation_failed:{exc}"
                    else:
                        normalized.extend(chunk_decisions)
            for key in usage:
                usage[key] += int(chunk_usage[key] or 0)
            chunk_results.append({
                "id": chunk["group_id"],
                "source_group_sha256": chunk["source_group_sha256"],
                "status": "failed" if chunk_error else "completed",
                "error": chunk_error or None,
                "usage": chunk_usage,
                "decision_count": 0 if chunk_error else len(chunk_decisions),
            })
            if chunk_error:
                error = f"{chunk['group_id']}:{chunk_error}"
                failure_text = f"{chunk_error}\n{getattr(result, 'text', '')}".lower()
                if any(marker in failure_text for marker in (
                    "cli_not_found", "not authenticated", "usage limit", "quota",
                    "unauthorized", "invalid api key",
                )):
                    provider_unavailable = chunk_error
                normalized = []
                break
        total_input += int(usage["input_tokens"])
        total_output += int(usage["output_tokens"])
        total_cached += int(usage["cached_tokens"])
        total_duration += int(usage["duration_ms"])
        represented_model_calls += len(chunk_results)
        group_results.append({
            "id": source_group["group_id"],
            "left_pages": source_group["left_pages"],
            "right_pages": source_group["right_pages"],
            "left_labels": source_group["left_labels"],
            "right_labels": source_group["right_labels"],
            "source_group_sha256": source_group["source_group_sha256"],
            "status": "failed" if error else "completed",
            "error": error or None,
            "reported_model": model_reported or None,
            "usage": usage,
            "chunks": chunk_results,
            "decisions": normalized,
        })

    completed = sum(group["status"] == "completed" for group in group_results)
    failed = len(group_results) - completed
    status = "completed" if not failed else "failed" if not completed else "partial"
    generated_at = _utc_now()
    review_payload = {
        "version": text_ai_reviewer.VERSION,
        "kind": text_ai_reviewer.KIND,
        "pair_id": pair_id,
        "generated_at": generated_at,
        "source_signature": signature,
        "prompt_version": text_ai_reviewer.PROMPT_VERSION,
        "validator_version": text_ai_reviewer.VALIDATOR_VERSION,
        "model": text_ai_reviewer.PRODUCTION_MODEL,
        "reasoning_effort": text_ai_reviewer.PRODUCTION_REASONING_EFFORT,
        "status": status,
        "sheet_groups": group_results,
        "summary": {
            "total_groups": len(group_results), "completed_groups": completed,
            "failed_groups": failed, "input_tokens": total_input,
            "output_tokens": total_output, "cached_tokens": total_cached,
            "duration_ms": total_duration,
            "represented_model_calls": represented_model_calls,
            "fresh_model_calls": fresh_model_calls,
            "chunks_total": chunks_total,
        },
        "constraints": {
            "text_only": True, "images_sent": False, "norms_sent": False,
            "sheet_links_mutated": False, "raw_artifacts_mutated": False,
            "max_preliminary_per_chunk": text_ai_reviewer.PRODUCTION_MAX_PRELIMINARY_PER_CHUNK,
        },
    }
    final_payload = text_ai_reviewer.build_final_comparison(
        pair_id=pair_id, generated_at=generated_at,
        review_payload=review_payload, differences=differences,
    )
    with _lock:
        _latest_comparison, _latest_differences, latest_signature = _current_text_ai_signature(
            session_id, pair_id
        )
        if latest_signature != signature:
            raise ValueError("text_sources_changed_during_ai_review")
        _atomic_write_json(paths_mod.text_ai_review_path(session_id, pair_id), review_payload)
        _atomic_write_json(paths_mod.text_final_comparison_path(session_id, pair_id), final_payload)
    return {
        "text_ai_review": text_ai_reviewer.public_review_view(review_payload, stale=False),
        "text_final_comparison": text_ai_reviewer.public_final_view(final_payload, stale=False),
    }


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
        existing_exclusions = _read_json(
            paths_mod.text_exclusions_path(session_id, pair_id)
        )
        # Identical inputs return the byte-for-byte same deterministic result.
        if (
            isinstance(existing, dict)
            and existing.get("version") == 1
            and existing.get("source_signature") == signature
            and isinstance(existing_exclusions, dict)
            and existing_exclusions.get("version") == 1
            and existing_exclusions.get("source_signature") == signature
            and text_comparison.text_exclusion_contract_is_valid(existing_exclusions)
        ):
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
        pdf_line_comparison = text_comparison.compare_pdf_text_lines(
            Path(str((pair.get("left") or {})["pdf_path"])),
            Path(str((pair.get("right") or {})["pdf_path"])),
            links,
            fitz,
        )
        block_matches = text_comparison.compare_exact_text_blocks(
            fragments["left"],
            fragments["right"],
            Path(str((pair.get("left") or {})["md_path"])).parent / "blocks.json",
            Path(str((pair.get("right") or {})["md_path"])).parent / "blocks.json",
            links,
        )
        text_comparison.apply_pdf_line_metrics(
            metrics, summary, pdf_line_comparison
        )
        pdf_metrics_by_link = {
            str(item.get("link_id") or ""): item
            for item in pdf_line_comparison.get("link_metrics") or []
        }
        hints = [
            hint for hint in hints
            if float(pdf_metrics_by_link.get(
                str(hint.get("link_id") or ""), {}
            ).get("linked_percent") or 0) < 80.0
        ]
        summary["hints"] = len(hints)
        structured_overlays = text_comparison.build_overlays(
            comparison["matches"], labels
        )
        pdf_line_overlays = text_comparison.build_overlays(
            pdf_line_comparison["matches"], labels
        )
        overlays = text_comparison.prefer_pdf_line_overlays(
            structured_overlays, pdf_line_overlays
        )
        block_overlays = text_comparison.build_overlays(block_matches, labels)
        overlays = text_comparison.prefer_exact_block_overlays(
            overlays, block_overlays
        )
        summary["exact_block_matches"] = len(block_matches)
        generated_at = _utc_now()
        exclusion_contract = text_comparison.build_text_exclusion_contract(
            pair_id=pair_id,
            source_signature=signature,
            generated_at=generated_at,
            structured_comparison=comparison,
            pdf_comparison=pdf_line_comparison,
            overlays=overlays,
        )
        payload = {
            "version": 1,
            "pair_id": pair_id,
            "algorithm": "deterministic_exact_text_v1_13",
            "generated_at": generated_at,
            "source_signature": signature,
            "fragments": fragments,
            "matches": comparison["matches"],
            "remaining": comparison["remaining"],
            "remaining_status": "remaining_for_comparison",
            "overlays": overlays,
            "link_metrics": metrics,
            "sheet_link_hints": hints,
            "summary": summary,
            "downstream_exclusions": {
                "required": True,
                "artifact": "text_exclusions.json",
                "version": exclusion_contract["version"],
                "contract_sha256": exclusion_contract["contract_sha256"],
                "counts": exclusion_contract["counts"],
            },
            "constraints": {
                "llm": False,
                "vision": False,
                "ocr_rerun": False,
                "vector_graphics_comparison": False,
                "pdf_modified": False,
                "sheet_links_modified_automatically": False,
            },
        }
        _atomic_write_json(
            paths_mod.text_exclusions_path(session_id, pair_id),
            exclusion_contract,
        )
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
    "get_sheet_link_repairs_state",
    "run_sheet_matching",
    "save_sheet_links",
    "undo_sheet_link_repair",
    "get_text_comparison_state",
    "get_text_exclusions_state",
    "require_text_exclusions_for_downstream",
    "run_text_comparison",
    "get_text_differences_state",
    "run_text_differences",
    "get_text_ai_review_state",
    "get_text_final_comparison_state",
    "get_project_change_summary_state",
    "run_project_change_summary",
    "run_text_ai_review",
    "render_pdf_page_svg",
    "page_svg_payload",
    "page_thumbnail_payload",
    "page_info_payload",
    "page_preview_payload",
    "page_tile_payload",
    "pdf_text_search_payload",
    "assert_path_in_allowlist",
]
