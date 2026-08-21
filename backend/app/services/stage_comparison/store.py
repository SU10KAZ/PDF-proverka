"""Persistence for PDF pairs, text-only sheet suggestions and user links."""
from __future__ import annotations

import gzip
import hashlib
import json
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


def _session_payload(meta: dict) -> dict:
    session_id = str(meta["id"])
    pairs = [pair for pair_id in _pair_ids(session_id) if (pair := _load_pair(session_id, pair_id))]
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
        right_page = suggestion.get("primary_right_page")
        if right_page is None:
            continue
        effective_left.add(left_page)
        effective_right.add(int(right_page))
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
            indexes[side] = [by_page.get(page, placeholder) for page, placeholder in enumerate(placeholders, 1)]

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
    "render_pdf_page_svg",
    "page_svg_payload",
    "assert_path_in_allowlist",
]
