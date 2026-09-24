"""Read-only data for the stage-2 semantic block workspace.

Nothing here writes under COMPARISON_ROOT. Every function reads what already
exists — frozen V3 runs, Human Mapping ui_data/history, the pair's upload
artifacts (blocks.json + Markdown) and the catalog — and returns one of the
``stage-block-mapping-*/1`` contracts (see corpus-audits/
20260924_unified_sheet_human_mapping_plan/DATA_CONTRACT_PLAN.md §4–§5).
Human decisions keep going through the existing Human Mapping API.
"""
from __future__ import annotations

import hashlib
import json
import os
import struct
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from backend.app.services.human_mapping_production import storage as hm_storage
from backend.app.services.human_mapping_production import validation
from backend.app.services.project_change_v3 import run_storage, source_prep
from backend.app.services.stage_comparison import paths

WRITES_FLAG = "STAGE_BLOCK_MAPPING_WRITES"
UI_DATA_SCHEMA = "human-mapping-ui-data/1"
BRIDGE_NOTICE = "Это предварительная проверка; якорный анализ пока недоступен."
SIDES = ("OLD", "NEW")
MAX_PAGES = 30
REVIEW_STATUSES = {"HUMAN_CONFIRMED", "HUMAN_REJECTED", "HUMAN_UNCERTAIN"}
# Canonical order: the first failing condition is the reason shown (flag last).
WRITE_REASON_ORDER = ("RUN_INVALID", "HM_UNAVAILABLE", "SOURCE_PDF_CHANGED", "SOURCE_BLOCKS_CHANGED",
                      "HISTORY_POISONED", "WRITES_DISABLED")
_VALIDATE_TTL = 30.0
_BRIDGE_TTL = 600.0


class BlockMappingError(Exception):
    """HTTP-mappable refusal: {detail: {error: code, ok: false, ...extra}}."""

    def __init__(self, status: int, code: str, **extra: Any):
        super().__init__(code)
        self.status, self.code, self.extra = status, code, extra


def writes_enabled() -> bool:
    return os.environ.get(WRITES_FLAG, "").strip().lower() in {"1", "true", "yes", "on"}


def now() -> str:
    return datetime.now(timezone.utc).isoformat()


# ── small caches keyed by file identity (path, size, mtime_ns) ──────────────
_cache: dict[Any, tuple[float, Any]] = {}
_cache_lock = threading.Lock()
_CACHE_MAX = 512


def _stat_key(path: Path) -> tuple[str, int | None, int | None]:
    try:
        st = Path(path).stat()
        return str(path), st.st_size, st.st_mtime_ns
    except OSError:
        return str(path), None, None


def _memo(key: Any, compute: Callable[[], Any], ttl: float | None = None) -> Any:
    now_mono = time.monotonic()
    with _cache_lock:
        hit = _cache.get(key)
        if hit and (ttl is None or now_mono - hit[0] < ttl):
            return hit[1]
    value = compute()
    with _cache_lock:
        if len(_cache) >= _CACHE_MAX:
            _cache.clear()
        _cache[key] = (now_mono, value)
    return value


def clear_cache() -> None:
    with _cache_lock:
        _cache.clear()


def _sha(path: Path) -> str | None:
    key = _stat_key(path)
    if key[1] is None:
        return None

    def compute():
        with Path(path).open("rb") as handle:
            return hashlib.file_digest(handle, "sha256").hexdigest()
    return _memo(("sha",) + key, compute)


def _read_json(path: Path) -> Any:
    return json.loads(Path(path).read_bytes())


def _png_size(path: Path) -> tuple[int, int] | None:
    try:
        with Path(path).open("rb") as handle:
            head = handle.read(24)
    except OSError:
        return None
    if head[:8] != b"\x89PNG\r\n\x1a\n" or head[12:16] != b"IHDR":
        return None
    width, height = struct.unpack(">II", head[16:24])
    return (width, height) if width and height else None


# ── scope ───────────────────────────────────────────────────────────────────
def _safe(value: Any, kind: str) -> str:
    try:
        return hm_storage.require_safe_id(value, kind)
    except hm_storage.InvalidScopeId as exc:
        raise BlockMappingError(400, "INVALID_SCOPE_ID", kind=kind) from exc


def _side(value: Any) -> str:
    side = str(value or "").upper()
    if side not in SIDES:
        raise BlockMappingError(400, "INVALID_SIDE")
    return side


def parse_pages(raw: Any) -> list[int]:
    try:
        pages = [int(part) for part in str(raw or "").split(",") if part.strip()]
    except ValueError as exc:
        raise BlockMappingError(400, "INVALID_PAGES") from exc
    if not pages or len(pages) > MAX_PAGES or any(page < 1 for page in pages):
        raise BlockMappingError(400, "INVALID_PAGES")
    return list(dict.fromkeys(pages))


def _pair(session_id: str, pair_id: str) -> dict[str, Any]:
    from backend.app.services.stage_comparison import store

    sid, pid = _safe(session_id, "session"), _safe(pair_id, "pair")
    if not paths.session_json_path(sid).is_file():
        raise BlockMappingError(404, "SESSION_NOT_FOUND")
    if not paths.pair_json_path(sid, pid).is_file():
        raise BlockMappingError(404, "PAIR_NOT_FOUND")
    try:
        return store.get_pair_for_production(sid, pid)
    except KeyError as exc:
        raise BlockMappingError(404, "PAIR_NOT_FOUND") from exc


def _documents(pair: dict[str, Any]) -> dict[str, dict[str, Path]]:
    from backend.app.services.stage_comparison.production_orchestrator import _resolved_document_paths

    return {"OLD": _resolved_document_paths(pair.get("left") or {}),
            "NEW": _resolved_document_paths(pair.get("right") or {})}


def _object_id(session_id: str) -> str | None:
    from backend.app.services.project_change_v3 import scope

    try:
        return scope.object_id_for_session(session_id)
    except Exception:  # unresolved scope never breaks the read-only view
        return None


# ── B-src: recognition blocks from the upload artifacts (no model, no render) ─
def _live_side(documents: dict[str, Path]) -> dict[str, Any]:
    """Rows equal to V3 page.json rows on the same input (source_prep, pure functions)."""
    key = ("live",) + tuple(_stat_key(documents[k]) for k in ("pdf", "blocks", "markdown"))
    return _memo(key, lambda: _build_live_side(documents))


def _build_live_side(documents: dict[str, Path]) -> dict[str, Any]:
    empty = {"available": False, "reason": None, "pdf_sha256": _sha(documents["pdf"]), "blocks_sha256": None,
             "markdown_sha256": None, "page_count": None, "block_count": None, "stamp_count": None,
             "rows": [], "pages_meta": []}
    if not documents["pdf"].is_file():
        return {**empty, "reason": "PDF_MISSING"}
    if not documents["blocks"].is_file():
        return {**empty, "reason": "BLOCKS_JSON_MISSING"}
    if not documents["markdown"].is_file():
        return {**empty, "reason": "MARKDOWN_MISSING"}
    empty.update(blocks_sha256=_sha(documents["blocks"]), markdown_sha256=_sha(documents["markdown"]))
    try:
        payload = _read_json(documents["blocks"])
    except (OSError, ValueError):
        return {**empty, "reason": "BLOCKS_JSON_INVALID"}
    blocks = payload.get("blocks") if isinstance(payload, dict) else None
    if not isinstance(blocks, list):
        return {**empty, "reason": "BLOCKS_JSON_INVALID"}
    try:
        md = source_prep.parse_md_blocks(documents["markdown"])
    except UnicodeDecodeError:
        return {**empty, "reason": "MARKDOWN_NOT_UTF8"}
    rows = []
    for block in blocks:
        try:
            page = source_prep._block_page(block)
        except source_prep.SourcePreparationError:
            return {**empty, "reason": "INVALID_PAGE_INDEX"}
        body = md.get(block.get("block_id"), "")
        tables = [m.group().strip() for m in source_prep._TABLE_RE.finditer(body)]
        try:
            modality = source_prep._modality(block.get("block_type"), tables)
        except source_prep.SourcePreparationError:
            return {**empty, "reason": "UNKNOWN_BLOCK_TYPE"}
        try:
            bbox = source_prep._block_bbox(block)
        except source_prep.SourcePreparationError:
            return {**empty, "reason": "INVALID_BBOX"}
        rows.append({"physical_page": page, "block_id": block["block_id"], "modality": modality,
                     "source_block_type": block.get("block_type"), "bbox": bbox, "structured_md": body,
                     "tables": tables, "existing_description": body if modality == "GRAPHIC" else ""})
    pages_meta = payload.get("pages") if isinstance(payload.get("pages"), list) else []
    return {**empty, "available": True, "page_count": len(pages_meta) or None, "block_count": len(rows),
            "stamp_count": sum(r["source_block_type"] == "stamp" for r in rows), "rows": rows,
            "pages_meta": pages_meta}


def _public_side(live: dict[str, Any]) -> dict[str, Any]:
    return {k: live[k] for k in ("available", "reason", "pdf_sha256", "blocks_sha256", "markdown_sha256",
                                 "page_count", "block_count", "stamp_count")}


def _page_meta(live: dict[str, Any], page: int) -> dict[str, Any] | None:
    meta = next((m for m in live["pages_meta"] if isinstance(m, dict) and m.get("page_index") == page - 1), None)
    if meta is None and 0 < page <= len(live["pages_meta"]):
        meta = live["pages_meta"][page - 1]
    return meta if isinstance(meta, dict) else None


def _geometry_from_meta(meta: dict[str, Any] | None) -> dict[str, Any] | None:
    if not meta or not meta.get("width_px") or not meta.get("height_px"):
        return None
    rotation = meta.get("rotation")
    return {"width_px": int(meta["width_px"]), "height_px": int(meta["height_px"]),
            "rotation": rotation if rotation in (0, 90, 180, 270) else None,
            "geometry_source": "BLOCKS_JSON_PAGE_META"}


# ── B-run: frozen V3 runs ─────────────────────────────────────────────────
def _run_dir(session_id: str, pair_id: str, run_id: str) -> Path:
    return run_storage.run_dir(session_id, pair_id, run_id)


def _manifest(session_id: str, pair_id: str, run_id: str) -> dict[str, Any] | None:
    try:
        return run_storage.read(_run_dir(session_id, pair_id, run_id) / "run_manifest.json")
    except (OSError, ValueError):
        return None


def _validated(session_id: str, pair_id: str, run_id: str) -> dict[str, Any] | None:
    manifest_path = _run_dir(session_id, pair_id, run_id) / "run_manifest.json"

    def compute():
        try:
            return run_storage.validate(session_id, pair_id, run_id)
        except (ValueError, OSError, KeyError, TypeError):
            return None
    return _memo(("validate", session_id, pair_id, run_id) + _stat_key(manifest_path), compute, ttl=_VALIDATE_TTL)


def _require_run(session_id: str, pair_id: str, run_id: str) -> dict[str, Any]:
    run_id = _safe(run_id, "run")
    if _manifest(session_id, pair_id, run_id) is None:
        raise BlockMappingError(404, "RUN_NOT_FOUND")
    manifest = _validated(session_id, pair_id, run_id)
    if manifest is None:
        raise BlockMappingError(409, "RUN_INVALID")
    return manifest


def _ui_data(session_id: str, pair_id: str, run_id: str, object_id: str | None) -> tuple[dict | None, str | None, str | None]:
    """(ui_data, hm_reason, sha256) — same scope condition as the bridge and the catalog."""
    path = _run_dir(session_id, pair_id, run_id) / "human_mapping" / "ui_data.json"

    def compute():
        if not path.is_file():
            return None, "HM_UI_DATA_MISSING", None
        try:
            raw = path.read_bytes()
            data = json.loads(raw)
        except (OSError, ValueError):
            return None, "HM_UI_DATA_UNREADABLE", None
        if not isinstance(data, dict) or not isinstance(data.get("regions"), list):
            return None, "HM_UI_DATA_UNREADABLE", None
        return data, None, hashlib.sha256(raw).hexdigest()
    data, reason, digest = _memo(("ui",) + _stat_key(path), compute)
    if data is None:
        return None, reason, None
    if data.get("schema") != UI_DATA_SCHEMA:
        return None, "HM_SCHEMA_MISMATCH", None
    if object_id is None:
        return None, "OBJECT_UNRESOLVED", None
    if (data.get("run_id"), data.get("session_id"), data.get("object_id"), data.get("pair_key")) != \
            (run_id, session_id, object_id, pair_id):
        return None, "HM_SCOPE_MISMATCH", None
    return data, None, digest


def _pdf_match(manifest: dict[str, Any], documents: dict[str, dict[str, Path]]) -> bool | None:
    try:
        live = [_sha(documents[side]["pdf"]) for side in SIDES]
        recorded = [manifest.get("old_pdf_sha256"), manifest.get("new_pdf_sha256")]
    except (KeyError, TypeError):
        return None
    if None in live or not all(recorded):
        return None
    return live == recorded


def _source_stale(session_id: str, pair_id: str, run_id: str) -> bool:
    from backend.app.services.project_change_v3 import presentation

    result = run_storage.read(_run_dir(session_id, pair_id, run_id) / "project_change_v3_result.json") or {}
    return bool(presentation._stale(session_id, pair_id, result))


def _proj(page: int, row: dict[str, Any]) -> list[Any]:
    # Canonical field projection (DATA_CONTRACT_PLAN §5.2): everything source_prep derives without
    # rendering; graphic_crop_ref / graphic_crop_sha256 are excluded.
    return [page, row.get("block_id"), row.get("modality"), row.get("source_block_type"), row.get("bbox"),
            row.get("structured_md"), row.get("tables"), row.get("existing_description")]


def _run_side_rows(session_id: str, pair_id: str, run_id: str, side: str) -> list[list[Any]] | None:
    directory = _run_dir(session_id, pair_id, run_id) / "project_change_v3" / "source" / side.lower()

    def compute():
        rows = []
        for page_dir in sorted(directory.glob("p[0-9][0-9][0-9]")):
            try:
                record = _read_json(page_dir / "page.json")
            except (OSError, ValueError):
                return None
            rows.extend(_proj(int(record.get("physical_page") or page_dir.name[1:]), b) for b in record.get("blocks") or [])
        return sorted(rows, key=lambda r: (r[0], str(r[1])))
    # A frozen run never changes; key on the manifest identity.
    return _memo(("runrows", session_id, pair_id, run_id, side) + _stat_key(directory.parent.parent.parent / "run_manifest.json"), compute)


def _blocks_content_match(session_id: str, pair_id: str, run_id: str, documents: dict[str, dict[str, Path]]) -> bool:
    for side in SIDES:
        live = _live_side(documents[side])
        run_rows = _run_side_rows(session_id, pair_id, run_id, side)
        if not live["available"] or run_rows is None:
            return False
        live_rows = sorted((_proj(r["physical_page"], r) for r in live["rows"]), key=lambda r: (r[0], str(r[1])))
        canon = lambda rows: json.dumps(rows, ensure_ascii=False, sort_keys=True)
        if canon(run_rows) != canon(live_rows):
            return False
    return True


# ── history check (per-event checks of human_mapping_bridge._build) ─────────
def _read_jsonl(path: Path) -> tuple[list[dict[str, Any]], bool]:
    if not path.is_file():
        return [], True
    try:
        rows = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
    except (OSError, ValueError):
        return [], False
    return [r for r in rows if isinstance(r, dict)], all(isinstance(r, dict) for r in rows)


def _history(session_id: str, pair_id: str, run_id: str, object_id: str | None, ui: dict[str, Any] | None) -> dict[str, Any]:
    directory = _run_dir(session_id, pair_id, run_id) / "human_mapping"
    files = [directory / "reviews.jsonl", directory / "human_block_link_edits.jsonl", directory / "ui_data.json"]
    return _memo(("history", session_id, pair_id, run_id, object_id) + tuple(_stat_key(f) for f in files),
                 lambda: _check_history(files, pair_id, run_id, object_id, ui))


def _check_history(files: list[Path], pair_id: str, run_id: str, object_id: str | None,
                   ui: dict[str, Any] | None) -> dict[str, Any]:
    from backend.app.services.project_change_v3.human_mapping_bridge import BridgeError, event_time

    reviews, reviews_ok = _read_jsonl(files[0])
    edits, edits_ok = _read_jsonl(files[1])
    codes: list[str] = []
    add = lambda code: codes.append(code) if code not in codes else None
    if not (reviews_ok and edits_ok):
        add("UNREADABLE_HISTORY")
    regions = {r.get("id"): r for r in (ui or {}).get("regions") or []}
    seen: set[str] = set()
    for event in [*reviews, *edits]:
        if (event.get("object_id"), event.get("pair_key"), event.get("comparison_id"), event.get("run_id")) != \
                (object_id, pair_id, pair_id, run_id):
            add("HUMAN_EVENT_SCOPE_MISMATCH")
            continue
        region = regions.get(event.get("region_id"))
        if region is None:
            add("STALE_REGION")
            continue
        eid = event.get("review_id") or event.get("event_id")
        if not isinstance(eid, str) or not eid or eid in seen:
            add("DUPLICATE_OR_MISSING_EVENT_ID")
            continue
        seen.add(eid)
        try:
            event_time(event)
        except BridgeError:
            add("INVALID_EVENT_TIMESTAMP")
            continue
        for side in SIDES:
            key = side.lower()
            ids = event.get(key + "_block_ids") if "review_id" in event else [event.get(key + "_block_id")]
            if not isinstance(ids, list) or not ids:
                add("EMPTY_REVIEW_ENDPOINTS")
                break
            if not set(ids) <= validation.allowed_block_ids(region, side):
                add("BLOCK_OUTSIDE_REGION")
                break
    if any(r.get("status") not in REVIEW_STATUSES for r in reviews):
        add("INVALID_STATUS")
    replayed: list[dict[str, Any]] = []
    for event in edits:
        region = regions.get(event.get("region_id"))
        if region is None:
            continue
        try:
            validation.validate_block_link_event(
                event_type=event.get("event_type"), region=region, events=replayed, link_id=event.get("link_id"),
                old_block_id=event.get("old_block_id"), new_block_id=event.get("new_block_id"),
                previous_link_id=event.get("previous_link_id"))
        except (validation.BlockLinkValidationError, KeyError, TypeError):
            add("INVALID_BLOCK_LINK_REPLAY")
            break
        replayed.append(event)
    return {"reviews": len(reviews), "block_link_events": len(edits), "poisoned": bool(codes), "poison_codes": codes}


# ── E1: status ─────────────────────────────────────────────────────────────
def _parse_time(value: Any) -> float:
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp()
    except (TypeError, ValueError):
        return 0.0


def _run_entry(session_id: str, pair_id: str, run_id: str, manifest: dict[str, Any], object_id: str | None,
               current_run_id: str | None, documents: dict[str, dict[str, Path]]) -> dict[str, Any]:
    from backend.app.services.project_change_catalog.catalog import model_display

    validated = _validated(session_id, pair_id, run_id)
    run_ok = bool(validated) and manifest.get("frozen") is True and manifest.get("comparison_id") == session_id \
        and (object_id is None or manifest.get("object_id") == object_id)
    ui, hm_reason, _digest = _ui_data(session_id, pair_id, run_id, object_id)
    pdf_match = _pdf_match(manifest, documents)
    stale = _source_stale(session_id, pair_id, run_id)
    content = _blocks_content_match(session_id, pair_id, run_id, documents) if stale and pdf_match else None
    history = _history(session_id, pair_id, run_id, object_id, ui) if ui is not None else \
        {"reviews": 0, "block_link_events": 0, "poisoned": False, "poison_codes": []}
    failing = {
        "RUN_INVALID": not run_ok,
        "HM_UNAVAILABLE": ui is None,
        "SOURCE_PDF_CHANGED": pdf_match is not True,
        "SOURCE_BLOCKS_CHANGED": stale and content is not True,
        "HISTORY_POISONED": history["poisoned"],
        "WRITES_DISABLED": not writes_enabled(),
    }
    reasons = [code for code in WRITE_REASON_ORDER if failing[code]]
    return {
        "run_id": run_id, "state": manifest.get("state"), "frozen": manifest.get("frozen") is True,
        "is_current": run_id == current_run_id, "created_at": manifest.get("created_at"),
        "completed_at": manifest.get("completed_at"), "engine_version": manifest.get("engine_version"),
        "provider": manifest.get("provider"), "model": manifest.get("model"),
        "model_display": model_display(manifest.get("model")), "reasoning": manifest.get("reasoning"),
        "projectchange_count": manifest.get("projectchange_count"),
        "region_count": len(ui["regions"]) if ui is not None else manifest.get("semantic_region_count"),
        "hm_available": ui is not None, "hm_reason": hm_reason, "pdf_match": pdf_match, "source_stale": stale,
        "blocks_content_match": content, "history": history, "writable": not reasons,
        "write_block_reason": reasons[0] if reasons else None, "write_block_reasons": reasons,
    }


def _snapshots(object_id: str | None, pair_id: str) -> list[dict[str, Any]]:
    if object_id is None:
        return []
    try:
        from backend.app.services.project_change_catalog import catalog
        entries = catalog.cached_catalog().get("entries") or []
    except Exception:  # the catalog is optional context for this view
        return []
    out = []
    for entry in entries:
        hm = entry.get("human_mapping") or {}
        result_id = (entry.get("provenance") or {}).get("result_id") or ""
        if entry.get("result_source") != "SEALED_SNAPSHOT" or entry.get("object_id") != object_id \
                or entry.get("pair_id") != pair_id or not hm.get("available") or not result_id.startswith("pcv3snap_"):
            continue
        reviews, _ = _read_jsonl(hm_storage.comparison_root() / object_id / pair_id / "reviews.jsonl")
        edits, _ = _read_jsonl(hm_storage.comparison_root() / object_id / pair_id / "human_block_link_edits.jsonl")
        out.append({
            "result_id": result_id, "catalog_entry_id": str(entry.get("catalog_entry_id") or ""),
            "label": str(entry.get("title") or ""), "hm_available": True, "hm_url": hm.get("url"),
            "legacy_history": {
                "reviews_total": len(reviews),
                "reviews_this_result": sum(r.get("result_id") == result_id for r in reviews),
                "block_link_events_total": len(edits),
                "block_link_events_this_result": sum(e.get("result_id") == result_id for e in edits),
            },
            "writable": False, "write_block_reason": "SNAPSHOT_READ_ONLY",
        })
    return out


def status(session_id: str, pair_id: str) -> dict[str, Any]:
    pair = _pair(session_id, pair_id)
    documents = _documents(pair)
    object_id = _object_id(session_id)
    try:
        current_run_id = run_storage.current(session_id, pair_id)
    except (ValueError, OSError, KeyError, TypeError, hm_storage.InvalidScopeId):
        current_run_id = None
    attempts, runs = [], []
    for run_id in run_storage.run_ids(session_id, pair_id):
        manifest = _manifest(session_id, pair_id, run_id)
        if not isinstance(manifest, dict):
            continue
        state_file = run_storage.read(_run_dir(session_id, pair_id, run_id) / "state.json") or {}
        attempts.append({"run_id": run_id, "state": str(manifest.get("state") or ""),
                         "reason_code": state_file.get("reason_code"), "created_at": manifest.get("created_at"),
                         "completed_at": manifest.get("completed_at")})
        if manifest.get("state") in run_storage.TERMINAL:
            runs.append(_run_entry(session_id, pair_id, run_id, manifest, object_id, current_run_id, documents))
    by_time = lambda item: _parse_time(item.get("created_at"))
    runs.sort(key=by_time, reverse=True)
    latest = max(attempts, key=by_time) if attempts else None
    return {
        "schema": "stage-block-mapping-status/1", "session_id": session_id, "pair_id": pair_id,
        "object_id": object_id, "object_error": None if object_id else "OBJECT_UNRESOLVED",
        "generated_at": now(), "current_run_id": current_run_id, "latest_attempt": latest, "runs": runs,
        "snapshots": _snapshots(object_id, pair_id),
        "source_blocks": {side: _public_side(_live_side(documents[side])) for side in SIDES},
        "capabilities": {"block_mapping_writes": writes_enabled()},
    }


# ── E2: region index ─────────────────────────────────────────────────────────
def _cardinality(olds: int, news: int) -> str:
    if not olds or not news:
        return "EMPTY"
    if olds == 1 and news == 1:
        return "1:1"
    if olds == 1:
        return "1:N"
    return "N:1" if news == 1 else "N:N"


def _allowed_ordered(region: dict[str, Any], side: str) -> list[str]:
    allowed = validation.allowed_block_ids(region, side)
    members = region.get("old_blocks" if side == "OLD" else "new_blocks") or []
    ordered = [b["id"] for b in members] + [b["id"] for p in (region.get("pages") or {}).get(side) or []
                                              for b in p.get("blocks") or []]
    return [block_id for block_id in dict.fromkeys(ordered) if block_id in allowed]


def region_index(session_id: str, pair_id: str, run_id: str) -> tuple[dict[str, Any], str]:
    _pair(session_id, pair_id)
    _require_run(session_id, pair_id, run_id)
    object_id = _object_id(session_id)
    ui, hm_reason, digest = _ui_data(session_id, pair_id, run_id, object_id)
    if ui is None:
        raise BlockMappingError(409, "HM_UNAVAILABLE", hm_reason=hm_reason)
    regions = []
    for region in ui["regions"]:
        members = {side: [{"id": b["id"], "page": int(b["page"]), "type": b["type"]}
                          for b in region.get(key) or []]
                   for side, key in (("OLD", "old_blocks"), ("NEW", "new_blocks"))}
        regions.append({
            "id": region["id"], "title": str(region.get("domain") or ""), "domain": str(region.get("domain") or ""),
            "scope": str(region.get("scope") or ""), "confidence": region.get("confidence"),
            "membership_state": {side: (region.get("membership_state") or {}).get(side) or
                                 ("MAPPED" if members[side] else "EMPTY") for side in SIDES},
            "mapping_state": region.get("mapping_state") or "MAPPED",
            "member_cardinality": _cardinality(len(members["OLD"]), len(members["NEW"])),
            "pages": {side: [int(p["page"]) for p in (region.get("pages") or {}).get(side) or []] for side in SIDES},
            "members": members,
            "allowed": {side: _allowed_ordered(region, side) for side in SIDES},
            "invalid_ref_count": len(region.get("invalid_membership_refs") or []),
        })
    payload = {
        "schema": "stage-block-mapping-region-index/1", "session_id": session_id, "pair_id": pair_id,
        "object_id": object_id, "run_id": run_id, "ui_data_schema": UI_DATA_SCHEMA, "ui_data_sha256": digest,
        "semantic_map_sha256": ui.get("source_sha256"), "region_count": len(regions), "regions": regions,
    }
    return payload, digest


# ── E3/E5: page blocks, E4/E6: block detail ─────────────────────────────────
def _block_row(row: dict[str, Any]) -> dict[str, Any]:
    return {"block_id": row["block_id"], "modality": row["modality"], "source_block_type": row["source_block_type"],
            "bbox": row["bbox"]}


def _run_source_dir(session_id: str, pair_id: str, run_id: str, side: str) -> Path:
    return _run_dir(session_id, pair_id, run_id) / "project_change_v3" / "source" / side.lower()


def _run_rotation_allowed(session_id: str, pair_id: str, run_id: str, manifest: dict[str, Any],
                          documents: dict[str, dict[str, Path]]) -> bool:
    # C7: live blocks.json rotation is trusted only when the live source still is the run's source.
    if _pdf_match(manifest, documents) is not True:
        return False
    return not _source_stale(session_id, pair_id, run_id) or _blocks_content_match(session_id, pair_id, run_id, documents)


def page_blocks_run(session_id: str, pair_id: str, run_id: str, side: str, pages: list[int]) -> tuple[dict[str, Any], str]:
    pair = _pair(session_id, pair_id)
    side = _side(side)
    manifest = _require_run(session_id, pair_id, run_id)
    documents = _documents(pair)
    source_dir = _run_source_dir(session_id, pair_id, run_id, side)
    page_count = len(list(source_dir.glob("p[0-9][0-9][0-9]")))
    live = _live_side(documents[side]) if _run_rotation_allowed(session_id, pair_id, run_id, manifest, documents) else None
    out, tags = [], []
    for page in pages:
        page_dir = source_dir / f"p{page:03d}"
        row = {"physical_page": page, "available": False, "reason": None, "geometry": None, "blocks": []}
        if not (page_dir / "page.json").is_file():
            row["reason"] = "PAGE_OUT_OF_RANGE" if page > page_count else "PAGE_JSON_MISSING"
        else:
            try:
                record = _read_json(page_dir / "page.json")
                blocks = [_block_row(b) for b in record.get("blocks") or []]
            except (OSError, ValueError, KeyError, TypeError, AttributeError):
                row["reason"] = "PAGE_JSON_UNREADABLE"
            else:
                size = _png_size(page_dir / "full_page.png")
                rotation = None
                if live is not None and live["available"]:
                    meta = _geometry_from_meta(_page_meta(live, page))
                    rotation = meta["rotation"] if meta else None
                row.update(available=True, blocks=blocks, geometry={
                    "width_px": size[0], "height_px": size[1], "rotation": rotation,
                    "geometry_source": "RUN_FULL_PAGE_PNG"} if size else None)
                tags.append(f"{page}:{_sha(page_dir / 'page.json')}:{rotation}")
        out.append(row)
    payload = {
        "schema": "stage-block-mapping-page-blocks/1", "layer": "RUN", "session_id": session_id, "pair_id": pair_id,
        "run_id": run_id, "side": side,
        "source_identity": {"pdf_sha256": manifest.get(side.lower() + "_pdf_sha256"), "blocks_sha256": None,
                            "markdown_sha256": None},
        "pages": out,
    }
    return payload, hashlib.sha256("|".join([run_id, side, *tags]).encode()).hexdigest()


def _require_live(documents: dict[str, dict[str, Path]], side: str) -> dict[str, Any]:
    live = _live_side(documents[side])
    if not live["available"]:
        # Fail closed: never substitute extracted_text for missing Markdown/blocks.
        raise BlockMappingError(409, "SOURCE_BLOCKS_UNAVAILABLE", reason=live["reason"], side=side)
    return live


def page_blocks_source(session_id: str, pair_id: str, side: str, pages: list[int]) -> tuple[dict[str, Any], str]:
    pair = _pair(session_id, pair_id)
    side = _side(side)
    live = _require_live(_documents(pair), side)
    out = []
    for page in pages:
        in_range = live["page_count"] is None or page <= live["page_count"]
        out.append({"physical_page": page, "available": in_range, "reason": None if in_range else "PAGE_OUT_OF_RANGE",
                    "geometry": _geometry_from_meta(_page_meta(live, page)) if in_range else None,
                    "blocks": [_block_row(r) for r in live["rows"] if r["physical_page"] == page] if in_range else []})
    payload = {
        "schema": "stage-block-mapping-page-blocks/1", "layer": "SOURCE", "session_id": session_id,
        "pair_id": pair_id, "run_id": None, "side": side,
        "source_identity": {k: live[k] for k in ("pdf_sha256", "blocks_sha256", "markdown_sha256")},
        "pages": out,
    }
    return payload, f"{live['blocks_sha256']}:{live['markdown_sha256']}:{','.join(map(str, pages))}"


def _run_block_pages(session_id: str, pair_id: str, run_id: str, side: str) -> dict[str, int]:
    source_dir = _run_source_dir(session_id, pair_id, run_id, side)

    def compute():
        index: dict[str, int] = {}
        for page_dir in sorted(source_dir.glob("p[0-9][0-9][0-9]")):
            try:
                record = _read_json(page_dir / "page.json")
            except (OSError, ValueError):
                continue
            for block in record.get("blocks") or []:
                index.setdefault(block.get("block_id"), int(page_dir.name[1:]))
        return index
    return _memo(("blockpages", session_id, pair_id, run_id, side) +
                 _stat_key(_run_dir(session_id, pair_id, run_id) / "run_manifest.json"), compute)


def block_run(session_id: str, pair_id: str, run_id: str, side: str, block_id: str, page: int | None = None) -> dict[str, Any]:
    _pair(session_id, pair_id)
    side = _side(side)
    block_id = _safe(block_id, "block")
    manifest = _require_run(session_id, pair_id, run_id)
    if page is None:
        page = _run_block_pages(session_id, pair_id, run_id, side).get(block_id)
    record = None
    if page is not None:
        try:
            record = _read_json(_run_source_dir(session_id, pair_id, run_id, side) / f"p{int(page):03d}" / "page.json")
        except (OSError, ValueError):
            record = None
    block = next((b for b in (record or {}).get("blocks") or [] if b.get("block_id") == block_id), None)
    if block is None:
        raise BlockMappingError(404, "BLOCK_NOT_FOUND")
    crop = f"assets/{side.lower()}/p{int(page):03d}/{block_id}.png"
    has_crop = block.get("modality") == "GRAPHIC" and (_run_dir(session_id, pair_id, run_id) / "human_mapping" / crop).is_file()
    return {
        "schema": "stage-block-mapping-block/1", "layer": "RUN", "session_id": session_id, "pair_id": pair_id,
        "run_id": run_id, "side": side, "block_id": block_id, "physical_page": int(page),
        "modality": block.get("modality"), "source_block_type": block.get("source_block_type"),
        "bbox": block.get("bbox"), "structured_md": block.get("structured_md") or "",
        "tables": list(block.get("tables") or []), "hm_crop_asset": crop if has_crop else None,
        "source_identity": {"pdf_sha256": manifest.get(side.lower() + "_pdf_sha256"), "blocks_sha256": None,
                            "markdown_sha256": None},
    }


def block_source(session_id: str, pair_id: str, side: str, block_id: str) -> dict[str, Any]:
    pair = _pair(session_id, pair_id)
    side = _side(side)
    block_id = _safe(block_id, "block")
    live = _require_live(_documents(pair), side)
    row = next((r for r in live["rows"] if r["block_id"] == block_id), None)
    if row is None:
        raise BlockMappingError(404, "BLOCK_NOT_FOUND")
    return {
        "schema": "stage-block-mapping-block/1", "layer": "SOURCE", "session_id": session_id, "pair_id": pair_id,
        "run_id": None, "side": side, "block_id": block_id, "physical_page": row["physical_page"],
        "modality": row["modality"], "source_block_type": row["source_block_type"], "bbox": row["bbox"],
        "structured_md": row["structured_md"], "tables": list(row["tables"]), "hm_crop_asset": None,
        "source_identity": {k: live[k] for k in ("pdf_sha256", "blocks_sha256", "markdown_sha256")},
    }


# ── E7: bridge check (build_snapshot without writing) ────────────────────────
_flight_locks: dict[Any, threading.Lock] = {}
_flight_guard = threading.Lock()


def _bridge_inputs(session_id: str, pair_id: str, run_id: str, documents: dict[str, dict[str, Path]]) -> tuple:
    from backend.app.services.common import object_service

    directory = _run_dir(session_id, pair_id, run_id)
    files = [directory / "human_mapping" / "reviews.jsonl", directory / "human_mapping" / "human_block_link_edits.jsonl",
             directory / "run_manifest.json", directory / "human_mapping" / "ui_data.json",
             directory / "project_change_v3" / "DOCUMENT_STRUCTURE.json", directory / "project_change_v3_result.json",
             documents["OLD"]["pdf"], documents["NEW"]["pdf"], paths.pair_json_path(session_id, pair_id),
             Path(object_service.OBJECTS_FILE)]
    return tuple(_stat_key(f) for f in files)


def bridge_check(session_id: str, pair_id: str, run_id: str) -> dict[str, Any]:
    from backend.app.services.project_change_v3 import human_mapping_bridge as bridge

    pair = _pair(session_id, pair_id)
    _require_run(session_id, pair_id, run_id)
    object_id = _object_id(session_id)
    if object_id is None:
        raise BlockMappingError(409, "HM_UNAVAILABLE", hm_reason="OBJECT_UNRESOLVED")
    key = ("bridge", session_id, pair_id, run_id, object_id) + _bridge_inputs(session_id, pair_id, run_id, _documents(pair))
    with _flight_guard:
        lock = _flight_locks.setdefault(key[:5], threading.Lock())
    with lock:  # single flight: one build_snapshot per key at a time
        hit = _cache.get(key)
        if hit and time.monotonic() - hit[0] < _BRIDGE_TTL:
            return {**hit[1], "cached": True}

        directory = _run_dir(session_id, pair_id, run_id) / "human_mapping"
        reviews, _ = _read_jsonl(directory / "reviews.jsonl")
        edits, _ = _read_jsonl(directory / "human_block_link_edits.jsonl")
        payload = {"schema": "stage-block-mapping-bridge-check/1", "session_id": session_id, "pair_id": pair_id,
                   "run_id": run_id, "object_id": object_id, "checked_at": now(), "cached": False, "dry_run": True,
                   "inputs": {"reviews": len(reviews), "block_link_events": len(edits)},
                   "ok": True, "error": None, "result": None, "notice": BRIDGE_NOTICE}
        try:
            # build_snapshot only reads; Snapshot.write is never called here.
            value = bridge.build_snapshot(object_id=object_id, comparison_id=session_id, pair_id=pair_id,
                                          source_run_id=run_id).value()
        except bridge.BridgeError as exc:
            payload.update(ok=False, error={"code": exc.code, "reason": exc.reason,
                                            "details": json.loads(json.dumps(exc.details, default=str))})
        else:
            edge = lambda r: {"link_id": str(r["link_id"]), "region_id": r["region_id"], "old_block_id": r["old_block_id"],
                              "new_block_id": r["new_block_id"], "source": r["source"]}
            payload["inputs"] = {"reviews": len(value["review_snapshot"]["reviews"]),
                                 "block_link_events": len(value["review_snapshot"]["block_link_edits"])}
            payload["result"] = {
                "bridge_version": value["bridge_version"], "review_snapshot_sha256": value["review_snapshot_sha256"],
                "confirmed_anchor_count": len(value["confirmed_anchors"]),
                "rejected_link_count": len(value["rejected_links"]),
                "unconstrained_region_count": len(value["unconstrained"]["region_ids"]),
                "anchors": [edge(r) for r in value["confirmed_anchors"]],
                "rejected": [edge(r) for r in value["rejected_links"]],
            }
        with _cache_lock:
            _cache[key] = (time.monotonic(), payload)
        return payload
