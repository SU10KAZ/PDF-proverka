"""Write-once snapshot of the human prelinks a V3 run was launched with.

Written by the orchestrator INSIDE ``production_pair_lock``, after the run id
is reserved and BEFORE the V3 engine is called — and never passed to it.  The
engine does not know this file (guard: test_mapper_isolation_guard.py), so a
wrong human link cannot change the Mapper input or output; the snapshot only
freezes what the person had at launch for the post-analysis reconciliation.

``sessions/<sid>/pairs/<pid>/prelink_runs/<run_id>.json``
(``human-prelink-run-snapshot/1``): exclusive create, fsync, read-back.
Later edits of the drafts never change it; a new analysis is a new run id and a
new snapshot.  If it cannot be written the launch is refused before the engine
(``PrelinkSnapshotError`` → HTTP 409, no run, 0 model calls).
"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
from typing import Any

from . import paths, prelink_drafts
from .production_store import ProductionConflictError

CONTRACT = "human-prelink-run-snapshot/1"

_SHA_OR_NULL = {"oneOf": [{"type": "string", "pattern": "^[0-9a-f]{64}$"}, {"type": "null"}]}
_SIDE = {"type": "object", "additionalProperties": False,
         "required": ["pdf_sha256", "blocks_sha256", "markdown_sha256", "version_id"],
         "properties": {"pdf_sha256": _SHA_OR_NULL, "blocks_sha256": _SHA_OR_NULL, "markdown_sha256": _SHA_OR_NULL,
                        "version_id": {"type": ["string", "null"]}}}
_ITEM = {**prelink_drafts.PRELINK_SCHEMA,
         "required": prelink_drafts.PRELINK_SCHEMA["required"] + ["validity_at_launch"],
         "properties": {**prelink_drafts.PRELINK_SCHEMA["properties"],
                        "validity_at_launch": {"enum": ["VALID", "REVALIDATED"]}}}
JSON_SCHEMA: dict[str, Any] = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$id": CONTRACT,
    "type": "object", "additionalProperties": False,
    "required": ["schema", "run_id", "session_id", "pair_id", "object_id", "created_at", "mapper_prelinks_used",
                 "mapper_sheet_map_used", "engine_contract", "source_identity", "source_identity_id", "sheet_map",
                 "drafts"],
    "properties": {
        "schema": {"const": CONTRACT},
        "run_id": {"type": "string", "pattern": "^[A-Za-z0-9_-]{1,128}$"},
        "session_id": {"type": "string"}, "pair_id": {"type": "string"},
        "object_id": {"type": ["string", "null"]},
        "created_at": {"type": "string"},
        "mapper_prelinks_used": {"const": False},
        "mapper_sheet_map_used": {"const": False},
        "engine_contract": {"type": "object", "additionalProperties": False,
                            "required": ["engine_version", "mapper_prompt_sha256", "map_schema_sha256"],
                            "properties": {"engine_version": {"type": "string"},
                                           "mapper_prompt_sha256": {"type": "string", "pattern": "^[0-9a-f]{64}$"},
                                           "map_schema_sha256": {"type": "string", "pattern": "^[0-9a-f]{64}$"}}},
        "source_identity": {"type": "object", "additionalProperties": False, "required": ["OLD", "NEW"],
                            "properties": {"OLD": _SIDE, "NEW": _SIDE}},
        "source_identity_id": {"type": ["string", "null"]},
        "sheet_map": {"type": "object", "additionalProperties": False,
                      "required": ["state", "sheet_links_sha256", "groups"],
                      "properties": {
                          "state": {"enum": ["INCLUDED", "EMPTY", "FILE_MISSING", "FILE_INVALID"]},
                          "sheet_links_sha256": _SHA_OR_NULL,
                          "groups": {"type": "array", "items": {
                              "type": "object", "additionalProperties": False,
                              "required": ["group_id", "link_id", "old_pages", "new_pages", "basis"],
                              "properties": {"group_id": {"type": "string"}, "link_id": {"type": "string"},
                                             "old_pages": {"type": "array", "items": {"type": "integer"}},
                                             "new_pages": {"type": "array", "items": {"type": "integer"}},
                                             "basis": {"type": "string"}}}}}},
        "drafts": {"type": "object", "additionalProperties": False,
                   "required": ["state", "revision", "file_sha256", "items", "excluded"],
                   "properties": {
                       "state": {"enum": ["INCLUDED", "NO_DRAFTS", "FILE_INVALID"]},
                       "revision": {"type": ["integer", "null"]},
                       "file_sha256": _SHA_OR_NULL,
                       "items": {"type": "array", "items": _ITEM},
                       "excluded": {"type": "array", "items": {
                           "type": "object", "additionalProperties": False,
                           "required": ["prelink_id", "label_no", "reason", "validity_at_launch", "details"],
                           "properties": {"prelink_id": {"type": "string"}, "label_no": {"type": "integer"},
                                          "reason": {"enum": ["EXCLUDED_STALE"]},
                                          "validity_at_launch": {"enum": ["STALE_PDF", "STALE_BLOCKS", "STALE_TEXT",
                                                                          "SOURCE_UNAVAILABLE"]},
                                          "details": {"type": "array"}}}}}},
    },
}


class PrelinkSnapshotError(ProductionConflictError):
    """The snapshot could not be frozen: the launch is refused before the engine (0 model calls)."""

    code = "PRELINK_SNAPSHOT_FAILED"


def snapshot_path(session_id: str, pair_id: str, run_id: str) -> Path:
    return paths.prelink_run_snapshot_path(session_id, pair_id, run_id)


def _basis(link: dict[str, Any]) -> str:
    source = str(link.get("source") or "")
    reasons = {str(r) for r in link.get("reason") or []}
    if source == "manual" and "user_corrected" in reasons:
        return "USER_CORRECTED"
    if source == "manual" and "user_reordered" in reasons:
        return "USER_REORDERED"
    if source == "auto" and "user_accepted" in reasons:
        return "USER_ACCEPTED_SUGGESTION"
    if source == "auto_repair":
        return "AUTOMATIC_REPAIR"
    return "UNKNOWN"


def _sheet_map(session_id: str, pair_id: str) -> dict[str, Any]:
    """Saved sheet links as display groups — ONE read of the bytes; never given to any model."""
    try:
        raw = paths.sheet_links_path(session_id, pair_id).read_bytes()
    except FileNotFoundError:
        return {"state": "FILE_MISSING", "sheet_links_sha256": None, "groups": []}
    digest = hashlib.sha256(raw).hexdigest()
    try:
        value = json.loads(raw.decode("utf-8"))
        links = [link for link in value.get("links") or [] if isinstance(link, dict)]
        rows = []
        for link in links:
            left = sorted({int(p) for p in link.get("left_pages") or []})
            right = sorted({int(p) for p in link.get("right_pages") or []})
            if left and right:
                rows.append({"link_id": str(link.get("id") or ""), "old_pages": left, "new_pages": right,
                             "basis": _basis(link)})
    except (UnicodeDecodeError, ValueError, TypeError, AttributeError):
        return {"state": "FILE_INVALID", "sheet_links_sha256": digest, "groups": []}
    rows.sort(key=lambda r: (r["old_pages"][0], r["new_pages"][0], r["old_pages"], r["new_pages"], r["link_id"]))
    for number, row in enumerate(rows, start=1):
        row["group_id"] = f"SG{number:02d}"
    return {"state": "INCLUDED" if rows else "EMPTY", "sheet_links_sha256": digest, "groups": rows}


def _engine_contract() -> dict[str, str]:
    """What the unchanged V3 engine will call the Mapper with (read, never altered)."""
    from backend.app.services.project_change_v3 import contracts

    schema = json.dumps(contracts.MAP_SCHEMA, ensure_ascii=False, sort_keys=True)
    return {"engine_version": contracts.ENGINE_VERSION, "mapper_prompt_sha256": contracts.MAPPER_PROMPT_SHA256,
            "map_schema_sha256": hashlib.sha256(schema.encode("utf-8")).hexdigest()}


def _object_id(session_id: str) -> str | None:
    from backend.app.services.project_change_v3.scope import object_id_for_session

    try:
        return object_id_for_session(session_id)
    except Exception:  # noqa: BLE001 — an unresolved scope is recorded as unknown
        return None


def _live(session_id: str, pair_id: str) -> dict[str, Any]:
    try:
        return prelink_drafts.live_context(session_id, pair_id, fresh=True)
    except prelink_drafts.PrelinkError:
        # The engine reports an unresolvable pair itself (source_preparation_failed).
        empty = {"pdf_sha256": None, "blocks_sha256": None, "markdown_sha256": None, "version_id": None}
        unavailable = {"available": False, "reason": "PAIR_UNRESOLVED", "rows": []}
        return {"live": {"OLD": unavailable, "NEW": unavailable}, "identity": {"OLD": empty, "NEW": dict(empty)},
                "identity_id": None, "available": False}


def build(session_id: str, pair_id: str, run_id: str, *, created_at: str | None = None) -> dict[str, Any]:
    context = _live(session_id, pair_id)
    try:
        raw, drafts = prelink_drafts.read(session_id, pair_id)
        drafts_state = "INCLUDED" if (drafts or {}).get("prelinks") else "NO_DRAFTS"
    except prelink_drafts.PrelinkDraftsInvalid:
        raw, drafts = prelink_drafts.drafts_path(session_id, pair_id).read_bytes(), None
        drafts_state = "FILE_INVALID"
    items, excluded = [], []
    for row in prelink_drafts.evaluate(drafts, context):
        stored = {k: row[k] for k in prelink_drafts.PRELINK_SCHEMA["properties"]}
        if row["validity"] in prelink_drafts.PASSABLE:
            if row["validity"] == "REVALIDATED":
                # Checked against the live source: it carries that identity (no re-binding, same blocks).
                stored["source_identity_id"] = context["identity_id"]
            items.append({**stored, "validity_at_launch": row["validity"]})
        else:
            excluded.append({"prelink_id": row["prelink_id"], "label_no": row["label_no"], "reason": "EXCLUDED_STALE",
                             "validity_at_launch": row["validity"], "details": row["validity_details"]})
    return {
        "schema": CONTRACT, "run_id": run_id, "session_id": session_id, "pair_id": pair_id,
        "object_id": _object_id(session_id), "created_at": created_at or prelink_drafts.now(),
        "mapper_prelinks_used": False, "mapper_sheet_map_used": False,
        "engine_contract": _engine_contract(),
        "source_identity": context["identity"], "source_identity_id": context["identity_id"],
        "sheet_map": _sheet_map(session_id, pair_id),
        "drafts": {"state": drafts_state, "revision": (drafts or {}).get("revision"),
                   "file_sha256": hashlib.sha256(raw).hexdigest() if raw is not None else None,
                   "items": items, "excluded": excluded},
    }


def validate(snapshot: dict[str, Any]) -> dict[str, Any]:
    import jsonschema

    jsonschema.validate(snapshot, JSON_SCHEMA)
    return snapshot


def write_once(path: Path, snapshot: dict[str, Any]) -> str:
    """Exclusive create of the canonical bytes + fsync + read-back; returns the file sha256."""
    data = prelink_drafts.canonical(snapshot)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "xb") as handle:
        handle.write(data)
        handle.flush()
        os.fsync(handle.fileno())
    if path.read_bytes() != data:
        raise OSError(f"prelink snapshot read-back differs from what was written: {path}")
    return hashlib.sha256(data).hexdigest()


def freeze_if_enabled(session_id: str, pair_id: str, run_id: str) -> str | None:
    """Call under the pair lock before the V3 engine; nothing at all when STAGE_PRELINK_DRAFTS is off."""
    if not prelink_drafts.drafts_enabled():
        return None
    try:
        return write_once(snapshot_path(session_id, pair_id, run_id), validate(build(session_id, pair_id, run_id)))
    except Exception as exc:  # noqa: BLE001 — any failure refuses the launch before the engine
        raise PrelinkSnapshotError(
            f"PRELINK_SNAPSHOT_FAILED: снимок предварительных связей не записан ({type(exc).__name__}); "
            "анализ не запущен") from exc


def read(session_id: str, pair_id: str, run_id: str) -> tuple[bytes, dict[str, Any]] | None:
    """(bytes, value) of a run's snapshot, or None when the run has none."""
    path = snapshot_path(session_id, pair_id, run_id)
    try:
        raw = path.read_bytes()
    except FileNotFoundError:
        return None
    return raw, validate(json.loads(raw.decode("utf-8")))
