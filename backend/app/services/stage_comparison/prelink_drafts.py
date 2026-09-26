"""Pre-analysis human block links of a pair (HumanPrelink drafts, ``human-prelink-drafts/1``).

A prelink says "I think these OLD blocks and these NEW blocks correspond —
check it after the AI analysis".  It is NEVER shown to any model: the V3
engine does not read this file (guard: test_mapper_isolation_guard.py).  It is
NOT a claim that anything changed, NOT one ProjectChange and NOT a Human
Mapping decision.  After a run the frozen copy of the drafts is reconciled
deterministically with the run's regions, and a person decides explicitly.

Storage: ``sessions/<sid>/pairs/<pid>/prelink_drafts.json`` — mutable current
state with a revision; every write is compare-and-set under the per-file lock
of ``common.atomic_json.load_modify_save``.  One prelink is ONE group
``{old_blocks, new_blocks}``: no edges, no Cartesian meaning.

Validity is computed on every read, never stored: a prelink points at the very
blocks it was made on (exact canonical projection) or it is stale.  There is no
re-binding by similarity.
"""
from __future__ import annotations

import copy
import hashlib
import json
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from backend.app.services.common.atomic_json import load_modify_save

from . import paths

CONTRACT = "human-prelink-drafts/1"
VIEW_CONTRACT = "human-prelink-drafts-view/1"
FLAG = "STAGE_PRELINK_DRAFTS"
MAX_PRELINKS = 60
MAX_ENDPOINTS = 400
MAX_BLOCKS_PER_SIDE = 12
MAX_NOTE = 500
SIDES = ("OLD", "NEW")
VALIDITY_ORDER = ("VALID", "REVALIDATED", "STALE_TEXT", "STALE_BLOCKS", "STALE_PDF", "SOURCE_UNAVAILABLE")
PASSABLE = {"VALID", "REVALIDATED"}

_SHA256 = {"type": "string", "pattern": "^[0-9a-f]{64}$"}
_SIDE_IDENTITY = {
    "type": "object", "additionalProperties": False,
    "required": ["pdf_sha256", "blocks_sha256", "markdown_sha256", "version_id"],
    "properties": {"pdf_sha256": _SHA256, "blocks_sha256": _SHA256, "markdown_sha256": _SHA256,
                   "version_id": {"type": ["string", "null"]}},
}
_ENDPOINT = {
    "type": "object", "additionalProperties": False,
    "required": ["block_id", "physical_page", "modality", "source_block_type", "bbox", "content_sha256"],
    "properties": {
        "block_id": {"type": "string", "pattern": "^[A-Za-z0-9_-]{1,128}$"},
        "physical_page": {"type": "integer", "minimum": 1},
        "modality": {"enum": ["TEXT", "TABLE", "GRAPHIC"]},
        "source_block_type": {"enum": ["text", "image"]},
        "bbox": {"type": "array", "items": {"type": "number"}, "minItems": 4, "maxItems": 4},
        "content_sha256": _SHA256,
    },
}
PRELINK_SCHEMA: dict[str, Any] = {
    "type": "object", "additionalProperties": False,
    "required": ["prelink_id", "label_no", "source_identity_id", "old_blocks", "new_blocks", "cardinality",
                 "created_at", "updated_at", "note"],
    "properties": {
        "prelink_id": {"type": "string", "pattern": "^pl_[0-9a-f]{32}$"},
        "label_no": {"type": "integer", "minimum": 1},
        "source_identity_id": {"type": "string", "pattern": "^sid_[0-9a-f]{16}$"},
        "old_blocks": {"type": "array", "minItems": 1, "maxItems": MAX_BLOCKS_PER_SIDE, "items": _ENDPOINT},
        "new_blocks": {"type": "array", "minItems": 1, "maxItems": MAX_BLOCKS_PER_SIDE, "items": _ENDPOINT},
        "cardinality": {"enum": ["1:1", "1:N", "N:1", "N:N"]},
        "created_at": {"type": "string", "minLength": 10},
        "updated_at": {"type": "string", "minLength": 10},
        "note": {"type": "string", "maxLength": MAX_NOTE},
    },
}
PAIR_IDENTITY_SCHEMA: dict[str, Any] = {
    "type": "object", "additionalProperties": False, "required": ["OLD", "NEW"],
    "properties": {"OLD": _SIDE_IDENTITY, "NEW": _SIDE_IDENTITY},
}
JSON_SCHEMA: dict[str, Any] = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$id": CONTRACT,
    "type": "object", "additionalProperties": False,
    "required": ["schema", "session_id", "pair_id", "revision", "next_label_no", "updated_at",
                 "source_identities", "prelinks"],
    "properties": {
        "schema": {"const": CONTRACT},
        "session_id": {"type": "string", "pattern": "^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$"},
        "pair_id": {"type": "string", "pattern": "^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$"},
        "revision": {"type": "integer", "minimum": 0},
        "next_label_no": {"type": "integer", "minimum": 1},
        "updated_at": {"type": ["string", "null"]},
        "source_identities": {"type": "object", "propertyNames": {"pattern": "^sid_[0-9a-f]{16}$"},
                              "additionalProperties": PAIR_IDENTITY_SCHEMA},
        "prelinks": {"type": "array", "maxItems": MAX_PRELINKS, "items": PRELINK_SCHEMA},
    },
}


class PrelinkDraftsInvalid(ValueError):
    """The drafts file exists but does not satisfy its contract: never silently ignored or rewritten."""


class PrelinkError(Exception):
    """HTTP-mappable refusal of the drafts API: {detail: {error: code, ok: false, ...extra}}."""

    def __init__(self, status: int, code: str, **extra: Any):
        super().__init__(code)
        self.status, self.code, self.extra = status, code, extra


def drafts_enabled() -> bool:
    """Same parsing as the sibling stage-2 flag STAGE_BLOCK_MAPPING_WRITES."""
    return os.environ.get(FLAG, "").strip().lower() in {"1", "true", "yes", "on"}


def now() -> str:
    return datetime.now(timezone.utc).isoformat()


def drafts_path(session_id: str, pair_id: str) -> Path:
    return paths.prelink_drafts_path(session_id, pair_id)


def canonical(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


def source_identity_id(identity: dict[str, Any]) -> str:
    return "sid_" + hashlib.sha256(canonical({"OLD": identity["OLD"], "NEW": identity["NEW"]})).hexdigest()[:16]


def projection(row: dict[str, Any]) -> list[Any]:
    """Canonical block projection (DATA_CONTRACT_PLAN §5.2 = stage_block_mapping.service._proj)."""
    return [row["physical_page"], row.get("block_id"), row.get("modality"), row.get("source_block_type"),
            row.get("bbox"), row.get("structured_md"), row.get("tables"), row.get("existing_description")]


def content_sha256(row: dict[str, Any]) -> str:
    return hashlib.sha256(canonical(projection(row))).hexdigest()


def endpoint_of(row: dict[str, Any]) -> dict[str, Any]:
    """The stored end of a prelink, built from a live recognition row (never from client geometry)."""
    return {"block_id": row["block_id"], "physical_page": int(row["physical_page"]), "modality": row["modality"],
            "source_block_type": row["source_block_type"], "bbox": list(row["bbox"]),
            "content_sha256": content_sha256(row)}


def cardinality(old_count: int, new_count: int) -> str:
    if old_count == 1 and new_count == 1:
        return "1:1"
    if old_count == 1:
        return "1:N"
    if new_count == 1:
        return "N:1"
    return "N:N"


def empty_state(session_id: str, pair_id: str) -> dict[str, Any]:
    return {"schema": CONTRACT, "session_id": session_id, "pair_id": pair_id, "revision": 0, "next_label_no": 1,
            "updated_at": None, "source_identities": {}, "prelinks": []}


def check_value(value: Any) -> dict[str, Any]:
    """Contract check of an already decoded state (schema + cross-field rules)."""
    import jsonschema

    try:
        jsonschema.validate(value, JSON_SCHEMA)
    except jsonschema.ValidationError as exc:
        raise PrelinkDraftsInvalid(f"schema: {exc.message}") from exc
    endpoints = 0
    for item in value["prelinks"]:
        if item["source_identity_id"] not in value["source_identities"]:
            raise PrelinkDraftsInvalid(f"unknown source identity of {item['prelink_id']}")
        if item["cardinality"] != cardinality(len(item["old_blocks"]), len(item["new_blocks"])):
            raise PrelinkDraftsInvalid(f"cardinality mismatch of {item['prelink_id']}")
        for key in ("old_blocks", "new_blocks"):
            ids = [b["block_id"] for b in item[key]]
            if len(ids) != len(set(ids)):
                raise PrelinkDraftsInvalid(f"duplicate block in {item['prelink_id']}")
        endpoints += len(item["old_blocks"]) + len(item["new_blocks"])
    if endpoints > MAX_ENDPOINTS:
        raise PrelinkDraftsInvalid(f"{endpoints} prelink ends exceed the limit {MAX_ENDPOINTS}")
    labels = [item["label_no"] for item in value["prelinks"]]
    if len(labels) != len(set(labels)) or any(n >= value["next_label_no"] for n in labels):
        raise PrelinkDraftsInvalid("label numbers are not unique or not below next_label_no")
    ids = [item["prelink_id"] for item in value["prelinks"]]
    if len(ids) != len(set(ids)):
        raise PrelinkDraftsInvalid("duplicate prelink ids")
    return value


def parse(raw: bytes) -> dict[str, Any]:
    try:
        value = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, ValueError) as exc:
        raise PrelinkDraftsInvalid(f"{type(exc).__name__}: {exc}") from exc
    return check_value(value)


def read(session_id: str, pair_id: str) -> tuple[bytes | None, dict[str, Any] | None]:
    """ONE read of the bytes; parsed value from exactly those bytes (None, None when there is no file)."""
    path = drafts_path(session_id, pair_id)
    try:
        raw = path.read_bytes()
    except FileNotFoundError:
        return None, None
    return raw, parse(raw)


def label(item: dict[str, Any]) -> str:
    return f"PL-{item['label_no']}"


def _worst(values: list[str]) -> str:
    return max(values, key=VALIDITY_ORDER.index) if values else "VALID"


def validity(
    item: dict[str, Any],
    stored_identity: dict[str, Any],
    live: dict[str, dict[str, Any]],
    live_identity: dict[str, Any],
) -> tuple[str, list[dict[str, Any]]]:
    """Validity of one prelink against the live recognition (STALE_PRELINK_POLICY §2)."""
    details: list[dict[str, Any]] = []
    per_side: list[str] = []
    for side, key in (("OLD", "old_blocks"), ("NEW", "new_blocks")):
        side_live = live.get(side) or {}
        if not side_live.get("available"):
            details.append({"side": side, "reason": side_live.get("reason") or "SOURCE_UNAVAILABLE"})
            per_side.append("SOURCE_UNAVAILABLE")
            continue
        stored, current = stored_identity[side], live_identity[side]
        if stored["pdf_sha256"] != current["pdf_sha256"] or stored["version_id"] != current["version_id"]:
            details.append({"side": side, "reason": "PDF_CHANGED"})
            per_side.append("STALE_PDF")
            continue
        if stored["blocks_sha256"] == current["blocks_sha256"] and \
                stored["markdown_sha256"] == current["markdown_sha256"]:
            per_side.append("VALID")
            continue
        rows = {row["block_id"]: row for row in side_live["rows"]}
        verdicts = []
        for endpoint in item[key]:
            row = rows.get(endpoint["block_id"])
            if row is None:
                details.append({"side": side, "block_id": endpoint["block_id"], "reason": "BLOCK_MISSING"})
                verdicts.append("STALE_BLOCKS")
            elif (int(row["physical_page"]) != endpoint["physical_page"] or row["modality"] != endpoint["modality"]
                  or row["source_block_type"] != endpoint["source_block_type"]
                  or list(row["bbox"]) != endpoint["bbox"]):
                details.append({"side": side, "block_id": endpoint["block_id"],
                                "reason": "GEOMETRY_OR_TYPE_CHANGED"})
                verdicts.append("STALE_BLOCKS")
            elif content_sha256(row) != endpoint["content_sha256"]:
                details.append({"side": side, "block_id": endpoint["block_id"], "reason": "TEXT_CHANGED"})
                verdicts.append("STALE_TEXT")
            else:
                verdicts.append("REVALIDATED")
        per_side.append(_worst(verdicts))
    return _worst(per_side), details


# ── live recognition of the pair (the same rows stage 2 shows) ──────────────
def live_context(session_id: str, pair_id: str) -> dict[str, Any]:
    """Live rows, identity and availability of both sides; raises PrelinkError 404 on scope problems."""
    from backend.app.services.stage_block_mapping import service

    try:
        pair = service._pair(session_id, pair_id)
    except service.BlockMappingError as exc:
        raise PrelinkError(exc.status, exc.code, **exc.extra) from exc
    documents = service._documents(pair)
    live = {side: service._live_side(documents[side]) for side in SIDES}
    identity = {}
    for side, key in (("OLD", "left"), ("NEW", "right")):
        identity[side] = {"pdf_sha256": live[side]["pdf_sha256"], "blocks_sha256": live[side]["blocks_sha256"],
                          "markdown_sha256": live[side]["markdown_sha256"],
                          "version_id": (pair.get(key) or {}).get("version_id")}
    complete = all(identity[s][k] for s in SIDES for k in ("pdf_sha256", "blocks_sha256", "markdown_sha256"))
    return {"pair": pair, "documents": documents, "live": live, "identity": identity,
            "identity_id": source_identity_id(identity) if complete else None,
            "available": all(live[s]["available"] for s in SIDES)}


def evaluate(drafts: dict[str, Any] | None, context: dict[str, Any]) -> list[dict[str, Any]]:
    """Every stored prelink with its computed validity (never written back)."""
    rows = []
    for item in (drafts or {}).get("prelinks") or []:
        stored = drafts["source_identities"][item["source_identity_id"]]
        state, details = validity(item, stored, context["live"], context["identity"])
        rows.append({**item, "label": label(item), "validity": state, "validity_details": details,
                     "pages": {"OLD": sorted({b["physical_page"] for b in item["old_blocks"]}),
                               "NEW": sorted({b["physical_page"] for b in item["new_blocks"]})}})
    return rows


def launch_summary(rows: list[dict[str, Any]]) -> dict[str, int]:
    return {"will_be_reconciled": sum(r["validity"] in PASSABLE for r in rows),
            "stale_not_reconciled": sum(r["validity"] not in PASSABLE for r in rows)}


def _running_run(session_id: str, pair_id: str) -> dict[str, Any] | None:
    from .production_orchestrator import active_run_control

    control = active_run_control(session_id, pair_id)
    return {"run_id": control.run_id} if control is not None else None


def _read_or_conflict(session_id: str, pair_id: str) -> tuple[bytes | None, dict[str, Any] | None]:
    try:
        return read(session_id, pair_id)
    except PrelinkDraftsInvalid as exc:
        raw = drafts_path(session_id, pair_id).read_bytes()
        raise PrelinkError(409, "PRELINK_DRAFTS_INVALID", file_sha256=hashlib.sha256(raw).hexdigest(),
                           message=str(exc)) from exc


def view(session_id: str, pair_id: str) -> dict[str, Any]:
    """GET …/prelinks — readable at any flag; writes nothing."""
    context = live_context(session_id, pair_id)
    _raw, drafts = _read_or_conflict(session_id, pair_id)
    rows = evaluate(drafts, context)
    enabled = drafts_enabled()
    blocked = None if enabled and context["available"] else (
        "PRELINK_DRAFTS_DISABLED" if not enabled else "SOURCE_BLOCKS_UNAVAILABLE")
    reason = next((context["live"][s]["reason"] for s in SIDES if not context["live"][s]["available"]), None)
    return {
        "schema": VIEW_CONTRACT, "session_id": session_id, "pair_id": pair_id,
        "revision": (drafts or {}).get("revision", 0),
        "live_source_identity": context["identity"], "live_source_identity_id": context["identity_id"],
        "source_available": {"OLD": context["live"]["OLD"]["available"],
                             "NEW": context["live"]["NEW"]["available"], "reason": reason},
        "capabilities": {"drafts_api": enabled, "drafts_writable": blocked is None, "blocked_reason": blocked},
        "prelinks": rows,
        "counts": {"total": len(rows), "valid": sum(r["validity"] == "VALID" for r in rows),
                   "revalidated": sum(r["validity"] == "REVALIDATED" for r in rows),
                   "stale": sum(r["validity"] not in PASSABLE for r in rows)},
        "running_run": _running_run(session_id, pair_id),
        "launch_summary": launch_summary(rows),
    }


# ── writes (compare-and-set under the per-file lock) ─────────────────────────
def _endpoints(context: dict[str, Any], side: str, block_ids: list[str]) -> list[dict[str, Any]]:
    other = "NEW" if side == "OLD" else "OLD"
    rows = {row["block_id"]: row for row in context["live"][side]["rows"]}
    other_ids = {row["block_id"] for row in context["live"][other]["rows"]}
    out = []
    for block_id in block_ids:
        row = rows.get(block_id)
        if row is None:
            code = "PRELINK_WRONG_SIDE" if block_id in other_ids else "PRELINK_BLOCK_NOT_FOUND"
            raise PrelinkError(400, code, side=side, block_id=block_id)
        if row.get("source_block_type") == "stamp":
            raise PrelinkError(400, "PRELINK_STAMP_NOT_ALLOWED", side=side, block_id=block_id)
        out.append(endpoint_of(row))
    return sorted(out, key=lambda e: (e["physical_page"], e["block_id"]))


def _composition(context: dict[str, Any], old_ids: list[str], new_ids: list[str], note: str) -> dict[str, Any]:
    old_ids, new_ids = [str(x) for x in old_ids or []], [str(x) for x in new_ids or []]
    if not old_ids or not new_ids:
        raise PrelinkError(400, "PRELINK_SIDE_EMPTY")
    if len(old_ids) > MAX_BLOCKS_PER_SIDE or len(new_ids) > MAX_BLOCKS_PER_SIDE:
        raise PrelinkError(400, "PRELINK_TOO_MANY_BLOCKS", limit=MAX_BLOCKS_PER_SIDE)
    if len(set(old_ids)) != len(old_ids) or len(set(new_ids)) != len(new_ids):
        raise PrelinkError(400, "PRELINK_DUPLICATE_BLOCK")
    if len(note or "") > MAX_NOTE:
        raise PrelinkError(400, "PRELINK_NOTE_TOO_LONG", limit=MAX_NOTE)
    old_blocks, new_blocks = _endpoints(context, "OLD", old_ids), _endpoints(context, "NEW", new_ids)
    return {"old_blocks": old_blocks, "new_blocks": new_blocks,
            "cardinality": cardinality(len(old_blocks), len(new_blocks)), "note": note or ""}


def _members(item: dict[str, Any]) -> tuple[frozenset[str], frozenset[str]]:
    return (frozenset(b["block_id"] for b in item["old_blocks"]), frozenset(b["block_id"] for b in item["new_blocks"]))


def _write(session_id: str, pair_id: str, expected_revision: int, change) -> dict[str, Any]:
    """Compare-and-set: ``change(state)`` edits a deep copy; nothing is written on any refusal."""
    path = drafts_path(session_id, pair_id)

    def mutate(current: Any) -> Any:
        state = check_value(current) if current is not None else empty_state(session_id, pair_id)
        if state["session_id"] != session_id or state["pair_id"] != pair_id:
            raise PrelinkDraftsInvalid("drafts file belongs to another pair")
        if int(expected_revision) != state["revision"]:
            raise PrelinkError(409, "PRELINK_REVISION_CONFLICT", current_revision=state["revision"])
        state = copy.deepcopy(state)
        change(state)
        used = {item["source_identity_id"] for item in state["prelinks"]}
        state["source_identities"] = {k: v for k, v in state["source_identities"].items() if k in used}
        endpoints = sum(len(i["old_blocks"]) + len(i["new_blocks"]) for i in state["prelinks"])
        if len(state["prelinks"]) > MAX_PRELINKS or endpoints > MAX_ENDPOINTS:
            raise PrelinkError(400, "PRELINK_LIMIT_REACHED", max_prelinks=MAX_PRELINKS,
                               max_endpoints=MAX_ENDPOINTS)
        state["revision"] += 1
        state["updated_at"] = now()
        return check_value(state)

    try:
        load_modify_save(path, mutate, default=None)
    except (PrelinkDraftsInvalid, json.JSONDecodeError, UnicodeDecodeError) as exc:
        raw = path.read_bytes() if path.is_file() else b""
        raise PrelinkError(409, "PRELINK_DRAFTS_INVALID", file_sha256=hashlib.sha256(raw).hexdigest(),
                           message=str(exc)) from exc
    return view(session_id, pair_id)


def _require_writable(context: dict[str, Any]) -> None:
    if not drafts_enabled():
        raise PrelinkError(403, "PRELINK_DRAFTS_DISABLED")
    if not context["available"]:
        side = next(s for s in SIDES if not context["live"][s]["available"])
        raise PrelinkError(409, "SOURCE_BLOCKS_UNAVAILABLE", side=side, reason=context["live"][side]["reason"])


def create(session_id: str, pair_id: str, *, expected_revision: int, old_block_ids: list[str],
           new_block_ids: list[str], note: str = "") -> dict[str, Any]:
    context = live_context(session_id, pair_id)
    _require_writable(context)
    body = _composition(context, old_block_ids, new_block_ids, note)

    def change(state: dict[str, Any]) -> None:
        if _members(body) in {_members(item) for item in state["prelinks"]}:
            raise PrelinkError(409, "PRELINK_DUPLICATE")
        stamp = now()
        state["source_identities"][context["identity_id"]] = copy.deepcopy(context["identity"])
        state["prelinks"].append({"prelink_id": "pl_" + uuid.uuid4().hex, "label_no": state["next_label_no"],
                                  "source_identity_id": context["identity_id"], **body,
                                  "created_at": stamp, "updated_at": stamp})
        state["next_label_no"] += 1

    return _write(session_id, pair_id, expected_revision, change)


def replace(session_id: str, pair_id: str, prelink_id: str, *, expected_revision: int, old_block_ids: list[str],
            new_block_ids: list[str], note: str = "") -> dict[str, Any]:
    """PUT: the whole composition is replaced (reassign / add / remove a block / replace the group)."""
    context = live_context(session_id, pair_id)
    _require_writable(context)
    body = _composition(context, old_block_ids, new_block_ids, note)

    def change(state: dict[str, Any]) -> None:
        item = next((i for i in state["prelinks"] if i["prelink_id"] == prelink_id), None)
        if item is None:
            raise PrelinkError(404, "PRELINK_NOT_FOUND", current_revision=state["revision"])
        if _members(body) in {_members(i) for i in state["prelinks"] if i["prelink_id"] != prelink_id}:
            raise PrelinkError(409, "PRELINK_DUPLICATE")
        state["source_identities"][context["identity_id"]] = copy.deepcopy(context["identity"])
        item.update(body, source_identity_id=context["identity_id"], updated_at=now())

    return _write(session_id, pair_id, expected_revision, change)


def delete(session_id: str, pair_id: str, prelink_id: str, *, expected_revision: int) -> dict[str, Any]:
    """DELETE works at any flag and without live recognition (a stale prelink can always be removed)."""
    from backend.app.services.stage_block_mapping import service

    try:
        service._pair(session_id, pair_id)
    except service.BlockMappingError as exc:
        raise PrelinkError(exc.status, exc.code, **exc.extra) from exc
    if not drafts_path(session_id, pair_id).is_file():
        raise PrelinkError(404, "PRELINK_NOT_FOUND", current_revision=0)

    def change(state: dict[str, Any]) -> None:
        before = len(state["prelinks"])
        state["prelinks"] = [i for i in state["prelinks"] if i["prelink_id"] != prelink_id]
        if len(state["prelinks"]) == before:
            raise PrelinkError(404, "PRELINK_NOT_FOUND", current_revision=state["revision"])

    return _write(session_id, pair_id, expected_revision, change)
