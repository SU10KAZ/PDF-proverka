"""Semantic block review API and redirects for retired Human Mapping bookmarks.

Every route is scoped to one comparison: ``object_id`` + comparison/pair id.
Both are validated (strict safe-ID, HTTP 400) BEFORE any filesystem access.
The sealed АР1/ИОС4.2 fixtures stay reachable only as compatibility aliases
of the fixture object; they never control an arbitrary comparison.

A REAL pair without its own published HM data whose two source PDFs are
byte-identical to an approved sealed fixture (``fixture_binding``) opens that
fixture's frozen mapping as initial read-only data; its reviews and BlockLink
edits are stored under the real pair, never in the fixture stores.
"""
from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlencode

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response
from fastapi.responses import JSONResponse, RedirectResponse

from backend.app.services.human_mapping_production import fixture_binding, storage
from backend.app.services.human_mapping_production.validation import (
    BlockLinkValidationError,
    allowed_block_ids,
    validate_block_link_event,
)

APP_ROOT = Path(__file__).resolve().parents[2]  # backend/app
FIXTURES = APP_ROOT / "data" / "human_mapping_fixtures"
FIXTURE_OBJECT = "4f3e5916"
OBJECT_DEFAULT = FIXTURE_OBJECT  # backward-compatible name
# Compatibility aliases for sealed AR1/IOS4.2 fixtures of FIXTURE_OBJECT only.
PAIR_KEYS = {
    "A": "ad0a31a342a666082f2ef66a",
    "B": "caea6d2810c334ec0368de8e",
}
PAIR_BY_KEY = {v: k for k, v in PAIR_KEYS.items()}

async def result_scope(request: Request):
    """Pin every HM request to one result; never fall through for unknown live IDs.

    Разрешение области (обход сессий, sha256 артефактов прогона) — в потоке:
    раньше оно шло в event loop на каждом запросе Human Mapping (план, этап 8).
    ContextVar'ы ставятся здесь, в async-части, чтобы их видел обработчик.
    """
    import asyncio as _asyncio

    snapshot_id = request.query_params.get("result_id")
    scope = await _asyncio.to_thread(_resolve_result_scope, request)
    snapshot_token = storage.SNAPSHOT_SCOPE.set(snapshot_id)
    token = storage.RESULT_SCOPE.set(scope)
    try:
        yield
    finally:
        storage.RESULT_SCOPE.reset(token)
        storage.SNAPSHOT_SCOPE.reset(snapshot_token)


def _resolve_result_scope(request: Request):
    from backend.app.services.project_change_v3 import run_storage
    from backend.app.services.project_change_v3.scope import sessions_for_object
    q = request.query_params
    oid = request.path_params.get('object_id') or q.get('object')
    pid = request.path_params.get('pair') or q.get('comparison') or q.get('pair')
    rid, sid = q.get('run_id'), q.get('session_id')
    scope = None
    snapshot_id = q.get("result_id")
    if oid and pid:
        oid, pid, letter = _scope(oid, pid)
        try:
            if snapshot_id:
                from backend.app.services.project_change_catalog import catalog
                entries = [e for e in catalog.cached_catalog()['entries'] if e['object_id'] == oid
                           and e['pair_id'] == pid and e['provenance']['result_id'] == snapshot_id
                           and e['result_source'] == catalog.SEALED_SNAPSHOT]
                if len(entries) != 1:
                    raise ValueError('snapshot outside comparison')
                seed = fixture_binding.seed_for(oid, pid)
                if seed['match_status'] != 'BOUND':
                    raise ValueError('snapshot HM unavailable')
            elif rid:
                storage.require_safe_id(rid, 'run')
                if not sid or sid not in sessions_for_object(oid):
                    raise ValueError('run outside object')
                run_storage.validate(sid, pid, rid)
                scope = (oid, pid, sid, rid)
            elif not letter:
                matches = [(session, run_storage.current(session, pid)) for session in sessions_for_object(oid)]
                matches = [(session, run) for session, run in matches if run]
                if len(matches) > 1:
                    raise ValueError('ambiguous comparison; session required')
                if matches:
                    sid, rid = matches[0]
                    run_storage.validate(sid, pid, rid)
                    scope = (oid, pid, sid, rid)
        except (ValueError, OSError, KeyError) as exc:
            raise HTTPException(404, 'Human Mapping result unavailable') from exc
    elif rid:
        raise HTTPException(400, 'Object and comparison required')
    return scope


router = APIRouter(tags=["Human Mapping"], dependencies=[Depends(result_scope)])
api_router = APIRouter(prefix="/api/human-mapping/objects/{object_id}", tags=["Human Mapping"], dependencies=[Depends(result_scope)])



def _scope(object_id: str, pair: str) -> tuple[str, str, str | None]:
    """Validate IDs, then resolve (object, storage pair id, fixture letter)."""
    try:
        storage.require_safe_id(object_id, "object")
        storage.require_safe_id(pair, "pair")
    except storage.InvalidScopeId as exc:
        raise HTTPException(400, detail={"error": "INVALID_SCOPE_ID", "kind": exc.kind, "ok": False}) from exc
    if object_id == FIXTURE_OBJECT:
        if pair in PAIR_KEYS:
            return object_id, PAIR_KEYS[pair], pair
        if pair in PAIR_BY_KEY:
            return object_id, pair, PAIR_BY_KEY[pair]
    return object_id, pair, None


def _data_source(object_id: str, pair_id: str, letter: str | None) -> tuple[str | None, dict | None]:
    """(kind, seed receipt): published V3 data > fixture alias > source-identical seed."""
    if storage.SNAPSHOT_SCOPE.get():
        seed = fixture_binding.seed_for(object_id, pair_id)
        return ('SOURCE_IDENTICAL_FIXTURE_SEED', seed) if seed['match_status'] == 'BOUND' else (None, None)
    if (storage.pair_dir(object_id, pair_id, smoke=False) / "ui_data.json").is_file():
        return "PUBLISHED", None
    if letter:
        return ("FIXTURE_ALIAS", None) if (FIXTURES / f"UI_DATA_PAIR_{letter}.json").is_file() else (None, None)
    seed = fixture_binding.seed_for(object_id, pair_id)
    return ("SOURCE_IDENTICAL_FIXTURE_SEED", seed) if seed["match_status"] == "BOUND" else (None, None)


def _load_ui_data(object_id: str, pair_id: str, letter: str | None) -> dict:
    kind, seed = _data_source(object_id, pair_id, letter)
    if kind == "PUBLISHED":
        generic = storage.pair_dir(object_id, pair_id, smoke=False) / "ui_data.json"
        return json.loads(generic.read_text(encoding="utf-8"))
    if kind == "FIXTURE_ALIAS":
        return json.loads((FIXTURES / f"UI_DATA_PAIR_{letter}.json").read_text(encoding="utf-8"))
    if kind == "SOURCE_IDENTICAL_FIXTURE_SEED":
        return fixture_binding.seeded_ui_data(seed)
    raise HTTPException(404, "HUMAN_MAPPING_UI_DATA_NOT_FOUND")


def _region(object_id: str, pair_id: str, letter: str | None, region_id: str):
    data = _load_ui_data(object_id, pair_id, letter)
    for r in data.get("regions") or []:
        if r.get("id") == region_id:
            return data, r
    return data, None


@router.get("/human-mapping/", response_class=RedirectResponse)
@router.get("/human-mapping", response_class=RedirectResponse)
def human_mapping_page(
    object: str | None = Query(default=None),
    pair: str | None = Query(default=None),
    comparison: str | None = Query(default=None),
):
    requested_pair = comparison or pair
    if object is None and requested_pair is None:
        object_id, pair_id, letter = FIXTURE_OBJECT, None, None
    elif object is None:
        raise HTTPException(400, detail={"error": "OBJECT_REQUIRED", "ok": False})
    elif requested_pair is None:
        try:
            storage.require_safe_id(object, "object")
        except storage.InvalidScopeId as exc:
            raise HTTPException(400, detail={"error": "INVALID_SCOPE_ID", "kind": exc.kind, "ok": False}) from exc
        if object != FIXTURE_OBJECT:
            raise HTTPException(400, detail={"error": "COMPARISON_REQUIRED", "ok": False})
        object_id, pair_id, letter = object, None, None
    else:
        object_id, pair_id, letter = _scope(object, requested_pair)
    _kind, seed = _data_source(object_id, pair_id, letter) if pair_id else (None, None)
    scope = storage.RESULT_SCOPE.get()
    # Old bookmarks open the same result in stage 2. The standalone editor is retired;
    # the append-only review APIs and stored history remain shared with the sheet UI.
    session_id = scope[2] if scope else (seed or {}).get("session_id")
    query = {"object": object_id}
    if pair_id and session_id:
        query.update(session=session_id, pair=pair_id, view="blocks")
        if scope:
            query["run"] = scope[3]
        elif storage.SNAPSHOT_SCOPE.get():
            query["result"] = storage.SNAPSHOT_SCOPE.get()
    return RedirectResponse("/#/stage-comparison?" + urlencode(query), status_code=307)


@api_router.get("/pairs/{pair}/ui-data")
@api_router.get("/comparisons/{pair}/ui-data")
def ui_data(object_id: str, pair: str):
    object_id, pair_id, letter = _scope(object_id, pair)
    data = _load_ui_data(object_id, pair_id, letter)
    raw = json.dumps(data, ensure_ascii=False)
    raw = raw.replace(
        '"assets/',
        f'"/api/human-mapping/objects/{object_id}/comparisons/{pair}/assets/',
    )
    data = json.loads(raw)
    scope = storage.RESULT_SCOPE.get()
    if scope:
        def pin(value):
            if isinstance(value, str) and value.startswith('/api/human-mapping/'):
                return value + f'?session_id={scope[2]}&run_id={scope[3]}'
            if isinstance(value, dict):
                return {k: pin(v) for k, v in value.items()}
            if isinstance(value, list):
                return [pin(v) for v in value]
            return value
        data = pin(data)
    return JSONResponse(data)


def _serve_asset(bases: list[Path], asset_path: str) -> Response:
    for base in bases:
        file = (base / asset_path).resolve()
        if file.is_relative_to(base) and file.is_file():
            media = "image/png" if file.suffix.lower() == ".png" else "application/octet-stream"
            return Response(file.read_bytes(), media_type=media, headers={"Cache-Control": "no-store"})
    raise HTTPException(404, "Asset not found")


@api_router.get("/pairs/{pair}/assets/{asset_path:path}")
@api_router.get("/comparisons/{pair}/assets/{asset_path:path}")
def pair_asset(object_id: str, pair: str, asset_path: str):
    object_id, pair_id, letter = _scope(object_id, pair)
    bases = [] if storage.SNAPSHOT_SCOPE.get() else [(storage.pair_dir(object_id, pair_id, smoke=False) / "assets").resolve()]
    kind, seed = _data_source(object_id, pair_id, letter)
    if kind == "FIXTURE_ALIAS":
        bases.append((FIXTURES / "assets").resolve())
    elif kind == "SOURCE_IDENTICAL_FIXTURE_SEED":
        bases.append(Path(seed["_assets_dir"]).resolve())
    return _serve_asset(bases, asset_path)


@api_router.get("/assets/{asset_path:path}")
def fixture_asset(object_id: str, asset_path: str):
    try:
        storage.require_safe_id(object_id, "object")
    except storage.InvalidScopeId as exc:
        raise HTTPException(400, detail={"error": "INVALID_SCOPE_ID", "kind": exc.kind, "ok": False}) from exc
    if object_id != FIXTURE_OBJECT:
        raise HTTPException(404, "Asset not found")
    return _serve_asset([(FIXTURES / "assets").resolve()], asset_path)


@api_router.get("/pairs/{pair}/reviews")
@api_router.get("/comparisons/{pair}/reviews")
def get_reviews(object_id: str, pair: str, smoke: bool = Query(default=False)):
    object_id, pair_id, _letter = _scope(object_id, pair)
    return storage.list_reviews(object_id, pair_id, smoke=smoke)


@api_router.post("/pairs/{pair}/reviews")
@api_router.post("/comparisons/{pair}/reviews")
async def post_review(
    object_id: str, pair: str, request: Request, smoke: bool = Query(default=False)
):
    object_id, pair_id, letter = _scope(object_id, pair)
    try:
        raw = await request.json()
        status = raw["status"]
        assert status in {"HUMAN_CONFIRMED", "HUMAN_REJECTED", "HUMAN_UNCERTAIN"}
        assert raw.get("old_block_ids") and raw.get("new_block_ids")
        region_id = raw["region_id"]
    except Exception as exc:
        raise HTTPException(400, "Bad review") from exc
    _data, region = _region(object_id, pair_id, letter, region_id)
    if region is None:
        raise HTTPException(400, detail={"error": "REGION_NOT_FOUND", "ok": False})
    # A block outside the region's allowed set would make the history unusable for the bridge forever.
    for side, key in (("OLD", "old_block_ids"), ("NEW", "new_block_ids")):
        ids = raw[key]
        if not isinstance(ids, list) or not all(isinstance(i, str) for i in ids) \
                or not set(ids) <= allowed_block_ids(region, side):
            raise HTTPException(400, detail={"error": "REVIEW_BLOCK_NOT_IN_REGION", "ok": False})
    row = {
        "review_id": str(uuid.uuid4()),
        "pair": letter or pair_id,
        "pair_key": pair_id,
        "comparison_id": pair_id,
        "object_id": object_id,
        "region_id": region_id,
        "old_block_ids": raw["old_block_ids"],
        "new_block_ids": raw["new_block_ids"],
        "status": status,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "comment": raw.get("comment", ""),
        "run_id": storage.RESULT_SCOPE.get()[3] if storage.RESULT_SCOPE.get() else None,
        "result_id": storage.SNAPSHOT_SCOPE.get(),
        "reviewer_source": "HUMAN",
        "supersedes_review_id": raw.get("previous_review_id"),
    }
    storage.append_review(object_id, pair_id, row, smoke=smoke)
    return row


@api_router.get("/pairs/{pair}/block-links")
@api_router.get("/comparisons/{pair}/block-links")
def get_block_links(object_id: str, pair: str, smoke: bool = Query(default=False)):
    object_id, pair_id, _letter = _scope(object_id, pair)
    return storage.list_block_links(object_id, pair_id, smoke=smoke)


@api_router.post("/pairs/{pair}/block-links")
@api_router.post("/comparisons/{pair}/block-links")
async def post_block_link(
    object_id: str, pair: str, request: Request, smoke: bool = Query(default=False)
):
    object_id, pair_id, letter = _scope(object_id, pair)
    try:
        raw = await request.json()
        event_type = raw["event_type"]
        region_id = raw["region_id"]
        link_id = raw["link_id"]
        old_block_id = raw["old_block_id"]
        new_block_id = raw["new_block_id"]
    except Exception as exc:
        raise HTTPException(400, detail={"error": "BAD_BLOCK_LINK_EVENT", "ok": False}) from exc

    _data, region = _region(object_id, pair_id, letter, region_id)
    if region is None:
        raise HTTPException(400, detail={"error": "REGION_NOT_FOUND", "ok": False})

    events = storage.list_block_links(object_id, pair_id, smoke=smoke)
    try:
        resolved = validate_block_link_event(
            event_type=event_type,
            region=region,
            events=events,
            link_id=link_id,
            old_block_id=old_block_id,
            new_block_id=new_block_id,
            previous_link_id=raw.get("previous_link_id"),
        )
    except BlockLinkValidationError as exc:
        raise HTTPException(400, detail={"error": exc.code, "ok": False}) from exc

    if resolved.get("noop"):
        return {"error": "NO_CHANGE", "ok": True}

    row = {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "pair": letter or pair_id,
        "pair_key": pair_id,
        "comparison_id": pair_id,
        "object_id": object_id,
        "region_id": region_id,
        "link_id": link_id,
        "old_block_id": old_block_id,
        "new_block_id": new_block_id,
        "previous_old_block_id": resolved.get("previous_old_block_id"),
        "previous_new_block_id": resolved.get("previous_new_block_id"),
        "previous_link_id": resolved.get("previous_link_id"),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "comment": raw.get("comment", ""),
        "run_id": storage.RESULT_SCOPE.get()[3] if storage.RESULT_SCOPE.get() else None,
        "result_id": storage.SNAPSHOT_SCOPE.get(),
        "reviewer_source": "HUMAN",
    }
    storage.append_block_link(object_id, pair_id, row, smoke=smoke)
    return row
