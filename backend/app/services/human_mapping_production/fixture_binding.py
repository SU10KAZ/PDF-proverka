"""Seed a REAL comparison pair's Human Mapping from an approved sealed fixture.

A real pair opens a sealed fixture's frozen semantic mapping as its INITIAL,
read-only Human Mapping data only when BOTH of its source PDFs are
byte-identical to the fixture's sources: sha256(left PDF) == fixture OLD and
sha256(right PDF) == fixture NEW (``human_mapping_fixture_sources.json``, built
and cross-checked by ``scripts/build_hm_fixture_source_identity.py``).  This is
the same source-PDF identity rule that binds the sealed ProjectChange cards to
real pairs (``project_change_v3.presentation.bind_snapshot_to_real_pairs``).
File names, disciplines, document codes, page counts and labels never decide.

The HM context stays the REAL one (object, session, pair): human reviews and
BlockLink edits are stored under the real pair.  Fixture files are only read.
"""
from __future__ import annotations

import hashlib
import json
import logging
import threading
import time
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

APP_DATA = Path(__file__).resolve().parents[2] / "data"
IDENTITY = APP_DATA / "human_mapping_fixture_sources.json"
_TTL_SECONDS = 10.0
_lock = threading.Lock()
_cache: dict[tuple[str, str], tuple[float, dict[str, Any]]] = {}
_verified: dict[tuple[str, int, int], str] = {}


def _file_sha256(path: Path) -> str | None:
    """sha256 cached by (path, size, mtime): a replaced file is hashed again."""
    try:
        stat = path.stat()
    except OSError:
        return None
    key = (str(path), stat.st_size, stat.st_mtime_ns)
    if key not in _verified:
        with path.open("rb") as handle:
            _verified[key] = hashlib.file_digest(handle, "sha256").hexdigest()
    return _verified[key]


def approved_fixtures(identity: Path | None = None) -> list[dict[str, Any]]:
    """Fixtures whose ui_data file is byte-identical to the sealed identity record."""
    identity = identity or IDENTITY
    try:
        record = json.loads(identity.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        logger.warning("HM fixture identity unreadable: %s", identity)
        return []
    fixtures_dir = identity.parent / str(record.get("fixtures_dir") or "")
    out = []
    for fixture in record.get("fixtures") or []:
        ui_path = fixtures_dir / str(fixture.get("ui_data") or "")
        if fixtures_dir == identity.parent or ui_path.parent != fixtures_dir:
            continue
        if _file_sha256(ui_path) != fixture.get("ui_data_sha256"):
            logger.warning("HM fixture %s differs from its sealed identity; not used", ui_path.name)
            continue
        out.append({**fixture, "ui_data_path": ui_path, "assets_dir": fixtures_dir / "assets"})
    return out


def _real_pairs(object_id: str, pair_id: str) -> list[tuple[str, dict[str, Any]]]:
    from backend.app.services.project_change_v3.scope import sessions_for_object
    from backend.app.services.stage_comparison import store

    found = []
    for session_id in sessions_for_object(object_id):
        for pair in (store.get_session(session_id) or {}).get("pairs") or []:
            if str(pair.get("id")) == pair_id:
                found.append((session_id, pair))
    return found


def bind_real_pair(object_id: str, pair_id: str, *, identity: Path | None = None) -> dict[str, Any]:
    """Receipt of the source-PDF identity check (never raises for a missing match)."""
    receipt: dict[str, Any] = {
        "method": "source_pdf_sha256", "object_id": object_id, "real_pair": pair_id,
        "session_id": None, "old_sha256": None, "new_sha256": None,
        "fixture_source": None, "match_status": "NO_MATCH",
    }
    try:
        found = _real_pairs(object_id, pair_id)
    except Exception as exc:  # noqa: BLE001 — no binding is the safe answer
        logger.warning("HM real pair lookup failed: object=%s pair=%s: %s", object_id, pair_id, exc)
        return {**receipt, "match_status": "REAL_PAIR_LOOKUP_FAILED"}
    if not found:
        return {**receipt, "match_status": "REAL_PAIR_NOT_FOUND"}
    hashes = set()
    for _session_id, pair in found:
        old = _file_sha256(Path(str((pair.get("left") or {}).get("pdf_path") or "")))
        new = _file_sha256(Path(str((pair.get("right") or {}).get("pdf_path") or "")))
        hashes.add((old, new))
    if len(hashes) != 1:
        return {**receipt, "match_status": "AMBIGUOUS_REAL_PAIR", "sessions": [s for s, _p in found]}
    (old, new), = hashes
    receipt.update(session_id=found[0][0] if len(found) == 1 else None,
                   sessions=[s for s, _p in found], old_sha256=old, new_sha256=new)
    if not old or not new:
        return {**receipt, "match_status": "SOURCE_PDF_UNREADABLE"}
    matches = [f for f in approved_fixtures(identity)
               if f["old"]["pdf_sha256"] == old and f["new"]["pdf_sha256"] == new]
    if len(matches) > 1:
        return {**receipt, "match_status": "AMBIGUOUS_FIXTURE"}
    if not matches:
        return receipt
    fixture = matches[0]
    receipt.update(match_status="BOUND", fixture_source={
        "ui_data": fixture["ui_data"], "ui_data_sha256": fixture["ui_data_sha256"],
        "pair_key": fixture["pair_key"], "label": fixture["label"], "regions": fixture["regions"],
        "old_pdf_sha256": fixture["old"]["pdf_sha256"], "new_pdf_sha256": fixture["new"]["pdf_sha256"],
    })
    receipt["_ui_data_path"] = str(fixture["ui_data_path"])
    receipt["_assets_dir"] = str(fixture["assets_dir"])
    return receipt


def seed_for(object_id: str, pair_id: str) -> dict[str, Any]:
    """Cached (short TTL) binding receipt for one real pair."""
    key = (object_id, pair_id)
    now = time.monotonic()
    with _lock:
        hit = _cache.get(key)
        if hit and now - hit[0] < _TTL_SECONDS:
            return hit[1]
    receipt = bind_real_pair(object_id, pair_id)
    with _lock:
        _cache[key] = (now, receipt)
    return receipt


def public_receipt(receipt: dict[str, Any]) -> dict[str, Any]:
    return {k: v for k, v in receipt.items() if not k.startswith("_")}


def seeded_ui_data(receipt: dict[str, Any]) -> dict[str, Any]:
    """The fixture's frozen mapping, re-labelled with the REAL comparison identity."""
    data = json.loads(Path(receipt["_ui_data_path"]).read_text(encoding="utf-8"))
    data["pair"] = receipt["real_pair"]
    data["pair_key"] = receipt["real_pair"]
    data["object_id"] = receipt["object_id"]
    data["session_id"] = receipt.get("session_id")
    data["seed"] = {**public_receipt(receipt), "role": "INITIAL_HM_DATA_READ_ONLY"}
    return data


def clear_cache() -> None:
    with _lock:
        _cache.clear()
