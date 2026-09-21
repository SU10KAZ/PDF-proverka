"""Catalog of accepted, frozen ProjectChange V3 results (read-only, 0 model calls).

Entries are DISCOVERED from persisted data; no result is listed by hand.

* ``LIVE_RUN`` — a stage-comparison pair whose persisted V3 state is terminal
  and whose final result belongs to that run (the same gate the change list
  uses: ``project_change_v3.presentation.published_run``), and whose two source
  PDFs are still byte-identical (sha256) to the ones the run recorded.
* ``SEALED_SNAPSHOT`` — every pair of an accepted sealed snapshot listed in
  ``project_comparison_catalog_sources.json``, bound to a REAL comparison pair
  only when sha256 of both source PDFs matches (never by file name or code).

A running, failed, cancelled, stale or partial run is never a catalog result;
it may only appear in the separate diagnostics list, without cards.  Counts,
model provenance and region counts are read from the result / mapping
artifacts.  Evaluation data (truth sets, verdicts, recall) is never read and a
guard rejects any entry that would carry such markers.  Nothing is written.
"""
from __future__ import annotations

import hashlib
import json
import logging
import re
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

CATALOG_SCHEMA = "project-comparison-catalog/1"
ENTRY_SCHEMA = "project-comparison-catalog-entry/1"
APP_DATA = Path(__file__).resolve().parents[2] / "data"
SOURCES_REGISTRY = APP_DATA / "project_comparison_catalog_sources.json"
SOURCES_SCHEMA = "project-comparison-catalog-sources/1"
SNAPSHOT_SCHEMA = "project-change-production-snapshot/1"
RESULT_SCHEMA_PREFIX = "projectchange_v3_final/"
LIVE_RUN = "LIVE_RUN"
SEALED_SNAPSHOT = "SEALED_SNAPSHOT"
COMPLETED_FROZEN = "COMPLETED_FROZEN"
ENGINE_LABEL = "ProjectChange V3"
_SHA256 = re.compile(r"[0-9a-f]{64}")
# Evaluation vocabulary that must never reach a production catalog entry.
# Upper-case verdict tokens are matched case-sensitively (JSON ``false`` is not a verdict).
TRUTH_TOKENS = re.compile(r"\b(REAL15|PROVEN10|F13|SUPPORTED|PARTIALLY_SUPPORTED|NOT_SUPPORTED|CORRECT|FALSE)\b")
TRUTH_KEYS = re.compile(r"source[_-]first|expected[_-]changes|recall|quality[_-]audit|miss[_-]trace", re.IGNORECASE)
_CACHE_TTL_SECONDS = 15.0
_cache_lock = threading.Lock()
_cache: dict[bool, tuple[float, dict[str, Any]]] = {}
_snapshot_services: dict[str, Any] = {}
_region_counts: dict[tuple[str, int, int], int] = {}


class EntryRejected(ValueError):
    """A discovered result does not qualify as a completed catalog entry."""

    def __init__(self, label: str, reason: str, context: dict[str, Any] | None = None):
        super().__init__(reason)
        self.label = label
        self.reason = reason
        self.context = context or {}


# ─── helpers ────────────────────────────────────────────────────────────────

def _digest(value: Any) -> str:
    raw = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _file_sha256(path: Path) -> str:
    with path.open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


def _utc(value: Any) -> str | None:
    """ISO timestamp normalized to UTC, or None when absent/unparseable."""
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).isoformat()


def model_display(model: str | None) -> str:
    """Human label derived from the recorded model id (never from prompt text).

    ``claude-opus-5`` → ``Claude Opus 5``; ``gpt-6-astra`` → ``GPT-6 Astra``.
    Unknown shapes are shown as recorded.
    """
    raw = str(model or "").strip()
    if not raw:
        return ""
    parts = raw.split("-")
    words: list[str] = []
    for part in parts:
        if not part:
            return raw
        if part.lower() == "gpt":
            words.append("GPT")
        elif part.isdigit() and words and words[-1] == "GPT":
            words[-1] = f"GPT-{part}"
        elif part.isalpha():
            words.append(part[:1].upper() + part[1:])
        else:
            words.append(part)
    return " ".join(words)


def _sha(value: Any) -> str | None:
    text = str(value or "").lower()
    return text if _SHA256.fullmatch(text) else None


def _pair_group_key(object_id: str, old_sha: str, new_sha: str) -> str:
    return "pcpair1_" + _digest({"object": object_id, "old": old_sha, "new": new_sha})[:24]


def _entry_id(kind: str, object_id: str, session_id: str, pair_id: str, variant: str) -> str:
    return "pcc1_" + _digest({"kind": kind, "object": object_id, "session": session_id,
                              "pair": pair_id, "variant": variant})[:32]


def _objects() -> dict[str, str]:
    from backend.app.services.stage_comparison import objects as objects_mod

    return {str(item["id"]): str(item.get("name") or item["id"])
            for item in objects_mod.list_objects().get("items") or [] if item.get("id")}


def _document(doc: dict[str, Any], sha: str) -> dict[str, Any]:
    return {
        "document_code": str(doc.get("document_code") or ""),
        "filename": str(doc.get("filename") or ""),
        "version_id": str(doc.get("version_id") or ""),
        "pdf_sha256": sha,
    }


def _title(old: dict[str, Any], new: dict[str, Any]) -> str:
    return new["document_code"] or new["filename"] or old["document_code"] or old["filename"]


def _hm_url(object_id: str, pair_id: str) -> str:
    return f"/human-mapping/?object={object_id}&comparison={pair_id}"


def _published_hm(object_id: str, pair_id: str) -> dict[str, Any] | None:
    from backend.app.services.human_mapping_production import storage

    path = storage.pair_dir(object_id, pair_id, smoke=False) / "ui_data.json"
    if not path.is_file():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {"_unreadable": True}


def _regions_in(path: Path) -> int:
    stat = path.stat()
    key = (str(path), stat.st_size, stat.st_mtime_ns)
    if key not in _region_counts:
        data = json.loads(path.read_text(encoding="utf-8"))
        _region_counts[key] = len(data.get("regions") or [])
    return _region_counts[key]


def assert_no_truth(value: Any) -> None:
    """Raise when a serialized entry carries evaluation vocabulary."""
    raw = json.dumps(value, ensure_ascii=False)
    hit = TRUTH_TOKENS.search(raw) or TRUTH_KEYS.search(raw)
    if hit:
        raise EntryRejected("DIAGNOSTIC_ONLY", f"EVALUATION_MARKER_PRESENT:{hit.group(0)}")


# ─── LIVE_RUN discovery ─────────────────────────────────────────────────────

def _diagnostic_label(state: dict[str, Any]) -> str:
    status = str(state.get("status") or "").upper()
    reason = str(state.get("reason_code") or "").lower()
    if status == "FAILED":
        return "TRANSPORT_FAILED" if "transport" in reason else "FAILED"
    if status.startswith("CANCEL"):
        return "CANCELLED"
    return "PARTIAL"


def _live_candidates() -> list[tuple[str, str]]:
    from backend.app.services.stage_comparison import paths, store

    out = []
    for session in store.list_sessions():
        session_id = str(session.get("id") or "")
        if not session_id:
            continue
        for pair in (store.get_session(session_id) or {}).get("pairs") or []:
            pair_id = str(pair.get("id") or "")
            if pair_id and (paths.production_dir(session_id, pair_id) / "state.json").is_file():
                out.append((session_id, pair_id))
    return out


def _live_entry(session_id: str, pair_id: str, objects: dict[str, str]) -> dict[str, Any] | None:
    """Catalog entry of one pair's persisted V3 run; None when the pair has no V3 state."""
    from backend.app.services.human_mapping_production.storage import InvalidScopeId
    from backend.app.services.project_change_v3 import presentation
    from backend.app.services.project_change_v3.contracts import ENGINE_NAME
    from backend.app.services.project_change_v3.scope import ScopeUnresolved, object_id_for_session
    from backend.app.services.stage_comparison import paths, store

    state = presentation._load(session_id, pair_id, "state")
    if not isinstance(state, dict) or state.get("engine") != ENGINE_NAME:
        return None
    published = presentation.published_run(session_id, pair_id)
    if published is None:
        provenance = state.get("provenance") if isinstance(state.get("provenance"), dict) else {}
        # Diagnostics never carry cards or counts of an unpublished run.
        raise EntryRejected(_diagnostic_label(state), f"RUN_NOT_PUBLISHED:{state.get('status')}", {
            "run_id": state.get("run_id"), "run_status": state.get("status"), "reason_code": state.get("reason_code"),
            "engine_version": state.get("engine_version"), "model": provenance.get("model"),
            "model_display": model_display(provenance.get("model")), "reasoning": provenance.get("reasoning")})
    state, result = published
    if not str(result.get("schema") or "").startswith(RESULT_SCHEMA_PREFIX):
        raise EntryRejected("DIAGNOSTIC_ONLY", "RESULT_SCHEMA_UNKNOWN")
    if not isinstance(result.get("projectchanges"), list):
        raise EntryRejected("DIAGNOSTIC_ONLY", "RESULT_WITHOUT_PROJECTCHANGES")
    provenance = result.get("provenance") or {}
    if provenance.get("engine") != ENGINE_NAME or not provenance.get("model"):
        raise EntryRejected("DIAGNOSTIC_ONLY", "RESULT_PROVENANCE_INCOMPLETE")
    manifest = result.get("source_manifest") or {}
    old_sha, new_sha = _sha(manifest.get("old_pdf_sha256")), _sha(manifest.get("new_pdf_sha256"))
    if not old_sha or not new_sha:
        raise EntryRejected("DIAGNOSTIC_ONLY", "SOURCE_IDENTITY_MISSING")
    try:
        object_id = object_id_for_session(session_id)
    except (ScopeUnresolved, InvalidScopeId, ValueError) as exc:
        raise EntryRejected("DIAGNOSTIC_ONLY", "OBJECT_UNRESOLVED") from exc
    # Source hash binding: the run is shown only while its sources are unchanged.
    resolved = presentation._resolved_paths(session_id, pair_id)
    try:
        current = {side: _file_sha256(Path(resolved[side]["pdf"])) for side in ("OLD", "NEW")}
    except (KeyError, OSError, TypeError) as exc:
        raise EntryRejected("DIAGNOSTIC_ONLY", "SOURCE_PDF_UNREADABLE") from exc
    if (current["OLD"], current["NEW"]) != (old_sha, new_sha):
        raise EntryRejected("DIAGNOSTIC_ONLY", "SOURCE_PDF_CHANGED_AFTER_RUN")
    if presentation._stale(session_id, pair_id, result):
        raise EntryRejected("DIAGNOSTIC_ONLY", "SOURCE_FILES_CHANGED_AFTER_RUN")

    pair = store.get_pair_for_production(session_id, pair_id)
    old, new = _document(pair.get("left") or {}, old_sha), _document(pair.get("right") or {}, new_sha)
    run_id = str(result["run_id"])
    result_path = paths.production_project_change_v3_result_path(session_id, pair_id)
    semantic = presentation._load(session_id, pair_id, "project_change_v3_semantic_map")
    semantic_regions = len(semantic.get("regions") or []) if isinstance(semantic, dict) \
        and str(semantic.get("pair")) == pair_id else None
    hm = _published_hm(object_id, pair_id)
    if hm and not hm.get("_unreadable") and str(hm.get("run_id")) == run_id \
            and str(hm.get("session_id")) == session_id:
        human_mapping = {"available": True, "regions": len(hm.get("regions") or []),
                         "data_source": "PUBLISHED_BY_RUN", "url": _hm_url(object_id, pair_id), "reason": None}
    else:
        human_mapping = {"available": False, "regions": None, "data_source": None, "url": None,
                         "reason": "PAIR_HM_BELONGS_TO_OTHER_RUN" if hm else "NOT_PUBLISHED_BY_RUN"}
    thinking = provenance.get("thinking") if isinstance(provenance.get("thinking"), dict) else {}
    return {
        "schema": ENTRY_SCHEMA,
        "catalog_entry_id": _entry_id(LIVE_RUN, object_id, session_id, pair_id, run_id),
        "result_source": LIVE_RUN,
        "result_status": COMPLETED_FROZEN,
        "object_id": object_id,
        "object_name": objects.get(object_id, object_id),
        "session_id": session_id,
        "pair_id": pair_id,
        "run_id": run_id,
        "pair_group_key": _pair_group_key(object_id, old_sha, new_sha),
        "title": _title(old, new),
        "documents": {"old": old, "new": new},
        "engine": {
            "label": ENGINE_LABEL,
            "engine": ENGINE_NAME,
            "engine_version": str(provenance.get("engine_version") or ""),
            "engine_variant": str(provenance.get("engine_variant") or ""),
            "provider": str(provenance.get("provider") or "") or None,
            "model": str(provenance.get("model")),
            "model_display": model_display(provenance.get("model")),
            "reasoning": str(provenance.get("reasoning") or thinking.get("effort") or "") or None,
        },
        "frozen_at": _utc(state.get("completed_at") or result.get("created_at")),
        "frozen_at_source": "run completed_at" if state.get("completed_at") else "result created_at",
        "counts": {
            "projectchanges": len(result["projectchanges"]),
            "unresolved_hints": len(result.get("unresolved_hints") or []),
            "semantic_regions": semantic_regions,
            "semantic_regions_source": "run semantic map" if semantic_regions is not None else None,
        },
        "human_mapping": human_mapping,
        "open": {
            "available": True, "reason": None, "object_id": object_id, "session_id": session_id,
            "pair_id": pair_id, "source_run_id": run_id, "tab": "diffs",
            "presentation_api": f"/api/stage-comparison/objects/{object_id}/project-changes",
            "pair_changes_api": f"/api/stage-comparison/sessions/{session_id}/pairs/{pair_id}/production/changes",
        },
        "provenance": {
            "result_id": "pcv3res_" + _file_sha256(result_path)[:32],
            "result_sha256": _file_sha256(result_path),
            "result_schema": str(result.get("schema")),
            "run_status": str(state.get("status")),
            "model_calls_recorded": result.get("model_calls"),
            "prompt_sha256": {k: str(provenance.get(k) or "") for k in
                              ("mapper_prompt_sha256", "miner_prompt_sha256", "dedupe_prompt_sha256")},
            "source_binding": {"method": "source_pdf_sha256", "old_sha256": old_sha, "new_sha256": new_sha,
                               "verified_against_current_pdfs": True},
        },
        "_explicit_default": False,
    }


# ─── SEALED_SNAPSHOT discovery ──────────────────────────────────────────────

def _registry(path: Path | None = None) -> list[dict[str, Any]]:
    path = path or SOURCES_REGISTRY
    try:
        record = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        logger.warning("Catalog sources registry unreadable: %s", path)
        return []
    if record.get("schema") != SOURCES_SCHEMA:
        logger.warning("Catalog sources registry has unknown schema: %s", path)
        return []
    return [s for s in record.get("sealed_snapshots") or [] if isinstance(s, dict)]


def _snapshot_service(root: Path):
    from backend.app.services.project_change_preview.service import PreviewService

    key = str(root.resolve())
    service = _snapshot_services.get(key)
    if service is None:
        service = PreviewService(root)
        _snapshot_services[key] = service
    service.assert_current()
    return service


def _live_published_pairs(object_id: str) -> set[tuple[str, str]]:
    from backend.app.services.project_change_v3 import presentation
    from backend.app.services.project_change_v3.scope import sessions_for_object

    return {(sid, pid) for sid in sessions_for_object(object_id) for pid in presentation._pair_ids(sid)
            if presentation.published_run(sid, pid) is not None}


def _snapshot_entries(spec: dict[str, Any], objects: dict[str, str],
                      diagnostics: list[dict[str, Any]], data_root: Path) -> list[dict[str, Any]]:
    from backend.app.services.human_mapping_production import fixture_binding
    from backend.app.services.project_change_preview.service import OBJECT as SERVING_OBJECT
    from backend.app.services.project_change_v3 import presentation
    from backend.app.services.stage_comparison import store

    name = str(spec.get("snapshot_dir") or "")
    root = (data_root / name).resolve()
    if not name or not root.is_relative_to(data_root.resolve()) or not (root / "MANIFEST.json").is_file():
        diagnostics.append({"result_source": SEALED_SNAPSHOT, "snapshot": name, "label": "DIAGNOSTIC_ONLY",
                            "reason": "SNAPSHOT_NOT_FOUND"})
        return []
    try:
        service = _snapshot_service(root)
        if service.manifest.get("schema") != SNAPSHOT_SCHEMA:
            raise ValueError("schema")
        engine = json.loads(service.path("source-manifest.json").read_text(encoding="utf-8")).get("engine") or {}
        receipts = json.loads(service.path("source-receipts.json").read_text(encoding="utf-8"))
        envelope = service.envelope()
    except (ValueError, OSError, KeyError) as exc:
        diagnostics.append({"result_source": SEALED_SNAPSHOT, "snapshot": name, "label": "DIAGNOSTIC_ONLY",
                            "reason": f"SNAPSHOT_UNAVAILABLE:{type(exc).__name__}"})
        return []
    if not engine.get("model") or engine.get("engine") != "projectchange_v3":
        diagnostics.append({"result_source": SEALED_SNAPSHOT, "snapshot": name, "label": "DIAGNOSTIC_ONLY",
                            "reason": "SNAPSHOT_PROVENANCE_INCOMPLETE"})
        return []
    manifest_sha = service.receipts["MANIFEST.json"]
    live = _live_published_pairs(SERVING_OBJECT)
    bound = presentation.bind_snapshot_to_real_pairs(envelope, service.data, SERVING_OBJECT, skip=live)
    report = bound["snapshot_binding"]
    finals = {str(v.get("pair_key")): v for v in (receipts.get("finals") or {}).values() if isinstance(v, dict)}
    frozen_at = _utc(spec.get("accepted_at"))
    entries = []
    for sealed_pair in service.data.get("pairs") or {}:
        cards = [i for i in envelope["items"]
                 if {str(e.get("pair_id")) for e in i.get("evidence") or []} == {sealed_pair}]
        old_sha = _sha(service.data["documents"][f"{sealed_pair}:old"].get("source_sha256"))
        new_sha = _sha(service.data["documents"][f"{sealed_pair}:new"].get("source_sha256"))
        served = [r for r in report["bound"] if r["snapshot_pair"] == sealed_pair]
        shadowed = [r for r in report["skipped_live_v3"] if r["snapshot_pair"] == sealed_pair]
        targets = served or shadowed
        if not old_sha or not new_sha:
            diagnostics.append({"result_source": SEALED_SNAPSHOT, "snapshot": name, "sealed_pair": sealed_pair,
                                "label": "DIAGNOSTIC_ONLY", "reason": "SOURCE_IDENTITY_MISSING"})
            continue
        if not targets:
            diagnostics.append({"result_source": SEALED_SNAPSHOT, "snapshot": name, "sealed_pair": sealed_pair,
                                "label": "DIAGNOSTIC_ONLY", "reason": "NO_REAL_PAIR_WITH_IDENTICAL_SOURCE_PDFS"})
            continue
        target = targets[0]
        session_id, pair_id = target["session_id"], target["pair_id"]
        real = store.get_pair_for_production(session_id, pair_id)
        old = _document(real.get("left") or {}, old_sha)
        new = _document(real.get("right") or {}, new_sha)
        # Human Mapping: the pair's own published HM wins in the HM router, so
        # the sealed mapping is this entry's only while nothing is published.
        hm_published = _published_hm(SERVING_OBJECT, pair_id)
        seed = fixture_binding.bind_real_pair(SERVING_OBJECT, pair_id)
        fixture = seed.get("fixture_source") or {}
        if hm_published:
            human_mapping = {"available": False, "regions": None, "data_source": None, "url": None,
                             "reason": "PAIR_HM_BELONGS_TO_OTHER_RUN"}
        elif seed.get("match_status") == "BOUND" and (fixture.get("old_pdf_sha256"), fixture.get("new_pdf_sha256")) \
                == (old_sha, new_sha):
            human_mapping = {"available": True, "regions": _regions_in(Path(seed["_ui_data_path"])),
                             "data_source": "SOURCE_IDENTICAL_SEALED_MAPPING",
                             "url": _hm_url(SERVING_OBJECT, pair_id), "reason": None}
        else:
            human_mapping = {"available": False, "regions": None, "data_source": None, "url": None,
                             "reason": f"NO_SEALED_MAPPING:{seed.get('match_status')}"}
        source_run_id = str(envelope.get("source_run_id") or service.manifest.get("source_run_id") or "")
        variant = f"snapshot:{manifest_sha}:{sealed_pair}"
        final = finals.get(sealed_pair) or {}
        entries.append({
            "schema": ENTRY_SCHEMA,
            "catalog_entry_id": _entry_id(SEALED_SNAPSHOT, SERVING_OBJECT, session_id, pair_id, variant),
            "result_source": SEALED_SNAPSHOT,
            "result_status": COMPLETED_FROZEN,
            "object_id": SERVING_OBJECT,
            "object_name": objects.get(SERVING_OBJECT, SERVING_OBJECT),
            "session_id": session_id,
            "pair_id": pair_id,
            "run_id": None,
            "pair_group_key": _pair_group_key(SERVING_OBJECT, old_sha, new_sha),
            "title": _title(old, new),
            "documents": {"old": old, "new": new},
            "engine": {
                "label": ENGINE_LABEL,
                "engine": str(engine.get("engine")),
                "engine_version": str(engine.get("engine_version") or ""),
                "engine_variant": "",
                "provider": str(engine.get("provider") or "") or None,
                "model": str(engine.get("model")),
                "model_display": model_display(engine.get("model")),
                "reasoning": str(engine.get("reasoning") or "") or None,
            },
            "frozen_at": frozen_at,
            "frozen_at_source": str(spec.get("accepted_at_source") or "catalog sources registry"),
            "counts": {
                "projectchanges": len(cards),
                "unresolved_hints": None,
                "semantic_regions": human_mapping["regions"],
                "semantic_regions_source": "sealed Human Mapping" if human_mapping["available"] else None,
            },
            "human_mapping": human_mapping,
            "open": {
                "available": bool(served), "reason": None if served else "PAIR_VIEW_SHOWS_LIVE_RUN",
                "object_id": SERVING_OBJECT, "session_id": session_id, "pair_id": pair_id,
                "source_run_id": source_run_id, "tab": "diffs",
                "presentation_api": f"/api/stage-comparison/objects/{SERVING_OBJECT}/project-changes",
                "pair_changes_api": None,
            },
            "provenance": {
                "result_id": "pcv3snap_" + _digest({"manifest": manifest_sha, "pair": sealed_pair})[:32],
                "result_sha256": str(final.get("sha256") or "") or None,
                "result_schema": str(service.manifest.get("schema")),
                "snapshot": {"dir": name, "manifest_sha256": manifest_sha, "sealed_pair_key": sealed_pair,
                             "source_run_id": source_run_id, "read_only": True},
                "real_pair_bindings": [{"session_id": r["session_id"], "pair_id": r["pair_id"],
                                        "served": r in served} for r in targets],
                "prompt_sha256": {"mapper_prompt_sha256": str(engine.get("mapper_prompt_version") or ""),
                                  "miner_prompt_sha256": str(engine.get("miner_prompt_version") or ""),
                                  "dedupe_prompt_sha256": str(engine.get("dedupe_version") or "")},
                "source_binding": {"method": "source_pdf_sha256", "old_sha256": old_sha, "new_sha256": new_sha,
                                   "verified_against_current_pdfs": True},
            },
            "_explicit_default": bool(spec.get("default")),
        })
    return entries


# ─── catalog ────────────────────────────────────────────────────────────────

def _assign_primary(entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """One primary entry per OLD/NEW pair group, by product rule — never by model.

    1. an entry explicitly marked default in accepted metadata (exactly one);
    2. otherwise the entry the pair's change list actually shows;
    3. then the most recently frozen; the entry id breaks ties.
    """
    groups: dict[str, list[dict[str, Any]]] = {}
    for entry in entries:
        groups.setdefault(entry["pair_group_key"], []).append(entry)
    out = []
    for key in sorted(groups):
        members = groups[key]
        explicit = [e for e in members if e["_explicit_default"]]
        if len(explicit) == 1:
            primary, rule = explicit[0], "EXPLICIT_DEFAULT"
        else:
            served = [e for e in members if e["open"]["available"]]
            pool = served or members
            rule = "SINGLE_RESULT" if len(members) == 1 else (
                "SHOWN_BY_PAIR_VIEW" if 0 < len(served) < len(members) else "LATEST_FROZEN")
            primary = max(pool, key=lambda e: (e["frozen_at"] or "", e["catalog_entry_id"]))
        for entry in members:
            entry["is_primary"] = entry is primary
            entry["primary_rule"] = rule
            entry["variants_in_pair"] = len(members)
            out.append(entry)
    return out


def _public(entry: dict[str, Any]) -> dict[str, Any]:
    return {k: v for k, v in entry.items() if not k.startswith("_")}


def build_catalog(*, include_diagnostics: bool = False, registry: Path | None = None,
                  data_root: Path | None = None) -> dict[str, Any]:
    """Deterministic catalog of completed/frozen results (no timestamps of its own)."""
    objects = _objects()
    diagnostics: list[dict[str, Any]] = []
    entries: list[dict[str, Any]] = []
    for session_id, pair_id in _live_candidates():
        try:
            entry = _live_entry(session_id, pair_id, objects)
            if entry is not None:
                assert_no_truth(_public(entry))
                entries.append(entry)
        except EntryRejected as exc:
            diagnostics.append({"result_source": LIVE_RUN, "session_id": session_id, "pair_id": pair_id,
                                "label": exc.label, "reason": exc.reason, **exc.context})
        except Exception as exc:  # noqa: BLE001 — one broken pair never hides the catalog
            logger.warning("Catalog: live pair %s/%s skipped: %s", session_id, pair_id, exc)
            diagnostics.append({"result_source": LIVE_RUN, "session_id": session_id, "pair_id": pair_id,
                                "label": "DIAGNOSTIC_ONLY", "reason": f"READ_ERROR:{type(exc).__name__}"})
    for spec in _registry(registry):
        try:
            for entry in _snapshot_entries(spec, objects, diagnostics, data_root or APP_DATA):
                try:
                    assert_no_truth(_public(entry))
                    entries.append(entry)
                except EntryRejected as exc:
                    diagnostics.append({"result_source": SEALED_SNAPSHOT, "snapshot": spec.get("snapshot_dir"),
                                        "label": exc.label, "reason": exc.reason})
        except Exception as exc:  # noqa: BLE001
            logger.warning("Catalog: snapshot %s skipped: %s", spec.get("snapshot_dir"), exc)
            diagnostics.append({"result_source": SEALED_SNAPSHOT, "snapshot": spec.get("snapshot_dir"),
                                "label": "DIAGNOSTIC_ONLY", "reason": f"READ_ERROR:{type(exc).__name__}"})
    ids = [e["catalog_entry_id"] for e in entries]
    if len(ids) != len(set(ids)):
        raise RuntimeError("catalog entry id collision")
    public = [_public(e) for e in _assign_primary(entries)]
    public.sort(key=lambda e: (e["object_name"].casefold(), e["title"].casefold(),
                               e["frozen_at"] or "", e["catalog_entry_id"]))
    groups: dict[str, dict[str, Any]] = {}
    for e in public:
        group = groups.setdefault(e["pair_group_key"], {"pair_group_key": e["pair_group_key"],
                                                        "primary_entry_id": None, "entry_ids": []})
        group["entry_ids"].append(e["catalog_entry_id"])
        if e["is_primary"]:
            group["primary_entry_id"] = e["catalog_entry_id"]
    catalog = {
        "schema": CATALOG_SCHEMA,
        "model_calls": 0,
        "entries": public,
        "groups": [groups[k] for k in sorted(groups)],
        "summary": {"completed_results": len(public), "pairs": len(groups),
                    "excluded": len(diagnostics)},
    }
    catalog["catalog_revision"] = "pccat1_" + _digest(public)[:24]
    if include_diagnostics:
        catalog["diagnostics"] = sorted(diagnostics, key=lambda d: json.dumps(d, sort_keys=True))
    return catalog


def cached_catalog(*, include_diagnostics: bool = False) -> dict[str, Any]:
    now = time.monotonic()
    with _cache_lock:
        hit = _cache.get(include_diagnostics)
        if hit and now - hit[0] < _CACHE_TTL_SECONDS:
            return hit[1]
    value = build_catalog(include_diagnostics=include_diagnostics)
    with _cache_lock:
        _cache[include_diagnostics] = (now, value)
    return value


def clear_cache() -> None:
    with _cache_lock:
        _cache.clear()
    _snapshot_services.clear()
