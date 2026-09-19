"""Presentation adapter: persisted V3 ProjectChanges → user-facing contracts.

The V3 result is NOT converted back into the legacy semantic pipeline.  This
module maps ``project_change_v3_result`` (and nothing else from V3 runtime)
into:

* ``project-change-view/1`` — the object-scoped envelope the stage-comparison
  UI (``project-change-list``) already renders;
* a versioned pair-level contract for ``/production/changes``.

Only whitelisted result fields are emitted; research evaluation data never
exists in a production result and is never read here.  A result is published
only when the pair's state belongs to the same V3 run (``run_id``) and is
terminal (COMPLETED/REVIEW).  Evidence is bound to the REAL stage-comparison
pair (its PDF path and version), so the UI shows it for the opened pair.
"""
from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path
from typing import Any

from .contracts import ENGINE_NAME
from .provenance import provenance_lines

PUBLISHED_STATUSES = frozenset({"COMPLETED", "REVIEW"})
CHANGES_KIND = "stage_comparison_project_change_v3_changes"
CHANGES_SCHEMA_VERSION = 1
UNVERSIONED = "unversioned"
EVIDENCE_ID_RE = re.compile(r"v3ev_[0-9a-f]{40}")
ROLE_RU = {
    "OLD_STATE": "Исходное состояние объекта.",
    "NEW_STATE": "Состояние объекта в новой редакции.",
}


def v3_presentation_enabled() -> bool:
    """V3 is the presentation source unless the engine is explicitly legacy."""
    import os

    return os.environ.get("PROJECT_COMPARISON_ENGINE", "v3").strip().lower() != "legacy"


def serves_state(state: dict[str, Any] | None) -> bool:
    """True when a pair state belongs to a V3 run and V3 presentation is on."""
    return isinstance(state, dict) and state.get("engine") == ENGINE_NAME and v3_presentation_enabled()


class EvidenceUnavailable(LookupError):
    """Evidence is unknown, outside the object scope or its source changed."""


def _load(session_id: str, pair_id: str, name: str) -> dict[str, Any] | None:
    from backend.app.services.stage_comparison import production_store

    return production_store.load_artifact(session_id, pair_id, name, include_domain_keys=True)


def _documents(session_id: str, pair_id: str) -> dict[str, dict[str, Any]]:
    from backend.app.services.stage_comparison import store

    pair = store.get_pair_for_production(session_id, pair_id)
    out = {}
    for side, key in (("OLD", "left"), ("NEW", "right")):
        doc = pair.get(key) or {}
        out[side] = {
            "pdf_path": str(doc.get("pdf_path") or ""),
            "version_id": str(doc.get("version_id") or "") or UNVERSIONED,
            "document_code": str(doc.get("document_code") or ""),
            "filename": str(doc.get("filename") or ""),
            "discipline": str(doc.get("discipline") or ""),
        }
    return out


def published_run(session_id: str, pair_id: str) -> tuple[dict[str, Any], dict[str, Any]] | None:
    """(state, result) of the published V3 generation for a pair, else None."""
    state = _load(session_id, pair_id, "state")
    if not isinstance(state, dict) or state.get("engine") != ENGINE_NAME:
        return None
    if state.get("status") not in PUBLISHED_STATUSES or not state.get("run_id"):
        return None
    result = _load(session_id, pair_id, "project_change_v3_result")
    if not isinstance(result, dict) or result.get("run_id") != state.get("run_id") \
            or str(result.get("pair_id")) != str(pair_id):
        return None
    return state, result


def _resolved_paths(session_id: str, pair_id: str) -> dict[str, dict[str, Path]]:
    from backend.app.services.stage_comparison import store
    from backend.app.services.stage_comparison.production_orchestrator import _resolved_document_paths

    pair = store.get_pair_for_production(session_id, pair_id)
    return {"OLD": _resolved_document_paths(pair.get("left") or {}),
            "NEW": _resolved_document_paths(pair.get("right") or {})}


def _stale(session_id: str, pair_id: str, result: dict[str, Any]) -> bool:
    recorded = result.get("source_identity") or {}
    try:
        current = _resolved_paths(session_id, pair_id)
        for side in ("OLD", "NEW"):
            for key in ("pdf", "blocks", "markdown"):
                stat = Path(current[side][key]).stat()
                if recorded[side][key] != {"size": stat.st_size, "mtime_ns": stat.st_mtime_ns}:
                    return True
    except (KeyError, OSError, TypeError):
        return True
    return False


def _evidence_id(session_id: str, pair_id: str, run_id: str, owner: str, index: int) -> str:
    raw = "\x1f".join((session_id, pair_id, run_id, owner, str(index)))
    return "v3ev_" + hashlib.sha256(raw.encode("utf-8")).hexdigest()[:40]


def _region(bbox: Any) -> dict[str, Any] | None:
    if not isinstance(bbox, list) or len(bbox) != 4:
        return None
    x0, y0, x1, y1 = (float(v) for v in bbox)
    if x1 <= x0 or y1 <= y0:
        return None
    return {"x": x0, "y": y0, "width": x1 - x0, "height": y1 - y0, "units": "normalized"}


def _relative_crop_ref(crop_ref: str, session_id: str, pair_id: str) -> str:
    if not crop_ref:
        return ""
    from backend.app.services.stage_comparison import paths

    work = (paths.production_dir(session_id, pair_id) / "project_change_v3").resolve()
    try:
        return str(Path(crop_ref).resolve().relative_to(work))
    except ValueError:
        return ""


def _evidence(
    items: list[dict[str, Any]], *, session_id: str, pair_id: str, run_id: str, owner: str,
    documents: dict[str, dict[str, Any]], object_id: str | None,
) -> list[dict[str, Any]]:
    out = []
    for index, e in enumerate(items or []):
        side = e.get("side")
        doc = documents.get(side) or {}
        evidence_id = _evidence_id(session_id, pair_id, run_id, owner, index)
        region = _region(e.get("bbox"))
        out.append({
            "id": evidence_id,
            "source_type": e.get("block_type"),
            "side": side,
            "pair_id": pair_id,
            "page": e.get("physical_page"),
            "region": region,
            "crop_precision": "EXACT_REGION" if region else "PAGE_LEVEL",
            "image_url": (
                f"/api/stage-comparison/objects/{object_id}/project-changes/evidence/{evidence_id}/crop"
                if object_id else ""
            ),
            "document": {
                "id": doc.get("document_code") or doc.get("filename") or "",
                "label": doc.get("document_code") or doc.get("filename") or "",
                "version": doc.get("version_id") or UNVERSIONED,
                "pdf_path": doc.get("pdf_path") or "",
            },
            "quote": str(e.get("relevant_fragment") or ""),
            "short_explanation_ru": ROLE_RU.get(str(e.get("evidence_role") or ""), str(e.get("evidence_role") or "")),
            "block_id": e.get("block_id"),
            "bbox": e.get("bbox"),
            "physical_page": e.get("physical_page"),
            "evidence_role": e.get("evidence_role"),
            "crop_ref": _relative_crop_ref(str(e.get("crop_ref") or ""), session_id, pair_id),
        })
    return out


def _details(parameters: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for p in parameters or []:
        unit = str(p.get("unit") or "")
        location = str(p.get("location") or "")
        with_unit = (lambda v: f"{v} {unit}".strip() if v and unit else str(v or ""))
        out.append({
            "label": str(p.get("name") or "") + (f" ({location})" if location else ""),
            "old": with_unit(p.get("old_value")),
            "new": with_unit(p.get("new_value")),
            "name": p.get("name"),
            "old_value": p.get("old_value"),
            "new_value": p.get("new_value"),
            "unit": p.get("unit"),
            "location": p.get("location"),
        })
    return out


def _pair_view(session_id: str, pair_id: str, documents: dict[str, dict[str, Any]]) -> dict[str, Any]:
    return {
        "id": pair_id,
        "session_id": session_id,
        "left": {k: documents["OLD"][k] for k in ("pdf_path", "version_id", "document_code", "filename")},
        "right": {k: documents["NEW"][k] for k in ("pdf_path", "version_id", "document_code", "filename")},
    }


def pair_presentation(session_id: str, pair_id: str, *, object_id: str | None) -> dict[str, Any] | None:
    """View items/hints/run meta of one published V3 pair, else None."""
    published = published_run(session_id, pair_id)
    if published is None:
        return None
    state, result = published
    documents = _documents(session_id, pair_id)
    run_id = str(result["run_id"])
    stale = _stale(session_id, pair_id, result)
    provenance = result.get("provenance") or {}
    regions = result.get("projectchange_regions") or {}
    right = documents["NEW"]
    items = []
    for change in result.get("projectchanges") or []:
        pc_id = str(change.get("projectchange_id"))
        explanation = "Изменение найдено движком V3 и ещё не проверено инженером. Сверьте OLD и NEW по доказательствам."
        if stale:
            explanation = "Исходные файлы пары изменились после анализа V3 — перед проверкой перезапустите анализ. " + explanation
        items.append({
            "id": f"v3:{pair_id}:{pc_id}",
            "projectchange_id": pc_id,
            "pair_id": pair_id,
            "session_id": session_id,
            "region_id": regions.get(pc_id, ""),
            "summary_ru": str(change.get("change_summary") or ""),
            "change_type": "OTHER",
            "status": "REVIEW",
            "importance": "NORMAL",
            "cipher": right["document_code"] or right["filename"],
            "discipline": right["discipline"],
            "engineering_system": "",
            "engineering_subject": str(change.get("engineering_subject") or ""),
            "scope": change.get("scope"),
            "locations": change.get("locations") or [],
            "old_state": str(change.get("old_state") or ""),
            "new_state": str(change.get("new_state") or ""),
            "details": _details(change.get("changed_parameters") or []),
            "changed_parameters": change.get("changed_parameters") or [],
            "old_pages": change.get("old_pages") or [],
            "new_pages": change.get("new_pages") or [],
            "modalities": change.get("modalities") or [],
            "confidence": change.get("confidence"),
            "why_one_event": change.get("why_one_event"),
            "dedupe_lineage": change.get("dedupe_lineage") or [pc_id],
            "dedupe_reason": change.get("dedupe_reason"),
            "evidence": _evidence(change.get("evidence_items") or [], session_id=session_id, pair_id=pair_id,
                                  run_id=run_id, owner=pc_id, documents=documents, object_id=object_id),
            "conflicts": [],
            "review_question": "Подтверждается ли это инженерное изменение по источникам OLD и NEW?",
            "review_explanation_ru": explanation,
            "technical_provenance": [
                f"projectchange_id: {pc_id}", f"region_id: {regions.get(pc_id, '')}",
                f"run_id: {run_id}", f"confidence: {change.get('confidence')}",
                f"dedupe_lineage: {', '.join(change.get('dedupe_lineage') or [pc_id])}",
                *provenance_lines(provenance),
            ],
            "source_run_id": run_id,
            "candidate_version": ENGINE_NAME,
            "research_status": "",
            "decision_key": "",
            "binding_signature": "",
            "decision_state": "NONE",
            "effective_decision": None,
            "stale": stale,
        })
    hints = []
    for hint in result.get("unresolved_hints") or []:
        hint_id = str(hint.get("hint_id"))
        hints.append({
            "id": f"v3hint:{pair_id}:{hint_id}",
            "hint_id": hint_id,
            "kind": hint.get("kind"),
            "pair_id": pair_id,
            "status": "CONFLICT" if hint.get("kind") == "SOURCE_CONFLICT" else "REVIEW",
            "engineering_subject": hint.get("engineering_subject"),
            "suspected_change": hint.get("suspected_change"),
            "missing_proof_or_conflict": hint.get("missing_proof_or_conflict"),
            "old_pages": hint.get("old_pages") or [],
            "new_pages": hint.get("new_pages") or [],
            "evidence": _evidence(hint.get("evidence_items") or [], session_id=session_id, pair_id=pair_id,
                                  run_id=run_id, owner=f"hint:{hint_id}", documents=documents, object_id=object_id),
        })
    run = {
        "session_id": session_id,
        "pair_id": pair_id,
        "run_id": run_id,
        "status": state.get("status"),
        "reason_code": state.get("reason_code"),
        "stale": stale,
        "projectchange_count": len(items),
        "unresolved_hint_count": len(hints),
        "human_mapping_published": bool(state.get("human_mapping_published")),
        "provenance": provenance,
        "source_manifest": result.get("source_manifest") or {},
        "model_calls": result.get("model_calls", 0),
    }
    return {"items": items, "unresolved_hints": hints, "run": run,
            "pair": _pair_view(session_id, pair_id, documents)}


def _pair_ids(session_id: str) -> list[str]:
    from backend.app.services.stage_comparison import store

    from backend.app.services.stage_comparison import paths

    session = store.get_session(session_id) or {}
    # Only pairs that ever persisted a V3 result can publish one.
    return [
        str(p["id"]) for p in session.get("pairs") or []
        if p.get("id") and paths.production_project_change_v3_result_path(session_id, str(p["id"])).is_file()
    ]


def object_v3_parts(object_id: str) -> dict[str, Any]:
    from .scope import sessions_for_object

    parts: dict[str, Any] = {"items": [], "unresolved_hints": [], "runs": [], "pairs": []}
    for session_id in sessions_for_object(object_id):
        for pair_id in _pair_ids(session_id):
            view = pair_presentation(session_id, pair_id, object_id=object_id)
            if view is None:
                continue
            parts["items"].extend(view["items"])
            parts["unresolved_hints"].extend(view["unresolved_hints"])
            parts["runs"].append(view["run"])
            parts["pairs"].append(view["pair"])
    return parts


def _revision(runs: list[dict[str, Any]]) -> str:
    raw = json.dumps(sorted((r["session_id"], r["pair_id"], r["run_id"]) for r in runs))
    return "v3:" + hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]


def object_envelope(object_id: str) -> dict[str, Any] | None:
    """``project-change-view/1`` for an object's published V3 runs, else None."""
    parts = object_v3_parts(object_id)
    if not parts["runs"]:
        return None
    return {
        "schema_version": "project-change-view/1",
        "object_id": object_id,
        "origin": "PRODUCTION",
        "mode": "PRODUCTION_V3",
        "availability": "AVAILABLE",
        "engine": ENGINE_NAME,
        "candidate_version": ENGINE_NAME,
        "decision_mode": "READ_ONLY",
        "decision_revision": 0,
        "revision": _revision(parts["runs"]),
        "capabilities": {"decisions": False, "history": False},
        # Binding registry only (mode is not BACKEND_PREVIEW, so the UI keeps
        # its own real session); pairs carry exact PDF paths and versions.
        "viewer_session": {"id": None, "pairs": parts["pairs"]},
        "items": parts["items"],
        "unresolved_hints": parts["unresolved_hints"],
        "runs": parts["runs"],
        "summary": {
            "total": len(parts["items"]),
            "review": len(parts["items"]),
            "unresolved_hints": len(parts["unresolved_hints"]),
            "pairs": len(parts["runs"]),
        },
    }


def merge_into_snapshot(envelope: dict[str, Any], object_id: str) -> dict[str, Any]:
    """Append published V3 runs of the snapshot object; unchanged when none."""
    parts = object_v3_parts(object_id)
    if not parts["runs"]:
        return envelope
    merged = dict(envelope)
    merged["items"] = [*envelope.get("items", []), *parts["items"]]
    merged["unresolved_hints"] = parts["unresolved_hints"]
    merged["runs"] = parts["runs"]
    viewer = dict(envelope.get("viewer_session") or {"id": None})
    viewer["pairs"] = [*(viewer.get("pairs") or []), *parts["pairs"]]
    merged["viewer_session"] = viewer
    merged["revision"] = f"{envelope.get('revision')}+{_revision(parts['runs'])}"
    return merged


def pair_changes(session_id: str, pair_id: str) -> dict[str, Any]:
    """Versioned V3 contract for ``/production/changes`` (no legacy rows)."""
    from .scope import ScopeUnresolved, object_id_for_session

    try:
        object_id = object_id_for_session(session_id)
    except (ScopeUnresolved, ValueError):
        object_id = None
    view = pair_presentation(session_id, pair_id, object_id=object_id)
    state = _load(session_id, pair_id, "state") or {}
    items = view["items"] if view else []
    return {
        "kind": CHANGES_KIND,
        "schema_version": CHANGES_SCHEMA_VERSION,
        "engine": ENGINE_NAME,
        "version": 1,
        "available": view is not None,
        "run_status": state.get("status"),
        "run_id": state.get("run_id"),
        # Nothing published is never "current"; a published run is current only
        # while its OLD/NEW sources are unchanged.
        "stale": view["run"]["stale"] if view else True,
        "object_id": object_id,
        # Legacy review rows are not produced by V3; the UI renders the
        # ProjectChangeView items below.
        "rows": [],
        "summary": {"total": len(items), "APPROVED": 0, "PENDING_REVIEW": len(items), "REJECTED": 0},
        "project_changes": items,
        "unresolved_hints": view["unresolved_hints"] if view else [],
        "run": view["run"] if view else None,
        "pair": view["pair"] if view else None,
    }


def evidence_crop(object_id: str, evidence_id: str) -> bytes:
    """PNG for one V3 evidence of an object (GRAPHIC crop or PDF region)."""
    if not EVIDENCE_ID_RE.fullmatch(evidence_id or ""):
        raise EvidenceUnavailable("evidence id")
    from backend.app.services.stage_comparison import paths

    from .scope import sessions_for_object

    for session_id in sessions_for_object(object_id):
        for pair_id in _pair_ids(session_id):
            view = pair_presentation(session_id, pair_id, object_id=object_id)
            if view is None:
                continue
            for owner in [*view["items"], *view["unresolved_hints"]]:
                for e in owner["evidence"]:
                    if e["id"] != evidence_id:
                        continue
                    if view["run"]["stale"]:
                        raise EvidenceUnavailable("source changed after the V3 run")
                    work = (paths.production_dir(session_id, pair_id) / "project_change_v3").resolve()
                    if e["source_type"] == "GRAPHIC" and e["crop_ref"]:
                        crop = (work / e["crop_ref"]).resolve()
                        if crop.is_relative_to(work) and crop.is_file():
                            return crop.read_bytes()
                    return _render_region(work, e)
    raise EvidenceUnavailable("evidence not found")


def _render_region(work: Path, evidence: dict[str, Any]) -> bytes:
    import fitz

    side = str(evidence["side"]).lower()
    page_no = int(evidence["page"])
    record_path = (work / "source" / side / f"p{page_no:03d}" / "page.json").resolve()
    if not record_path.is_relative_to(work) or not record_path.is_file():
        raise EvidenceUnavailable("page record missing")
    record = json.loads(record_path.read_text(encoding="utf-8"))
    bbox = evidence.get("bbox")
    with fitz.open(record["source_pdf"]) as doc:
        page = doc[page_no - 1]
        rect = page.rect
        clip = rect
        if isinstance(bbox, list) and len(bbox) == 4:
            clip = fitz.Rect(bbox[0] * rect.width, bbox[1] * rect.height,
                             bbox[2] * rect.width, bbox[3] * rect.height) & rect
            if clip.is_empty:
                clip = rect
        scale = min(1200 / max(clip.width, clip.height), 3.0)
        return page.get_pixmap(matrix=fitz.Matrix(scale, scale), clip=clip, alpha=False).tobytes("png")
