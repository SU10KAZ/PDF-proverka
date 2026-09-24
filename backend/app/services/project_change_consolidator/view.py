"""Read-only «Исходные | Итоговые» view of COMPLETED shadow consolidations of a pair.

Nothing here generates or changes a result.  A consolidation is shown only
when its shadow run is COMPLETED and frozen, every artifact hash verifies, and
it is bound to the exact source result by ``source_run_id`` + sha256:

* ``PRODUCTION_RUN`` — the source is a validated run of this pair
  (``runs/<source_run_id>``), shown only while its source files are unchanged;
* ``IMPORTED_FROZEN_RUN`` — the source is a frozen run imported next to the
  shadow runs (``<shadow root>/<source_run_id>/SOURCE_IMPORT``) with a receipt
  of byte-identical copies, shown only while the pair's current PDFs have the
  source's PDF sha256.

Evidence ids follow the V3 scheme (session, pair, source run, owner, index;
hint owners ``hint:<region_id>/<hint_id>`` — R-04), so a consolidated card
opens exactly the member's page, bbox, side and block.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .contracts import EVIDENCE_REF_RE
from .identity import canonical_hints
from .storage import SHADOW_DIR_NAME, ShadowStore, ShadowStoreError

IMPORT_DIR = "SOURCE_IMPORT"
IMPORT_RECEIPT = "SOURCE_IMPORT_RECEIPT.json"
IMPORT_SCHEMA = "projectchange-consolidator-source-import/1"
PRODUCTION_RUN, IMPORTED_FROZEN_RUN = "PRODUCTION_RUN", "IMPORTED_FROZEN_RUN"


class ViewUnavailable(LookupError):
    pass


def _sha(path: Path) -> str:
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def shadow_root(session_id: str, pair_id: str) -> Path:
    from backend.app.services.stage_comparison import paths

    return paths.production_dir(session_id, pair_id) / SHADOW_DIR_NAME


@dataclass
class Source:
    kind: str
    source_run_id: str
    result: dict[str, Any]
    result_sha256: str
    hint_keys: list[str]                   # per result hint: "<region>/<hint>" (exact) or "#<n>/<hint>"
    package_dir: Path
    page_sha256: dict[str, str] | None
    label: str
    engine: dict[str, Any]


def _pdf_binding_ok(session_id: str, pair_id: str, result: dict[str, Any]) -> bool:
    from backend.app.services.project_change_v3 import presentation

    manifest = result.get("source_manifest") or {}
    try:
        resolved = presentation._resolved_paths(session_id, pair_id)
        current = {side: _sha(Path(resolved[side]["pdf"])) for side in ("OLD", "NEW")}
    except (KeyError, OSError, TypeError, ValueError):
        return False
    return (current["OLD"], current["NEW"]) == (manifest.get("old_pdf_sha256"), manifest.get("new_pdf_sha256"))


def load_source(session_id: str, pair_id: str, source_run_id: str) -> Source:
    from backend.app.services.project_change_v3 import presentation, run_storage
    from backend.app.services.project_change_v3.hint_identity import hint_identities

    run_dir = run_storage.run_dir(session_id, pair_id, source_run_id)
    if (run_dir / "run_manifest.json").is_file():
        try:
            manifest = run_storage.validate(session_id, pair_id, source_run_id)
        except (ValueError, OSError, KeyError) as exc:
            raise ViewUnavailable(f"source run is not a valid completed run: {exc}") from exc
        result = json.loads((run_dir / "project_change_v3_result.json").read_bytes())
        if presentation._stale(session_id, pair_id, result) or not _pdf_binding_ok(session_id, pair_id, result):
            raise ViewUnavailable("source files changed after the source run")
        miner_path = run_dir / "project_change_v3_miner_results.json"
        miner = json.loads(miner_path.read_bytes()) if miner_path.is_file() else None
        prov = result.get("provenance") or {}
        return Source(PRODUCTION_RUN, source_run_id, result,
                      manifest["artifacts"]["project_change_v3_result"]["sha256"],
                      [i["key"] for i in hint_identities(result, miner)], run_dir / "project_change_v3" / "source",
                      None, f"Прогон V3 {source_run_id[:8]}",
                      {"engine_version": prov.get("engine_version"), "model": prov.get("model"),
                       "reasoning": prov.get("reasoning")})
    base = shadow_root(session_id, pair_id) / source_run_id / IMPORT_DIR
    try:
        receipt = json.loads((base / IMPORT_RECEIPT).read_bytes())
    except (OSError, ValueError) as exc:
        raise ViewUnavailable("no completed source for this consolidation") from exc
    if receipt.get("schema") != IMPORT_SCHEMA or receipt.get("source_run_id") != source_run_id or receipt.get(
            "pair_id") != pair_id or receipt.get("session_id") != session_id or receipt.get("content_mutated"):
        raise ViewUnavailable("source import receipt does not bind this pair")
    for row in receipt.get("files") or []:
        path = (base / row["rel"]).resolve()
        if not path.is_relative_to(base.resolve()) or not path.is_file() or _sha(path) != row["sha256"]:
            raise ViewUnavailable(f"imported source file changed: {row['rel']}")
    result_path = base / "SOURCE_RESULT.json"
    result = json.loads(result_path.read_bytes())
    if _sha(result_path) != receipt["source_result_sha256"] or str(result.get("run_id")) != source_run_id:
        raise ViewUnavailable("imported source result does not match its receipt")
    if not _pdf_binding_ok(session_id, pair_id, result):
        raise ViewUnavailable("the pair's PDFs differ from the imported source")
    from .source_view import _region_sources

    regions = json.loads((base / "SOURCE_HINT_REGIONS.json").read_bytes())
    table = canonical_hints(result.get("unresolved_hints") or [],
                            _region_sources(receipt["hint_source_kind"], regions), method=receipt["hint_method"])
    pages = {row["rel"][len("source/"):]: row["sha256"] for row in receipt["files"] if row["rel"].startswith("source/")}
    return Source(IMPORTED_FROZEN_RUN, source_run_id, result, receipt["source_result_sha256"],
                  [e.ref for e in table.entries], base / "source", pages, str(receipt.get("label") or ""),
                  dict(receipt.get("engine") or {}))


def _completed(session_id: str, pair_id: str, source: Source) -> list[dict[str, Any]]:
    store = ShadowStore(shadow_root(session_id, pair_id))
    out = []
    for manifest in store.manifests(source.source_run_id):
        if manifest.get("state") != "COMPLETED" or manifest.get("source_result_sha256") != source.result_sha256:
            continue
        try:
            out.append(store.load_completed(source.source_run_id, manifest["consolidator_run_id"],
                                            source_result_sha256=source.result_sha256))
        except ShadowStoreError:
            continue
    return sorted(out, key=lambda x: str(x["manifest"].get("completed_at") or ""))


def pair_consolidations(session_id: str, pair_id: str) -> list[dict[str, Any]]:
    """Every COMPLETED shadow consolidation of this pair that binds to its source (0 model calls)."""
    root = shadow_root(session_id, pair_id)
    if not root.is_dir():
        return []
    from backend.app.services.project_change_v3 import run_storage

    current = run_storage.current(session_id, pair_id)
    out = []
    for d in sorted(p for p in root.iterdir() if p.is_dir()):
        try:
            source = load_source(session_id, pair_id, d.name)
        except (ViewUnavailable, ValueError, OSError, KeyError):
            continue
        for run in _completed(session_id, pair_id, source):
            m = run["manifest"]
            out.append({"source_run_id": source.source_run_id, "consolidator_run_id": m["consolidator_run_id"],
                        "source_kind": source.kind, "source_label": source.label, "source_engine": source.engine,
                        "source_is_current_run": source.source_run_id == current,
                        "source_result_sha256": source.result_sha256, "shadow_result_sha256": m["shadow_result_sha256"],
                        "model": m.get("model"), "reasoning": m.get("reasoning"), "completed_at": m.get("completed_at"),
                        "stats": m.get("stats") or {}})
    return out


class _Evidence:
    """UI evidence objects of one source run, keyed by owner/index (ids = V3 scheme)."""

    def __init__(self, session_id: str, pair_id: str, source: Source, object_id: str | None):
        from backend.app.services.project_change_v3 import presentation

        self.session_id, self.pair_id, self.source = session_id, pair_id, source
        self.documents = presentation._documents(session_id, pair_id)
        self.object_id = object_id
        self._presentation = presentation

    def build(self, items: list[dict[str, Any]], owner: str) -> list[dict[str, Any]]:
        out = self._presentation._evidence(items, session_id=self.session_id, pair_id=self.pair_id,
                                           run_id=self.source.source_run_id, owner=owner, documents=self.documents,
                                           object_id=self.object_id)
        for e in out:
            e["image_url"] = (f"/api/stage-comparison/sessions/{self.session_id}/pairs/{self.pair_id}/consolidated/"
                              f"{self.source.source_run_id}/evidence/{e['id']}/crop")
            e["crop_ref"] = ""
        return out


def _card_view(card: dict[str, Any], region_id: str, ev: _Evidence) -> dict[str, Any]:
    pc_id = str(card.get("projectchange_id"))
    return {
        "id": pc_id, "region_id": region_id, "title": card.get("engineering_subject") or "",
        "summary": card.get("change_summary") or "", "old_state": card.get("old_state") or "",
        "new_state": card.get("new_state") or "", "why_one_event": card.get("why_one_event") or "",
        "region_scope_claim": card.get("scope") or "", "locations": list(card.get("locations") or []),
        "parameters": [{"name": p.get("name") or "", "old": p.get("old_value") or "", "new": p.get("new_value") or "",
                        "unit": p.get("unit") or "", "location": p.get("location") or ""}
                       for p in card.get("changed_parameters") or []],
        "old_pages": list(card.get("old_pages") or []), "new_pages": list(card.get("new_pages") or []),
        "evidence": ev.build(card.get("evidence_items") or [], pc_id),
    }


def _hint_view(hint: dict[str, Any], key: str, ev: _Evidence) -> dict[str, Any]:
    return {"hint_ref": key if not key.startswith("#") else "", "hint_key": key, "hint_id": hint.get("hint_id"),
            "region_id": key.split("/", 1)[0] if not key.startswith("#") else "", "kind": hint.get("kind"),
            "subject": hint.get("engineering_subject") or "", "text": hint.get("missing_proof_or_conflict") or "",
            "suspected": hint.get("suspected_change") or "",
            "evidence": ev.build(hint.get("evidence_items") or [], f"hint:{key}")}


def consolidated_view(session_id: str, pair_id: str, source_run_id: str, consolidator_run_id: str, *,
                      object_id: str | None = None) -> dict[str, Any]:
    source = load_source(session_id, pair_id, source_run_id)
    try:
        run = ShadowStore(shadow_root(session_id, pair_id)).load_completed(
            source_run_id, consolidator_run_id, source_result_sha256=source.result_sha256)
    except ShadowStoreError as exc:
        raise ViewUnavailable(str(exc)) from exc
    shadow, manifest = run["result"], run["manifest"]
    ev = _Evidence(session_id, pair_id, source, object_id)
    regions = source.result.get("projectchange_regions") or {}
    originals = {str(c.get("projectchange_id")): _card_view(c, str(regions.get(str(c.get("projectchange_id")), "")), ev)
                 for c in source.result.get("projectchanges") or []}
    hints = {key: _hint_view(h, key, ev) for h, key in zip(source.result.get("unresolved_hints") or [],
                                                          source.hint_keys)}

    def evidence_of(ref: str) -> dict[str, Any]:
        m = EVIDENCE_REF_RE.fullmatch(ref)
        if m:
            return originals[m.group(1)]["evidence"][int(m.group(2)) - 1]
        key, _, n = ref.rpartition("#h")
        return hints[key]["evidence"][int(n) - 1]

    def conflict(c: dict[str, Any]) -> dict[str, Any]:
        hint = c.get("hint")
        return {"source": c.get("source"), "statement": c.get("statement") or c.get("text") or "",
                "disposition_reason": c.get("disposition_reason") or "",
                "hint": hints.get(hint["hint_ref"]) if hint else None}

    items = []
    for channel_list in ("engineering_changes", "review_items", "documentary_changes"):
        for x in shadow[channel_list]:
            if x["origin"] == "CONSOLIDATED":
                lineage = x["lineage"]
                items.append({
                    "kind": "CONSOLIDATED", "id": f"pcc:{pair_id}:{source_run_id}:{consolidator_run_id}:{x['consolidated_id']}",
                    "consolidated_id": x["consolidated_id"], "channel": x["channel"], "flags": x["flags"],
                    "title": x["engineering_subject"], "summary": x["change_summary"], "old_state": x["old_state"],
                    "new_state": x["new_state"], "why_one_event": x["why_one_event"],
                    "system_designations": x["system_designations"],
                    "scope": {"scope_type": x["scope"]["scope_type"], "scope_basis": x["scope"]["scope_basis"],
                              "locations": [loc["text"] for loc in x["scope"]["affected_locations"]]},
                    "parameters": [{"key": p["param_key"], "name": p["name"], "status": p["status"],
                                    "old": p["old_value"], "new": p["new_value"], "unit": p["unit"],
                                    "locations": [loc["text"] for loc in p["applies_to_locations"]],
                                    "values": [{k: s.get(k) for k in ("card_id", "old_value", "new_value", "unit",
                                                                       "location", "duplicate")}
                                               for s in p["source_values"]]}
                                   for p in x["changed_parameters"]],
                    "manifestations": [{"locations": [loc["text"] for loc in m["locations"]], "members": m["member_ids"],
                                        "note": m["local_note"]} for m in x["manifestations"]],
                    "evidence": [evidence_of(e["ref"]) for e in x["evidence_items"]],
                    "conflicts": [conflict(c) for c in x["open_conflicts"]], "claim_status": x["claim_status"],
                    "lineage": {"members": [{"id": mid, "region_id": lineage["member_regions"].get(mid, ""),
                                             "title": originals[mid]["title"], "summary": originals[mid]["summary"]}
                                            for mid in lineage["member_ids"]],
                                "relations": lineage["relations"], "merge_basis": lineage["merge_basis"],
                                "distinguishing_check": lineage["distinguishing_check"],
                                "cluster_id": lineage["cluster_id"], "group_id": lineage["group_id"]},
                    "related_engineering_card_ref": x.get("related_engineering_card_ref") or "",
                })
            else:
                card = originals[x["projectchange_id"]]
                items.append({
                    **card, "kind": "SOURCE_CARD", "id": f"pcs:{pair_id}:{source_run_id}:{x['projectchange_id']}",
                    "projectchange_id": x["projectchange_id"], "channel": x["channel"], "flags": x["flags"],
                    "decision": x["decision"], "scope": {"scope_type": "NOT_ASSESSED", "scope_basis": "",
                                                         "locations": card["locations"]},
                    "conflicts": [conflict({**a, "source": "HINT", "statement": a.get("text")})
                                  for a in x["conflict_annotations"]],
                    "possible_same_event": x["possible_same_event"],
                    "related_engineering_card_ref": x.get("related_engineering_card_ref") or "",
                    "fallback": x.get("fallback"),
                })
    return {
        "schema": "projectchange-consolidated-view/1", "available": True,
        "session_id": session_id, "pair_id": pair_id, "source_run_id": source_run_id,
        "consolidator_run_id": consolidator_run_id, "source_kind": source.kind, "source_label": source.label,
        "source_engine": source.engine, "source_result_sha256": source.result_sha256,
        "shadow_result_sha256": manifest["shadow_result_sha256"], "model": manifest.get("model"),
        "reasoning": manifest.get("reasoning"), "completed_at": manifest.get("completed_at"),
        "stats": shadow["stats"],
        "original": list(originals.values()),
        "consolidated": items,
        "unresolved_hints": list(hints.values()),
        "relations": shadow["relations"],
    }


def evidence_crop(session_id: str, pair_id: str, source_run_id: str, evidence_id: str) -> bytes:
    """PNG of one evidence of a bound source (graphic crop verified by sha, else the PDF region)."""
    from backend.app.services.project_change_v3 import presentation

    if not presentation.EVIDENCE_ID_RE.fullmatch(evidence_id or ""):
        raise ViewUnavailable("evidence id")
    source = load_source(session_id, pair_id, source_run_id)
    ev = _Evidence(session_id, pair_id, source, None)
    raw: list[tuple[dict[str, Any], dict[str, Any]]] = []
    for c in source.result.get("projectchanges") or []:
        built = ev.build(c.get("evidence_items") or [], str(c.get("projectchange_id")))
        raw += list(zip(built, c.get("evidence_items") or []))
    for h, key in zip(source.result.get("unresolved_hints") or [], source.hint_keys):
        raw += list(zip(ev.build(h.get("evidence_items") or [], f"hint:{key}"), h.get("evidence_items") or []))
    matches = [(ui, item) for ui, item in raw if ui["id"] == evidence_id]
    if len(matches) != 1:
        raise ViewUnavailable("evidence not found" if not matches else "ambiguous evidence id")
    ui, item = matches[0]
    side, page_no = str(item["side"]).lower(), int(item["physical_page"])
    rel = f"{side}/p{page_no:03d}/page.json"
    record_path = (source.package_dir / rel).resolve()
    if not record_path.is_relative_to(source.package_dir.resolve()) or not record_path.is_file():
        raise ViewUnavailable("page record missing")
    data = record_path.read_bytes()
    if source.page_sha256 is not None and source.page_sha256.get(rel) != hashlib.sha256(data).hexdigest():
        raise ViewUnavailable("page record changed")
    record = json.loads(data)
    blocks = [b for b in record.get("blocks") or [] if b.get("block_id") == item["block_id"]]
    if len(blocks) != 1:
        raise ViewUnavailable("block not found in the source package")
    block = blocks[0]
    if item.get("block_type") == "GRAPHIC" and block.get("graphic_crop_sha256"):
        crop = (source.package_dir / side / f"p{page_no:03d}" / f"{item['block_id']}.png").resolve()
        if crop.is_relative_to(source.package_dir.resolve()) and crop.is_file() and _sha(crop) == block[
                "graphic_crop_sha256"]:
            return crop.read_bytes()
    resolved = presentation._resolved_paths(session_id, pair_id)
    pdf = Path(resolved["OLD" if side == "old" else "NEW"]["pdf"])
    return _render(pdf, page_no, item.get("bbox"))


def _render(pdf: Path, page_no: int, bbox: Any) -> bytes:
    import fitz

    with fitz.open(str(pdf)) as doc:
        page = doc[page_no - 1]
        rect = page.rect
        clip = rect
        if isinstance(bbox, list) and len(bbox) == 4:
            clip = fitz.Rect(bbox[0] * rect.width, bbox[1] * rect.height, bbox[2] * rect.width,
                             bbox[3] * rect.height) & rect
            if clip.is_empty:
                clip = rect
        scale = min(1200 / max(clip.width, clip.height), 3.0)
        return page.get_pixmap(matrix=fitz.Matrix(scale, scale), clip=clip, alpha=False).tobytes("png")
