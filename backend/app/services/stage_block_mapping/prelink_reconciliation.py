"""Deterministic reconciliation of a run's frozen human prelinks with its AI regions (0 model calls).

A pure function of two immutable artifacts: the run's prelink snapshot
(``stage_comparison.prelink_run_snapshot``) and the run's Human Mapping region
membership (``human_mapping/ui_data.json``, built by ``hm_builder``).  It reads
set membership only — never block text, region prose or the person's note — and
infers no engineering meaning.  Precision over recall: when in doubt,
UNRESOLVED.  Nothing is stored and nothing is written; a person decides
explicitly afterwards through the existing Human Mapping API.

"Block is in a region" means Human Mapping membership: the same set HM allows as
link ends (``validation.allowed_block_ids``), so MATCHED always means the link
can be recorded in that region.

States (checked in order; RECONCILIATION_ALGORITHM.md §4):
* MATCHED — every block of the prelink is a member of one region with both sides mapped;
* PARTIAL_MATCH — some region holds members of both sides, none holds all
  (SPLIT_COVERED: such regions together cover the prelink; SUBSET: otherwise);
* CONFLICT — the AI placed EVERY block and the regions of the two sides never
  meet, not even through page context;
* UNRESOLVED — anything else (CONTEXT_ONLY / MEMBER_OF_INCOMPLETE_REGION / BLOCK_UNPLACED);
* NOT_EVALUATED — cannot be reconciled (no snapshot, stale at launch, run source mismatch, run not ready).
"""
from __future__ import annotations

import hashlib
import json
import uuid
from typing import Any

from backend.app.services.human_mapping_production import validation
from backend.app.services.stage_comparison import prelink_drafts, prelink_run_snapshot

from . import service

CONTRACT = "prelink-reconciliation/1"
ALGORITHM = "prelink-reconciliation-algorithm/1"
# Fixed once: deterministic ids of Human Mapping links created from a prelink (never reused for anything else).
PROMOTION_NS = uuid.UUID("5b0d6e4e-3c1a-4f55-9c1e-8a2f0e7d6c41")
COMPLETED = {"COMPLETED", "COMPLETED_FROZEN"}
STATES = ("MATCHED", "PARTIAL_MATCH", "CONFLICT", "UNRESOLVED", "NOT_EVALUATED")
SIDES = ("OLD", "NEW")


def promotion_link_id(run_id: str, region_id: str, prelink_id: str, old_block_id: str, new_block_id: str) -> str:
    return "human:" + str(uuid.uuid5(PROMOTION_NS, f"{run_id}|{region_id}|{prelink_id}|{old_block_id}|{new_block_id}"))


def prelink_edges(item: dict[str, Any]) -> list[tuple[str, str]]:
    """Edges the person drew: 1→1 one, 1→N / N→1 a star; N↔N none (a group, not pairs)."""
    olds = [b["block_id"] for b in item["old_blocks"]]
    news = [b["block_id"] for b in item["new_blocks"]]
    if len(olds) == 1:
        return [(olds[0], n) for n in news]
    if len(news) == 1:
        return [(o, news[0]) for o in olds]
    return []


# ── pure core ────────────────────────────────────────────────────────────────
def region_sets(region: dict[str, Any]) -> dict[str, Any]:
    members = {side: {b["id"] for b in region.get("old_blocks" if side == "OLD" else "new_blocks") or []}
               for side in SIDES}
    context = {side: {b["id"] for page in (region.get("pages") or {}).get(side) or [] for b in page.get("blocks") or []}
               for side in SIDES}
    state = region.get("membership_state") or {}
    eligible = all((state.get(side) or ("MAPPED" if members[side] else "EMPTY")) == "MAPPED" for side in SIDES)
    return {"id": region["id"], "members": members, "context": context, "eligible": eligible,
            "allowed": {side: validation.allowed_block_ids(region, side) for side in SIDES}}


def classify(old_ids: set[str], new_ids: set[str], regions: list[dict[str, Any]]) -> dict[str, Any]:
    """The four states of one prelink over precomputed ``region_sets`` (RECONCILIATION_ALGORITHM.md §4)."""
    blocks = {("OLD", b) for b in old_ids} | {("NEW", b) for b in new_ids}
    hits = []
    for r in regions:
        ho, hn = old_ids & r["members"]["OLD"], new_ids & r["members"]["NEW"]
        hits.append({"region": r, "old": ho, "new": hn})
    full = [h["region"]["id"] for h in hits if h["region"]["eligible"] and h["old"] == old_ids and h["new"] == new_ids]
    if full:
        return {"state": "MATCHED", "partial_kind": None, "reason": None, "targets": sorted(full), "outside": []}
    both = [h for h in hits if h["region"]["eligible"] and h["old"] and h["new"]]
    if both:
        covered = {("OLD", b) for h in both for b in h["old"]} | {("NEW", b) for h in both for b in h["new"]}
        return {"state": "PARTIAL_MATCH", "partial_kind": "SPLIT_COVERED" if covered == blocks else "SUBSET",
                "reason": None, "targets": sorted(h["region"]["id"] for h in both),
                "outside": sorted(f"{s}:{b}" for s, b in blocks - covered)}
    placed = {("OLD", b) for h in hits if h["region"]["eligible"] for b in h["old"]} | \
             {("NEW", b) for h in hits if h["region"]["eligible"] for b in h["new"]}
    near = [h["region"]["id"] for h in hits
            if (h["old"] and new_ids & h["region"]["context"]["NEW"]) or (h["new"] and old_ids & h["region"]["context"]["OLD"])]
    if placed == blocks and not near:
        return {"state": "CONFLICT", "partial_kind": None, "reason": None, "targets": [], "outside": []}
    incomplete = {("OLD", b) for h in hits if not h["region"]["eligible"] for b in h["old"]} | \
                 {("NEW", b) for h in hits if not h["region"]["eligible"] for b in h["new"]}
    if near:
        reason = "CONTEXT_ONLY"
    elif (blocks - placed) and (blocks - placed) <= incomplete:
        reason = "MEMBER_OF_INCOMPLETE_REGION"
    else:
        reason = "BLOCK_UNPLACED"
    return {"state": "UNRESOLVED", "partial_kind": None, "reason": reason, "targets": sorted(near),
            "outside": sorted(f"{s}:{b}" for s, b in blocks - placed)}


def promotion(run_id: str, item: dict[str, Any], regions: list[dict[str, Any]],
              effective: dict[str, list[dict[str, Any]]]) -> dict[str, Any]:
    """Edges a person may record, by the HM rule: both ends ⊆ allowed_block_ids of the region."""
    edges = []
    for old_id, new_id in prelink_edges(item):
        for r in regions:
            if old_id in r["allowed"]["OLD"] and new_id in r["allowed"]["NEW"]:
                expected = promotion_link_id(run_id, r["id"], item["prelink_id"], old_id, new_id)
                same = [l for l in effective.get(r["id"], []) if (l["old_block_id"], l["new_block_id"]) == (old_id, new_id)]
                if any(l["link_id"] == expected for l in same):
                    already = "PROMOTED"
                elif any(str(l["link_id"]).startswith("ai:") for l in same):
                    already = "AI_LINK"
                elif same:
                    already = "HUMAN_LINK"
                else:
                    already = "NONE"
                edges.append({"region_id": r["id"], "old_block_id": old_id, "new_block_id": new_id,
                              "link_id": expected, "already": already})
    per_edge: dict[tuple[str, str], set[str]] = {}
    for e in edges:
        per_edge.setdefault((e["old_block_id"], e["new_block_id"]), set()).add(e["region_id"])
    nn_links = []
    if not prelink_edges(item):
        olds = {b["block_id"] for b in item["old_blocks"]}
        news = {b["block_id"] for b in item["new_blocks"]}
        for r in regions:
            for link in effective.get(r["id"], []):
                if link["old_block_id"] in olds and link["new_block_id"] in news:
                    nn_links.append({"region_id": r["id"], **{k: link[k] for k in ("link_id", "old_block_id",
                                                                                   "new_block_id")}})
    blocked = None
    if not prelink_edges(item):
        blocked = "GROUP_HAS_NO_PAIRS"
    elif not edges:
        blocked = "NO_REGION_ALLOWS_THE_LINK"
    return {"available": bool(edges), "blocked_reason": blocked, "edges": edges,
            "needs_region_choice": any(len(v) > 1 for v in per_edge.values()), "group_links_in_regions": nn_links}


# ── binding to the run (B1–B5) and the GET payload ───────────────────────────
def _map_sha(semantic_map: dict[str, Any]) -> str:
    return hashlib.sha256(json.dumps(semantic_map, ensure_ascii=False, sort_keys=True).encode("utf-8")).hexdigest()


def _run_rows(session_id: str, pair_id: str, run_id: str) -> dict[tuple[str, int, str], str] | None:
    out = {}
    for side in SIDES:
        rows = service._run_side_rows(session_id, pair_id, run_id, side)
        if rows is None:
            return None
        for row in rows:
            out[(side, int(row[0]), str(row[1]))] = hashlib.sha256(prelink_drafts.canonical(row)).hexdigest()
    return out


def _item_row(item: dict[str, Any]) -> dict[str, Any]:
    # Excluded (stale-at-launch) entries carry the composition as drawn (older snapshots: none).
    return {"prelink_id": item["prelink_id"], "label": f"PL-{item['label_no']}", "label_no": item["label_no"],
            "cardinality": item.get("cardinality"), "note": item.get("note", ""),
            "old_blocks": item.get("old_blocks", []), "new_blocks": item.get("new_blocks", [])}


def _not_evaluated(item: dict[str, Any], reason: str) -> dict[str, Any]:
    return {**_item_row(item), "state": "NOT_EVALUATED", "partial_kind": None, "reason": reason, "targets": [],
            "outside": [], "blocks": [], "needs_region_choice": False,
            "promotion": {"available": False, "blocked_reason": reason, "edges": [], "needs_region_choice": False,
                          "group_links_in_regions": []}}


def _drafts_changed_since(session_id: str, pair_id: str, snapshot: dict[str, Any]) -> dict[str, int] | None:
    try:
        _raw, drafts = prelink_drafts.read(session_id, pair_id)
    except prelink_drafts.PrelinkDraftsInvalid:
        return None
    then = {i["prelink_id"]: i for i in snapshot["drafts"]["items"]}
    then.update({e["prelink_id"]: e for e in snapshot["drafts"]["excluded"]})
    now = {i["prelink_id"]: i for i in (drafts or {}).get("prelinks") or []}
    changed = sum(1 for pid in set(then) & set(now) if "updated_at" in then[pid]
                  and then[pid]["updated_at"] != now[pid]["updated_at"])
    return {"added": len(set(now) - set(then)), "removed": len(set(then) - set(now)), "changed": changed}


def reconcile(session_id: str, pair_id: str, run_id: str) -> dict[str, Any]:
    """GET …/block-mapping/runs/{run_id}/prelink-reconciliation — writes nothing, calls no model."""
    service._pair(session_id, pair_id)
    run_id = service._safe(run_id, "run")
    manifest = service._require_run(session_id, pair_id, run_id)
    loaded = prelink_run_snapshot.read(session_id, pair_id, run_id)
    base = {"schema": CONTRACT, "algorithm_version": ALGORITHM, "session_id": session_id, "pair_id": pair_id,
            "run_id": run_id, "snapshot_sha256": None, "ui_data_sha256": None}
    counts = {state: 0 for state in STATES}
    if loaded is None:
        return {**base, "binding": {"status": "NO_SNAPSHOT", "reason": None}, "snapshot": None,
                "drafts_changed_since": None, "items": [], "counts": counts}
    raw, snapshot = loaded
    base["snapshot_sha256"] = hashlib.sha256(raw).hexdigest()
    summary = {"drafts_revision": snapshot["drafts"]["revision"], "drafts_state": snapshot["drafts"]["state"],
               "created_at": snapshot["created_at"], "sheet_map_state": snapshot["sheet_map"]["state"],
               "mapper_prelinks_used": snapshot["mapper_prelinks_used"],
               "mapper_sheet_map_used": snapshot["mapper_sheet_map_used"]}
    items = snapshot["drafts"]["items"]

    def finish(binding: dict[str, Any], rows: list[dict[str, Any]]) -> dict[str, Any]:
        rows += [{**_not_evaluated(e, "EXCLUDED_STALE"), "validity_at_launch": e["validity_at_launch"],
                  "validity_details": e["details"]} for e in snapshot["drafts"]["excluded"]]
        rows.sort(key=lambda r: r["label_no"])
        for row in rows:
            counts[row["state"]] += 1
        return {**base, "binding": binding, "snapshot": summary,
                "drafts_changed_since": _drafts_changed_since(session_id, pair_id, snapshot),
                "items": rows, "counts": counts}

    object_id = service._object_id(session_id)
    ui, hm_reason, digest = service._ui_data(session_id, pair_id, run_id, object_id)
    base["ui_data_sha256"] = digest
    if manifest.get("state") not in COMPLETED:
        return finish({"status": "RUN_NOT_READY", "reason": manifest.get("state")},
                      [_not_evaluated(i, "RUN_NOT_READY") for i in items])
    if ui is None:
        return finish({"status": "HM_UNAVAILABLE", "reason": hm_reason},
                      [_not_evaluated(i, "HM_UNAVAILABLE") for i in items])
    semantic_map = service.run_storage.read(service._run_dir(session_id, pair_id, run_id)
                                            / "project_change_v3_semantic_map.json")
    if not isinstance(semantic_map, dict) or ui.get("source_sha256") != _map_sha(semantic_map):
        return finish({"status": "HM_UNAVAILABLE", "reason": "HM_MAP_MISMATCH"},
                      [_not_evaluated(i, "HM_UNAVAILABLE") for i in items])
    identity = snapshot["source_identity"]
    if (manifest.get("old_pdf_sha256"), manifest.get("new_pdf_sha256")) != \
            (identity["OLD"]["pdf_sha256"], identity["NEW"]["pdf_sha256"]):
        return finish({"status": "SOURCE_MISMATCH", "reason": "PDF_DIFFERS_FROM_RUN"},
                      [_not_evaluated(i, "SOURCE_MISMATCH") for i in items])
    run_rows = _run_rows(session_id, pair_id, run_id)
    if run_rows is None:
        return finish({"status": "SOURCE_MISMATCH", "reason": "RUN_SOURCE_UNREADABLE"},
                      [_not_evaluated(i, "SOURCE_MISMATCH") for i in items])
    regions = [region_sets(r) for r in ui["regions"]]
    edits, _ok = service._read_jsonl(service._run_dir(session_id, pair_id, run_id) / "human_mapping"
                                     / "human_block_link_edits.jsonl")
    effective = {r["id"]: validation.effective_links(r, edits) for r in ui["regions"]}
    rows = []
    for item in items:
        bound = all(run_rows.get((side, e["physical_page"], e["block_id"])) == e["content_sha256"]
                    for side, key in (("OLD", "old_blocks"), ("NEW", "new_blocks")) for e in item[key])
        if not bound:
            rows.append(_not_evaluated(item, "SOURCE_MISMATCH"))
            continue
        old_ids = {b["block_id"] for b in item["old_blocks"]}
        new_ids = {b["block_id"] for b in item["new_blocks"]}
        verdict = classify(old_ids, new_ids, regions)
        placement = [{"side": side, "block_id": b["block_id"], "physical_page": b["physical_page"],
                      "member_of": sorted(r["id"] for r in regions if b["block_id"] in r["members"][side]),
                      "context_of": sorted(r["id"] for r in regions if b["block_id"] in r["context"][side])}
                     for side, key in (("OLD", "old_blocks"), ("NEW", "new_blocks")) for b in item[key]]
        promo = promotion(run_id, item, regions, effective)
        rows.append({**_item_row(item), **verdict, "blocks": placement,
                     "needs_region_choice": promo["needs_region_choice"], "promotion": promo})
    return finish({"status": "BOUND", "reason": None}, rows)
