"""Deterministic reconciliation of frozen human prelinks with AI regions — states, binding, promotion; 0 models."""
from __future__ import annotations

import hashlib
import json
import time
from pathlib import Path

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.app.api.routers import stage_block_mapping
from backend.app.services.stage_block_mapping import prelink_reconciliation as rec
from backend.app.services.stage_comparison import prelink_drafts, prelink_run_snapshot

from .conftest import PID, RUN, SID, build_run, hm_event, region

BASE = f"/api/stage-comparison/sessions/{SID}/pairs/{PID}/block-mapping"


# ── pure states (RECONCILIATION_ALGORITHM.md §4) ─────────────────────────────
def R(rid, olds, news, ctx_old=(), ctx_new=()):
    shape = {"id": rid, "old_blocks": [{"id": b} for b in olds], "new_blocks": [{"id": b} for b in news],
             "pages": {"OLD": [{"blocks": [{"id": b} for b in [*olds, *ctx_old]]}],
                       "NEW": [{"blocks": [{"id": b} for b in [*news, *ctx_new]]}]},
             "membership_state": {"OLD": "MAPPED" if olds else "EMPTY", "NEW": "MAPPED" if news else "EMPTY"}}
    return rec.region_sets(shape)


def state(old, new, regions):
    return rec.classify(set(old), set(new), regions)


def item(old, new, pid="pl_" + "a" * 32):
    return {"prelink_id": pid, "old_blocks": [{"block_id": b} for b in old], "new_blocks": [{"block_id": b} for b in new]}


def test_one_to_one_matched():
    out = state(["o1"], ["n1"], [R("R1", ["o1", "o2"], ["n1"]), R("R2", ["o3"], ["n2"])])
    assert (out["state"], out["targets"]) == ("MATCHED", ["R1"])


def test_wrong_link_is_conflict_only_when_every_block_is_placed_and_regions_never_meet():
    regions = [R("R1", ["o1"], ["n9"]), R("R2", ["o9"], ["n2"])]
    assert state(["o1"], ["n2"], regions)["state"] == "CONFLICT"
    # A block the AI did not place is never a conflict.
    unplaced = state(["o1"], ["n7"], regions)
    assert (unplaced["state"], unplaced["reason"], unplaced["outside"]) == ("UNRESOLVED", "BLOCK_UNPLACED", ["NEW:n7"])
    # The other side on the pages of the region (context, not membership) is not a conflict either.
    near = state(["o1"], ["n2"], [R("R1", ["o1"], ["n9"], ctx_new=["n2"]), R("R2", ["o9"], ["n2"])])
    assert (near["state"], near["reason"], near["targets"]) == ("UNRESOLVED", "CONTEXT_ONLY", ["R1"])


def test_one_to_many_and_many_to_one():
    assert state(["o1"], ["n1", "n2"], [R("R1", ["o1"], ["n1", "n2"])])["state"] == "MATCHED"
    subset = state(["o1"], ["n1", "n2"], [R("R1", ["o1"], ["n1"]), R("R2", ["o5"], ["n2"])])
    assert (subset["state"], subset["partial_kind"], subset["outside"]) == ("PARTIAL_MATCH", "SUBSET", ["NEW:n2"])
    mirror = state(["o1", "o2"], ["n1"], [R("R1", ["o1"], ["n1"]), R("R2", ["o2"], ["n5"])])
    assert (mirror["state"], mirror["partial_kind"], mirror["outside"]) == ("PARTIAL_MATCH", "SUBSET", ["OLD:o2"])


def test_split_covered():
    out = state(["o1"], ["n1", "n2"], [R("R1", ["o1"], ["n1"]), R("R2", ["o1"], ["n2"])])
    assert (out["state"], out["partial_kind"], out["targets"], out["outside"]) == \
        ("PARTIAL_MATCH", "SPLIT_COVERED", ["R1", "R2"], [])


def test_multiple_candidate_regions_are_all_returned_and_need_a_choice():
    regions = [R("R1", ["o1"], ["n1"]), R("R2", ["o1", "o3"], ["n1"])]
    out = state(["o1"], ["n1"], regions)
    assert (out["state"], out["targets"]) == ("MATCHED", ["R1", "R2"])
    promo = rec.promotion("run", item(["o1"], ["n1"]), regions, {})
    assert promo["needs_region_choice"] is True and {e["region_id"] for e in promo["edges"]} == {"R1", "R2"}


def test_many_to_many_is_a_group_without_pairs():
    regions = [R("R1", ["o1", "o2"], ["n1", "n2"])]
    assert state(["o1", "o2"], ["n1", "n2"], regions)["state"] == "MATCHED"
    effective = {"R1": [{"link_id": "human:x", "old_block_id": "o1", "new_block_id": "n2"},
                        {"link_id": "human:y", "old_block_id": "o9", "new_block_id": "n1"}]}
    promo = rec.promotion("run", item(["o1", "o2"], ["n1", "n2"]), regions, effective)
    assert promo["edges"] == [] and promo["blocked_reason"] == "GROUP_HAS_NO_PAIRS"
    assert [(l["old_block_id"], l["new_block_id"]) for l in promo["group_links_in_regions"]] == [("o1", "n2")]


def test_region_with_an_empty_side_is_never_a_positive_match_but_hm_may_allow_the_link():
    empty_side = R("R3", ["o1"], [], ctx_new=["n1"])
    out = state(["o1"], ["n1"], [empty_side])
    assert (out["state"], out["reason"]) == ("UNRESOLVED", "CONTEXT_ONLY")
    promo = rec.promotion("run", item(["o1"], ["n1"]), [empty_side], {})
    assert [e["region_id"] for e in promo["edges"]] == ["R3"]  # HM offers page context of an EMPTY side
    incomplete = state(["o1"], ["n1"], [R("R4", ["o1"], []), R("R5", ["o7"], ["n1"])])
    assert (incomplete["state"], incomplete["reason"]) == ("UNRESOLVED", "MEMBER_OF_INCOMPLETE_REGION")


def test_promotion_ids_are_deterministic_and_star_shaped():
    assert rec.prelink_edges(item(["o1"], ["n1", "n2"])) == [("o1", "n1"), ("o1", "n2")]
    assert rec.prelink_edges(item(["o1", "o2"], ["n1"])) == [("o1", "n1"), ("o2", "n1")]
    one = rec.promotion_link_id("run", "R1", "pl_1", "o1", "n1")
    assert one == rec.promotion_link_id("run", "R1", "pl_1", "o1", "n1") and one.startswith("human:")
    assert len({one, rec.promotion_link_id("run2", "R1", "pl_1", "o1", "n1"),
                rec.promotion_link_id("run", "R2", "pl_1", "o1", "n1"),
                rec.promotion_link_id("run", "R1", "pl_2", "o1", "n1"),
                rec.promotion_link_id("run", "R1", "pl_1", "o1", "n2")}) == 5


# ── through the API on the synthetic pair (binding B1–B5) ────────────────────
@pytest.fixture
def client(world, monkeypatch):
    monkeypatch.setenv("STAGE_PRELINK_DRAFTS", "1")
    app = FastAPI()
    app.include_router(stage_block_mapping.router)
    return TestClient(app)


def fresh():
    from backend.app.services.stage_block_mapping import service
    service.clear_cache()


def add_drafts(groups):
    view = prelink_drafts.view(SID, PID)
    for olds, news in groups:
        view = prelink_drafts.create(SID, PID, expected_revision=view["revision"], old_block_ids=olds,
                                     new_block_ids=news)
    return view


def freeze(run_id=RUN, mutate=None):
    snapshot = prelink_run_snapshot.build(SID, PID, run_id)
    if mutate:
        mutate(snapshot)
    prelink_run_snapshot.write_once(prelink_run_snapshot.snapshot_path(SID, PID, run_id),
                                    prelink_run_snapshot.validate(snapshot))
    return snapshot


def get(client, run_id=RUN):
    response = client.get(f"{BASE}/runs/{run_id}/prelink-reconciliation")
    assert response.status_code == 200 and response.headers["cache-control"] == "no-store", response.text
    return response.json()


def by_label(body):
    return {row["label"]: row for row in body["items"]}


def test_run_without_snapshot(client, world):
    body = get(client)
    assert body["binding"]["status"] == "NO_SNAPSHOT" and body["items"] == []


def test_bound_run_states_and_promotion_candidates(client, world):
    # conftest regions: R1 = o1, o2 ↔ n1, n2 (N:N, no ai links); R2 = o3 ↔ n1 (1:1 → ai link).
    add_drafts([(["o1"], ["n1"]), (["o3"], ["n1"]), (["o1", "o3"], ["n2"])])
    freeze()
    body = get(client)
    assert body["binding"]["status"] == "BOUND" and body["snapshot"]["mapper_prelinks_used"] is False
    rows = by_label(body)
    assert (rows["PL-1"]["state"], rows["PL-1"]["targets"]) == ("MATCHED", ["R1"])
    assert [e["already"] for e in rows["PL-1"]["promotion"]["edges"]] == ["NONE"]
    assert (rows["PL-2"]["state"], rows["PL-2"]["targets"]) == ("MATCHED", ["R2"])
    assert [e["already"] for e in rows["PL-2"]["promotion"]["edges"]] == ["AI_LINK"]
    assert (rows["PL-3"]["state"], rows["PL-3"]["partial_kind"]) == ("PARTIAL_MATCH", "SUBSET")
    assert rows["PL-3"]["outside"] == ["OLD:o3"]
    assert body["counts"] == {"MATCHED": 2, "PARTIAL_MATCH": 1, "CONFLICT": 0, "UNRESOLVED": 0, "NOT_EVALUATED": 0}
    # A link recorded with the expected id is recognised as promoted (derived, never stored).
    edge = rows["PL-1"]["promotion"]["edges"][0]
    hm_event(world["run_dir"], "edit", region_id="R1", event_type="ADD_BLOCK_LINK", link_id=edge["link_id"],
             old_block_id="o1", new_block_id="n1", previous_old_block_id=None, previous_new_block_id=None)
    fresh()
    assert [e["already"] for e in by_label(get(client))["PL-1"]["promotion"]["edges"]] == ["PROMOTED"]


def test_stale_at_launch_is_not_evaluated(client, world):
    add_drafts([(["o1"], ["n1"])])
    path = world["documents"]["NEW"]["markdown"]
    path.write_text(path.read_text(encoding="utf-8").replace("Текст NEW 1", "Текст NEW 1 (изм.)"), encoding="utf-8")
    fresh()
    freeze()
    [row] = get(client)["items"]
    assert (row["state"], row["reason"]) == ("NOT_EVALUATED", "EXCLUDED_STALE")
    # The card shows what was drawn and why it was stale — never an empty composition, never a promotion.
    assert (row["label"], row["cardinality"], row["validity_at_launch"]) == ("PL-1", "1:1", "STALE_TEXT")
    assert [b["block_id"] for b in row["old_blocks"]] == ["o1"] and [b["block_id"] for b in row["new_blocks"]] == ["n1"]
    assert [d["reason"] for d in row["validity_details"]] == ["TEXT_CHANGED"]
    assert row["promotion"]["edges"] == [] and row["promotion"]["available"] is False


def test_snapshot_without_the_composition_of_an_excluded_link_still_reads(client, world):
    # Snapshots frozen before the composition was kept: the card says so (nothing invented).
    add_drafts([(["o1"], ["n1"])])
    world["documents"]["OLD"]["pdf"].write_bytes(b"%PDF another version")
    fresh()

    def older(snapshot):
        for entry in snapshot["drafts"]["excluded"]:
            for key in ("cardinality", "old_blocks", "new_blocks"):
                entry.pop(key)
    freeze(mutate=older)
    [row] = get(client)["items"]
    assert (row["reason"], row["validity_at_launch"], row["old_blocks"], row["new_blocks"], row["cardinality"]) == \
        ("EXCLUDED_STALE", "STALE_PDF", [], [], None)


def test_run_source_differs_from_the_snapshot(client, world):
    # The recognition changed BEFORE the drafts were made, the run was built earlier: B4 catches it.
    path = world["documents"]["NEW"]["markdown"]
    path.write_text(path.read_text(encoding="utf-8").replace("Текст NEW 1", "Текст NEW 1 (новый)"), encoding="utf-8")
    fresh()
    add_drafts([(["o1"], ["n1"]), (["o3"], ["n2"])])
    freeze()
    rows = by_label(get(client))
    assert (rows["PL-1"]["state"], rows["PL-1"]["reason"]) == ("NOT_EVALUATED", "SOURCE_MISMATCH")
    assert rows["PL-2"]["state"] != "NOT_EVALUATED"  # only the prelink whose blocks differ


def test_pdf_of_the_run_differs_from_the_snapshot(client, world):
    add_drafts([(["o1"], ["n1"])])

    def other_pdf(snapshot):
        snapshot["source_identity"]["OLD"]["pdf_sha256"] = "0" * 64
    freeze(mutate=other_pdf)
    body = get(client)
    assert body["binding"]["status"] == "SOURCE_MISMATCH" and body["items"][0]["state"] == "NOT_EVALUATED"


def test_hm_region_map_must_match_the_semantic_map(client, world):
    add_drafts([(["o1"], ["n1"])])
    freeze()
    ui_path = world["run_dir"] / "human_mapping" / "ui_data.json"
    ui = json.loads(ui_path.read_text(encoding="utf-8"))
    ui["source_sha256"] = "f" * 64
    ui_path.write_text(json.dumps(ui), encoding="utf-8")
    fresh()
    body = get(client)
    assert (body["binding"]["status"], body["binding"]["reason"]) == ("HM_UNAVAILABLE", "HM_MAP_MISMATCH")


def test_later_draft_edits_are_reported_but_never_change_the_result(client, world):
    view = add_drafts([(["o1"], ["n1"])])
    freeze()
    first = get(client)
    prelink_drafts.replace(SID, PID, view["prelinks"][0]["prelink_id"], expected_revision=view["revision"],
                           old_block_ids=["o3"], new_block_ids=["n2"])
    add_drafts([(["o2"], ["n2"])])
    second = get(client)
    assert second["items"] == first["items"]
    assert second["drafts_changed_since"] == {"added": 1, "removed": 0, "changed": 1}


def test_second_run_is_reconciled_with_its_own_snapshot(client, world):
    add_drafts([(["o1"], ["n1"])])
    freeze()
    build_run(world["documents"], run_id="run2", regions=[region("R1", ["o3"], ["n2"]), region("R2", ["o1"], ["n1"])])
    add_drafts([(["o3"], ["n2"])])
    freeze("run2")
    one, two = by_label(get(client)), by_label(get(client, "run2"))
    assert set(one) == {"PL-1"} and set(two) == {"PL-1", "PL-2"}
    assert one["PL-1"]["targets"] == ["R1"] and two["PL-1"]["targets"] == ["R2"]


def test_reconciliation_writes_nothing_and_calls_no_model(client, world):
    add_drafts([(["o1"], ["n1"]), (["o1", "o2"], ["n1", "n2"])])
    freeze()

    def tree(root: Path):
        return {str(p): (hashlib.sha256(p.read_bytes()).hexdigest(), p.stat().st_mtime_ns)
                for p in sorted(root.rglob("*")) if p.is_file()}
    before = tree(world["tmp"])
    time.sleep(0.01)
    for _ in range(2):
        get(client)
    assert tree(world["tmp"]) == before
    assert world["sentinel"].calls == []
