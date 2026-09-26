"""Explicit post-analysis promotion of a prelink into the EXISTING Human Mapping contract — 0 models.

Nothing is written until a person acts; the action is ordinary HM events through
the unchanged HM API (ADD_BLOCK_LINK with the server's deterministic link id,
then a region review), exactly the shape of a manual stage-2 write.  The bridge
sees only those events: an anchor after "Подтвердить", an exact-edge rejection
after "Отклонить", no constraint after "Не уверен".
"""
from __future__ import annotations

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.app.services.stage_block_mapping import prelink_reconciliation as rec
from backend.app.services.stage_block_mapping import service
from backend.app.services.stage_comparison import prelink_drafts, prelink_run_snapshot

from .conftest import OID, PID, RUN, SID

HM = f"/api/human-mapping/objects/{OID}/comparisons/{PID}"
SCOPE = f"?session_id={SID}&run_id={RUN}"


@pytest.fixture
def client(world, monkeypatch):
    from backend.app.api.routers import human_mapping
    from backend.app.services.project_change_v3 import scope
    monkeypatch.setattr(scope, "sessions_for_object", lambda object_id: [SID] if object_id == OID else [])
    monkeypatch.setenv("STAGE_PRELINK_DRAFTS", "1")
    app = FastAPI()
    app.include_router(human_mapping.api_router)
    return TestClient(app)


def prepare(groups):
    view = prelink_drafts.view(SID, PID)
    for olds, news in groups:
        view = prelink_drafts.create(SID, PID, expected_revision=view["revision"], old_block_ids=olds,
                                     new_block_ids=news)
    snapshot = prelink_run_snapshot.validate(prelink_run_snapshot.build(SID, PID, RUN))
    prelink_run_snapshot.write_once(prelink_run_snapshot.snapshot_path(SID, PID, RUN), snapshot)
    return {row["label"]: row for row in rec.reconcile(SID, PID, RUN)["items"]}


def history(world):
    directory = world["run_dir"] / "human_mapping"
    return [(directory / name).read_bytes() if (directory / name).exists() else b""
            for name in ("reviews.jsonl", "human_block_link_edits.jsonl")]


def bridge():
    service.clear_cache()
    return service.bridge_check(SID, PID, RUN)["result"]


def add(client, edge, comment):
    response = client.post(HM + "/block-links" + SCOPE, json={
        "event_type": "ADD_BLOCK_LINK", "region_id": edge["region_id"], "link_id": edge["link_id"],
        "old_block_id": edge["old_block_id"], "new_block_id": edge["new_block_id"], "comment": comment})
    assert response.status_code == 200, response.text
    return response.json()


def review(client, region_id, status, olds, news):
    response = client.post(HM + "/reviews" + SCOPE, json={"region_id": region_id, "status": status,
                                                          "old_block_ids": olds, "new_block_ids": news,
                                                          "comment": "Из предварительной связи"})
    assert response.status_code == 200, response.text
    return response.json()


def test_nothing_is_written_before_a_person_acts(client, world):
    before = history(world)
    rows = prepare([(["o1"], ["n1"]), (["o1", "o2"], ["n1", "n2"])])
    assert rows["PL-1"]["state"] == "MATCHED" and rows["PL-1"]["promotion"]["available"]
    assert history(world) == before
    assert bridge()["confirmed_anchor_count"] == 0 and bridge()["rejected_link_count"] == 0


def test_confirm_becomes_an_anchor_only_after_the_region_review(client, world):
    [edge] = prepare([(["o1"], ["n1"])])["PL-1"]["promotion"]["edges"]
    event = add(client, edge, "Из предварительной связи PL-1 (анализ run1)")
    manual = add(client, {"region_id": "R1", "link_id": "human:manual-test", "old_block_id": "o2",
                          "new_block_id": "n2"}, "")
    assert set(event) == set(manual)  # the same event shape as a manual stage-2 write
    assert event["reviewer_source"] == "HUMAN" and event["link_id"] == edge["link_id"]
    assert bridge()["confirmed_anchor_count"] == 0  # a link without a review is not an anchor
    review(client, "R1", "HUMAN_CONFIRMED", ["o1"], ["n1"])
    result = bridge()
    assert [(a["old_block_id"], a["new_block_id"]) for a in result["anchors"]] == [("o1", "n1")]
    assert [e["already"] for e in rec.reconcile(SID, PID, RUN)["items"][0]["promotion"]["edges"]] == ["PROMOTED"]


def test_reject_forbids_exactly_that_edge(client, world):
    [edge] = prepare([(["o2"], ["n2"])])["PL-1"]["promotion"]["edges"]
    add(client, edge, "Из предварительной связи PL-1")
    review(client, "R1", "HUMAN_REJECTED", ["o2"], ["n2"])
    result = bridge()
    assert [(r["old_block_id"], r["new_block_id"]) for r in result["rejected"]] == [("o2", "n2")]
    assert result["confirmed_anchor_count"] == 0


def test_uncertain_constrains_nothing(client, world):
    [edge] = prepare([(["o1"], ["n2"])])["PL-1"]["promotion"]["edges"]
    add(client, edge, "")
    review(client, "R1", "HUMAN_UNCERTAIN", ["o1"], ["n2"])
    result = bridge()
    assert result["confirmed_anchor_count"] == 0 and result["rejected_link_count"] == 0


def test_a_group_without_pairs_offers_no_edges(client, world):
    rows = prepare([(["o1", "o2"], ["n1", "n2"])])
    promo = rows["PL-1"]["promotion"]
    assert promo["edges"] == [] and promo["blocked_reason"] == "GROUP_HAS_NO_PAIRS"
