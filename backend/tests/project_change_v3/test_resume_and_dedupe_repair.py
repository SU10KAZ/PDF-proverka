"""3.11.0: неверный ответ Dedupe не роняет прогон; упавший прогон дособирается с Dedupe.

Что должно держаться:
* верный ответ Dedupe применяется как раньше, без пометок;
* решение, которое нельзя применить как есть, не применяется: его карточки остаются
  отдельными, прогон уходит в REVIEW, сырой ответ сохранён до проверки;
* досборка не вызывает Mapper и Miner, берёт ответы донора, заново их проверяет и
  называет донора в провенансе;
* досборка отказывает до вызовов модели, если донор не упал, упал раньше Miner или
  его источники изменились.
"""
from __future__ import annotations

import json

from backend.app.services.project_change_v3 import run_storage
from backend.app.services.project_change_v3.dedupe import apply_dedupe, repair_decisions
from backend.app.services.project_change_v3.provider import FakeProvider, set_test_provider
from backend.tests.project_change_v3 import generic_fixture as gf
from backend.tests.project_change_v3.test_failure_states import _stored
from backend.tests.project_change_v3.test_failure_states import env  # noqa: F401 — fixture


def _card(pc_id):
    return {"projectchange_id": pc_id, "locations": [pc_id], "modalities": ["TEXT"], "old_pages": [1],
            "new_pages": [1], "evidence_items": [], "changed_parameters": [], "engineering_subject": pc_id,
            "scope": "s", "change_summary": "c", "old_state": "1", "new_state": "2"}


def _keep(*ids):
    return {"decision": "KEEP_SEPARATE", "projectchange_ids": list(ids), "reason": "r"}


def _merge(*ids):
    return {"decision": "MERGE_DUPLICATES", "projectchange_ids": list(ids), "reason": "r"}


def test_a_valid_answer_is_applied_unchanged():
    cards = [_card("a"), _card("b"), _card("c")]
    raw = {"pair": "p", "decisions": [_merge("a", "b"), _keep("c")], "notes": []}
    repaired, repairs = repair_decisions(cards, raw)
    assert repaired is raw and repairs == []


def test_an_invalid_decision_keeps_its_cards_separate_and_nothing_is_lost():
    cards = [_card(x) for x in "abcdef"]
    raw = {"pair": "p", "notes": [], "decisions": [
        _merge("a", "b"),               # верное — применяется
        _merge("c", "ZZZ"),             # неизвестная карточка
        _keep("d", "e"),                # «оставить отдельно» сразу две
    ]}                                  # f не упомянута
    repaired, repairs = repair_decisions(cards, raw)
    assert [r["problems"] for r in repairs] == [["unknown_id"], ["keep_separate_with_several_ids"], ["not_mentioned"]]
    assert repairs[0]["unknown_ids"] == ["ZZZ"]
    out = apply_dedupe("p", cards, repaired)
    assert [c["dedupe_lineage"] for c in out] == [["a", "b"], ["c"], ["d"], ["e"], ["f"]]


def test_a_card_claimed_by_two_decisions_is_merged_by_neither():
    cards = [_card(x) for x in "abc"]
    raw = {"pair": "p", "notes": [], "decisions": [_merge("a", "b"), _merge("b", "c")]}
    repaired, repairs = repair_decisions(cards, raw)
    assert all("id_in_several_decisions" in r["problems"] for r in repairs)
    assert [c["dedupe_lineage"] for c in apply_dedupe("p", cards, repaired)] == [["a"], ["b"], ["c"]]


# ---- Прогон движка -----------------------------------------------------------------

def _run(session_id, **kwargs):
    from backend.app.services.stage_comparison.production_orchestrator import run_production_comparison

    return run_production_comparison(session_id, gf.PAIR_ID, input_mode="DOCUMENT", **kwargs)


def _with_dedupe(handler):
    fake = FakeProvider(handlers={**gf.fake_handlers(), "DEDUPE": handler})
    set_test_provider(fake)
    return fake


def _bad_dedupe(**_kwargs):
    return {"pair": gf.PAIR_ID, "notes": [], "decisions": [
        {"decision": "KEEP_SEPARATE", "projectchange_ids": ["PC-R-001-C001", "PC-НЕТ-ТАКОЙ"], "reason": "r"}]}


def test_an_invalid_dedupe_answer_is_review_not_failed_and_the_raw_answer_is_kept(env):  # noqa: F811
    _with_dedupe(_bad_dedupe)
    state = _run(env["session_id"])
    assert state["status"] == "REVIEW" and state["reason_code"] == "v3_completed", state
    assert state["dedupe_repaired_decisions"] == 1
    result = _stored(env["session_id"], "project_change_v3_result")
    assert [c["projectchange_id"] for c in result["projectchanges"]] == ["PC-R-001-C001"]
    assert result["dedupe_repair"]["problems"] == ["keep_separate_with_several_ids", "unknown_id"]
    kept = _stored(env["session_id"], "project_change_v3_dedupe")
    assert kept["raw_answer"] == _bad_dedupe() and kept["repairs"]


def _failed_donor(session_id):
    """Прогон, упавший на Dedupe после принятого Miner (как на объекте 256)."""
    _with_dedupe(lambda **_k: {"pair": "ЧУЖАЯ", "notes": [], "decisions": []})
    donor = _run(session_id)
    assert donor["status"] == "FAILED" and donor["reason_code"] == "dedupe_failed", donor
    return donor


def test_resume_reuses_map_and_miner_and_calls_only_dedupe(env):  # noqa: F811
    donor = _failed_donor(env["session_id"])
    donor_changes = json.loads((run_storage.run_dir(env["session_id"], gf.PAIR_ID, donor["run_id"])
                                / "project_change_v3_miner_results.json").read_text("utf-8"))["projectchanges"]
    fake = _with_dedupe(gf.fake_handlers()["DEDUPE"])

    state = _run(env["session_id"], resume_from_run_id=donor["run_id"])

    assert state["status"] in {"REVIEW", "COMPLETED"} and state["reason_code"] == "v3_completed", state
    assert [c["stage"] for c in fake.calls] == ["DEDUPE"]
    assert state["resumed_from_run_id"] == donor["run_id"]
    result = _stored(env["session_id"], "project_change_v3_result")
    assert result["run_id"] == state["run_id"] != donor["run_id"]
    assert result["provenance"]["resumed_from"]["run_id"] == donor["run_id"]
    assert result["provenance"]["resumed_from"]["failed_reason"] == "dedupe_failed"
    assert [c["projectchange_id"] for c in result["projectchanges"]] == [c["projectchange_id"] for c in donor_changes]
    assert run_storage.current(env["session_id"], gf.PAIR_ID) == state["run_id"]


def test_the_state_offers_resume_only_for_a_failed_run_with_saved_miner_answers(env):  # noqa: F811
    from backend.app.services.stage_comparison.production_orchestrator import get_production_state

    donor = _failed_donor(env["session_id"])
    assert get_production_state(env["session_id"], gf.PAIR_ID)["resume_available"] is True
    (run_storage.run_dir(env["session_id"], gf.PAIR_ID, donor["run_id"])
     / "project_change_v3_miner_results.json").unlink()
    assert get_production_state(env["session_id"], gf.PAIR_ID)["resume_available"] is False
    _with_dedupe(gf.fake_handlers()["DEDUPE"])
    assert _run(env["session_id"])["status"] in {"REVIEW", "COMPLETED"}
    assert get_production_state(env["session_id"], gf.PAIR_ID)["resume_available"] is False


def test_resume_is_refused_before_any_call_when_the_donor_did_not_fail(env):  # noqa: F811
    fake = _with_dedupe(gf.fake_handlers()["DEDUPE"])
    completed = _run(env["session_id"])
    assert completed["status"] in {"REVIEW", "COMPLETED"}
    fake.calls.clear()

    state = _run(env["session_id"], resume_from_run_id=completed["run_id"])

    assert state["status"] == "FAILED" and state["reason_code"] == "resume_refused"
    assert "только упавший прогон" in state["message"]
    assert fake.calls == []


def test_resume_is_refused_when_the_sources_changed(env):  # noqa: F811
    donor = _failed_donor(env["session_id"])
    manifest_path = run_storage.run_dir(env["session_id"], gf.PAIR_ID, donor["run_id"]) / \
        "project_change_v3_source_manifest.json"
    manifest = json.loads(manifest_path.read_text("utf-8"))
    manifest["new_pdf_sha256"] = "0" * 64
    manifest_path.write_text(json.dumps(manifest), "utf-8")
    fake = _with_dedupe(gf.fake_handlers()["DEDUPE"])

    state = _run(env["session_id"], resume_from_run_id=donor["run_id"])

    assert state["reason_code"] == "resume_refused" and "new_pdf_sha256" in state["message"]
    assert fake.calls == []


def test_resume_is_refused_when_the_donor_failed_before_miner_results(env):  # noqa: F811
    donor = _failed_donor(env["session_id"])
    (run_storage.run_dir(env["session_id"], gf.PAIR_ID, donor["run_id"])
     / "project_change_v3_miner_results.json").unlink()
    _with_dedupe(gf.fake_handlers()["DEDUPE"])

    state = _run(env["session_id"], resume_from_run_id=donor["run_id"])

    assert state["reason_code"] == "resume_refused" and "упал раньше" in state["message"]


def test_replay_accepts_unmatched_batches_whose_page_keys_came_back_from_json(tmp_path):
    """На объекте 256 сухой прогон поймал: ключи страниц из JSON — списки, проверка ждёт кортежи."""
    from pathlib import Path

    from backend.app.services.project_change_v3 import resume
    from backend.app.services.project_change_v3.supplemental import validate_supplemental_answer

    region = {"region_id": "A-R001", "crop": str(tmp_path / "donor" / "c.png")}
    answer = {"projectchanges": [], "coverage_notes": [],
              "unresolved_hints": [{"old_pages": [7], "new_pages": [], "evidence_items": []}]}
    batch = {"batch_id": "U-R001", "primary_keys": [["OLD", 7]], "status": "ACCEPTED",
             "region": {"region_id": "U-R001"}, "result": answer}
    donor = resume.Donor(
        run_id="d" * 32, state={}, provenance={}, semantic_map={"regions": [region]}, mapper_portions=None,
        miner_results={"regions": [{"region_id": "A-R001"}, answer], "projectchanges": [],
                       "unresolved_hints": answer["unresolved_hints"]},
        supplemental={"enabled": True, "batches": [batch]},
        checkpoint={"regions": [{"region_id": "A-R001", "result": {"region_id": "A-R001"}}]},
        source_manifest={}, work_dir=tmp_path / "donor",
    )
    donor.rebase_dirs = (str(tmp_path / "donor"), str(tmp_path / "ours"))
    replayed = resume.replay(donor, pair_id="p", pages_by_key={}, validate_miner=lambda *_a: None,
                             validate_supplemental_answer=validate_supplemental_answer)
    assert replayed["reviewed_unmatched_pages"] == {("OLD", 7)}
    assert Path(donor.rebase(region)["crop"]) == tmp_path / "ours" / "c.png"


def test_api_refuses_a_resume_from_an_unknown_run_before_starting(tmp_path, monkeypatch):
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from backend.app.api.routers import stage_comparison as router_mod

    monkeypatch.setenv("COMPARISON_ROOT", str(tmp_path / "comparison"))
    app = FastAPI()
    app.include_router(router_mod.router)
    client = TestClient(app)
    url = "/api/stage-comparison/sessions/s1/pairs/p1/production/run"
    refused = client.post(url, json={"input_mode": "DOCUMENT", "resume_from_run_id": "a" * 32})
    assert refused.status_code == 400 and "Досборка невозможна" in refused.json()["detail"]
    assert client.post(url, json={"input_mode": "DOCUMENT", "resume_from_run_id": "../x"}).status_code == 422
