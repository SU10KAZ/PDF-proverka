"""Attempt store + bounded structural retry (engine 3.5.2) — zero model calls.

The first full Opus run of DEV 5 lost a paid Miner answer: it was rejected with
``Escaped/one-sided pages`` and existed only in process memory.  What must hold:

* EVERY completed Miner answer, accepted or rejected, is on disk (append-only,
  run-scoped) BEFORE a retry or a failure acts on its verdict; an answer that
  cannot be kept stops the run with no further model call;
* the structural rejection has separate codes and names the offending
  candidates and pages; a candidate violating both rules carries both codes;
* the acceptance rule is unchanged: one-sided and out-of-region ProjectChanges
  are still rejected, never turned into hints, never published;
* one identical retry (same payload hash, no corrective prompt), never a third.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from backend.app.services.project_change_v3 import engine, validate
from backend.tests.project_change_v3 import generic_fixture as gf
from backend.tests.project_change_v3.test_provenance_retry import RecordingFake, _artifact, _r1, _run
from backend.tests.project_change_v3.test_provenance_retry import env  # noqa: F401 — fixture


def one_sided(answer):
    answer["projectchanges"][0]["new_pages"] = []


def outside(answer):
    answer["projectchanges"][0]["old_pages"] = [1, 2]  # region R-001 holds OLD page 1 only


def one_sided_evidence(answer):
    change = answer["projectchanges"][0]
    change["evidence_items"] = [e for e in change["evidence_items"] if e["side"] == "OLD"]


def both(answer):
    one_sided(answer)
    outside(answer)


def bad_on(breaker, attempts=(1,)):
    def script(answer, attempt, _kwargs):
        if attempt in attempts:
            breaker(answer)
        return answer
    return script


def _attempts(built, state) -> list[dict]:
    from backend.app.services.stage_comparison import paths

    folder = (paths.production_dir(built["session_id"], gf.PAIR_ID) / "project_change_v3"
              / engine.MINER_ATTEMPTS_DIR / state["run_id"])
    return [json.loads(p.read_text(encoding="utf-8")) for p in sorted(folder.glob("*.json"))]


def _nothing_published(built):
    for name in ("project_change_v3_result", "project_change_v3_miner_results", "project_change_v3_human_mapping_ui"):
        assert _artifact(built, name) is None, name


def test_a_rejected_answer_is_saved_with_its_reason_and_usage(env):  # noqa: F811
    provider = RecordingFake(gf.fake_handlers(), mining_answers={"R-001": bad_on(one_sided, (1, 2))})
    state = _run(env, provider)
    assert state["status"] == "FAILED" and state["reason_code"] == "miner_structural_rejected"
    first, second = _attempts(env, state)
    assert (first["region_id"], first["region_ordinal"], first["attempt"], second["attempt"]) == ("R-001", 1, 1, 2)
    raw = json.loads(first["raw_response"])
    assert raw == first["parsed_response"] and raw["projectchanges"][0]["new_pages"] == []
    assert first["accepted"] is False and first["published"] is False and first["internal_run_artifact"] is True
    assert first["validation"] == {"schema": "PASS", "provenance": "NOT_REACHED", "structural": "FAILED",
                                   "verdict": "REJECTED"}
    assert first["rejection_codes"] == ["MINER_ONE_SIDED_PROJECTCHANGE"]
    assert first["rejection_message"] == "MINER_ONE_SIDED_PROJECTCHANGE: PC-R-001-C001"
    assert first["offending_projectchange_ids"] == ["PC-R-001-C001"]
    (violation,) = first["structural_violations"]
    assert violation["missing_sides"] == ["NEW"] and violation["old_pages_outside_region"] == []
    for key in ("run_id", "pair_id", "provider", "model", "reasoning", "miner_prompt_sha256", "miner_schema_sha256",
                "model_visible_payload_sha256", "region_source_data_sha256", "source", "recorded_at"):
        assert first.get(key), key
    assert "usage" in first and "transport_receipt" in first and "image_transport" in first
    _nothing_published(env)
    store = state["provenance"]["miner_attempt_store"]
    assert (store["saved"], store["rejected"]) == (2, 2)


def test_one_structural_retry_with_the_identical_input_accepts_the_region(env):  # noqa: F811
    provider = RecordingFake(gf.fake_handlers(), mining_answers={"R-001": bad_on(one_sided)})
    state = _run(env, provider)
    assert state["status"] == "REVIEW" and state["reason_code"] == "v3_completed"
    first, second = _r1(provider)  # exactly two calls
    for key in ("payload_sha256", "prompt_sha256", "data_sha256", "schema_sha256", "images"):
        assert first[key] == second[key], key  # a clean resampling: nothing appended, nothing fixed
    saved = [a for a in _attempts(env, state) if a["region_id"] == "R-001"]
    assert [(a["attempt"], a["accepted"]) for a in saved] == [(1, False), (2, True)]
    assert saved[0]["model_visible_payload_sha256"] == saved[1]["model_visible_payload_sha256"] == first["payload_sha256"]
    assert saved[1]["validation"]["verdict"] == "ACCEPTED" and saved[1]["rejection_codes"] == []
    result = _artifact(env, "project_change_v3_result")
    assert [c["projectchange_id"] for c in result["projectchanges"]] == ["PC-R-001-C001"]
    assert all(c["old_pages"] and c["new_pages"] for c in result["projectchanges"])  # the rejected one never leaks
    rows = [a for a in result["provenance"]["miner_attempts"] if a["region_id"] == "R-001"]
    assert [r["validation"] for r in rows] == ["REJECTED", "ACCEPTED"] and rows[0]["attempt_file"]
    # Accepted checkpoint and attempt store keep their own meanings.
    checkpoint = json.loads(Path(result["provenance"]["miner_checkpoint"]["path"]).read_text(encoding="utf-8"))
    assert [r["region_id"] for r in checkpoint["regions"]] == ["R-001", "R-002"]
    assert len(_attempts(env, state)) == 3  # R-001 x2 + R-002


def test_two_structural_failures_fail_the_run_after_exactly_two_calls(env):  # noqa: F811
    provider = RecordingFake(gf.fake_handlers(), mining_answers={"R-001": bad_on(outside, (1, 2, 3))})
    state = _run(env, provider)
    assert state["status"] == "FAILED" and state["reason_code"] == "miner_structural_rejected"
    assert len(_r1(provider)) == 2 and [s["stage"] for s in provider.seen] == ["MAPPING", "MINING", "MINING"]
    saved = _attempts(env, state)
    assert [(a["attempt"], a["accepted"], a["rejection_codes"]) for a in saved] == [
        (1, False, ["MINER_PAGE_OUTSIDE_REGION"]), (2, False, ["MINER_PAGE_OUTSIDE_REGION"])]
    assert saved[0]["structural_violations"][0]["old_pages_outside_region"] == [2]
    _nothing_published(env)


@pytest.mark.parametrize("breaker, codes", [
    (one_sided, ["MINER_ONE_SIDED_PROJECTCHANGE"]),
    (one_sided_evidence, ["MINER_ONE_SIDED_PROJECTCHANGE"]),
    (outside, ["MINER_PAGE_OUTSIDE_REGION"]),
    (both, ["MINER_ONE_SIDED_PROJECTCHANGE", "MINER_PAGE_OUTSIDE_REGION"]),
])
def test_the_contract_is_not_relaxed_and_each_reason_is_named(breaker, codes):
    handlers = gf.fake_handlers()
    region = {"region_id": "R-001", "old_pages": [1], "new_pages": [1]}
    answer = {"projectchanges": [{"projectchange_id": "C1", "old_pages": [1], "new_pages": [1],
                                  "evidence_items": [{"side": "OLD"}, {"side": "NEW"}]}]}
    assert validate.miner_structural_violations(region, answer) == []
    breaker(answer)
    (violation,) = validate.miner_structural_violations(region, answer)
    assert sorted(violation["codes"]) == codes
    with pytest.raises(validate.MinerStructuralError) as error:
        validate.validate_miner(gf.PAIR_ID, region, {"pair": gf.PAIR_ID, "region_id": "R-001", **answer}, {
            ("OLD", 1): {"blocks": []}, ("NEW", 1): {"blocks": []}})
    assert error.value.codes == codes and handlers


def test_an_answer_that_cannot_be_saved_stops_the_run_before_any_retry(env, monkeypatch):  # noqa: F811
    def broken(path, record):
        raise OSError("disk full")

    monkeypatch.setattr(engine, "write_miner_attempt", broken)
    provider = RecordingFake(gf.fake_handlers(), mining_answers={"R-001": bad_on(one_sided)})
    state = _run(env, provider)
    assert state["status"] == "FAILED" and state["reason_code"] == "miner_attempt_persistence_failed"
    assert len(_r1(provider)) == 1  # the retry was NOT spent
    _nothing_published(env)


def test_the_store_is_append_only_and_run_scoped(env, tmp_path):  # noqa: F811
    first = _run(env, RecordingFake(gf.fake_handlers()))
    second = _run(env, RecordingFake(gf.fake_handlers()))
    assert first["run_id"] != second["run_id"]
    assert len(_attempts(env, first)) == len(_attempts(env, second)) == 2
    path = engine.miner_attempt_path(tmp_path, "run", 1, "R-001", 1)
    engine.write_miner_attempt(path, {"response_sha256": "x"})
    with pytest.raises(FileExistsError):
        engine.write_miner_attempt(path, {"response_sha256": "y"})
    assert engine.miner_attempt_path(tmp_path, "../../x", 1, "../y", 1).parent.parent == tmp_path / engine.MINER_ATTEMPTS_DIR


def test_other_failures_are_still_final(env):  # noqa: F811
    def script(answer, _attempt, _kwargs):
        answer["region_id"] = "R-999"
        return answer

    provider = RecordingFake(gf.fake_handlers(), mining_answers={"R-001": script})
    state = _run(env, provider)
    assert state["status"] == "FAILED" and state["reason_code"] == "miner_validation_failed"
    assert len(_r1(provider)) == 1
    (saved,) = _attempts(env, state)  # rejected for another reason — still kept
    assert saved["rejection_code"] == "not_retryable" and "identity mismatch" in saved["rejection_message"]
    assert engine.MINER_RETRY_POLICY["retry_only_on"] == [
        "untraceable_evidence", "graphic_crop_mismatch", "MINER_PAGE_OUTSIDE_REGION", "MINER_ONE_SIDED_PROJECTCHANGE"]
    assert engine.MINER_MAX_ATTEMPTS == 2 and engine.MINER_RETRY_POLICY["corrective_prompt"] is False


def test_an_answer_the_provider_itself_rejected_is_kept_too(env):  # noqa: F811
    """schema_invalid: the model answered and was paid for; the answer is kept, the run fails, no retry."""
    from backend.app.services.project_change_v3.provider import ProviderError

    class SchemaRejecting(RecordingFake):
        last_response = None

        def complete(self, **kwargs):
            self.last_response = None
            answer = super().complete(**kwargs)
            if kwargs["stage"] == "MINING":
                self.last_response = {"pair": gf.PAIR_ID, "garbage": True}
                raise ProviderError("schema_invalid", "'region_id' is a required property")
            return answer

    provider = SchemaRejecting(gf.fake_handlers())
    state = _run(env, provider)
    assert state["status"] == "FAILED" and state["reason_code"] == "schema_invalid" and len(_r1(provider)) == 1
    (saved,) = _attempts(env, state)
    assert saved["parsed_response"] == {"pair": gf.PAIR_ID, "garbage": True} and saved["accepted"] is False
    assert saved["validation"]["schema"] == "FAILED" and saved["rejection_code"] == "schema_invalid"
    _nothing_published(env)
