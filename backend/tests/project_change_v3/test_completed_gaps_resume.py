"""3.26.0: досчёт пробелов завершённого прогона и повтор TRANSIENT-отказов пакетов.

Что должно держаться:
* завершённый прогон (REVIEW) с упавшими пакетами разбора unmatched-страниц или
  проверки параметров предлагается к досборке, без пробелов — нет;
* досборка той же моделью вызывает заново ТОЛЬКО упавшие пакеты разбора, с тем же
  вопросом, что у донора; Mapper и Miner не вызываются;
* ответ Dedupe донора берётся без вызова, если карточки не изменились;
* пакет проверки, чей вопрос совпал с принятым у донора, берётся без вызова;
* TRANSIENT-отказ пакета повторяется тем же вызовом после паузы, пауза прерывается отменой.

Объект 256 (15.10): 19 из 24 пакетов разбора упали на 429 за одну минуту.
"""
from __future__ import annotations

import json
import time

import pytest

from backend.app.services.project_change_v3 import engine, parallel_calls, run_storage
from backend.app.services.project_change_v3.provider import FakeProvider, ProviderError, set_test_provider
from backend.tests.project_change_v3 import generic_fixture as gf
from backend.tests.project_change_v3.test_failure_states import env  # noqa: F401 — fixture
from backend.tests.project_change_v3.test_review_parallel import _stored, attempts, prepare


def _run(session_id, **kwargs):
    from backend.app.services.stage_comparison.production_orchestrator import run_production_comparison

    return run_production_comparison(session_id, gf.PAIR_ID, input_mode="DOCUMENT", **kwargs)


def _failing(handler, should_fail):
    def wrapped(**kwargs):
        if should_fail(kwargs):
            raise ProviderError("TRANSIENT", "exceeded retry limit, last status: 429 Too Many Requests")
        return handler(**kwargs)
    return wrapped


@pytest.fixture
def no_retry(monkeypatch):
    monkeypatch.setattr(engine, "REVIEW_TRANSIENT_RETRY_DELAYS_S", ())


def _stages(fake):
    return [(call["stage"], call["call_id"]) for call in fake.calls]


def _question(kwargs, run_id):
    """Вопрос модели без папки прогона: кадры у досборки свои, побайтно те же (check_compatible)."""
    return json.dumps(kwargs["data"], ensure_ascii=False, sort_keys=True).replace(f"/runs/{run_id}/", "/runs/RUN/")


def test_failed_unmatched_batches_of_a_completed_run_are_the_only_new_calls(env, monkeypatch, no_retry):  # noqa: F811
    handlers = prepare(env, monkeypatch, "UNMATCHED_REVIEW", 3)
    questions = []
    base = handlers["UNMATCHED_REVIEW"]

    def good(**kwargs):
        if kwargs["call_id"].endswith("U-R002_UNMATCHED"):
            questions.append(kwargs)
        return base(**kwargs)

    handlers["UNMATCHED_REVIEW"] = _failing(good, lambda k: k["call_id"].endswith("U-R002_UNMATCHED")
                                            and not questions.append(k))
    set_test_provider(FakeProvider(handlers=handlers))
    donor = _run(env["session_id"])
    assert donor["status"] == "REVIEW" and donor["coverage_content_unmatched_pending"] > 0, donor["message"]

    from backend.app.services.stage_comparison.production_orchestrator import get_production_state
    public = get_production_state(env["session_id"], gf.PAIR_ID)
    assert public["resume_available"] is True
    assert public["resume_options"]["mode"] == "gaps" and public["resume_options"]["can_switch_model"] is False
    assert public["resume_options"]["gaps"] == {"unmatched_batches_failed": 1, "verification_batches_failed": 0}

    handlers["UNMATCHED_REVIEW"] = good
    fake = FakeProvider(handlers=handlers)
    set_test_provider(fake)
    state = _run(env["session_id"], resume_from_run_id=donor["run_id"])

    assert state["status"] in {"REVIEW", "COMPLETED"} and state["reason_code"] == "v3_completed", state["message"]
    assert _stages(fake) == [("UNMATCHED_REVIEW", f"{gf.PAIR_ID}_U-R002_UNMATCHED"), ("DEDUPE", f"{gf.PAIR_ID}_DEDUPE")]
    assert state["coverage_content_unmatched_pending"] == 0
    retried = next(r for r in attempts(state, "UNMATCHED_REVIEW") if r["batch_id"] == "U-R002")
    assert retried["status"] == "ACCEPTED"
    assert len(questions) == 2
    assert _question(questions[0], donor["run_id"]) == _question(questions[1], state["run_id"]), \
        "the retried batch asks exactly the donor's question"
    changes = _stored(env["session_id"], "project_change_v3_miner_results")["projectchanges"]
    assert [c["projectchange_id"] for c in changes[1:]] == [f"PC-U-R{i:03}-C001" for i in range(1, 4)]
    supplemental = _stored(env["session_id"], "project_change_v3_supplemental_results")
    assert supplemental["limits"]["retried_from_donor"] == ["U-R002"]
    result = _stored(env["session_id"], "project_change_v3_result")
    assert result["provenance"]["resumed_from"]["completed_donor"] is True
    assert get_production_state(env["session_id"], gf.PAIR_ID)["resume_available"] is False


def test_failed_verification_batch_is_redone_and_the_rest_is_reused(env, monkeypatch, no_retry):  # noqa: F811
    handlers = prepare(env, monkeypatch, "SOURCE_VERIFICATION", 16)
    good = handlers["SOURCE_VERIFICATION"]
    handlers["SOURCE_VERIFICATION"] = _failing(good, lambda k: k["call_id"].endswith("VERIFY-B002"))
    set_test_provider(FakeProvider(handlers=handlers))
    donor = _run(env["session_id"])
    assert donor["status"] == "REVIEW" and donor["quality_source_verification_pending"] == 8, donor["message"]

    handlers["SOURCE_VERIFICATION"] = good
    fake = FakeProvider(handlers=handlers)
    set_test_provider(fake)
    state = _run(env["session_id"], resume_from_run_id=donor["run_id"])

    assert state["status"] in {"REVIEW", "COMPLETED"}, state["message"]
    assert _stages(fake) == [("SOURCE_VERIFICATION", f"{gf.PAIR_ID}_VERIFY-B002")], "no Mapper, Miner or Dedupe call"
    assert state["quality_source_verification_pending"] == 0
    assert state["quality_independently_verified"] == 16
    result = _stored(env["session_id"], "project_change_v3_result")
    assert result["provenance"]["dedupe_reused_from_run_id"] == donor["run_id"]
    rows = {r["batch_id"]: r for r in _stored(env["session_id"], "project_change_v3_verification_results")["batches"]}
    assert rows["VERIFY-B001"]["resumed_from_run_id"] == donor["run_id"]
    assert "resumed_from_run_id" not in rows["VERIFY-B002"]


def test_a_completed_run_without_gaps_is_not_resumable(env):  # noqa: F811
    from backend.app.services.stage_comparison.production_orchestrator import get_production_state

    fake = FakeProvider(handlers=gf.fake_handlers())
    set_test_provider(fake)
    completed = _run(env["session_id"])
    assert completed["status"] in {"REVIEW", "COMPLETED"}
    assert get_production_state(env["session_id"], gf.PAIR_ID)["resume_available"] is False
    fake.calls.clear()

    state = _run(env["session_id"], resume_from_run_id=completed["run_id"])

    assert state["reason_code"] == "resume_refused" and "нечего досчитывать" in state["message"]
    assert fake.calls == []


def test_a_transient_review_failure_is_retried_after_a_pause(env, monkeypatch):  # noqa: F811
    monkeypatch.setattr(engine, "REVIEW_TRANSIENT_RETRY_DELAYS_S", (0.05, 0.05))
    handlers = prepare(env, monkeypatch, "UNMATCHED_REVIEW", 2)
    good = handlers["UNMATCHED_REVIEW"]
    seen = []

    def flaky(**kwargs):
        seen.append((kwargs["call_id"], time.monotonic()))
        if kwargs["call_id"] == f"{gf.PAIR_ID}_U-R001_UNMATCHED":
            raise ProviderError("TRANSIENT", "429 Too Many Requests")
        return good(**kwargs)

    handlers["UNMATCHED_REVIEW"] = flaky
    set_test_provider(FakeProvider(handlers=handlers))
    state = _run(env["session_id"])

    assert state["coverage_content_unmatched_pending"] == 0, state["message"]
    first = next(r for r in attempts(state, "UNMATCHED_REVIEW") if r["batch_id"] == "U-R001")
    assert first["status"] == "ACCEPTED" and first["attempts"] == 2
    assert first["call_id"] == f"{gf.PAIR_ID}_U-R001_UNMATCHED_RETRY_1"
    times = dict(seen)
    assert times[f"{gf.PAIR_ID}_U-R001_UNMATCHED_RETRY_1"] - times[f"{gf.PAIR_ID}_U-R001_UNMATCHED"] >= 0.05


def test_a_non_transient_review_failure_is_not_retried(env, monkeypatch):  # noqa: F811
    monkeypatch.setattr(engine, "REVIEW_TRANSIENT_RETRY_DELAYS_S", (0.01,))
    handlers = prepare(env, monkeypatch, "UNMATCHED_REVIEW", 2)
    handlers["UNMATCHED_REVIEW"] = _failing(handlers["UNMATCHED_REVIEW"], lambda k: "U-R001" in k["call_id"])
    fake = FakeProvider(handlers={**handlers})
    original = handlers["UNMATCHED_REVIEW"]

    def permanent(**kwargs):
        if "U-R001" in kwargs["call_id"]:
            raise ProviderError("PERMANENT", "usage limit reached")
        return original(**kwargs)

    fake.handlers["UNMATCHED_REVIEW"] = permanent
    set_test_provider(fake)
    state = _run(env["session_id"])

    assert [c for c in _stages(fake) if "U-R001" in c[1]] == [("UNMATCHED_REVIEW", f"{gf.PAIR_ID}_U-R001_UNMATCHED")]
    assert state["coverage_content_unmatched_pending"] > 0


def test_a_delayed_call_is_abandoned_when_the_run_is_cancelled():
    class Provider:
        def complete(self, **_kwargs):
            raise AssertionError("a cancelled run must not call the provider")

    flag = {"cancelled": False}
    started = time.monotonic()

    def cancelled():
        flag["cancelled"] = flag["cancelled"] or time.monotonic() - started > 0.2
        return flag["cancelled"]

    outcome = parallel_calls._complete(Provider(), {parallel_calls.DELAY_KEY: 60, "stage": "X"}, cancelled)
    assert outcome.error is not None and time.monotonic() - started < 5


def test_the_delay_key_never_reaches_the_provider():
    seen = {}

    class Provider:
        def complete(self, **kwargs):
            seen.update(kwargs)
            return {"ok": True}

    outcome = parallel_calls._complete(Provider(), {parallel_calls.DELAY_KEY: 0.01, "stage": "X"})
    assert outcome.response == {"ok": True} and seen == {"stage": "X"}
