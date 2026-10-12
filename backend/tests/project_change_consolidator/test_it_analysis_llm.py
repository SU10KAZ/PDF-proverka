"""Разбор итоговых карточек по корзинам моделью: пачки, проверка ответа, досчёт, приоритет ручного разбора."""
from __future__ import annotations

import io
from collections import Counter

import pytest
from openpyxl import load_workbook

from backend.app.services.project_change_consolidator import it_analysis
from backend.app.services.project_change_consolidator import it_analysis_llm as L
from backend.app.services.project_change_consolidator import report_xlsx as R
from backend.tests.project_change_consolidator.test_consolidated_view import _base, _production_run, _shadow
from backend.tests.project_change_consolidator.test_e2e_production_boundary import env  # noqa: F401
from backend.tests.project_change_v3 import generic_fixture as gf


def _card(rid, basket=R.BASKET_CHANGE, confidence="уверен"):
    return {**{k: f"{k} {rid}" for k in L.FIELDS}, "id": rid, "basket": basket, "confidence": confidence}


def test_batches_and_order():
    assert L.batches(12, 5) == [[0, 1, 2, 3, 4], [5, 6, 7, 8, 9], [10, 11]]
    rows = [{"id": i} for i in ("ИТ-002", "ИТ-001.3", "ИТ-001", "ИТ-001.2", "ИТ-010")]
    assert [r["id"] for r in L.ordered(rows)] == ["ИТ-001", "ИТ-001.2", "ИТ-001.3", "ИТ-002", "ИТ-010"]


def test_check_cards_accepts_tails_of_the_batch_only():
    rows, errors = L.check_cards({"cards": [_card("ИТ-001"), _card("ИТ-001.2", R.BASKET_FULL), _card("ИТ-002")]},
                                 ["ИТ-001", "ИТ-002"])
    assert errors == [] and [r["id"] for r in rows] == ["ИТ-001", "ИТ-001.2", "ИТ-002"]
    _, errors = L.check_cards({"cards": [_card("ИТ-001"), _card("ИТ-001"), _card("ИТ-007.2"),
                                         {**_card("ИТ-002"), "basket": "Доп."}]}, ["ИТ-001", "ИТ-002"])
    text = "; ".join(errors)
    assert "ИТ-001 повторяется" in text and "'ИТ-007.2'" in text and "не из легенды" in text and "нет карточек: ИТ-002" in text
    assert L.check_cards(None, ["ИТ-001"])[1] == ["нет массива cards"]


def test_schema_pins_the_legend():
    card = L.RESPONSE_SCHEMA["properties"]["cards"]["items"]
    assert card["properties"]["basket"]["enum"] == list(R.BASKETS)
    assert card["properties"]["confidence"]["enum"] == list(R.CONFIDENCE)
    assert set(card["required"]) == set(L.FIELDS) and L.PROMPT_PATH.is_file()


def test_rd_pages_follow_the_evidence_within_budget(monkeypatch):
    view = {"original": [], "consolidated": [
        {"kind": "SOURCE_CARD", "title": "A", "evidence": [{"side": "NEW", "page": 5}, {"side": "NEW", "page": 5},
                                                           {"side": "NEW", "page": 9}, {"side": "OLD", "page": 2}]},
        {"kind": "SOURCE_CARD", "title": "B", "evidence": [{"side": "NEW", "page": 7}]}]}
    pd = {1: "п" * 10, 2: "п" * 10}
    rd = {5: "р" * 60, 7: "р" * 60, 9: "р" * 60}
    monkeypatch.setattr(L, "RD_CHARS_BUDGET", 100)
    data, receipt = L.batch_payload(view, [0], system="X", pd=pd, rd=rd)
    assert list(data["rd_pages"]) == ["5"] and receipt["rd_pages_skipped"] == [9]  # чаще цитируемая — первой
    assert list(data["pd_pages"]) == ["1", "2"] and [x["id"] for x in data["all_items"]] == ["ИТ-001", "ИТ-002"]
    assert [x["id"] for x in data["items"]] == ["ИТ-001"]
    monkeypatch.setattr(L, "PD_CHARS_BUDGET", 10)
    data, receipt = L.batch_payload(view, [0], system="X", pd=pd, rd=rd)
    assert list(data["pd_pages"]) == ["2"]  # том ПД не влез: сперва страницы фрагментов


class FakeModel:
    """Отвечает по карточкам пачки; ``fail`` — номера вызовов, на которых модель «падает»."""

    def __init__(self, fail=()):
        self.calls, self.fail, self.last_response, self.last_transport = [], set(fail), None, {"usage": {}}

    def complete(self, *, stage, call_id, pair_id, prompt, data, schema, images):
        self.calls.append([x["id"] for x in data["items"]])
        assert stage == L.STAGE and images == [] and "Четыре корзины" in prompt and data["pd_pages"]
        if len(self.calls) in self.fail:
            raise RuntimeError("provider_timeout")
        self.last_response = {"cards": [_card(x["id"]) for x in data["items"]]}
        return self.last_response


@pytest.fixture()
def ready(env, monkeypatch):
    pytest.importorskip("jsonschema")  # теневой прогон Консолидатора
    from backend.app.services.project_change_consolidator import view as V

    run_id = _production_run(env)
    manifest = _shadow(env, run_id)
    data = V.consolidated_view(env["session_id"], gf.PAIR_ID, run_id, manifest["consolidator_run_id"])
    monkeypatch.setattr(L, "BATCH_SIZE", 1)
    return {**env, "view": data, "url": _base(env) + f"/consolidated/{run_id}/{manifest['consolidator_run_id']}"}


def _run(ready, model, **kw):
    return L.start(ready["session_id"], gf.PAIR_ID, ready["view"], document_code="GEN", object_label="Объект",
                   author="t", provider_factory=lambda: model, check_readiness=False, background=False, **kw)


def test_model_run_fills_the_report_and_finishes_missing_items_later(ready):
    sid, view = ready["session_id"], ready["view"]
    n = len(view["consolidated"])
    assert n >= 2
    model = FakeModel(fail={2})
    state = _run(ready, model)
    assert state["status"] == "PARTIAL" and state["failed_ids"] == ["ИТ-002"] and len(model.calls) == n
    status = it_analysis.status(sid, gf.PAIR_ID, view)
    assert status["source"] == "model" and status["failed_ids"] == ["ИТ-002"] and status["filled"] == n - 1

    again = FakeModel()
    state = _run(ready, again)  # без force — только недостающие
    assert again.calls == [["ИТ-002"]] and state["status"] == "COMPLETED"
    analysis = it_analysis.load(sid, gf.PAIR_ID, view)
    assert [r["id"] for r in analysis["rows"]] == [R.it_id(i) for i in range(n)] and not analysis["failed_ids"]
    with pytest.raises(L.RunRefused) as exc:
        _run(ready, FakeModel())
    assert exc.value.code == "already_done"

    report = ready["client"].get(ready["url"] + "/report.xlsx")
    cards = load_workbook(io.BytesIO(report.content))["Карточки ИТ"]
    assert cards["B2"].value == R.BASKET_CHANGE and cards["E2"].value == "resume ИТ-001"
    calls = sorted(p.name for p in (L._base(sid, gf.PAIR_ID, view) / "model_runs").glob("*/*/RESPONSE.json"))
    assert len(calls) == n + 1  # сырой ответ каждой пачки, включая упавшую


def test_manual_analysis_wins_unless_forced(ready):
    sid, view = ready["session_id"], ready["view"]
    rows = R.template_rows(view)
    for r in rows:
        r.update(basket=R.BASKET_FULL, confidence="сначала лист")
    it_analysis.store(sid, gf.PAIR_ID, view, rows, warnings=[], source=it_analysis.SOURCE_MANUAL, author="инженер")
    with pytest.raises(L.RunRefused) as exc:
        _run(ready, FakeModel())
    assert exc.value.code == "manual_exists"
    state = _run(ready, FakeModel(), force=True)
    assert state["status"] == "COMPLETED"
    assert it_analysis.load(sid, gf.PAIR_ID, view)["source"] == "model"
    assert len(list((L._base(sid, gf.PAIR_ID, view) / "history").glob("*.json"))) == 1  # ручной — в истории


def test_api_refuses_without_a_ready_provider_and_reports_state(ready, monkeypatch):
    client, url = ready["client"], ready["url"]
    assert client.get(url + "/it-analysis/model-run").json()["status"] == "NONE"
    monkeypatch.delenv("PROJECT_COMPARISON_V3_ALLOW_INFERENCE", raising=False)
    refused = client.post(url + "/it-analysis/model-run", json={"force": False})
    assert refused.status_code == 503 and refused.json()["detail"]["code"] == "provider_unavailable"
    assert client.post(url + "/it-analysis/model-run/cancel").json() == {"cancelled": False}
    _run(ready, FakeModel(fail={1}))
    assert client.get(url + "/it-analysis/model-run").json()["status"] == "PARTIAL"
    assert Counter(not r.get("basket") for r in it_analysis.load(ready["session_id"], gf.PAIR_ID, ready["view"])["rows"])[True] == 1


def test_readiness_checks_opus55_even_when_default_is_codex(monkeypatch):
    from backend.app.services.project_change_v3 import contracts, provider_gate

    seen = []
    monkeypatch.setattr(contracts, "DEFAULT_PROFILE_KEY", "astra")
    def gate():
        seen.append(contracts.active_profile().key)
        return {"available": True}
    monkeypatch.setattr(provider_gate, "check_provider_readiness", gate)
    L._readiness()
    assert seen == ["opus55"]
    assert contracts.active_profile().key == "astra"


@pytest.mark.parametrize("force", [False, True])
def test_manual_upload_during_run_is_never_overwritten(ready, force):
    sid, view = ready["session_id"], ready["view"]
    class UploadDuringCall(FakeModel):
        def complete(self, **kwargs):
            response = super().complete(**kwargs)
            if len(self.calls) == 1:
                it_analysis.store(sid, gf.PAIR_ID, view, R.template_rows(view), warnings=[],
                                  source=it_analysis.SOURCE_MANUAL, author="инженер")
            return response
    result = _run(ready, UploadDuringCall(), force=force)
    assert result["status"] == "FAILED" and "изменён" in result["errors"][-1]
    assert it_analysis.load(sid, gf.PAIR_ID, view)["source"] == it_analysis.SOURCE_MANUAL


def test_failed_initial_state_write_does_not_leave_run_locked(ready, monkeypatch):
    def fail(*_args):
        raise OSError("disk full")
    monkeypatch.setattr(L, "_write", fail)
    with pytest.raises(OSError):
        _run(ready, FakeModel())
    assert L._key(ready["view"], ready["session_id"], gf.PAIR_ID) not in L._running


def test_cancel_prevents_following_batches(ready):
    class CancelDuringCall(FakeModel):
        def complete(self, **kwargs):
            response = super().complete(**kwargs)
            assert L.cancel(ready["session_id"], gf.PAIR_ID, ready["view"])
            return response
    model = CancelDuringCall()
    result = _run(ready, model)
    assert result["status"] == "CANCELLED" and len(model.calls) == 1


def test_failed_replacement_keeps_current_analysis(ready, monkeypatch):
    sid, view = ready["session_id"], ready["view"]
    kwargs = dict(warnings=[], source=it_analysis.SOURCE_MANUAL, author="инженер")
    it_analysis.store(sid, gf.PAIR_ID, view, R.template_rows(view), **kwargs)
    before = it_analysis.load(sid, gf.PAIR_ID, view)
    def fail(*_args):
        raise OSError("disk full")
    monkeypatch.setattr(it_analysis, "_atomic_json", fail)
    with pytest.raises(OSError):
        it_analysis.store(sid, gf.PAIR_ID, view, [], **kwargs)
    assert it_analysis.load(sid, gf.PAIR_ID, view) == before
