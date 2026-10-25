"""Miner admission/splitting and compaction failures: synthetic sources, no model calls."""
import copy
import json
from collections import Counter
from itertools import product
from pathlib import Path

import pytest

from backend.app.services.project_change_v3 import contracts, engine, mapper_portions as mp, transport
from backend.app.services.project_change_v3.provider import FakeProvider, ProviderError, build_codex_payload, set_test_provider
from backend.tests.project_change_v3 import generic_fixture as gf
from backend.tests.project_change_v3.test_failure_states import env, _run, _stored  # noqa: F401
from backend.tests.project_change_v3.test_mapper_portions import _region
from backend.tests.project_change_v3.test_claude_opus_provider import png


def test_miner_budget_includes_text_schema_and_full_page_images(tmp_path, monkeypatch):
    image = tmp_path / "full_page.png"
    image.write_bytes(png(1024, 1024))
    monkeypatch.setattr(transport, "CODEX_MINER_INPUT_TOKENS", 2000)
    assert transport.plan_codex_miner("x" * 3000, [], {})
    assert transport.plan_codex_miner("x", [str(image)], {})
    with pytest.raises(transport.TransportIntegrityError, match="вход Miner"):
        transport.plan_codex_miner("x" * 3000, [str(image)], {})
    with pytest.raises(transport.TransportIntegrityError, match="вход Miner"):
        transport.plan_codex_miner("x" * 3000, [], {"description": "Ж" * 1000})


def test_split_region_covers_every_page_pair_and_filters_refs_on_both_sides():
    region = _region("R", [2, 5, 8], [3, 6, 9])
    for key in ("important_text_blocks", "important_table_blocks", "important_graphic_blocks"):
        region[key] = [{"side": side, "physical_page": p, "block_id": f"{side}-{p}"}
                       for side, numbers in (("OLD", region["old_pages"]), ("NEW", region["new_pages"]))
                       for p in numbers]
    before = copy.deepcopy(region)
    parts = mp.split_region_for_envelope(region, lambda r: len(r["old_pages"]) + len(r["new_pages"]) <= 2)
    seen = Counter()
    for part in parts:
        seen.update(product(part["old_pages"], part["new_pages"]))
        for key in ("important_text_blocks", "important_table_blocks", "important_graphic_blocks"):
            assert {(r["side"], r["physical_page"]) for r in part[key]} == {
                ("OLD", part["old_pages"][0]), ("NEW", part["new_pages"][0])}
    assert seen == Counter(product(region["old_pages"], region["new_pages"]))
    assert region == before
    assert len({p["region_id"] for p in parts}) == 9


def prepare_wide_region(monkeypatch):
    monkeypatch.setattr(contracts, "DEFAULT_PROFILE_KEY", "astra")
    monkeypatch.setenv(engine.UNMATCHED_REVIEW_ENV, "0")
    monkeypatch.setenv(engine.SOURCE_VERIFICATION_ENV, "0")
    handlers = gf.fake_handlers()
    handlers["MAPPING"] = lambda **kw: {
        "pair": gf.PAIR_ID, "regions": [_region("WIDE", [1, 2], [1, 2])],
        "unmatched_old": [], "unmatched_new": [], "coverage_notes": []}
    handlers["MINING"] = lambda **kw: {
        "pair": gf.PAIR_ID, "region_id": kw["data"]["frozen_region"]["region_id"],
        "projectchanges": [], "unresolved_hints": [], "coverage_notes": []}
    return handlers


def test_real_miner_budget_splits_both_sides_before_calls_and_finishes(env, monkeypatch):  # noqa: F811
    handlers = prepare_wide_region(monkeypatch)
    real_bundle = engine.optimized_region_bundle
    snapshots = []

    def large_bundle(**kwargs):
        data, images = real_bundle(**kwargs)
        for page in data["pages"]:
            page["synthetic_ocr"] = page["side"] + str(page["physical_page"]) + "Ж" * 70_000
        return data, images

    monkeypatch.setattr(engine, "optimized_region_bundle", large_bundle)

    class BudgetChecking(FakeProvider):
        def complete(self, **kwargs):
            if kwargs["stage"] == "MINING":
                payload, images, _ = build_codex_payload(kwargs["prompt"], kwargs["data"], kwargs["images"])
                budget = transport.plan_codex_miner(payload, images, kwargs["schema"])
                assert budget["estimated_input_tokens"] <= budget["input_budget_tokens"]
                assert all(p["synthetic_ocr"] == p["side"] + str(p["physical_page"]) + "Ж" * 70_000
                           for p in kwargs["data"]["pages"])
                expected_full_pages = {(p["side"], p["physical_page"]) for p in kwargs["data"]["pages"]
                                       if any(b["modality"] == "GRAPHIC" for b in p["blocks"])}
                assert {(i["label"]["side"], i["label"]["physical_page"]) for i in kwargs["images"]
                        if i["label"]["kind"] == "FULL_PAGE_CONTEXT"} == expected_full_pages
                snapshots.append(kwargs["data"]["frozen_region"])
            return super().complete(**kwargs)

    provider = BudgetChecking(handlers=handlers)
    set_test_provider(provider)
    state = _run(env["session_id"])
    assert state["status"] in {"COMPLETED", "REVIEW"}, state.get("message")
    assert [(r["old_pages"], r["new_pages"]) for r in snapshots] == [
        ([1], [1]), ([1], [2]), ([2], [1]), ([2], [2])]
    splits = state["provenance"]["miner_region_splits"]["regions"]
    assert splits == [{"region_id": "WIDE", "parts": [
        {key: r[key] for key in ("region_id", "old_pages", "new_pages")} for r in snapshots]}]
    assert len(state["provenance"]["miner_attempts"]) == 4
    assert _stored(env["session_id"], "project_change_v3_result") is not None


def test_oversized_single_page_pair_stops_before_any_miner_call(env, monkeypatch):  # noqa: F811
    provider = FakeProvider(handlers=prepare_wide_region(monkeypatch))
    set_test_provider(provider)
    monkeypatch.setattr(transport, "CODEX_MINER_INPUT_TOKENS", 1)
    state = _run(env["session_id"])
    assert state["reason_code"] == "miner_envelope_exceeded"
    assert "OLD [1] и NEW 1" in state["message"]
    assert [c["stage"] for c in provider.calls] == ["MAPPING"]
    assert _stored(env["session_id"], "project_change_v3_result") is None


def test_exact_final_regions_are_checked_before_any_miner_call(env, monkeypatch):  # noqa: F811
    monkeypatch.setenv("PROJECT_COMPARISON_V3_MINER_PLANNER", "legacy")
    provider = FakeProvider(handlers=prepare_wide_region(monkeypatch))
    set_test_provider(provider)
    monkeypatch.setattr(engine, "_miner_fits", lambda _p, r, _w, _c:
                        len(r["new_pages"]) <= 1 and r["region_id"] != "WIDE.2")
    state = _run(env["session_id"])
    assert state["reason_code"] == "miner_envelope_exceeded"
    assert "окончательный запрос Miner" in state["message"]
    assert [c["stage"] for c in provider.calls] == ["MAPPING"]


def test_compacted_miner_answer_is_saved_rejected_and_never_retried_or_published(env):  # noqa: F811
    class Compacted(FakeProvider):
        last_response = None

        def complete(self, **kwargs):
            response = super().complete(**kwargs)
            if kwargs["stage"] == "MINING":
                self.last_response = response
                raise ProviderError("CONTEXT_COMPACTED", "клиент сжал контекст (contextCompaction)")
            return response

    provider = Compacted(handlers=gf.fake_handlers())
    set_test_provider(provider)
    state = _run(env["session_id"])
    assert state["reason_code"] == "provider_context_compacted"
    assert "сжал контекст" in state["message"] and "инструмент" not in state["message"]
    assert [c["stage"] for c in provider.calls] == ["MAPPING", "MINING"]
    attempt, = state["provenance"]["miner_attempts"]
    assert attempt["accepted"] is False
    assert attempt["rejection_kind"] == "CONTEXT_COMPACTED"
    saved = json.loads(Path(attempt["attempt_file"]).read_text())
    assert saved["raw_response"]
    assert _stored(env["session_id"], "project_change_v3_result") is None


def test_excessive_plan_stops_before_mining_and_mapping_can_be_reused(env, monkeypatch):  # noqa: F811
    monkeypatch.setenv("PROJECT_COMPARISON_V3_MINER_PLANNER", "legacy")
    from backend.app.services.project_change_v3 import resume
    from backend.tests.project_change_v3.test_resume_and_dedupe_repair import _run as resumable_run

    fake = FakeProvider(handlers=prepare_wide_region(monkeypatch))
    set_test_provider(fake)
    monkeypatch.setenv('PROJECT_COMPARISON_V3_MAX_NEW_MINER_CALLS', '2')
    monkeypatch.setattr(engine, '_miner_fits', lambda _p, r, _w, _c:
                        len(r['old_pages']) == len(r['new_pages']) == 1)
    state = _run(env['session_id'])
    assert state['reason_code'] == 'miner_plan_budget_exceeded'
    assert [c['stage'] for c in fake.calls] == ['MAPPING']
    assert resume.resume_available(env['session_id'], gf.PAIR_ID, state['run_id'])
    assert resume.load_donor(env['session_id'], gf.PAIR_ID, state['run_id']).checkpoint['regions'] == []
    assert _stored(env['session_id'], 'project_change_v3_result') is None
    fake.calls.clear()
    monkeypatch.setattr(engine, '_miner_fits', lambda *_: True)
    completed = resumable_run(env['session_id'], resume_from_run_id=state['run_id'])
    assert completed['status'] in {'REVIEW', 'COMPLETED'}, completed
    assert [c['stage'] for c in fake.calls] == ['MINING']
