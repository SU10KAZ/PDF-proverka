"""Mapper по порциям (engine 3.10.0) — ноль обращений к модели.

Что должно держаться:
* порции режутся по проектам сборки, проект, не помещающийся сам, — по страницам;
* OLD уходит в каждую порцию целиком, NEW-страница — ровно в одну порцию;
* слияние даёт карту той же схемы: области с префиксом порции, OLD без пары —
  только то, что не взяла ни одна порция; финальная проверка покрытия проходит;
* пара, которая помещается в один вызов, идёт прежним путём (один вызов Mapper);
* сквозной прогон по порциям завершается, а ответы порций сохранены отдельно.
"""
from __future__ import annotations

import copy
from collections import Counter
from itertools import product

import pytest

from backend.app.services.project_change_v3 import contracts, engine
from backend.app.services.project_change_v3 import mapper_portions as mp
from backend.app.services.project_change_v3.provider import FakeProvider, set_test_provider
from backend.app.services.project_change_v3.validate import validate_map
from backend.tests.project_change_v3 import generic_fixture as gf
from backend.tests.project_change_v3.test_failure_states import _run, _stored
from backend.tests.project_change_v3.test_failure_states import env  # noqa: F401 — fixture


def _structure(old: int, new: int) -> list[dict]:
    return ([{"side": "OLD", "physical_page": p, "blocks": []} for p in range(1, old + 1)]
            + [{"side": "NEW", "physical_page": p, "blocks": []} for p in range(1, new + 1)])


def _fits_by_count(limit_new: int):
    return lambda pages: sum(1 for p in pages if p["side"] == "NEW") <= limit_new


def test_units_follow_assembly_projects_and_cover_loose_pages():
    structure = _structure(2, 7)
    sources = [{"document_code": "ВК-1", "page_start": 1, "page_end": 3},
               {"document_code": "ВК-2", "page_start": 4, "page_end": 6}]
    assert mp.new_units(structure, sources) == [("ВК-1", [1, 2, 3]), ("ВК-2", [4, 5, 6]), ("стр. 7", [7])]
    assert mp.new_units(_structure(1, 2), None) == [("стр. 1", [1]), ("стр. 2", [2])]


def test_whole_projects_pack_together_and_an_oversized_project_is_split_by_pages():
    structure = _structure(3, 10)
    units = [("ВК-1", [1, 2]), ("ВК-2", [3, 4]), ("ВК-АС", [5, 6, 7, 8, 9]), ("ВК.НС", [10])]
    portions = mp.plan_portions(structure, units, _fits_by_count(4))
    assert [p.new_pages for p in portions] == [[1, 2, 3, 4], [5, 6, 7, 8], [9, 10]]
    assert [p.units for p in portions] == [["ВК-1", "ВК-2"], ["ВК-АС"], ["ВК-АС", "ВК.НС"]]
    assert [p.total for p in portions] == [3, 3, 3]
    data = mp.portion_data("pair", structure, portions[1])
    assert [p["physical_page"] for p in data["pages"] if p["side"] == "OLD"] == [1, 2, 3]
    assert [p["physical_page"] for p in data["pages"] if p["side"] == "NEW"] == [5, 6, 7, 8]
    assert data["portion"]["index"] == 2 and "OLD передан целиком" in data["portion"]["note"]


def test_a_page_that_never_fits_is_refused_before_any_call():
    with pytest.raises(mp.PortioningImpossible):
        mp.plan_portions(_structure(1, 2), [("стр. 1", [1]), ("стр. 2", [2])], lambda pages: False)


@pytest.mark.parametrize("old_count,new_count,budget", [(7, 5, 4), (3, 8, 3), (1, 4, 2), (9, 2, 5)])
def test_two_sided_plan_covers_every_page_pair_once_without_changing_sources(old_count, new_count, budget):
    structure = _structure(old_count, new_count)
    original = copy.deepcopy(structure)
    fits = lambda pages: len(pages) <= budget
    portions = mp.plan_portions(structure, [("whole project", list(range(1, new_count + 1)))], fits)
    compared = Counter()
    for portion in portions:
        data = mp.portion_data("pair", structure, portion)
        assert fits(data["pages"])
        assert all(any(page is source for source in structure) for page in data["pages"])
        old = [p["physical_page"] for p in data["pages"] if p["side"] == "OLD"]
        new = [p["physical_page"] for p in data["pages"] if p["side"] == "NEW"]
        compared.update(product(old, new))
        if portion.old_pages is not None:
            assert data["portion"]["old_pages"] == old
            assert "Обе стороны переданы частично" in data["portion"]["note"]
            assert portion.summary()["old_pages"] == old
    assert compared == Counter(product(range(1, old_count + 1), range(1, new_count + 1)))
    assert structure == original
    assert [p.index for p in portions] == list(range(1, len(portions) + 1))
    assert all(p.total == len(portions) for p in portions)


def test_two_sided_plan_adapts_to_uneven_page_sizes():
    structure = _structure(5, 4)
    for page in structure:
        page["size"] = 6 if (page["side"], page["physical_page"]) == ("OLD", 3) else 1
    portions = mp.plan_portions(structure, mp.new_units(structure, None),
                                lambda pages: sum(p["size"] for p in pages) <= 8)
    assert any(p.old_pages == [3] for p in portions)
    assert any(len(p.old_pages) > 1 for p in portions)
    mp.validate_portion_coverage(structure, portions)


@pytest.mark.parametrize("damage", ["missing_tile", "duplicate_tile", "unknown_page"])
def test_incomplete_or_overlapping_search_space_is_rejected(damage):
    structure = _structure(3, 2)
    portions = mp.plan_portions(structure, mp.new_units(structure, None), lambda pages: len(pages) <= 2)
    if damage == "missing_tile":
        portions.pop()
    elif damage == "duplicate_tile":
        portions.append(copy.deepcopy(portions[0]))
    else:
        portions[0].old_pages = [99]
    with pytest.raises(mp.PortioningImpossible):
        mp.validate_portion_coverage(structure, portions)


def _region(rid, old, new):
    return {"region_id": rid, "old_pages": old, "new_pages": new, "engineering_domain": "ВК", "scope": "s",
            "locations": [], "reason_for_correspondence": "r", "important_text_blocks": [],
            "important_table_blocks": [], "important_graphic_blocks": [], "confidence": 0.8}


def test_merge_prefixes_regions_and_keeps_old_unmatched_only_when_no_portion_took_it():
    structure = _structure(3, 4)
    portions = mp.plan_portions(structure, [("A", [1, 2]), ("B", [3, 4])], _fits_by_count(2))
    answers = [
        {"pair": "pair", "regions": [_region("A-R001", [1], [1])], "unmatched_old": [2, 3],
         "unmatched_new": [2], "coverage_notes": ["первая"]},
        {"pair": "pair", "regions": [_region("A-R001", [1, 2], [3, 4])], "unmatched_old": [3],
         "unmatched_new": [], "coverage_notes": []},
    ]
    merged = mp.merge_portion_maps("pair", answers, portions, structure)
    assert [r["region_id"] for r in merged["regions"]] == ["P01.A-R001", "P02.A-R001"]
    assert merged["unmatched_old"] == [3]  # 2 взяла вторая порция
    assert merged["unmatched_new"] == [2]
    assert merged["coverage_notes"][0].startswith("[порция 1/2:")
    validate_map("pair", merged, structure)
    pytest.importorskip("jsonschema")
    engine._validate_json_schema(merged, contracts.MAP_SCHEMA)


def test_two_sided_merge_keeps_only_globally_unmatched_pages():
    structure = _structure(3, 3)
    portions = mp.plan_portions(structure, mp.new_units(structure, None), lambda pages: len(pages) <= 2)
    answers = []
    for portion in portions:
        old, new = portion.old_pages, portion.new_pages
        matched = (old, new) in [([1], [2]), ([2], [1])]
        answer = {"pair": "pair", "regions": [_region("R", old, new)] if matched else [],
                  "unmatched_old": [] if matched else old,
                  "unmatched_new": [] if matched else new, "coverage_notes": []}
        answers.append(answer)
    merged = mp.merge_portion_maps("pair", answers, portions, structure)
    assert merged["unmatched_old"] == merged["unmatched_new"] == [3]
    assert {(tuple(r["old_pages"]), tuple(r["new_pages"])) for r in merged["regions"]} == {
        ((1,), (2,)), ((2,), (1,))}
    validate_map("pair", merged, structure)
    # Recomputing global unmatched must not hide an invalid local answer.
    answers[0]["unmatched_new"] = []
    with pytest.raises(RuntimeError, match="NEW coverage failure"):
        mp.merge_portion_maps("pair", answers, portions, structure)


# ---- Сквозной прогон движка -----------------------------------------------------

def _portion_handlers(seen: list):
    handlers = gf.fake_handlers()
    mining = handlers["MINING"]

    def portion_mapping(**kwargs):
        data = kwargs["data"]
        pages = {(p["side"], p["physical_page"]): p for p in data["pages"]}
        new_pages = sorted(p for side, p in pages if side == "NEW")
        old_pages = sorted(p for side, p in pages if side == "OLD")
        seen.append({"portion": data.get("portion"), "new": new_pages, "old": old_pages})
        # Порция с NEW-стр. N сопоставляет её с OLD-стр. N (как фикстурные R-001/R-002).
        regions = [_region(f"R-00{n}", [n], [n]) for n in new_pages if n in old_pages]
        if 1 in new_pages and 1 in old_pages:
            regions[0]["important_text_blocks"] = [gf._ref(pages, "OLD", 1, "o1_text", "Расход OLD")]
        return {"pair": gf.PAIR_ID, "regions": regions, "unmatched_old": [p for p in old_pages if p not in new_pages],
                "unmatched_new": [p for p in new_pages if p not in old_pages], "coverage_notes": []}

    def prefixed_mining(**kwargs):
        # Фикстурный Miner знает области R-001/R-002; префикс порции он возвращает как был.
        region = kwargs["data"]["frozen_region"]
        bare = copy.deepcopy(kwargs)
        bare["data"] = {**kwargs["data"], "frozen_region": {**region, "region_id": region["region_id"].split(".", 1)[1]}}
        answer = mining(**bare)
        answer["region_id"] = region["region_id"]
        return answer

    return {**handlers, "MAPPING": portion_mapping, "MINING": prefixed_mining}


@pytest.mark.parametrize("profile", ["opus5", "astra"])
def test_engine_runs_the_mapper_in_portions_and_merges_them(env, monkeypatch, profile):  # noqa: F811
    seen: list = []
    fake = FakeProvider(handlers=_portion_handlers(seen))
    set_test_provider(fake)
    monkeypatch.setattr(contracts, "DEFAULT_PROFILE_KEY", profile)
    monkeypatch.setattr(engine, "mapper_portions_mod_single_envelope_error", lambda *_: "321 images in one call")
    monkeypatch.setattr(engine, "mapper_portions_mod_fits",
                        lambda _pair, pages: sum(1 for p in pages if p["side"] == "NEW") <= 1)

    state = _run(env["session_id"])

    assert state["status"] in {"REVIEW", "COMPLETED"}, state
    mapper_calls = [c for c in fake.calls if c["stage"] == "MAPPING"]
    assert [c["call_id"].rsplit("_", 1)[1] for c in mapper_calls] == ["P01", "P02"]
    assert [s["new"] for s in seen] == [[1], [2]]
    assert all(s["old"] == [1, 2] for s in seen)  # OLD целиком в каждой порции
    assert [s["portion"]["index"] for s in seen] == [1, 2]
    semantic_map = _stored(env["session_id"], "project_change_v3_semantic_map")
    assert [r["region_id"] for r in semantic_map["regions"]] == ["P01.R-001", "P02.R-002"]
    assert semantic_map["unmatched_old"] == []
    portions = _stored(env["session_id"], "project_change_v3_mapper_portions")
    assert portions["mode"] == "PORTIONED" and len(portions["portion_maps"]) == 2
    assert state["provenance"]["mapper_portioning"]["single_call_refused"] == "321 images in one call"


@pytest.mark.parametrize("profile", ["opus5", "astra"])
def test_a_pair_that_fits_one_call_keeps_the_single_mapper_call(env, monkeypatch, profile):  # noqa: F811
    fake = FakeProvider(handlers=gf.fake_handlers())
    set_test_provider(fake)
    monkeypatch.setattr(contracts, "DEFAULT_PROFILE_KEY", profile)

    state = _run(env["session_id"])

    assert state["status"] in {"REVIEW", "COMPLETED"}, state
    assert [c["call_id"] for c in fake.calls if c["stage"] == "MAPPING"] == [f"{gf.PAIR_ID}_SEMANTIC_MAPPING"]
    assert "mapper_portioning" not in state["provenance"]
    assert _stored(env["session_id"], "project_change_v3_mapper_portions") is None


def test_codex_real_budget_splits_large_text_without_losing_pages(env, monkeypatch):  # noqa: F811
    # Synthetic source only: two NEW pages each fit with OLD, both do not.
    seen: list = []
    fake = FakeProvider(handlers=_portion_handlers(seen))
    set_test_provider(fake)
    monkeypatch.setattr(contracts, "DEFAULT_PROFILE_KEY", "astra")
    prepare = engine.prepare_comparison_sources

    def large_source(**kwargs):
        prepared = prepare(**kwargs)
        for page in prepared["structure"]:
            if page["side"] == "NEW":
                page["synthetic_ocr"] = "А" * 120_000
        return prepared

    monkeypatch.setattr(engine, "prepare_comparison_sources", large_source)
    state = _run(env["session_id"])
    assert state["status"] in {"REVIEW", "COMPLETED"}, state
    assert [s["new"] for s in seen] == [[1], [2]]
    assert all(s["old"] == [1, 2] for s in seen)
    assert "бюджет контекста Codex" in state["provenance"]["mapper_portioning"]["single_call_refused"]


def test_codex_real_budget_splits_old_and_new_and_finishes_the_pipeline(env, monkeypatch):  # noqa: F811
    seen = []
    fake = FakeProvider(handlers=_portion_handlers(seen))
    set_test_provider(fake)
    monkeypatch.setattr(contracts, "DEFAULT_PROFILE_KEY", "astra")
    prepare = engine.prepare_comparison_sources

    def large_source(**kwargs):
        prepared = prepare(**kwargs)
        for page in prepared["structure"]:
            page["synthetic_ocr"] = "А" * 70_000
        return prepared

    monkeypatch.setattr(engine, "prepare_comparison_sources", large_source)
    state = _run(env["session_id"])
    assert state["status"] in {"REVIEW", "COMPLETED"}, state.get("message")
    assert [(s["old"], s["new"]) for s in seen] == [([1], [1]), ([1], [2]), ([2], [1]), ([2], [2])]
    semantic_map = _stored(env["session_id"], "project_change_v3_semantic_map")
    assert [r["region_id"] for r in semantic_map["regions"]] == ["P01.R-001", "P04.R-002"]
    assert semantic_map["unmatched_old"] == semantic_map["unmatched_new"] == []
    portions = _stored(env["session_id"], "project_change_v3_mapper_portions")
    assert portions["split_sides"] == ["OLD", "NEW"]
    assert [p["old_pages"] for p in portions["portions"]] == [[1], [1], [2], [2]]
    assert all(row["accepted"] for row in state["provenance"]["mapper_attempts"])


def test_codex_old_that_cannot_fit_stops_before_any_provider_call(env, monkeypatch):  # noqa: F811
    from backend.app.services.project_change_v3 import transport

    fake = FakeProvider(handlers=gf.fake_handlers())
    set_test_provider(fake)
    monkeypatch.setattr(contracts, "DEFAULT_PROFILE_KEY", "astra")
    monkeypatch.setattr(transport, "CODEX_MAPPER_INPUT_TOKENS", 1)
    state = _run(env["session_id"])
    assert state["status"] == "FAILED"
    assert state["reason_code"] == "assembly_mapper_envelope_exceeded"
    assert fake.calls == []


def test_one_oversized_page_pair_blocks_even_previously_planned_tiles(env, monkeypatch):  # noqa: F811
    fake = FakeProvider(handlers=_portion_handlers([]))
    set_test_provider(fake)
    monkeypatch.setattr(engine, "mapper_portions_mod_single_envelope_error", lambda *_: "too big")
    monkeypatch.setattr(engine, "mapper_portions_mod_fits",
                        lambda _pair, pages: len(pages) <= 2 and not any(
                            p["side"] == "OLD" and p["physical_page"] == 2 for p in pages))
    state = _run(env["session_id"])
    assert state["reason_code"] == "assembly_mapper_envelope_exceeded"
    assert "OLD [2] и NEW 1" in state["message"]
    assert fake.calls == []


def test_all_final_portion_notes_are_checked_before_first_call(env, monkeypatch):  # noqa: F811
    from backend.app.services.project_change_v3.transport import TransportIntegrityError

    fake = FakeProvider(handlers=gf.fake_handlers())
    set_test_provider(fake)
    monkeypatch.setattr(contracts, "DEFAULT_PROFILE_KEY", "astra")
    monkeypatch.setattr(engine, "mapper_portions_mod_single_envelope_error", lambda *_: "too big")
    monkeypatch.setattr(engine, "mapper_portions_mod_fits",
                        lambda _pair, pages: sum(p["side"] == "NEW" for p in pages) <= 1)

    def final_envelope(_pair, data):
        if data["portion"]["index"] == 2:
            raise TransportIntegrityError("final portion note is too large")

    monkeypatch.setattr(engine, "mapper_portions_mod_envelope", final_envelope)
    state = _run(env["session_id"])
    assert state["reason_code"] == "assembly_mapper_envelope_exceeded"
    assert fake.calls == []


# ---- Области, чей вызов Miner не помещается --------------------------------------

def test_an_oversized_region_is_split_by_new_pages_with_old_pages_in_every_part():
    region = _region("P02.A-R003", [7, 8], [10, 11, 12, 13, 14])
    region["important_text_blocks"] = [{"side": "NEW", "physical_page": 13, "block_id": "b", "block_type": "TEXT",
                                        "relevance": "r"},
                                       {"side": "OLD", "physical_page": 7, "block_id": "o", "block_type": "TEXT",
                                        "relevance": "r"}]
    parts = mp.split_region_for_envelope(region, lambda r: len(r["new_pages"]) <= 2)
    assert [p["region_id"] for p in parts] == ["P02.A-R003.1", "P02.A-R003.2", "P02.A-R003.3"]
    assert [p["new_pages"] for p in parts] == [[10, 11], [12, 13], [14]]
    assert all(p["old_pages"] == [7, 8] for p in parts)
    assert [len(p["important_text_blocks"]) for p in parts] == [1, 2, 1]  # OLD-ссылка — в каждой части
    assert "часть 2 из 3" in parts[1]["scope"]
    assert mp.split_region_for_envelope(region, lambda r: True) == [region]
    with pytest.raises(mp.PortioningImpossible):
        mp.split_region_for_envelope(region, lambda r: False)


def test_engine_splits_a_region_before_any_miner_call(env, monkeypatch):  # noqa: F811
    fake = FakeProvider(handlers=_portion_handlers([]))
    handlers = fake.handlers
    original_mapping = handlers["MAPPING"]

    def one_wide_region(**kwargs):
        answer = original_mapping(**kwargs)
        answer["regions"] = [_region("R-001", [1, 2], [1, 2])]
        answer["unmatched_old"] = []
        return answer

    handlers["MAPPING"] = one_wide_region
    handlers["MINING"] = lambda **kw: {"pair": gf.PAIR_ID, "region_id": kw["data"]["frozen_region"]["region_id"],
                                       "projectchanges": [], "unresolved_hints": [], "coverage_notes": []}
    set_test_provider(fake)
    monkeypatch.setattr(contracts, "DEFAULT_PROFILE_KEY", "opus5")
    monkeypatch.setattr(engine, "_miner_fits", lambda _pair, region, _wd, _c: len(region["new_pages"]) <= 1)

    state = _run(env["session_id"])

    semantic_map = _stored(env["session_id"], "project_change_v3_semantic_map")
    assert [r["region_id"] for r in semantic_map["regions"]] == ["R-001.1", "R-001.2"]
    assert [r["new_pages"] for r in semantic_map["regions"]] == [[1], [2]]
    miner_calls = [c["call_id"] for c in fake.calls if c["stage"] == "MINING"]
    assert miner_calls and all(".1" in c or ".2" in c for c in miner_calls)
    assert state["provenance"]["miner_region_splits"]["regions"][0]["region_id"] == "R-001"
