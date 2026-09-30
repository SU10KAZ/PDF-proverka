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
        regions = [_region(f"R-00{n}", [n], [n]) for n in new_pages]
        if 1 in new_pages:
            regions[0]["important_text_blocks"] = [gf._ref(pages, "OLD", 1, "o1_text", "Расход OLD")]
        return {"pair": gf.PAIR_ID, "regions": regions, "unmatched_old": [p for p in old_pages if p not in new_pages],
                "unmatched_new": [], "coverage_notes": []}

    def prefixed_mining(**kwargs):
        # Фикстурный Miner знает области R-001/R-002; префикс порции он возвращает как был.
        region = kwargs["data"]["frozen_region"]
        bare = copy.deepcopy(kwargs)
        bare["data"] = {**kwargs["data"], "frozen_region": {**region, "region_id": region["region_id"].split(".", 1)[1]}}
        answer = mining(**bare)
        answer["region_id"] = region["region_id"]
        return answer

    return {**handlers, "MAPPING": portion_mapping, "MINING": prefixed_mining}


def test_engine_runs_the_mapper_in_portions_and_merges_them(env, monkeypatch):  # noqa: F811
    seen: list = []
    fake = FakeProvider(handlers=_portion_handlers(seen))
    set_test_provider(fake)
    monkeypatch.setattr(contracts, "DEFAULT_PROFILE_KEY", "opus5")
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


def test_a_pair_that_fits_one_call_keeps_the_single_mapper_call(env, monkeypatch):  # noqa: F811
    fake = FakeProvider(handlers=gf.fake_handlers())
    set_test_provider(fake)
    monkeypatch.setattr(contracts, "DEFAULT_PROFILE_KEY", "opus5")

    state = _run(env["session_id"])

    assert state["status"] in {"REVIEW", "COMPLETED"}, state
    assert [c["call_id"] for c in fake.calls if c["stage"] == "MAPPING"] == [f"{gf.PAIR_ID}_SEMANTIC_MAPPING"]
    assert "mapper_portioning" not in state["provenance"]
    assert _stored(env["session_id"], "project_change_v3_mapper_portions") is None


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
