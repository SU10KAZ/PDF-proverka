"""Регрессии 14 профилей логических схем ALIA."""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from backend.app.pipeline.stages.block_grounding.alia_scheme_geometry import (
    ALL_PROFILES,
    PROFILE_AK_CONTROL,
    PROFILE_CABINET_COMM,
    PROFILE_CCTV,
    PROFILE_EXTERNAL,
    PROFILE_FIBER,
    PROFILE_FUNCTIONAL,
    PROFILE_METER_HEAT,
    PROFILE_METER_WATER,
    PROFILE_MGN,
    PROFILE_NICHE,
    PROFILE_SOUE,
    build_alia_scheme_graph,
    build_alia_scheme_graph_from_source,
    classify_alia_scheme_profile,
    evaluate_alia_scheme_gate,
    render_alia_scheme_markdown,
)
from backend.app.pipeline.stages.block_grounding.profiled_graph_localization import ru_profile


ROOT = Path(__file__).resolve().parents[1]
SS = ROOT / "experiments" / "блоки разных дисциплин" / "СС"
MANIFEST = SS / "ALIA_SCHEME_CORPUS.json"


def _cases():
    if not MANIFEST.exists():
        return []
    return json.loads(MANIFEST.read_text(encoding="utf-8"))


@pytest.fixture(scope="module")
def corpus_graphs():
    return {
        case["block_id"]: build_alia_scheme_graph(
            SS / case["output"], block_id=case["block_id"]
        )
        for case in _cases()
    }


def test_classifier_separates_logical_grammars():
    assert classify_alia_scheme_profile("АРМ СОТ ВК3.1.1.1 ОСПД3.1") == PROFILE_CCTV
    assert classify_alia_scheme_profile("ШСОУЭ0.1 V1 (Р=120Вт, L=390м)") == PROFILE_SOUE
    assert classify_alia_scheme_profile("ШСПА ШСОУЭ0.1 L1 L9") == PROFILE_FIBER
    assert classify_alia_scheme_profile("АСКУВ-5.1 (18 счет.)") == PROFILE_METER_WATER
    assert classify_alia_scheme_profile("АСКУТ-5.1 (7 счет.)") == PROFILE_METER_HEAT
    assert classify_alia_scheme_profile("Приток П2.Б2 PDS") == PROFILE_FUNCTIONAL
    assert classify_alia_scheme_profile("Наименование параметра ХТ1 TE1") == PROFILE_EXTERNAL
    assert classify_alia_scheme_profile("Гильзопакет СС/АК ЭОМ-СПЗ") == PROFILE_NICHE


@pytest.mark.skipif(not MANIFEST.exists(), reason="корпус ALIA не извлечён")
@pytest.mark.parametrize("case", _cases(), ids=lambda case: case["block_id"])
def test_every_alia_crop_builds_one_distinct_passing_profile(case, corpus_graphs):
    graph = corpus_graphs[case["block_id"]]
    assert graph is not None
    assert graph["profile_id"] in ALL_PROFILES
    assert graph["source"]["block_id"] == case["block_id"]
    assert graph["validation"]["nodes_total"] > 0
    assert graph["validation"]["node_types"]
    gate = evaluate_alia_scheme_gate(graph)
    assert gate["use"] is True and gate["complete"] is True
    markdown = render_alia_scheme_markdown(graph)
    assert ru_profile(graph["profile_id"]) in markdown and graph["profile_id"] not in markdown
    assert "## Узлы" in markdown

    source_pdf = ROOT / case["source_pdf"]
    result_path = source_pdf.with_name(source_pdf.stem + "_result.json")
    if not result_path.exists():
        result_path = next(source_pdf.parent.glob("*_result.json"), None)
    # Исходные проекты — локальные runtime-данные и не входят в репозиторий.
    # Проверка корпуса выше остаётся обязательной, а round-trip по оригинальному
    # PDF выполняется только там, где этот локальный источник доступен.
    if result_path is None or not source_pdf.exists():
        return
    source_result = json.loads(result_path.read_text(encoding="utf-8"))
    source_page = source_block = None
    for candidate_page in source_result["pages"]:
        source_block = next(
            (block for block in candidate_page["blocks"] if block.get("id") == case["block_id"]),
            None,
        )
        if source_block:
            source_page = candidate_page
            break
    assert source_page is not None and source_block is not None
    polygon = source_block.get("polygon_points_norm")
    if not polygon and source_block.get("polygon_points"):
        polygon = [[x / source_page["width"], y / source_page["height"]]
                   for x, y in source_block["polygon_points"]]
    source_graph = build_alia_scheme_graph_from_source(
        source_pdf, page_index=source_page["page_number"] - 1,
        bbox_norm=source_block["coords_norm"], polygon_norm=polygon,
        block_id=case["block_id"],
    )
    assert source_graph is not None
    assert source_graph["profile_id"] == graph["profile_id"]
    assert evaluate_alia_scheme_gate(source_graph)["use"] is True


@pytest.mark.skipif(not MANIFEST.exists(), reason="корпус ALIA не извлечён")
def test_corpus_has_exactly_one_case_per_profile_and_preserves_key_semantics(corpus_graphs):
    graphs = {
        graph["profile_id"]: graph
        for graph in corpus_graphs.values() if graph
    }
    assert set(graphs) == set(ALL_PROFILES)

    cctv = graphs[PROFILE_CCTV]["validation"]
    assert cctv["buildings_total"] == 4
    assert cctv["nodes_total"] == 72 and cctv["edges_total"] == 60

    soue = graphs[PROFILE_SOUE]["validation"]
    assert soue["circuits_total"] == 14 and soue["rooms_total"] == 166

    fiber = graphs[PROFILE_FIBER]["validation"]
    assert fiber["ring_closed"] is True and fiber["fiber_segments_total"] == 9

    assert graphs[PROFILE_METER_WATER]["validation"]["declared_meters_total"] == 606
    assert graphs[PROFILE_METER_HEAT]["validation"]["declared_meters_total"] == 295

    control = graphs[PROFILE_AK_CONTROL]["validation"]
    assert control["nodes_total"] >= 190 and control["edges_total"] >= 100

    assert graphs[PROFILE_MGN]["validation"]["floor_bands_total"] == 30
    cabinet = graphs[PROFILE_CABINET_COMM]["validation"]
    assert cabinet["colored_connections"] >= 5 and cabinet["traced_network_types"] >= 3
    functional = graphs[PROFILE_FUNCTIONAL]["validation"]
    assert functional["process_sequence_length"] >= 3
    assert functional["station_members_bound"] == functional["process_nodes_total"]
    assert graphs[PROFILE_EXTERNAL]["validation"]["connections_total"] == 17
    assert graphs[PROFILE_NICHE]["validation"]["discipline_allocations_total"] == 14

    assert all(evaluate_alia_scheme_gate(graph)["complete"] for graph in graphs.values())
