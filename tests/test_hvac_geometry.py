import json
import re
from pathlib import Path

import fitz
import pytest

from backend.app.pipeline.stages.block_grounding.hvac_geometry import (
    ALL_HVAC_PROFILES,
    build_hvac_graph_from_source,
    evaluate_hvac_gate,
    render_hvac_markdown,
)
from backend.app.pipeline.stages.block_grounding.block_source_router import resolve_block_source


ROOT = Path(__file__).resolve().parents[1]
HVAC = ROOT / "experiments" / "блоки разных дисциплин" / "ОВ"
MANIFEST = HVAC / "HVAC_DIVERSE_CORPUS.json"
OUT = HVAC / "hvac_out"


def cases():
    return json.loads(MANIFEST.read_text()) if MANIFEST.exists() else []


@pytest.fixture(scope="module")
def graphs():
    return {
        case["block_id"]: json.loads((OUT / f"{case['block_id']}.structure.json").read_text())
        for case in cases()
    }


def summary_records():
    summary=json.loads((OUT/"summary.json").read_text())
    return {record["block_id"]:record for record in summary["records"]}


@pytest.mark.parametrize("case", cases(), ids=lambda case: case["block_id"])
def test_hvac_corpus_has_vector_pdf_and_description(case, graphs):
    path = HVAC / case["output"]
    with fitz.open(path) as document:
        assert document.page_count == 1
        page = document[0]
        assert page.get_drawings()
        assert page.get_text() or page.get_images(full=True)

    graph = graphs[case["block_id"]]
    assert graph["profile_id"] == case["profile_id"]
    gate = evaluate_hvac_gate(graph)
    expected=summary_records()[case["block_id"]]
    assert gate["use"] is expected["gate_use"]
    assert gate["complete"] is expected["complete"]
    description = render_hvac_markdown(graph)
    assert "Эталонная текстовая разметка ОВ" in description
    assert graph["profile_id"] not in description


def test_all_nine_hvac_families_are_present(graphs):
    assert len(graphs) == 154
    assert {graph["profile_id"] for graph in graphs.values()} == set(ALL_HVAC_PROFILES)


def test_profile_specific_evidence_passes_strict_gates(graphs):
    for graph in graphs.values():
        if not evaluate_hvac_gate(graph)["complete"]:
            continue
        validation = graph["validation"]
        profile = graph["profile_id"]
        if profile == "hvac_floor_plan":
            assert validation["route_segments_total"] >= 20
            assert validation["route_components_total"] >= 1
        elif profile == "heating_axonometry":
            assert validation["systems_total"] + validation["risers_total"] >= 2
            assert validation["structural_edges_total"] >= 2
        elif profile == "ventilation_axonometry":
            assert validation["systems_total"] >= 2
            assert validation["route_segments_total"] >= 30
        elif profile == "hydronic_principle":
            assert validation["apparatus_total"] >= 3
            assert validation["route_segments_total"] >= 15
        elif profile == "hvac_installation_detail":
            assert validation["parts_total"] >= 3
            assert validation["line_segments_total"] >= 10
        elif profile == "hvac_section_layout":
            assert validation["views_total"] >= 1
            assert sum(validation[key] for key in ("equipment_total", "sizes_total", "elevations_total")) >= 3
        elif profile == "hvac_equipment_drawing":
            assert sum(validation[key] for key in ("models_total", "modules_total", "geometry_parts_total")) >= 3
        elif profile == "hvac_performance_chart":
            assert validation["curve_paths_total"] + validation.get("raster_regions_total", 0) >= 1
            assert validation["numeric_values_total"] >= 5
        elif profile == "hvac_site_overview":
            assert validation["buildings_total"] >= 4


def test_graph_references_are_integral_and_do_not_invent_multiway_pairs(graphs):
    for graph in graphs.values():
        nodes = graph.get("nodes", [])
        networks = graph.get("networks", [])
        containers = graph.get("containers", [])
        node_ids = {node["id"] for node in nodes}
        network_ids = {network["id"] for network in networks}
        assert len(node_ids) == len(nodes)
        assert len(network_ids) == len(networks)
        assert len({container["id"] for container in containers}) == len(containers)
        for network in networks:
            assert set(network.get("endpoint_ids", [])) <= node_ids
        for container in containers:
            assert set(container.get("member_ids", [])) <= node_ids
        for edge in graph.get("edges", []):
            assert edge["from"] in node_ids and edge["to"] in node_ids
            if "network_id" in edge:
                assert edge["network_id"] in network_ids
                network = next(item for item in networks if item["id"] == edge["network_id"])
                assert network.get("path_state") in (
                    "confirmed_pair", "nearest_system_geometry", "same_cad_component"
                )


def test_raster_fan_curve_is_marked_as_raster_not_fake_vector(graphs):
    graph = graphs["6G36-HFKH-CQV"]
    validation = graph["validation"]
    assert validation["curve_paths_total"] == 0
    assert validation["raster_regions_total"] >= 1
    assert validation["curve_representation_state"] == "embedded_raster"
    assert any("не векторизуются" in warning for warning in graph["warnings"])


def test_human_descriptions_do_not_expose_internal_english_codes(graphs):
    for graph in graphs.values():
        description = render_hvac_markdown(graph)
        forbidden = {graph["profile_id"]}
        subtype=graph["validation"]["subtype"]
        if re.fullmatch(r"[a-z0-9_]+",str(subtype)):forbidden.add(subtype)
        forbidden.update(network.get("network_type") for network in graph.get("networks", []))
        forbidden.update(network.get("path_state") for network in graph.get("networks", []))
        forbidden.update(edge.get("edge_type") for edge in graph.get("edges", []))
        forbidden.update(edge.get("edge_state") for edge in graph.get("edges", []))
        assert all(code not in description for code in forbidden if code)
        assert not re.search(r"\b(?:route|network|node)-\d+\b", description)


def test_descriptions_report_evidence_depth_instead_of_claiming_equal_topology(graphs):
    allowed = {
        "engineering_graph", "semantic_hierarchy", "physical_hierarchy",
        "geometry_inventory", "spatial_inventory", "analytical_geometry", "raster_inventory",
    }
    assert {graph["validation"]["description_depth"] for graph in graphs.values()} <= allowed
    assert sum(graph["validation"]["description_depth"] == "engineering_graph" for graph in graphs.values()) >= 5
    for graph in graphs.values():
        description = render_hvac_markdown(graph)
        assert "Уровень описания" in description
        assert "Инженерное дерево" in description


def test_repeated_ventilation_marks_are_merged_into_logical_systems(graphs):
    for graph in graphs.values():
        if graph["profile_id"] != "ventilation_axonometry":
            continue
        validation = graph["validation"]
        assert validation["unique_systems_total"] <= validation["systems_total"]
        assert validation["networks_total"] == validation["unique_systems_total"]


def test_physical_drawings_assign_elements_to_views_or_groups(graphs):
    for graph in graphs.values():
        if graph["profile_id"] not in (
            "hvac_installation_detail", "hvac_section_layout", "hvac_equipment_drawing"
        ):
            continue
        if not evaluate_hvac_gate(graph)["complete"]:
            continue
        member_ids = {
            member_id for container in graph["containers"] for member_id in container.get("member_ids", [])
        }
        assert member_ids == {node["id"] for node in graph["nodes"]}


def test_hvac_graph_can_be_built_from_original_pdf_polygon():
    case = next(case for case in cases() if case["block_id"] == "46E6-AM6E-P9J")
    result_path = ROOT / case["source_result"]
    if not result_path.exists() or not (ROOT / case["source_pdf"]).exists():
        pytest.skip("локальный исходный проект ОВ недоступен")
    data = json.loads(result_path.read_text())
    page_json = block = None
    for candidate_page in data["pages"]:
        for candidate_block in candidate_page["blocks"]:
            if candidate_block.get("id") == case["block_id"]:
                page_json, block = candidate_page, candidate_block
    assert page_json is not None and block is not None
    polygon = block.get("polygon_points_norm")
    if not polygon and block.get("polygon_points"):
        polygon = [
            [x / page_json["width"], y / page_json["height"]]
            for x, y in block["polygon_points"]
        ]
    graph = build_hvac_graph_from_source(
        ROOT / case["source_pdf"],
        page_index=page_json["page_number"] - 1,
        bbox_norm=block["coords_norm"],
        polygon_norm=polygon,
        block_id=case["block_id"],
    )
    assert graph and graph["profile_id"] == "hydronic_principle"
    assert evaluate_hvac_gate(graph)["complete"] is True


def test_block_source_router_returns_structured_hvac(tmp_path):
    case = next(case for case in cases() if case["block_id"] == "46E6-AM6E-P9J")
    result_path = ROOT / case["source_result"]
    if not result_path.exists() or not (ROOT / case["source_pdf"]).exists():
        pytest.skip("локальный исходный проект ОВ недоступен")
    result = json.loads(result_path.read_text())
    page_json = block = None
    for candidate_page in result["pages"]:
        for candidate_block in candidate_page["blocks"]:
            if candidate_block.get("id") == case["block_id"]:
                page_json, block = candidate_page, candidate_block
    assert page_json is not None and block is not None
    polygon = block.get("polygon_points_norm")
    if not polygon and block.get("polygon_points"):
        polygon = [
            [x / page_json["width"], y / page_json["height"]]
            for x, y in block["polygon_points"]
        ]

    output = tmp_path / "_output"
    output.mkdir()
    (tmp_path / "document.pdf").symlink_to(ROOT / case["source_pdf"])
    document_graph = {
        "pages": [{
            "page_index": page_json["page_number"] - 1,
            "image_blocks": [{
                "id": case["block_id"],
                "coords_norm": block["coords_norm"],
                "polygon_points_norm": polygon,
            }],
        }],
    }
    (output / "document_graph.json").write_text(json.dumps(document_graph))
    text, source_kind = resolve_block_source(output, case["block_id"], page_json["page_number"])
    assert source_kind == "structured_hvac"
    assert text and "Принципиальная гидравлическая схема" in text
    assert "Доказательность" in text
    assert "подтверждено непрерывной линией" in text
