import json,re
from pathlib import Path

import fitz
import pytest

from backend.app.pipeline.stages.block_grounding.block_source_router import resolve_block_source
from backend.app.pipeline.stages.block_grounding.general_plan_geometry import (
    ALL_GP_PROFILES,build_gp_graph_from_source,evaluate_gp_gate,render_gp_markdown,
)

ROOT=Path(__file__).resolve().parents[1];GP=ROOT/"experiments"/"блоки разных дисциплин"/"ГП"
MANIFEST=GP/"GP_DIVERSE_CORPUS.json";OUT=GP/"gp_out"


def cases():return json.loads(MANIFEST.read_text()) if MANIFEST.exists() else []


@pytest.fixture(scope="module")
def graphs():return {c["block_id"]:json.loads((OUT/f"{c['block_id']}.structure.json").read_text()) for c in cases()}


@pytest.mark.parametrize("case",cases(),ids=lambda case:case["block_id"])
def test_gp_corpus_has_vector_pdf_and_complete_graph(case,graphs):
    with fitz.open(GP/case["output"]) as doc:assert doc.page_count==1 and doc[0].get_text()
    graph=graphs[case["block_id"]];assert graph["profile_id"]==case["profile_id"]
    gate=evaluate_gp_gate(graph);assert gate["use"] is True and gate["complete"] is True


def test_all_gp_profiles_and_subtypes_are_present(graphs):
    assert len(graphs)==47 and {g["profile_id"] for g in graphs.values()}==set(ALL_GP_PROFILES)
    assert len({c["subtype"] for c in cases()})==16


def test_gp_references_are_integral(graphs):
    for graph in graphs.values():
        nodes=graph.get("nodes",[]);networks=graph.get("networks",[]);containers=graph.get("containers",[])
        node_ids={n["id"] for n in nodes};network_ids={n["id"] for n in networks}
        assert len(node_ids)==len(nodes) and len(network_ids)==len(networks)
        for network in networks:assert set(network.get("endpoint_ids",[]))<=node_ids
        for container in containers:assert set(container.get("member_ids",[]))<=node_ids
        for edge in graph.get("edges",[]):assert edge["from"] in node_ids and edge["to"] in node_ids


def test_road_constructions_cover_all_missed_vector_details(graphs):
    roads=[g for g in graphs.values() if g["profile_id"]=="gp_road_structure"]
    assert len(roads)==24 and all(g["validation"]["vector_hatching_state"]=="preserved_not_expanded" for g in roads)
    assert sum(g["validation"]["layers_total"]>0 for g in roads)>=20
    assert sum(g["validation"]["layer_order_edges_total"] for g in roads)>=40


def test_profile_specific_information_is_present(graphs):
    earth=graphs["A7VL-W7KW-6Y7"];assert earth["validation"]["elevations_total"]>=20
    drainage=graphs["4HNT-TT7Q-A39"];assert drainage["validation"]["drainage_elements_total"]>=5
    plan=graphs["474Q-GKNU-ACJ"];assert plan["validation"]["buildings_total"]>=5


def test_gp_human_output_is_russian_and_hides_internal_codes(graphs):
    for graph in graphs.values():
        text=render_gp_markdown(graph);assert "Эталонная текстовая разметка ГП" in text and "Инженерное дерево" in text
        forbidden={graph["profile_id"],graph["validation"]["subtype"]}
        forbidden.update(n.get("network_type") for n in graph.get("networks",[]));forbidden.update(n.get("path_state") for n in graph.get("networks",[]))
        forbidden.update(e.get("edge_state") for e in graph.get("edges",[]))
        assert all(code not in text for code in forbidden if code)
        assert not re.search(r"\b(?:node|network|route|edge)-\d+\b",text)


def test_gp_semantic_audit_has_no_known_losses():
    report=json.loads((GP/"GP_SEMANTIC_COVERAGE.json").read_text());assert report["blocks_total"]==47
    assert sum(r["pdf_misses_total"] for r in report["records"])==0


def source_case(block_id):
    case=next(c for c in cases() if c["block_id"]==block_id);result_path=ROOT/case["source_result"];source_pdf=ROOT/case["source_pdf"]
    if not result_path.exists() or not source_pdf.exists():pytest.skip("локальный исходный проект ГП недоступен")
    data=json.loads(result_path.read_text())
    for page in data["pages"]:
        for block in page["blocks"]:
            if block.get("id")==block_id:return case,page,block
    raise AssertionError(block_id)


def polygon(page,block):
    value=block.get("polygon_points_norm")
    if not value and block.get("polygon_points"):value=[[x/page["width"],y/page["height"]] for x,y in block["polygon_points"]]
    return value


def test_gp_builds_from_original_pdf_polygon():
    case,page,block=source_case("474Q-GKNU-ACJ")
    graph=build_gp_graph_from_source(ROOT/case["source_pdf"],page_index=page["page_number"]-1,bbox_norm=block["coords_norm"],
      polygon_norm=polygon(page,block),block_id=case["block_id"],profile_hint=case["profile_id"],subtype_hint=case["subtype"])
    assert graph and graph["profile_id"]=="gp_general_plan" and evaluate_gp_gate(graph)["use"] is True


def test_router_returns_structured_gp(tmp_path):
    case,page,block=source_case("474Q-GKNU-ACJ");output=tmp_path/"_output";output.mkdir();(tmp_path/"document.pdf").symlink_to(ROOT/case["source_pdf"])
    (output/"document_graph.json").write_text(json.dumps({"pages":[{"page_index":page["page_number"]-1,"image_blocks":[{
      "id":case["block_id"],"coords_norm":block["coords_norm"],"polygon_points_norm":polygon(page,block)}]}]}))
    text,kind=resolve_block_source(output,case["block_id"],page["page_number"])
    assert kind=="structured_general_plan" and text and "Эталонная текстовая разметка ГП" in text
