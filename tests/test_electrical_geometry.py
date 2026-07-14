import json
import re
from pathlib import Path

import fitz
import pytest

from backend.app.pipeline.stages.block_grounding.block_source_router import resolve_block_source
from backend.app.pipeline.stages.block_grounding.electrical_geometry import (
    ALL_ELECTRICAL_PROFILES,
    PROFILE_SINGLELINE,
    build_electrical_graph_from_source,
    evaluate_electrical_gate,
    render_electrical_markdown,
)
from backend.app.pipeline.stages.block_grounding.singleline_graph_geometry import evaluate_vectograf_gate


ROOT=Path(__file__).resolve().parents[1]
EOM=ROOT/"experiments"/"блоки разных дисциплин"/"ЭОМ"
MANIFEST=EOM/"EOM_DIVERSE_CORPUS.json";OUT=EOM/"eom_out"


def cases():return json.loads(MANIFEST.read_text()) if MANIFEST.exists() else []


@pytest.fixture(scope="module")
def graphs():return {case["block_id"]:json.loads((OUT/f"{case['block_id']}.structure.json").read_text()) for case in cases()}


@pytest.mark.parametrize("case",cases(),ids=lambda case:case["block_id"])
def test_eom_corpus_has_vector_pdf_and_accepted_graph(case,graphs):
    with fitz.open(EOM/case["output"]) as document:assert document.page_count==1
    assert case["drawing_paths"]>0 or case["embedded_images"]>0 or case["text_characters"]>0
    graph=graphs[case["block_id"]];assert graph["profile_id"]==case["profile_id"]
    gate=evaluate_vectograf_gate(graph) if graph["profile_id"]==PROFILE_SINGLELINE else evaluate_electrical_gate(graph)
    assert gate["use"] is True


def test_all_eom_families_and_subtypes_are_present(graphs):
    assert len(graphs)==157
    assert {graph["profile_id"] for graph in graphs.values()}==set(ALL_ELECTRICAL_PROFILES)
    assert len({case["subtype"] for case in cases()})==50


def test_generic_eom_references_are_integral(graphs):
    for graph in graphs.values():
        if graph["profile_id"]==PROFILE_SINGLELINE:continue
        nodes=graph.get("nodes",[]);networks=graph.get("networks",[]);containers=graph.get("containers",[])
        node_ids={node["id"] for node in nodes};network_ids={network["id"] for network in networks}
        assert len(node_ids)==len(nodes) and len(network_ids)==len(networks)
        assert len({container["id"] for container in containers})==len(containers)
        for network in networks:assert set(network.get("endpoint_ids",[]))<=node_ids
        for container in containers:assert set(container.get("member_ids",[]))<=node_ids
        for edge in graph.get("edges",[]):
            assert edge["from"] in node_ids and edge["to"] in node_ids
            if edge.get("network_id"):assert edge["network_id"] in network_ids


def test_generic_human_descriptions_are_russian_and_hide_internal_codes(graphs):
    for graph in graphs.values():
        if graph["profile_id"]==PROFILE_SINGLELINE:continue
        description=render_electrical_markdown(graph)
        assert "Эталонная текстовая разметка ЭОМ" in description and "Инженерное дерево" in description
        forbidden={graph["profile_id"],graph["validation"]["subtype"]}
        forbidden.update(network.get("network_type") for network in graph.get("networks",[]))
        forbidden.update(network.get("path_state") for network in graph.get("networks",[]))
        forbidden.update(edge.get("edge_state") for edge in graph.get("edges",[]))
        assert all(code not in description for code in forbidden if code)
        assert not re.search(r"\b(?:route|network|node|edge)-\d+\b",description)


def test_distinct_boundary_dialects_are_not_forced_into_vectograph(graphs):
    lighting=graphs["4UJ9-3D93-W7A"]
    assert lighting["profile_id"]=="panel_circuit_scheme"
    assert lighting["validation"]["protective_devices_total"]>=40
    assert lighting["validation"]["circuits_total"]>=40
    assert lighting["validation"]["confirmed_pairs_total"]>=1
    switchroom=graphs["GFEP-NT67-DEV"]
    assert switchroom["profile_id"]=="electrical_distribution_plan"
    assert switchroom["validation"]["panels_total"]>=8
    assert switchroom["validation"]["route_branches_total"]>=20


def test_only_source_limited_blocks_remain_partial(graphs):
    summary=json.loads((OUT/"summary.json").read_text())
    assert summary["gate_passed"]==157 and summary["complete_total"]==153
    partial={r["block_id"] for r in summary["records"] if not r["complete"]}
    assert partial=={"PVQW-637W-9L3","XN6M-XWHC-PRD","9WKW-LTVJ-4YU","FV7T-MNAR-ANC"}
    for block_id in partial:
        graph=graphs[block_id]
        assert graph["validation"]["source_layer_state"]=="no_pdf_text_layer"
        secondary=graph.get("secondary_description")
        if secondary:assert "x" not in secondary and "y" not in secondary


def test_eom_semantic_coverage_has_no_known_fact_losses():
    report=json.loads((EOM/"EOM_SEMANTIC_COVERAGE.json").read_text())
    assert report["blocks_total"]==157 and report["blocks_without_pdf_text_layer"]==4
    assert sum(record["pdf_misses_total"] for record in report["records"])==0


def test_common_words_do_not_become_protective_devices(graphs):
    false_prefix=re.compile(r"^(?:авар|автостоян|автор|автоматическ)",re.I)
    for graph in graphs.values():
        if graph["profile_id"]==PROFILE_SINGLELINE:continue
        assert not any(false_prefix.match(node.get("label","")) for node in graph.get("nodes",[])
          if node.get("node_type")=="protective_device")


def _source_case(block_id):
    case=next(case for case in cases() if case["block_id"]==block_id)
    result_path=ROOT/case["source_result"];source_pdf=ROOT/case["source_pdf"]
    if not result_path.exists() or not source_pdf.exists():pytest.skip("локальный исходный проект ЭОМ недоступен")
    result=json.loads(result_path.read_text())
    for page in result["pages"]:
        for block in page["blocks"]:
            if block.get("id")==block_id:return case,page,block
    raise AssertionError(block_id)


def _polygon(page,block):
    value=block.get("polygon_points_norm")
    if not value and block.get("polygon_points"):value=[[x/page["width"],y/page["height"]] for x,y in block["polygon_points"]]
    return value


def test_eom_graph_builds_from_original_pdf_polygon():
    case,page,block=_source_case("GFEP-NT67-DEV")
    graph=build_electrical_graph_from_source(ROOT/case["source_pdf"],page_index=page["page_number"]-1,
      bbox_norm=block["coords_norm"],polygon_norm=_polygon(page,block),block_id=case["block_id"],
      profile_hint=case["profile_id"],subtype_hint=case["subtype"])
    assert graph and graph["profile_id"]=="electrical_distribution_plan"
    assert evaluate_electrical_gate(graph)["use"] is True


def test_router_returns_structured_eom(tmp_path):
    case,page,block=_source_case("GFEP-NT67-DEV");output=tmp_path/"_output";output.mkdir()
    (tmp_path/"document.pdf").symlink_to(ROOT/case["source_pdf"])
    document_graph={"pages":[{"page_index":page["page_number"]-1,"image_blocks":[{
      "id":case["block_id"],"coords_norm":block["coords_norm"],"polygon_points_norm":_polygon(page,block)}]}]}
    (output/"document_graph.json").write_text(json.dumps(document_graph))
    text,kind=resolve_block_source(output,case["block_id"],page["page_number"])
    assert kind=="structured_electrical"
    assert text and "Эталонная текстовая разметка ЭОМ" in text and "План силовых сетей" in text
