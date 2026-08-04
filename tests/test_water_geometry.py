import json
import re
from pathlib import Path

import fitz
import pytest

from backend.app.pipeline.stages.block_grounding.block_source_router import resolve_block_source
from backend.app.pipeline.stages.block_grounding.water_geometry import (
    ALL_WATER_PROFILES,
    build_water_graph_from_source,
    evaluate_water_gate,
    render_water_markdown,
)


ROOT=Path(__file__).resolve().parents[1]
VK=ROOT/"experiments"/"блоки разных дисциплин"/"ВК"
MANIFEST=VK/"VK_DIVERSE_CORPUS.json";OUT=VK/"vk_out"


def cases():return json.loads(MANIFEST.read_text()) if MANIFEST.exists() else []


from backend.app.pipeline.stages.block_grounding.legend_geometry import (PROFILE_LEGEND,
    evaluate_legend_gate,render_legend_markdown)

# «Условные обозначения» — надведомственный профиль: легенда встречается в любом
# разделе, в набор профилей ВК не входит, гейт и рендер у неё свои.
def _is_legend(graph):return graph["profile_id"]==PROFILE_LEGEND
def _gate(graph):return evaluate_legend_gate(graph) if _is_legend(graph) else evaluate_water_gate(graph)
def _render(graph):return render_legend_markdown(graph) if _is_legend(graph) else render_water_markdown(graph)


@pytest.fixture(scope="module")
def graphs():return {case["block_id"]:json.loads((OUT/f"{case['block_id']}.structure.json").read_text()) for case in cases()}


@pytest.mark.parametrize("case",cases(),ids=lambda case:case["block_id"])
def test_vk_corpus_has_pdf_and_structured_description(case,graphs):
    with fitz.open(VK/case["output"]) as document:assert document.page_count==1
    assert case["drawing_paths"]>0 or case["embedded_images"]>0 or case["text_characters"]>0
    graph=graphs[case["block_id"]];assert graph["profile_id"]==case["profile_id"]
    gate=_gate(graph);assert gate["use"] is True
    description=_render(graph)
    assert ("Расшифровка обозначений" if _is_legend(graph)
            else "Эталонная текстовая разметка ВК") in description
    assert graph["profile_id"] not in description


def test_all_eleven_vk_families_are_present(graphs):
    assert len(graphs)==168
    assert {graph["profile_id"] for graph in graphs.values()}-{PROFILE_LEGEND}==set(ALL_WATER_PROFILES)


def test_vk_references_are_integral(graphs):
    for graph in graphs.values():
        nodes=graph.get("nodes",[]);networks=graph.get("networks",[]);containers=graph.get("containers",[])
        node_ids={node["id"] for node in nodes};network_ids={network["id"] for network in networks}
        assert len(node_ids)==len(nodes) and len(network_ids)==len(networks)
        assert len({container["id"] for container in containers})==len(containers)
        for network in networks:assert set(network.get("endpoint_ids",[]))<=node_ids
        for container in containers:assert set(container.get("member_ids",[]))<=node_ids
        for edge in graph.get("edges",[]):
            assert edge["from"] in node_ids and edge["to"] in node_ids
            if edge.get("network_id"):assert edge["network_id"] in network_ids


def test_vk_human_output_hides_internal_codes_and_states(graphs):
    for graph in graphs.values():
        description=_render(graph)
        forbidden={graph["profile_id"]}
        subtype=graph["validation"]["subtype"]
        if re.fullmatch(r"[a-z0-9_]+",str(subtype)):forbidden.add(subtype)
        forbidden.update(network.get("network_type") for network in graph.get("networks",[]))
        forbidden.update(network.get("path_state") for network in graph.get("networks",[]))
        forbidden.update(edge.get("edge_state") for edge in graph.get("edges",[]))
        assert all(code not in description for code in forbidden if code)
        assert not re.search(r"\b(?:route|network|node|circuit|system)-\d+\b",description)


def test_vk_descriptions_state_evidence_depth(graphs):
    allowed={"engineering_graph","semantic_hierarchy","physical_hierarchy","geometry_inventory",
             "spatial_inventory","analytical_geometry","raster_inventory"}
    depths={graph["validation"]["description_depth"] for graph in graphs.values()
            if not _is_legend(graph)}
    assert depths<=allowed and "engineering_graph" in depths and "physical_hierarchy" in depths
    for graph in graphs.values():
        if _is_legend(graph):
            assert "Уровень описания" in _render(graph);continue
        text=render_water_markdown(graph);assert "Уровень описания" in text and "Инженерное дерево" in text


def test_vk_semantic_coverage_has_no_known_fact_losses():
    report=json.loads((VK/"VK_SEMANTIC_COVERAGE.json").read_text())
    assert report["blocks_total"]==168
    assert sum(record["pdf_misses_total"] for record in report["records"])==0
    assert sum(record["manifest_hint_misses_total"] for record in report["records"])==0
    assert report["blocks_without_pdf_text_layer"]==27


def test_vk_secondary_facts_are_preserved_without_fake_coordinates(graphs):
    graph=graphs["7JJW-ATHW-AYM"];facts=graph["secondary_facts"]["facts"]
    assert graph["validation"]["source_layer_state"]=="no_pdf_text_layer"
    assert {fact["label"] for fact in facts}>={"Т3","Т4","В1","В12","Ø32x4.4","+1,400"}
    assert all(fact["evidence_state"]=="secondary_description_only" for fact in facts)
    assert all("x" not in fact and "y" not in fact for fact in facts)
    assert evaluate_water_gate(graph)["complete"] is False


def test_vk_document_codes_and_slopes_do_not_become_system_elevations(graphs):
    graph=graphs["6VW4-PCVA-TCN"]
    assert {node["label"] for node in graph["nodes"] if node["node_type"]=="system"}=={"К1","К1н"}
    assert not any(node["label"]=="0,014" for node in graph["nodes"] if node["node_type"]=="elevation")
    assert any(node["label"]=="0,014" for node in graph["nodes"] if node["node_type"]=="slope")


def _source_case(block_id):
    case=next(case for case in cases() if case["block_id"]==block_id);result_path=ROOT/case["source_result"];source_pdf=ROOT/case["source_pdf"]
    if not result_path.exists() or not source_pdf.exists():pytest.skip("локальный исходный проект ВК недоступен")
    result=json.loads(result_path.read_text())
    for page in result["pages"]:
        for block in page["blocks"]:
            if block.get("id")==block_id:return case,page,block
    raise AssertionError(block_id)


def _polygon(page,block):
    value=block.get("polygon_points_norm")
    if not value and block.get("polygon_points"):
        value=[[x/page["width"],y/page["height"]] for x,y in block["polygon_points"]]
    return value


def test_vk_graph_builds_from_original_pdf_polygon():
    case,page,block=_source_case("ACNM-K9RG-GAW")
    graph=build_water_graph_from_source(ROOT/case["source_pdf"],page_index=page["page_number"]-1,
      bbox_norm=block["coords_norm"],polygon_norm=_polygon(page,block),block_id=case["block_id"])
    assert graph and graph["profile_id"]=="vk_principle_scheme"
    assert evaluate_water_gate(graph)["use"] is True


def test_router_returns_structured_vk(tmp_path):
    case,page,block=_source_case("ACNM-K9RG-GAW");output=tmp_path/"_output";output.mkdir()
    (tmp_path/"document.pdf").symlink_to(ROOT/case["source_pdf"])
    document_graph={"pages":[{"page_index":page["page_number"]-1,"image_blocks":[{
      "id":case["block_id"],"coords_norm":block["coords_norm"],"polygon_points_norm":_polygon(page,block)}]}]}
    (output/"document_graph.json").write_text(json.dumps(document_graph))
    text,kind=resolve_block_source(output,case["block_id"],page["page_number"])
    assert kind=="structured_water"
    assert text and "Принципиальная схема узла ВК" in text
