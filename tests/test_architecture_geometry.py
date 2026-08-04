import json,re
from pathlib import Path
import fitz,pytest
from backend.app.pipeline.stages.block_grounding.architecture_geometry import ALL_AR_PROFILES,build_ar_graph_from_source,evaluate_ar_gate,render_ar_markdown
from backend.app.pipeline.stages.block_grounding.block_source_router import resolve_block_source
from backend.app.pipeline.stages.block_grounding.legend_geometry import (PROFILE_LEGEND,
    evaluate_legend_gate,render_legend_markdown)

# «Условные обозначения» — надведомственный профиль: легенда встречается в любом
# разделе, поэтому в наборы профилей дисциплины она не входит и проверяется
# своим гейтом и своим рендером.
def _gate(g):return evaluate_legend_gate(g) if g["profile_id"]==PROFILE_LEGEND else evaluate_ar_gate(g)
def _render(g):return render_legend_markdown(g) if g["profile_id"]==PROFILE_LEGEND else render_ar_markdown(g)
ROOT=Path(__file__).resolve().parents[1];AR=ROOT/"experiments"/"блоки разных дисциплин"/"АР";OUT=AR/"ar_out"
def cases():return json.loads((AR/"AR_DIVERSE_CORPUS.json").read_text()) if (AR/"AR_DIVERSE_CORPUS.json").exists() else []
@pytest.fixture(scope="module")
def graphs():return {c["block_id"]:json.loads((OUT/f"{c['block_id']}.structure.json").read_text()) for c in cases()}
@pytest.mark.parametrize("case",cases(),ids=lambda c:c["block_id"])
def test_ar_corpus_has_vector_pdf_and_accepted_graph(case,graphs):
    with fitz.open(AR/case["output"]) as d:assert d.page_count==1 and (d[0].get_text() or d[0].get_drawings() or d[0].get_images())
    g=graphs[case["block_id"]];assert g["profile_id"]==case["profile_id"] and _gate(g)["use"] is True
def test_all_ar_profiles_and_subtypes_present(graphs):
    assert len(graphs)==247
    assert {g["profile_id"] for g in graphs.values()}-{PROFILE_LEGEND}==set(ALL_AR_PROFILES)
    assert len({c["subtype"] for c in cases()})==28
def test_ar_reference_integrity(graphs):
    for g in graphs.values():
        ids={n["id"] for n in g.get("nodes",[])}
        assert len(ids)==len(g.get("nodes",[]))
        for c in g.get("containers",[]):assert set(c.get("member_ids",[]))<=ids
        for e in g.get("edges",[]):assert e["from"] in ids and e["to"] in ids
def test_ar_profile_specific_structures(graphs):
    assert graphs["7DU7-346V-DN6"]["validation"]["physical_line_segments_total"]>100
    assert graphs["69JM-X6EC-UTQ"]["validation"]["views_total"]>=2
    assert graphs["67AU-N79T-VUA"]["validation"]["openings_total"]>=1
    assert graphs["694C-YAJW-3Q6"]["validation"]["physical_line_segments_total"]>100
def test_ar_human_output_is_russian(graphs):
    for g in graphs.values():
        text=_render(g)
        if g["profile_id"]==PROFILE_LEGEND:
            assert "Расшифровка обозначений" in text and "| Код | Параметр |" in text
        else:
            assert "Эталонная текстовая разметка АР" in text and "Архитектурное дерево" in text
            assert g["validation"]["subtype"] not in text
        assert g["profile_id"] not in text
        assert not re.search(r"\b(?:node|view|edge)-\d+\b",text)
def test_ar_semantic_audit_has_no_known_losses():
    r=json.loads((AR/"AR_SEMANTIC_COVERAGE.json").read_text());assert r["blocks_total"]==247
    assert sum(x["pdf_misses_total"] for x in r["records"])==0
def source_case(block_id):
    c=next(c for c in cases() if c["block_id"]==block_id);result_path=ROOT/c["source_result"];source_pdf=ROOT/c["source_pdf"]
    if not result_path.exists() or not source_pdf.exists():pytest.skip("локальный исходный проект АР недоступен")
    d=json.loads(result_path.read_text())
    for p in d["pages"]:
        for b in p["blocks"]:
            if b.get("id")==block_id:return c,p,b
def polygon(page,b):
    v=b.get("polygon_points_norm")
    if not v and b.get("polygon_points"):v=[[x/page["width"],y/page["height"]] for x,y in b["polygon_points"]]
    return v
def test_ar_builds_from_original_polygon():
    c,p,b=source_case("7DU7-346V-DN6");g=build_ar_graph_from_source(ROOT/c["source_pdf"],page_index=p["page_number"]-1,bbox_norm=b["coords_norm"],polygon_norm=polygon(p,b),block_id=c["block_id"],profile_hint=c["profile_id"],subtype_hint=c["subtype"])
    assert g and g["profile_id"]=="ar_masonry_plan" and evaluate_ar_gate(g)["use"]
def test_router_returns_structured_ar(tmp_path):
    c,p,b=source_case("7DU7-346V-DN6");out=tmp_path/"_output";out.mkdir();(tmp_path/"document.pdf").symlink_to(ROOT/c["source_pdf"])
    (out/"document_graph.json").write_text(json.dumps({"pages":[{"page_index":p["page_number"]-1,"image_blocks":[{"id":c["block_id"],"coords_norm":b["coords_norm"],"polygon_points_norm":polygon(p,b)}]}]}))
    text,kind=resolve_block_source(out,c["block_id"],p["page_number"]);assert kind=="structured_architecture" and "Эталонная текстовая разметка АР" in text
