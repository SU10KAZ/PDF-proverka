import json,re
from pathlib import Path
import fitz,pytest
from backend.app.pipeline.stages.block_grounding.technology_geometry import ALL_TX_PROFILES,evaluate_tx_gate,render_tx_markdown
from backend.app.pipeline.stages.block_grounding.legend_geometry import (PROFILE_LEGEND,
    evaluate_legend_gate,render_legend_markdown)

# «Условные обозначения» — надведомственный профиль: в набор профилей ТХ не входит.
def _gate(g):return evaluate_legend_gate(g) if g["profile_id"]==PROFILE_LEGEND else evaluate_tx_gate(g)
def _render(g):return render_legend_markdown(g) if g["profile_id"]==PROFILE_LEGEND else render_tx_markdown(g)
ROOT=Path(__file__).resolve().parents[1];TX=ROOT/"experiments"/"блоки разных дисциплин"/"ТХ";OUT=TX/"tx_out"
def cases():return json.loads((TX/"TX_DIVERSE_CORPUS.json").read_text())
@pytest.mark.parametrize("case",cases(),ids=lambda c:c["block_id"])
def test_tx_corpus(case):
 with fitz.open(TX/case["output"]) as d:assert d.page_count==1
 g=json.loads((OUT/f"{case['block_id']}.structure.json").read_text());assert g["profile_id"]==case["profile_id"] and _gate(g)["use"]
 text=_render(g);assert g["profile_id"] not in text
 if g["profile_id"]==PROFILE_LEGEND:assert "Расшифровка обозначений" in text
 else:assert "Эталонная текстовая разметка ТХ" in text and g["validation"]["subtype"] not in text
 assert not re.search(r"\b(?:node|view)-\d+\b",text)
def test_tx_coverage_and_semantics():
 gs=[json.loads((OUT/f"{c['block_id']}.structure.json").read_text()) for c in cases()]
 assert len(gs)==35 and {g["profile_id"] for g in gs}-{PROFILE_LEGEND}==set(ALL_TX_PROFILES)-{"tx_waste_plan"} and len({c["subtype"] for c in cases()})==10
 report=json.loads((TX/"TX_SEMANTIC_COVERAGE.json").read_text());assert sum(r["pdf_misses_total"] for r in report["records"])==0
