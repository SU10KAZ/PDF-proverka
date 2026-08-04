"""Профиль «Условные обозначения»: классификация и построчная привязка.

Корпусные PDF в репозиторий не входят, поэтому легенда собирается синтетически —
с той же раскладкой, что в реальных чертежах: колонка кодов слева, размерная
надпись над графическим образцом, колонка расшифровок справа.
"""
from __future__ import annotations

import fitz
import pytest

from backend.app.pipeline.stages.block_grounding.legend_geometry import (
    PROFILE_LEGEND,
    build_legend_graph,
    classify_legend_profile,
    evaluate_legend_gate,
    render_legend_markdown,
)

ROWS = [
    ("СН-1.2", "250", "Стена из газобетона D600 - 250мм"),
    ("СН-2.1", "120", "Каркас с обшивкой плитой - 120мм"),
    ("СВ-1.2", "200", "Стена из газобетона D500 - 200мм"),
    ("СВ-1.4", "100", "Перегородка из газобетона - 100мм"),
    ("СВ-3.1", "190", "Стена из бетонных камней - 190мм"),
]


@pytest.fixture()
def legend_pdf(tmp_path):
    doc = fitz.open()
    page = doc.new_page(width=560, height=280)
    page.insert_text((110, 24), "Условные обозначения", fontsize=11, fontname="china-ss")
    y = 60
    for code, value, text in ROWS:
        # размерная надпись стоит НАД образцом, который измеряет
        page.insert_text((62, y - 6), value, fontsize=6, fontname="china-ss")
        page.draw_rect(fitz.Rect(84, y - 4, 124, y + 4), color=(0, 0, 0), fill=(0.8, 0.8, 0.8))
        page.insert_text((20, y + 3), code, fontsize=7, fontname="china-ss")
        page.insert_text((132, y + 3), "- " + text, fontsize=7, fontname="china-ss")
        y += 34
    path = tmp_path / "legend.pdf"
    doc.save(path)
    doc.close()
    return path


def _block_text(path) -> str:
    with fitz.open(path) as doc:
        return doc[0].get_text()


def test_classify_legend_by_description_subject(legend_pdf):
    assert classify_legend_profile(
        _block_text(legend_pdf),
        description="Легенда условных обозначений строительных конструкций.",
    ) == PROFILE_LEGEND


def test_classify_legend_by_own_title(legend_pdf):
    """Заголовок в самом блоке достаточен, когда описания блока нет."""
    assert classify_legend_profile(_block_text(legend_pdf), description="") == PROFILE_LEGEND


def test_legend_is_subject_not_detail():
    """«Схема ... с условными обозначениями» — это схема, а не легенда."""
    text = "План квартиры\n- вывод электрический\n- розетка\n"
    assert classify_legend_profile(
        text,
        description="Схема подвода воды и канализации с условными обозначениями для квартиры №266.",
    ) is None


def test_json_description_does_not_promote_to_legend():
    """Старое описание-JSON темы блока не несёт и легендой блок не делает."""
    assert classify_legend_profile(
        "план этажа\n1200\n1500\n",
        description='{ "location": { "grid_lines": "условные обозначения" } }',
    ) is None


def test_large_block_with_legend_in_corner_is_not_legend():
    """Крупный чертёж с легендой в углу остаётся чертежом."""
    body = "Условные обозначения\n" + "\n".join(f"стена {i} длиной {1000 + i}" for i in range(200))
    assert classify_legend_profile(body, description="") is None


def test_axes_block_legend_classification_rejected():
    """Осевые марки — признак чертежа, а не самостоятельной легенды."""
    body = "Условные обозначения\n3.Б\n3.В\n3.Г\n" + "\n".join(f"- строка {i}" for i in range(6))
    assert classify_legend_profile(body, description="") is None


def test_legend_rows_bind_code_value_and_meaning(legend_pdf):
    graph = build_legend_graph(legend_pdf, block_id="TEST-LEGEND")
    assert graph is not None
    assert graph["profile_id"] == PROFILE_LEGEND
    validation = graph["validation"]
    assert validation["legend_entries_total"] == len(ROWS)
    assert validation["legend_entries_with_code"] == len(ROWS)
    # каждая толщина привязана геометрически И подтверждена числом в расшифровке
    assert validation["legend_values_total"] == len(ROWS)
    assert validation["legend_values_text_confirmed"] == len(ROWS)
    assert validation["edges_total"] == 2 * len(ROWS)


def test_legend_value_binding_is_marked_when_text_disagrees(tmp_path):
    """Размер, которого нет в расшифровке, помечается как привязанный только геометрией."""
    doc = fitz.open()
    page = doc.new_page(width=520, height=120)
    page.insert_text((110, 20), "Условные обозначения", fontsize=10, fontname="china-ss")
    page.insert_text((62, 44), "777", fontsize=6, fontname="china-ss")
    page.draw_rect(fitz.Rect(84, 46, 124, 54), color=(0, 0, 0))
    page.insert_text((20, 53), "СВ-9.9", fontsize=7, fontname="china-ss")
    page.insert_text((132, 53), "- Перегородка без указания толщины", fontsize=7, fontname="china-ss")
    path = tmp_path / "legend_mismatch.pdf"
    doc.save(path)
    doc.close()

    graph = build_legend_graph(path, block_id="TEST-MISMATCH")
    assert graph is not None
    assert graph["validation"]["legend_values_total"] == 1
    assert graph["validation"]["legend_values_text_confirmed"] == 0
    states = {edge["edge_state"] for edge in graph["edges"] if edge["edge_type"] == "параметр"}
    assert states == {"legend_value_geometry_only"}


def test_legend_markdown_renders_decoding_table(legend_pdf):
    graph = build_legend_graph(legend_pdf, block_id="TEST-LEGEND")
    markdown = render_legend_markdown(graph)
    assert "Условные обозначения" in markdown
    assert "| Код | Параметр | Значение обозначения |" in markdown
    for code, value, text in ROWS:
        assert code in markdown
        assert value in markdown
        assert text in markdown


def test_legend_gate_requires_two_rows(tmp_path):
    doc = fitz.open()
    page = doc.new_page(width=520, height=80)
    page.insert_text((60, 20), "Условные обозначения", fontsize=10, fontname="china-ss")
    page.insert_text((40, 50), "- единственная строка расшифровки", fontsize=7, fontname="china-ss")
    path = tmp_path / "legend_short.pdf"
    doc.save(path)
    doc.close()

    graph = build_legend_graph(path, block_id="TEST-SHORT")
    gate = evaluate_legend_gate(graph)
    assert gate["use"] is False
    assert "строк легенды меньше двух" in gate["reasons"]


def test_legend_graph_is_none_without_text_layer(tmp_path):
    doc = fitz.open()
    doc.new_page(width=200, height=100)
    path = tmp_path / "empty.pdf"
    doc.save(path)
    doc.close()
    assert build_legend_graph(path, block_id="TEST-EMPTY") is None


def test_router_sends_legend_block_to_legend_profile(tmp_path, monkeypatch, legend_pdf):
    """Легенда на листе плана не должна наследовать профиль листа."""
    from types import SimpleNamespace

    from backend.app.pipeline.stages.block_grounding import block_source_router as router

    output = tmp_path / "objects/O/disciplines/AR/documents/D/versions/v1/out"
    output.mkdir(parents=True)
    graph_path = output / "document_graph.json"
    graph_path.write_text('{"pages": []}', encoding="utf-8")
    block_text = _block_text(legend_pdf)

    monkeypatch.setattr(router, "_locate", lambda _: (legend_pdf, graph_path))
    monkeypatch.setattr(
        router, "_extract_block",
        lambda *_a: ("План потолка и освещения", block_text, [0, 0, 1, 1], None, 1),
    )
    monkeypatch.setattr(
        router, "_load_chandra_description",
        lambda *_a: SimpleNamespace(
            block_type="image",
            classification_text="План потолка и освещения",
            short_description="ceiling_and_lighting",
            description="Легенда условных обозначений строительных конструкций.",
        ),
    )

    package = router.resolve_block_package(output, "LEGEND-BLOCK", 1, prefer_prepared=False)

    assert package["source_kind"] == "structured_legend"
    assert package["profile_id"] == PROFILE_LEGEND
    assert package["graph"]["validation"]["legend_entries_total"] == len(ROWS)
    assert "Расшифровка обозначений" in package["markdown"]
    for code, _value, _text in ROWS:
        assert code in package["markdown"]


def test_router_does_not_hijack_plan_block_into_legend(tmp_path, monkeypatch, legend_pdf):
    """Чертёж, лишь упоминающий условные обозначения, легендой не становится."""
    from types import SimpleNamespace

    from backend.app.pipeline.stages.block_grounding import block_source_router as router

    output = tmp_path / "objects/O/disciplines/AR/documents/D/versions/v1/out"
    output.mkdir(parents=True)
    graph_path = output / "document_graph.json"
    graph_path.write_text('{"pages": []}', encoding="utf-8")
    block_text = "Условные обозначения\n3.Б\n3.В\n3.Г\n" + "\n".join(
        f"- строка {i} длиной {1000 + i}" for i in range(8))

    monkeypatch.setattr(router, "_locate", lambda _: (legend_pdf, graph_path))
    monkeypatch.setattr(
        router, "_extract_block",
        lambda *_a: ("Кладочный план", block_text, [0, 0, 1, 1], None, 1),
    )
    monkeypatch.setattr(
        router, "_load_chandra_description",
        lambda *_a: SimpleNamespace(
            block_type="image", classification_text="Кладочный план",
            short_description="masonry", description="Кладочный план с условными обозначениями.",
        ),
    )

    package = router.resolve_block_package(output, "PLAN-BLOCK", 1, prefer_prepared=False)

    assert package["source_kind"] != "structured_legend"
    assert package["profile_id"] != PROFILE_LEGEND
