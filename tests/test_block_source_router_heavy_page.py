"""Тяжёлый лист не разбирается в вектор: блок идёт в Stage 01 картинкой.

Регрессия 22.10.2026: ЭОМ_2-1 с листами А0 (~1 млн контуров АР-подложки)
съел 22 ГБ пула block_context и повесил портал.
"""
from __future__ import annotations

import json

import fitz
import pytest

from backend.app.pipeline.stages.block_grounding import block_source_router as router


ENV = "BLOCK_VECTOR_PAGE_MAX_CONTENT_MB"


def _version(tmp_path, *, lines_on_heavy: int):
    """Версия v2-primary: 02_work/document.pdf + runs/r1/document_graph.json.

    Стр. 1 — лёгкая (одна линия), стр. 2 — «подложка» из lines_on_heavy штрихов.
    """
    version = tmp_path / "objects/O/disciplines/EOM/documents/D/versions/v001"
    work = version / "02_work"
    run = version / "03_analysis/runs/r1"
    work.mkdir(parents=True)
    run.mkdir(parents=True)
    doc = fitz.open()
    light = doc.new_page(width=600, height=400)
    light.draw_line((10, 10), (500, 10))
    light.insert_text((20, 50), "ЩР-1 QF1 ВВГнг-LS 3х2,5")
    heavy = doc.new_page(width=600, height=400)
    shape = heavy.new_shape()
    for i in range(lines_on_heavy):
        x = 10 + (i % 580)
        shape.draw_line((x, 20 + i % 300), (x + 0.3, 20.3 + i % 300))
    shape.finish(color=(0.86, 0.86, 0.86), width=0.72)
    shape.commit()
    heavy.insert_text((20, 50), "ЩР-2 QF2 ВВГнг-LS 5х4")
    doc.save(work / "document.pdf")
    doc.close()
    block = lambda bid: {"id": bid, "coords_norm": [0.0, 0.0, 1.0, 1.0]}
    graph = {"pages": [
        {"page_index": 0, "image_blocks": [block("B_LIGHT")]},
        {"page_index": 1, "image_blocks": [block("B_HEAVY")]},
    ]}
    (run / "document_graph.json").write_text(json.dumps(graph), encoding="utf-8")
    return work / "document.pdf", run


def _forbid_drawings(monkeypatch):
    def boom(*_a, **_k):
        raise AssertionError("get_drawings вызван на тяжёлом листе")

    monkeypatch.setattr(fitz.Page, "get_drawings", boom)
    monkeypatch.setattr(fitz.Page, "get_cdrawings", boom)


def test_limit_defaults_to_30_mb_and_zero_disables(monkeypatch):
    monkeypatch.delenv(ENV, raising=False)
    assert router.heavy_page_limit_mb() == 30.0
    monkeypatch.setenv(ENV, "garbage")
    assert router.heavy_page_limit_mb() == 30.0
    monkeypatch.setenv(ENV, "0")
    assert router._heavy_vector_page_mb("/nonexistent.pdf", 0) is None


def test_page_content_size_measures_heavy_page(tmp_path):
    pdf, _run = _version(tmp_path, lines_on_heavy=20000)
    light = router._page_content_mb(pdf, 0)
    heavy = router._page_content_mb(pdf, 1)
    assert light is not None and heavy is not None
    assert heavy > 0.2 > light


def test_heavy_page_block_goes_image_only_without_vector_parse(tmp_path, monkeypatch):
    _pdf, run = _version(tmp_path, lines_on_heavy=20000)
    monkeypatch.setenv(ENV, "0.2")
    _forbid_drawings(monkeypatch)

    package = router.resolve_block_package(run, "B_HEAVY", 2, prefer_prepared=False)

    assert package["source_kind"] == "image_only"
    assert package["user_text"] is None
    assert package["page"] == 2
    assert len(package["warnings"]) == 1
    assert package["warnings"][0].startswith("heavy_vector_page:")


def test_light_page_is_not_skipped(tmp_path, monkeypatch):
    pdf, _run = _version(tmp_path, lines_on_heavy=20000)
    monkeypatch.setenv(ENV, "0.2")
    assert router._heavy_vector_page_mb(pdf, 0) is None
    assert router._heavy_vector_page_mb(pdf, 1) == pytest.approx(router._page_content_mb(pdf, 1))


def test_unknown_block_skips_check_and_reports_block_not_found(tmp_path, monkeypatch):
    _pdf, run = _version(tmp_path, lines_on_heavy=20000)
    monkeypatch.setenv(ENV, "0.2")

    package = router.resolve_block_package(run, "B_MISSING", 1, prefer_prepared=False)

    assert package["source_kind"] == "block_not_found"
