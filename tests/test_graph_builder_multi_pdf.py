"""
test_graph_builder_multi_pdf.py
--------------------------------
#6: детерминированная защита от коллизий page_number в multi-PDF проектах.

build_document_graph_v2 загружает несколько *_result.json (по одному на PDF).
page_number в каждом 1-based ОТНОСИТЕЛЬНО своего PDF, поэтому у разных PDF
страницы «1,2,...» совпадают и затирают друг друга (findings page→sheet уезжают
не на ту страницу). Фикс ремапит коллизии в глобально-уникальные номера и
сохраняет исходные source_file/source_page_number. Single-PDF не меняется.
"""
from __future__ import annotations

import json
from pathlib import Path

from backend.app.pipeline.stages.prepare.graph_builder import build_document_graph_v2


def _write_result_json(path: Path, page_numbers: list[int], *, prefix: str) -> None:
    """Минимальный result.json с текстовыми блоками (без image → без locality)."""
    pages = []
    for pn in page_numbers:
        pages.append({
            "page_number": pn,
            "width": 1000,
            "height": 700,
            "blocks": [
                {
                    "id": f"{prefix}_p{pn}_b1",
                    "block_type": "text",
                    "ocr_text": f"{prefix} страница {pn}",
                    "coords_px": [0, 0, 100, 50],
                    "coords_norm": [0.0, 0.0, 0.1, 0.07],
                    "source": "ocr",
                },
            ],
        })
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"pages": pages}, ensure_ascii=False), encoding="utf-8")


def test_multi_pdf_page_collisions_are_remapped(tmp_path):
    """Два PDF с пересекающимися page_number → глобально-уникальные номера."""
    proj = tmp_path / "proj"
    out = proj / "_output"
    pdf1 = proj / "a_result.json"
    pdf2 = proj / "b_result.json"
    _write_result_json(pdf1, [1, 2], prefix="A")
    _write_result_json(pdf2, [1, 2], prefix="B")  # коллизия с pdf1

    graph = build_document_graph_v2(proj, out, result_json_paths=[pdf1, pdf2])
    assert graph is not None

    # 4 страницы, все page numbers уникальны и непрерывны
    assert graph["total_pages"] == 4
    pages = sorted(p["page"] for p in graph["pages"])
    assert pages == [1, 2, 3, 4]
    assert len(set(pages)) == 4  # нет дублей

    # Ремап зафиксирован: pdf2 p1→p3, p2→p4
    remapped = graph["remapped_pages"]
    assert len(remapped) == 2
    by_src = {r["source_page_number"]: r for r in remapped}
    assert by_src[1]["source_file"] == "b_result.json"
    assert by_src[1]["assigned_page_number"] == 3
    assert by_src[2]["assigned_page_number"] == 4
    assert all(r["source_pdf_index"] == 1 for r in remapped)

    # У каждой страницы есть source_* поля; исходный номер сохранён
    pages_by_assigned = {p["page"]: p for p in graph["pages"]}
    p3 = pages_by_assigned[3]
    assert p3["source_file"] == "b_result.json"
    assert p3["source_pdf_index"] == 1
    assert p3["source_page_number"] == 1  # был p1 внутри своего PDF

    # Блоки тоже несут source_file (привязка к правильному PDF)
    assert p3["text_blocks"][0]["source_file"] == "b_result.json"
    p1 = pages_by_assigned[1]
    assert p1["source_file"] == "a_result.json"
    assert p1["text_blocks"][0]["source_file"] == "a_result.json"


def test_single_pdf_unchanged_no_remap(tmp_path):
    """Single-PDF: коллизий нет → ремапа нет, номера страниц прежние."""
    proj = tmp_path / "proj"
    out = proj / "_output"
    pdf1 = proj / "only_result.json"
    _write_result_json(pdf1, [1, 2, 3], prefix="X")

    graph = build_document_graph_v2(proj, out, result_json_paths=[pdf1])
    assert graph is not None

    assert graph["total_pages"] == 3
    assert sorted(p["page"] for p in graph["pages"]) == [1, 2, 3]
    assert graph["remapped_pages"] == []  # ничего не ремапилось

    # Новые поля additive: source_page_number == page (нет сдвига)
    for p in graph["pages"]:
        assert p["source_page_number"] == p["page"]
        assert p["source_file"] == "only_result.json"
        assert p["source_pdf_index"] == 0


def test_three_pdfs_chain_remap(tmp_path):
    """Три PDF, у всех страница 1 → 1, 2, 3 (непрерывная нумерация)."""
    proj = tmp_path / "proj"
    out = proj / "_output"
    paths = []
    for i, name in enumerate(["a", "b", "c"]):
        p = proj / f"{name}_result.json"
        _write_result_json(p, [1], prefix=name.upper())
        paths.append(p)

    graph = build_document_graph_v2(proj, out, result_json_paths=paths)
    assert graph is not None
    assert sorted(p["page"] for p in graph["pages"]) == [1, 2, 3]
    # первый не ремапится, остальные два — да
    assert len(graph["remapped_pages"]) == 2
