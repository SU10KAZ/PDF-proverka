"""Тесты пространственной группировки вектор-текста блока (block_text_clustering).

Проверяют геометрическое ядро: вертикальный стек с X-перекрытием склеивается в одну группу,
разнесённые по горизонтали колонки — в разные, порядок слов в строке по word_no.
"""
from backend.app.pipeline.stages.block_grounding.block_text_clustering import (
    cluster_atoms,
    compute_text_groups,
    line_atoms,
    render_grouped_text,
)
from backend.app.pipeline.stages.block_grounding.block_source_router import _extract_block


def _w(x0, y0, x1, y1, word, bno, lno, wno):
    return (x0, y0, x1, y1, word, bno, lno, wno)


def test_line_atoms_orders_words_by_word_no():
    words = [_w(20, 10, 30, 20, "мир", 0, 0, 1), _w(10, 10, 18, 20, "привет", 0, 0, 0)]
    atoms = line_atoms(words)
    assert len(atoms) == 1
    assert atoms[0]["text"] == "привет мир"


def test_vertical_stack_with_x_overlap_is_one_group():
    # Две строки стопкой, X-интервалы перекрываются, малый верт. зазор → одна группа.
    words = [
        _w(10, 10, 50, 20, "ВРУ", 0, 0, 0),
        _w(10, 22, 60, 32, "(корпус", 0, 1, 0),
    ]
    clusters = cluster_atoms(line_atoms(words))
    assert len(clusters) == 1
    assert clusters[0]["atoms"][0]["text"] == "ВРУ"
    assert clusters[0]["atoms"][1]["text"] == "(корпус"


def test_horizontally_separated_columns_are_separate_groups():
    # Один Y, но большой горизонтальный зазор без X-перекрытия → две группы (лечит «кашу» 2D).
    words = [
        _w(10, 10, 50, 20, "A", 0, 0, 0),
        _w(400, 10, 440, 20, "B", 1, 0, 0),
    ]
    clusters = cluster_atoms(line_atoms(words))
    assert len(clusters) == 2


def test_far_vertical_gap_not_merged():
    # X перекрываются, но верт. зазор огромный (>> высоты строки) → разные группы.
    words = [
        _w(10, 10, 50, 20, "верх", 0, 0, 0),
        _w(10, 500, 50, 510, "низ", 0, 1, 0),
    ]
    clusters = cluster_atoms(line_atoms(words))
    assert len(clusters) == 2


def test_render_grouped_text_has_delimiters():
    words = [_w(10, 10, 50, 20, "строка", 0, 0, 0)]
    txt = render_grouped_text(cluster_atoms(line_atoms(words)))
    assert "━━━ группа 1 ━━━" in txt
    assert "строка" in txt


def test_empty_input_is_safe():
    assert cluster_atoms([]) == []
    assert line_atoms([]) == []
    assert render_grouped_text([]) == ""


def test_neighbour_line_above_block_is_excluded_from_overlay_and_source(tmp_path):
    """Регрессия 4UGL: строка в пределах старого margin=1% не входит в блок."""
    import fitz

    pdf = tmp_path / "document.pdf"
    doc = fitz.open()
    page = doc.new_page(width=1000, height=1000)
    page.insert_text((300, 292), "OUTSIDE NEIGHBOUR TEXT")
    page.insert_text((300, 400), "INSIDE BLOCK TEXT")
    doc.save(pdf)
    doc.close()

    bbox = [0.2, 0.3, 0.8, 0.8]
    groups = compute_text_groups(pdf, bbox, page_index=0)
    grouped_text = "\n".join(
        line for group in groups for line in group["text"]
    )
    assert "INSIDE BLOCK TEXT" in grouped_text
    assert "OUTSIDE NEIGHBOUR TEXT" not in grouped_text

    extracted = _extract_block(
        pdf,
        {"pages": [{"page_index": 0, "image_blocks": [
            {"id": "BLOCK", "coords_norm": bbox}
        ]}]},
        "BLOCK",
    )
    assert extracted is not None
    assert "INSIDE BLOCK TEXT" in extracted[1]
    assert "OUTSIDE NEIGHBOUR TEXT" not in extracted[1]
