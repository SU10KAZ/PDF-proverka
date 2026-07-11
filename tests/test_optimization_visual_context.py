import json

import pytest

from backend.app.pipeline.stages.optimization.visual_context import (
    add_page_overviews,
    collect_optimization_visual_context,
)


def _write_block_fixture(tmp_path, records):
    image_dir = tmp_path / "blocks_gemma_100"
    image_dir.mkdir()
    index_blocks = []
    for record in records:
        filename = f"block_{record['block_id']}.png"
        (image_dir / filename).write_bytes(b"png")
        index_blocks.append({"block_id": record["block_id"], "file": filename})
    (image_dir / "index.json").write_text(
        json.dumps({"blocks": index_blocks}), encoding="utf-8"
    )
    (tmp_path / "02_blocks_analysis.json").write_text(
        json.dumps({"block_analyses": records}), encoding="utf-8"
    )


def test_visual_context_spreads_selected_blocks_across_pages(tmp_path):
    records = [
        {"block_id": "A", "page": 1, "summary": "План армирования колонн", "key_values_read": ["бетон B40"]},
        {"block_id": "B", "page": 1, "summary": "Узел армирования плиты", "key_values_read": ["сетка A500C"]},
        {"block_id": "C", "page": 2, "summary": "Сечение балки", "key_values_read": ["арматура"]},
    ]
    _write_block_fixture(tmp_path, records)

    context = collect_optimization_visual_context(tmp_path, max_images=2, discipline="KJ")

    assert len(context.attachments) == 2
    assert {attachment.page for attachment in context.attachments} == {1, 2}
    assert "Для КЖ" in context.prompt_section


def test_add_page_overviews_renders_selected_pages(tmp_path):
    fitz = pytest.importorskip("fitz")
    records = [
        {"block_id": "A", "page": 1, "summary": "План отопления", "key_values_read": ["коллектор"]},
        {"block_id": "B", "page": 2, "summary": "Схема стояков", "key_values_read": ["трубопровод"]},
    ]
    _write_block_fixture(tmp_path, records)
    context = collect_optimization_visual_context(tmp_path, max_images=2, discipline="OV")

    pdf_path = tmp_path / "document.pdf"
    document = fitz.open()
    document.new_page(width=800, height=600)
    document.new_page(width=800, height=600)
    document.save(str(pdf_path))
    document.close()

    enriched = add_page_overviews(
        context,
        pdf_path=pdf_path,
        render_dir=tmp_path / "overviews",
        max_overviews=2,
        discipline="OV",
    )

    overview_items = [item for item in enriched.attachments if "page_overview" in item.reasons]
    assert len(overview_items) == 2
    assert all(item.image_path.is_file() for item in overview_items)
    assert len(enriched.image_paths) == 4
    assert "Для ОВ" in enriched.prompt_section
