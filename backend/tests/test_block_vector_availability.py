from backend.app.pipeline.stages.block_context.contract import (
    VECTOR_GRAPH_MISSING_MESSAGE,
    adapt_legacy_summary,
    decorate_blocks_vector_state,
    source_has_vector_text,
)


def test_only_real_pdf_vector_sources_enable_txt():
    assert source_has_vector_text("structured_hvac") is True
    assert source_has_vector_text("structured_singleline") is True
    assert source_has_vector_text("raw_vector") is True

    assert source_has_vector_text("image_only") is False
    assert source_has_vector_text("legacy_enrichment") is False
    assert source_has_vector_text("no_sources") is False
    assert source_has_vector_text(None) is False


def test_blocks_are_decorated_with_exact_missing_graph_message():
    blocks = [{"block_id": "vector"}, {"block_id": "raster"}, {"block_id": "unknown"}]
    summary = {
        "blocks": [
            {"block_id": "vector", "source_kind": "raw_vector"},
            {"block_id": "raster", "source_kind": "image_only"},
        ]
    }

    decorate_blocks_vector_state(blocks, summary)

    assert blocks[0]["vector_text_available"] is True
    assert blocks[0]["vector_graph_message"] is None
    assert blocks[1]["vector_text_available"] is False
    assert blocks[1]["vector_graph_message"] == VECTOR_GRAPH_MISSING_MESSAGE
    # Неизвестное состояние не выдаём за доказанное отсутствие вектора.
    assert "vector_text_available" not in blocks[2]


def test_legacy_vision_summary_is_not_mistaken_for_vector_text():
    summary = adapt_legacy_summary({
        "blocks_total": 2,
        "blocks": [
            {"block_id": "ocr", "final_profile": "gemma_100_base"},
            {"block_id": "vector", "base_response_source": "vector_skip"},
        ],
    })
    blocks = [{"block_id": "ocr"}, {"block_id": "vector"}]

    decorate_blocks_vector_state(blocks, summary)

    assert blocks[0]["vector_graph_source_kind"] == "legacy_enrichment"
    assert blocks[0]["vector_text_available"] is False
    assert blocks[1]["vector_graph_source_kind"] == "raw_vector"
    assert blocks[1]["vector_text_available"] is True
