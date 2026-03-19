"""Тесты для graph_builder — Document Knowledge Graph v2."""
import json
import pytest
from pathlib import Path

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from graph_builder import (
    build_document_graph_v2,
    build_local_text_links,
    _normalize_ocr_text,
    _extract_sheet_info,
    _normalize_sheet_no,
    _compute_locality_score,
    is_graph_v2,
    is_good_local_candidate,
    get_page_sheet_no,
    get_text_block_text,
    get_image_block_ocr,
    generate_locality_debug,
    normalize_block_id,
    normalize_block_ids_in_finding,
    LOCALITY_FAR_DISTANCE_THRESHOLD,
    LOCALITY_GOOD_SCORE_THRESHOLD,
)


# ─── _normalize_ocr_text ──────────────────────────────────────────────────

class TestNormalizeOcrText:
    def test_plain_text_passthrough(self):
        assert _normalize_ocr_text("Hello world") == "Hello world"

    def test_html_tags_removed(self):
        result = _normalize_ocr_text("<div><p>Text</p></div>")
        assert "<div>" not in result
        assert "<p>" not in result
        assert "Text" in result

    def test_html_entities_decoded(self):
        result = _normalize_ocr_text("A &amp; B &lt; C")
        assert result == "A & B < C"

    def test_br_to_newline(self):
        result = _normalize_ocr_text("Line1<br>Line2<br/>Line3")
        assert "Line1\nLine2\nLine3" == result

    def test_none_returns_empty(self):
        assert _normalize_ocr_text(None) == ""

    def test_empty_returns_empty(self):
        assert _normalize_ocr_text("") == ""

    def test_ocr_json_format(self):
        """OCR в JSON-формате должен нормализоваться в usable text."""
        html = '<div class="block"><p>Кабель ВВГнг(А)-FRLS 5x10</p></div>'
        result = _normalize_ocr_text(html)
        assert "Кабель ВВГнг(А)-FRLS 5x10" in result
        assert "<div" not in result


# ─── Sheet info extraction ─────────────────────────────────────────────────

class TestSheetInfo:
    def test_extract_from_stamp(self):
        blocks = [
            {"id": "A", "block_type": "text", "stamp_data": {
                "sheet_number": "5", "total_sheets": "15", "sheet_name": "План 1-го этажа"
            }},
        ]
        raw, name, conf = _extract_sheet_info(blocks)
        assert raw == "5 (из 15)"
        assert name == "План 1-го этажа"
        assert conf == "high"

    def test_missing_stamp(self):
        blocks = [{"id": "A", "block_type": "text"}]
        raw, name, conf = _extract_sheet_info(blocks)
        assert raw is None
        assert name is None
        assert conf == "missing"

    def test_normalize_sheet_no(self):
        assert _normalize_sheet_no("5 (из 15)") == "5"
        assert _normalize_sheet_no("12") == "12"
        assert _normalize_sheet_no(None) is None


# ─── Geometry-based locality ──────────────────────────────────────────────

class TestLocalityScore:
    def test_overlapping_blocks_high_score(self):
        """Перекрывающиеся блоки → высокий score."""
        img = [0.1, 0.1, 0.5, 0.5]
        txt = [0.2, 0.2, 0.4, 0.4]  # внутри image
        result = _compute_locality_score(img, txt, 1.41)
        assert result["score"] > 0.3
        assert result["containment"] > 0.5

    def test_distant_blocks_low_score(self):
        """Удалённые блоки → низкий score."""
        img = [0.0, 0.0, 0.1, 0.1]
        txt = [0.8, 0.8, 0.9, 0.9]
        result = _compute_locality_score(img, txt, 1.41)
        assert result["score"] < 0.2

    def test_text_right_bonus(self):
        """Текст справа от изображения → бонус."""
        img = [0.1, 0.2, 0.4, 0.6]
        txt = [0.42, 0.3, 0.6, 0.5]  # справа, вертикально перекрывается
        result = _compute_locality_score(img, txt, 1.41)
        assert result["position_bonus"] > 0

    def test_no_coords_zero_score(self):
        """Без координат → нулевой score."""
        result = _compute_locality_score([], [0.1, 0.1, 0.2, 0.2], 1.41)
        assert result["score"] == 0.0

    def test_nearby_blocks_medium_score(self):
        """Близко расположенные блоки → средний score."""
        img = [0.1, 0.1, 0.3, 0.3]
        txt = [0.1, 0.32, 0.3, 0.4]  # чуть ниже
        result = _compute_locality_score(img, txt, 1.41)
        assert result["score"] > 0.1


class TestIsGoodLocalCandidate:
    """Тесты для strict locality check."""

    def test_good_candidate(self):
        cand = {"score": 0.3, "distance_norm": 0.1, "reason": "nearby"}
        assert is_good_local_candidate(cand) is True

    def test_far_candidate_rejected(self):
        """far кандидат НЕ должен считаться хорошим."""
        cand = {"score": 0.2, "distance_norm": 0.4, "reason": "far"}
        assert is_good_local_candidate(cand) is False

    def test_low_score_rejected(self):
        cand = {"score": 0.05, "distance_norm": 0.1, "reason": "proximity"}
        assert is_good_local_candidate(cand) is False

    def test_high_distance_rejected(self):
        cand = {"score": 0.2, "distance_norm": 0.3, "reason": "proximity"}
        assert is_good_local_candidate(cand) is False

    def test_threshold_constants_sane(self):
        assert LOCALITY_FAR_DISTANCE_THRESHOLD > 0
        assert LOCALITY_FAR_DISTANCE_THRESHOLD < 1.0
        assert LOCALITY_GOOD_SCORE_THRESHOLD > 0
        assert LOCALITY_GOOD_SCORE_THRESHOLD < 1.0


class TestBuildLocalTextLinks:
    def test_basic_binding(self):
        """Базовый тест: ближайший text-блок привязывается к image."""
        page = {
            "text_blocks": [
                {"id": "T1", "text": "Спецификация", "coords_norm": [0.1, 0.1, 0.3, 0.2]},
                {"id": "T2", "text": "Примечание", "coords_norm": [0.7, 0.7, 0.9, 0.8]},
            ],
            "image_blocks": [
                {"id": "I1", "type": "схема", "coords_norm": [0.1, 0.25, 0.5, 0.6]},
            ],
        }
        result = build_local_text_links(page)
        assert "I1" in result
        assert len(result["I1"]) > 0
        ids = [c["text_block_id"] for c in result["I1"]]
        assert ids[0] == "T1"

    def test_no_text_blocks(self):
        page = {
            "text_blocks": [],
            "image_blocks": [{"id": "I1", "coords_norm": [0.1, 0.1, 0.5, 0.5]}],
        }
        result = build_local_text_links(page)
        assert result == {}

    def test_no_image_blocks(self):
        page = {
            "text_blocks": [{"id": "T1", "coords_norm": [0.1, 0.1, 0.3, 0.2]}],
            "image_blocks": [],
        }
        result = build_local_text_links(page)
        assert result == {}

    def test_top_k_limit(self):
        page = {
            "text_blocks": [
                {"id": f"T{i}", "text": f"Text {i}",
                 "coords_norm": [0.1 + i*0.05, 0.1, 0.2 + i*0.05, 0.2]}
                for i in range(10)
            ],
            "image_blocks": [
                {"id": "I1", "coords_norm": [0.1, 0.25, 0.5, 0.5]},
            ],
        }
        result = build_local_text_links(page, top_k=3)
        assert len(result["I1"]) <= 3

    def test_different_blocks_get_different_text(self):
        page = {
            "text_blocks": [
                {"id": "T_LEFT", "text": "Текст слева", "coords_norm": [0.05, 0.1, 0.15, 0.2]},
                {"id": "T_RIGHT", "text": "Текст справа", "coords_norm": [0.85, 0.1, 0.95, 0.2]},
            ],
            "image_blocks": [
                {"id": "I_LEFT", "coords_norm": [0.05, 0.25, 0.3, 0.5]},
                {"id": "I_RIGHT", "coords_norm": [0.7, 0.25, 0.95, 0.5]},
            ],
        }
        result = build_local_text_links(page)
        left_first = result["I_LEFT"][0]["text_block_id"]
        right_first = result["I_RIGHT"][0]["text_block_id"]
        assert left_first == "T_LEFT"
        assert right_first == "T_RIGHT"

    def test_stamp_blocks_excluded_from_locality(self):
        """Stamp/metadata блоки не должны участвовать в locality."""
        page = {
            "text_blocks": [
                {"id": "STAMP1", "text": "ООО Проект", "coords_norm": [0.1, 0.1, 0.3, 0.2],
                 "is_stamp": True},
                {"id": "T1", "text": "Спецификация", "coords_norm": [0.1, 0.3, 0.3, 0.4]},
            ],
            "image_blocks": [
                {"id": "I1", "coords_norm": [0.1, 0.45, 0.5, 0.7]},
            ],
        }
        result = build_local_text_links(page)
        candidate_ids = [c["text_block_id"] for c in result.get("I1", [])]
        assert "STAMP1" not in candidate_ids
        assert "T1" in candidate_ids


# ─── Graph version and compatibility ──────────────────────────────────────

class TestGraphCompatibility:
    def test_is_graph_v2(self):
        assert is_graph_v2({"version": 2}) is True
        assert is_graph_v2({"version": 1}) is False
        assert is_graph_v2({}) is False

    def test_get_page_sheet_no_v2(self):
        page = {"sheet_no_raw": "5 (из 15)", "sheet_no_normalized": "5"}
        assert get_page_sheet_no(page) == "5 (из 15)"

    def test_get_page_sheet_no_v1(self):
        page = {"sheet_no": "5 (из 15)"}
        assert get_page_sheet_no(page) == "5 (из 15)"

    def test_get_page_sheet_no_missing(self):
        page = {}
        assert get_page_sheet_no(page) is None

    def test_get_text_block_text(self):
        assert get_text_block_text({"text": "Hello"}) == "Hello"
        assert get_text_block_text({}) == ""

    def test_get_image_block_ocr_v2(self):
        ib = {"ocr_text_normalized": "План этажа", "ocr_raw": "<html>План</html>"}
        assert get_image_block_ocr(ib) == "План этажа"

    def test_get_image_block_ocr_v1(self):
        ib = {"ocr": "OCR description"}
        assert get_image_block_ocr(ib) == "OCR description"


# ─── Graph builder preserves data ─────────────────────────────────────────

class TestGraphBuilderPreservesData:
    def test_coords_preserved(self, tmp_path):
        """graph builder сохраняет coords_px и coords_norm."""
        result_json = {
            "pages": [{
                "page_number": 1,
                "width": 2480, "height": 3507,
                "blocks": [{
                    "id": "BLOCK1",
                    "page_index": 1,
                    "coords_px": [100, 200, 300, 400],
                    "coords_norm": [0.04, 0.057, 0.121, 0.114],
                    "block_type": "text",
                    "source": "user",
                    "shape_type": "rectangle",
                    "ocr_text": "Test text",
                }],
            }],
        }
        rj_path = tmp_path / "test_result.json"
        rj_path.write_text(json.dumps(result_json), encoding="utf-8")

        graph = build_document_graph_v2(tmp_path, tmp_path / "_output")
        assert graph is not None

        tb = graph["pages"][0]["text_blocks"][0]
        assert tb["coords_px"] == [100, 200, 300, 400]
        assert tb["coords_norm"] == [0.04, 0.057, 0.121, 0.114]

    def test_page_index_unified(self, tmp_path):
        """page = 1-based, page_index = 0-based, blocks use page (1-based)."""
        result_json = {
            "pages": [{
                "page_number": 5,
                "width": 2480, "height": 3507,
                "blocks": [{
                    "id": "B1",
                    "page_index": 5,
                    "coords_px": [10, 20, 30, 40],
                    "coords_norm": [0.01, 0.01, 0.02, 0.02],
                    "block_type": "image",
                    "source": "cloud",
                    "shape_type": "rectangle",
                    "ocr_text": "Plan",
                }],
            }],
        }
        rj_path = tmp_path / "test_result.json"
        rj_path.write_text(json.dumps(result_json), encoding="utf-8")

        graph = build_document_graph_v2(tmp_path, tmp_path / "_output")
        page = graph["pages"][0]
        assert page["page"] == 5          # 1-based
        assert page["page_index"] == 4    # 0-based

        ib = page["image_blocks"][0]
        assert ib["page"] == 5            # 1-based (unified with page-level)

    def test_missing_sheet_no_explicit(self, tmp_path):
        result_json = {
            "pages": [{
                "page_number": 1,
                "width": 100, "height": 100,
                "blocks": [{
                    "id": "B1",
                    "page_index": 1,
                    "coords_px": [0, 0, 50, 50],
                    "coords_norm": [0.0, 0.0, 0.5, 0.5],
                    "block_type": "text",
                    "source": "user",
                    "shape_type": "rectangle",
                    "ocr_text": "No stamp here",
                }],
            }],
        }
        rj_path = tmp_path / "test_result.json"
        rj_path.write_text(json.dumps(result_json), encoding="utf-8")

        graph = build_document_graph_v2(tmp_path, tmp_path / "_output")
        page = graph["pages"][0]
        assert page["sheet_no_raw"] is None
        assert page["sheet_confidence"] == "missing"

    def test_duplicate_page_numbers_no_crash(self, tmp_path):
        result_json = {
            "pages": [
                {
                    "page_number": 1, "width": 100, "height": 100,
                    "blocks": [{"id": "A", "page_index": 1,
                                "coords_px": [0,0,50,50], "coords_norm": [0,0,0.5,0.5],
                                "block_type": "text", "source": "user",
                                "shape_type": "rectangle", "ocr_text": "First"}],
                },
                {
                    "page_number": 1, "width": 100, "height": 100,
                    "blocks": [{"id": "B", "page_index": 1,
                                "coords_px": [0,0,50,50], "coords_norm": [0,0,0.5,0.5],
                                "block_type": "text", "source": "user",
                                "shape_type": "rectangle", "ocr_text": "Second"}],
                },
            ],
        }
        rj_path = tmp_path / "test_result.json"
        rj_path.write_text(json.dumps(result_json), encoding="utf-8")

        graph = build_document_graph_v2(tmp_path, tmp_path / "_output")
        assert graph is not None
        assert graph["total_pages"] == 2

    def test_stamp_blocks_marked(self, tmp_path):
        """Stamp blocks получают is_stamp=True."""
        result_json = {
            "pages": [{
                "page_number": 1, "width": 100, "height": 100,
                "blocks": [
                    {"id": "STAMP", "page_index": 1,
                     "coords_px": [0,0,50,50], "coords_norm": [0,0,0.5,0.5],
                     "block_type": "text", "source": "user",
                     "shape_type": "rectangle", "ocr_text": "Stamp",
                     "stamp_data": {"sheet_number": "1"}},
                    {"id": "NORMAL", "page_index": 1,
                     "coords_px": [50,0,100,50], "coords_norm": [0.5,0,1,0.5],
                     "block_type": "text", "source": "user",
                     "shape_type": "rectangle", "ocr_text": "Normal"},
                ],
            }],
        }
        rj_path = tmp_path / "test_result.json"
        rj_path.write_text(json.dumps(result_json), encoding="utf-8")

        graph = build_document_graph_v2(tmp_path, tmp_path / "_output")
        tbs = graph["pages"][0]["text_blocks"]
        stamp_tb = next(t for t in tbs if t["id"] == "STAMP")
        normal_tb = next(t for t in tbs if t["id"] == "NORMAL")
        assert stamp_tb["is_stamp"] is True
        assert normal_tb["is_stamp"] is False


# ─── page_sheet_map for v2 graph ──────────────────────────────────────────

class TestPageSheetMap:
    def test_v2_page_sheet_map_populated(self, tmp_path):
        """page_sheet_map для v2 графа реально наполняется из sheet_no_raw."""
        result_json = {
            "pages": [
                {
                    "page_number": 3, "width": 100, "height": 100,
                    "blocks": [{"id": "S1", "page_index": 3,
                                "coords_px": [0,0,50,50], "coords_norm": [0,0,0.5,0.5],
                                "block_type": "text", "source": "user",
                                "shape_type": "rectangle", "ocr_text": "stamp",
                                "stamp_data": {"sheet_number": "1", "total_sheets": "10"}}],
                },
                {
                    "page_number": 5, "width": 100, "height": 100,
                    "blocks": [{"id": "S2", "page_index": 5,
                                "coords_px": [0,0,50,50], "coords_norm": [0,0,0.5,0.5],
                                "block_type": "text", "source": "user",
                                "shape_type": "rectangle", "ocr_text": "stamp2",
                                "stamp_data": {"sheet_number": "3", "total_sheets": "10"}}],
                },
            ],
        }
        rj_path = tmp_path / "test_result.json"
        rj_path.write_text(json.dumps(result_json), encoding="utf-8")

        graph = build_document_graph_v2(tmp_path, tmp_path / "_output")
        assert graph is not None

        # Build page_sheet_map the same way as compact layer
        page_sheet_map = {}
        for pg in graph["pages"]:
            page_num = pg.get("page")
            sheet_no = pg.get("sheet_no_raw") or pg.get("sheet_no_normalized")
            if page_num is not None and sheet_no:
                page_sheet_map[str(page_num)] = sheet_no

        assert page_sheet_map["3"] == "1 (из 10)"
        assert page_sheet_map["5"] == "3 (из 10)"


# ─── _build_structured_block_context v2 ───────────────────────────────────

class TestStructuredBlockContextV2:
    def _make_v2_graph(self, pages):
        graph = {"version": 2, "pages": pages}
        from graph_builder import build_local_text_links
        for pg in graph["pages"]:
            if pg.get("text_blocks") and pg.get("image_blocks"):
                pg["local_text_links"] = build_local_text_links(pg)
        return graph

    def test_v2_context_has_local_and_target(self):
        from webapp.services.task_builder import _build_structured_block_context

        graph = self._make_v2_graph([{
            "page": 1,
            "sheet_no_raw": "1",
            "sheet_no_normalized": "1",
            "sheet_name": "План",
            "sheet_confidence": "high",
            "text_blocks": [
                {"id": "T1", "text": "Спецификация кабелей",
                 "coords_norm": [0.1, 0.1, 0.3, 0.2]},
            ],
            "image_blocks": [
                {"id": "I1", "type": "схема",
                 "ocr_text_normalized": "Однолинейная схема",
                 "coords_norm": [0.1, 0.25, 0.5, 0.6]},
            ],
        }])

        result = _build_structured_block_context(graph, ["I1"], [1])
        assert "[TARGET BLOCK OCR]" in result
        assert "Однолинейная схема" in result

    def test_v2_does_not_give_all_text_to_every_block(self):
        from webapp.services.task_builder import _build_structured_block_context

        graph = self._make_v2_graph([{
            "page": 1,
            "sheet_no_raw": "1",
            "sheet_no_normalized": "1",
            "sheet_name": "План",
            "sheet_confidence": "high",
            "text_blocks": [
                {"id": "T_LEFT", "text": "Текст для левого блока",
                 "coords_norm": [0.05, 0.1, 0.2, 0.2]},
                {"id": "T_RIGHT", "text": "Текст для правого блока",
                 "coords_norm": [0.8, 0.1, 0.95, 0.2]},
            ],
            "image_blocks": [
                {"id": "I_LEFT", "type": "план",
                 "ocr_text_normalized": "Левый план",
                 "coords_norm": [0.05, 0.25, 0.3, 0.5]},
                {"id": "I_RIGHT", "type": "схема",
                 "ocr_text_normalized": "Правый чертёж",
                 "coords_norm": [0.7, 0.25, 0.95, 0.5]},
            ],
        }])

        result = _build_structured_block_context(graph, ["I_LEFT", "I_RIGHT"], [1])
        sections = result.split("#### block_id:")
        assert len(sections) >= 3
        assert "[LOCAL TEXT CONTEXT]" in result or "[PAGE GLOBAL CONTEXT" in result

    def test_far_candidate_triggers_page_fallback(self):
        """Далёкий кандидат НЕ должен отключать PAGE GLOBAL CONTEXT."""
        from webapp.services.task_builder import _build_structured_block_context

        graph = self._make_v2_graph([{
            "page": 1,
            "sheet_no_raw": "1",
            "sheet_no_normalized": "1",
            "sheet_name": "План",
            "sheet_confidence": "high",
            "text_blocks": [
                # Далёкий text-блок — единственный кандидат
                {"id": "T_FAR", "text": "Далёкий текст",
                 "coords_norm": [0.9, 0.9, 0.99, 0.99]},
            ],
            "image_blocks": [
                {"id": "I1", "type": "план",
                 "ocr_text_normalized": "План",
                 "coords_norm": [0.05, 0.05, 0.3, 0.3]},
            ],
        }])

        result = _build_structured_block_context(graph, ["I1"], [1])
        # Далёкий кандидат → PAGE GLOBAL CONTEXT должен быть включён
        assert "PAGE GLOBAL CONTEXT" in result

    def test_v1_fallback_still_works(self):
        from webapp.services.task_builder import _build_structured_block_context

        graph = {"version": 1, "pages": [{
            "page": 1,
            "sheet_no": "1",
            "text_blocks": [
                {"id": "T1", "text": "Спецификация"},
            ],
            "image_blocks": [
                {"id": "I1", "type": "схема", "ocr": "OCR text"},
            ],
        }]}

        result = _build_structured_block_context(graph, ["I1"], [1])
        assert "T1" in result
        assert "Спецификация" in result
        assert "[TARGET BLOCK OCR]" not in result


# ─── Debug output ──────────────────────────────────────────────────────────

class TestDebugOutput:
    def test_generate_locality_debug(self, tmp_path):
        from graph_builder import build_local_text_links

        graph = {
            "version": 2,
            "pages": [{
                "page": 1,
                "sheet_no_raw": "1",
                "sheet_no_normalized": "1",
                "sheet_confidence": "high",
                "text_blocks": [
                    {"id": "T1", "text": "Spec", "coords_norm": [0.1, 0.1, 0.3, 0.2]},
                ],
                "image_blocks": [
                    {"id": "I1", "type": "plan", "coords_norm": [0.1, 0.25, 0.5, 0.5]},
                ],
            }],
        }
        for pg in graph["pages"]:
            pg["local_text_links"] = build_local_text_links(pg)

        debug_path = generate_locality_debug(graph, tmp_path)
        assert debug_path is not None
        assert debug_path.exists()

        data = json.loads(debug_path.read_text(encoding="utf-8"))
        assert len(data) == 1
        entry = data[0]
        assert entry["page"] == 1
        assert entry["sheet_no_raw"] == "1"
        assert entry["image_block_id"] == "I1"
        assert "selected_text_block_ids" in entry
        assert "good_local_candidates" in entry
        assert "local_text_candidates" in entry
        assert "page_global_text_used" in entry


# ─── Backfill locality in block_analyses ──────────────────────────────────

class TestBackfillLocality:
    def test_backfill_adds_fields(self, tmp_path):
        """Backfill добавляет selected_text_block_ids если LLM не заполнила."""
        # Создаём минимальный graph v2
        from graph_builder import build_document_graph_v2

        result_json = {
            "pages": [{
                "page_number": 1,
                "width": 2480, "height": 3507,
                "blocks": [
                    {"id": "T1", "page_index": 1,
                     "coords_px": [100,100,300,200], "coords_norm": [0.04,0.03,0.12,0.06],
                     "block_type": "text", "source": "user",
                     "shape_type": "rectangle", "ocr_text": "Спецификация"},
                    {"id": "IMG1", "page_index": 1,
                     "coords_px": [100,300,500,600], "coords_norm": [0.04,0.09,0.20,0.17],
                     "block_type": "image", "source": "cloud",
                     "shape_type": "rectangle", "ocr_text": "План этажа"},
                ],
            }],
        }
        rj_path = tmp_path / "test_result.json"
        rj_path.write_text(json.dumps(result_json), encoding="utf-8")
        output_dir = tmp_path / "_output"
        build_document_graph_v2(tmp_path, output_dir)

        # Симулируем block_analyses без locality полей (как от старой LLM)
        from blocks import _backfill_locality_from_graph

        block_analyses = [
            {"block_id": "IMG1", "page": 1, "findings": []},
        ]
        _backfill_locality_from_graph(output_dir, block_analyses)

        ba = block_analyses[0]
        assert "selected_text_block_ids" in ba
        assert "evidence_text_refs" in ba
        assert isinstance(ba["selected_text_block_ids"], list)
        assert isinstance(ba["evidence_text_refs"], list)

    def test_backfill_does_not_overwrite_llm_data(self, tmp_path):
        """Backfill НЕ перезаписывает данные от LLM."""
        from graph_builder import build_document_graph_v2
        from blocks import _backfill_locality_from_graph

        result_json = {
            "pages": [{
                "page_number": 1, "width": 100, "height": 100,
                "blocks": [
                    {"id": "T1", "page_index": 1,
                     "coords_px": [0,0,50,20], "coords_norm": [0,0,0.5,0.2],
                     "block_type": "text", "source": "user",
                     "shape_type": "rectangle", "ocr_text": "Text"},
                    {"id": "I1", "page_index": 1,
                     "coords_px": [0,30,50,80], "coords_norm": [0,0.3,0.5,0.8],
                     "block_type": "image", "source": "cloud",
                     "shape_type": "rectangle", "ocr_text": "Image"},
                ],
            }],
        }
        rj_path = tmp_path / "test_result.json"
        rj_path.write_text(json.dumps(result_json), encoding="utf-8")
        output_dir = tmp_path / "_output"
        build_document_graph_v2(tmp_path, output_dir)

        # LLM уже заполнила поля
        block_analyses = [
            {"block_id": "I1", "page": 1, "findings": [],
             "selected_text_block_ids": ["LLM_TB"],
             "evidence_text_refs": [{"text_block_id": "LLM_TB", "role": "table"}]},
        ]
        _backfill_locality_from_graph(output_dir, block_analyses)

        ba = block_analyses[0]
        # LLM данные сохранены
        assert ba["selected_text_block_ids"] == ["LLM_TB"]
        assert ba["evidence_text_refs"][0]["text_block_id"] == "LLM_TB"

    def test_backfill_without_graph_adds_empty_fields(self, tmp_path):
        """Без графа — гарантирует пустые поля."""
        from blocks import _backfill_locality_from_graph

        block_analyses = [
            {"block_id": "I1", "page": 1, "findings": []},
        ]
        # output_dir без document_graph.json
        output_dir = tmp_path / "_output"
        output_dir.mkdir()

        _backfill_locality_from_graph(output_dir, block_analyses)
        assert block_analyses[0]["selected_text_block_ids"] == []
        assert block_analyses[0]["evidence_text_refs"] == []


# ─── Block ID normalization ───────────────────────────────────────────────

class TestNormalizeBlockId:
    def test_filename_to_bare(self):
        assert normalize_block_id("block_IMG-001.png") == "IMG-001"

    def test_prefix_only(self):
        assert normalize_block_id("block_IMG-001") == "IMG-001"

    def test_extension_only(self):
        assert normalize_block_id("IMG-001.png") == "IMG-001"

    def test_bare_passthrough(self):
        assert normalize_block_id("IMG-001") == "IMG-001"

    def test_real_chandra_id(self):
        assert normalize_block_id("block_9HC9-XARH-C4R.png") == "9HC9-XARH-C4R"

    def test_bare_chandra_id(self):
        assert normalize_block_id("9HC9-XARH-C4R") == "9HC9-XARH-C4R"

    def test_jpg_extension(self):
        assert normalize_block_id("block_ABC.jpg") == "ABC"

    def test_none_returns_empty(self):
        assert normalize_block_id(None) == ""

    def test_empty_returns_empty(self):
        assert normalize_block_id("") == ""

    def test_whitespace_stripped(self):
        assert normalize_block_id("  IMG-001  ") == "IMG-001"


class TestNormalizeFinding:
    def test_normalizes_block_evidence(self):
        f = {"block_evidence": "block_IMG-001.png"}
        normalize_block_ids_in_finding(f)
        assert f["block_evidence"] == "IMG-001"

    def test_normalizes_related_block_ids(self):
        f = {"related_block_ids": ["block_A.png", "B", "block_C"]}
        normalize_block_ids_in_finding(f)
        assert f["related_block_ids"] == ["A", "B", "C"]

    def test_normalizes_evidence_block_id(self):
        f = {"evidence": [
            {"type": "image", "block_id": "block_X.png"},
            {"type": "text", "block_id": "Y"},
        ]}
        normalize_block_ids_in_finding(f)
        assert f["evidence"][0]["block_id"] == "X"
        assert f["evidence"][1]["block_id"] == "Y"

    def test_empty_finding_no_crash(self):
        f = {}
        normalize_block_ids_in_finding(f)
        assert f == {}

    def test_filters_empty_ids(self):
        f = {"related_block_ids": ["block_.png", "A", ""]}
        normalize_block_ids_in_finding(f)
        assert f["related_block_ids"] == ["A"]


class TestMergeNormalization:
    """Проверяем нормализацию в blocks.py merge path."""

    def test_merge_normalizes_findings(self):
        from blocks import _normalize_finding_block_ids

        finding = {
            "id": "G-001",
            "block_evidence": "block_9HC9-XARH-C4R.png",
            "related_block_ids": ["block_AAXX-HMJC-XLX.png"],
        }
        _normalize_finding_block_ids(finding)
        assert finding["block_evidence"] == "9HC9-XARH-C4R"
        assert finding["related_block_ids"] == ["AAXX-HMJC-XLX"]

    def test_already_bare_untouched(self):
        from blocks import _normalize_finding_block_ids

        finding = {
            "id": "G-002",
            "block_evidence": "9HC9-XARH-C4R",
        }
        _normalize_finding_block_ids(finding)
        assert finding["block_evidence"] == "9HC9-XARH-C4R"
