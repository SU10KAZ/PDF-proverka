from __future__ import annotations

import copy
import tempfile
import unittest
from pathlib import Path

import fitz

from experiments.stage_comparison_vector_blocks.comparator import compare_descriptions
from experiments.stage_comparison_vector_blocks.extractor import _signatures, extract_block


class VectorBlockResearchTests(unittest.TestCase):
    def _pdf(self, path: Path, *, offset: float = 0.0, text: str = "250 A") -> None:
        document = fitz.open()
        page = document.new_page(width=200, height=200)
        shape = page.new_shape()
        shape.draw_rect(fitz.Rect(20 + offset, 20 + offset, 100 + offset, 100 + offset))
        shape.draw_line(fitz.Point(20 + offset, 60 + offset), fitz.Point(100 + offset, 60 + offset))
        shape.finish(color=(0, 0, 0), width=0.5)
        shape.commit()
        circle = page.new_shape()
        circle.draw_circle(fitz.Point(60 + offset, 60 + offset), 12)
        circle.finish(color=(0, 0, 0), width=0.5)
        circle.commit()
        page.insert_text(fitz.Point(35 + offset, 45 + offset), text, fontsize=8)
        document.save(path)
        document.close()

    def test_primitives_text_and_human_layers(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "sample.pdf"
            self._pdf(path)
            result = extract_block(
                path, page_index=0, bbox_norm=(0.1, 0.1, 0.6, 0.6), block_id="sample"
            )
        types = result["primitive_summary"]["primitive_types"]
        self.assertIn("rectangle", types)
        self.assertIn("circle", types)
        self.assertEqual(result["texts"][0]["text"], "250 A")
        self.assertIn("raw", result["geometry"]["primitives"][0])
        self.assertIn("normalized", result["geometry"]["primitives"][0])
        self.assertTrue(result["anchors"])

    def test_bbox_normalization_removes_translation(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            first_path, second_path = Path(directory) / "a.pdf", Path(directory) / "b.pdf"
            self._pdf(first_path)
            self._pdf(second_path, offset=40)
            first = extract_block(
                first_path, page_index=0, bbox_norm=(0.1, 0.1, 0.6, 0.6), block_id="a"
            )
            second = extract_block(
                second_path, page_index=0, bbox_norm=(0.3, 0.3, 0.8, 0.8), block_id="b"
            )
        self.assertNotEqual(
            first["structural_signature"]["level_1_exact_vector"],
            second["structural_signature"]["level_1_exact_vector"],
        )
        self.assertEqual(
            first["structural_signature"]["level_2_normalized_geometry"],
            second["structural_signature"]["level_2_normalized_geometry"],
        )
        self.assertEqual(compare_descriptions(first, second)["status"], "NEAR_IDENTICAL")

    def test_same_geometry_changed_value(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            first_path, second_path = Path(directory) / "a.pdf", Path(directory) / "b.pdf"
            self._pdf(first_path, text="250 A")
            self._pdf(second_path, text="315 A")
            first = extract_block(
                first_path, page_index=0, bbox_norm=(0.1, 0.1, 0.6, 0.6), block_id="a"
            )
            second = extract_block(
                second_path, page_index=0, bbox_norm=(0.1, 0.1, 0.6, 0.6), block_id="b"
            )
        comparison = compare_descriptions(first, second)
        self.assertEqual(comparison["status"], "STRUCTURE_SAME_VALUES_CHANGED")
        self.assertTrue(comparison["text"]["value_changes"])

    def test_pdf_command_order_does_not_change_signature(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "sample.pdf"
            self._pdf(path)
            first = extract_block(
                path, page_index=0, bbox_norm=(0.1, 0.1, 0.6, 0.6), block_id="a"
            )
        second = copy.deepcopy(first)
        second["block_id"] = "b"
        second["geometry"]["primitives"].reverse()
        second["structural_signature"] = _signatures(
            second["geometry"]["primitives"], second["texts"], second["topology"]
        )
        self.assertEqual(compare_descriptions(first, second)["status"], "IDENTICAL")

    def test_text_only_block_is_insufficient(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "text.pdf"
            document = fitz.open()
            page = document.new_page(width=200, height=200)
            page.insert_text(fitz.Point(50, 50), "only text")
            document.save(path)
            document.close()
            result = extract_block(
                path, page_index=0, bbox_norm=(0, 0, 1, 1), block_id="text"
            )
        self.assertEqual(result["vector_quality"], "VECTOR_DATA_INSUFFICIENT")

    def test_polygon_filter_and_hatch_candidates(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "hatch.pdf"
            document = fitz.open()
            page = document.new_page(width=200, height=200)
            shape = page.new_shape()
            for index in range(25):
                y = 20 + index * 2
                shape.draw_line(fitz.Point(20, y), fitz.Point(80, y))
            shape.draw_line(fitz.Point(150, 20), fitz.Point(190, 20))
            shape.finish(color=(0, 0, 0), width=0.5)
            shape.commit()
            document.save(path)
            document.close()
            result = extract_block(
                path,
                page_index=0,
                bbox_norm=(0, 0, 1, 1),
                polygon_norm=((0, 0), (0.5, 0), (0.5, 1), (0, 1)),
                block_id="hatch",
            )
        self.assertTrue(result["hatch_like_structures"])
        for primitive in result["geometry"]["primitives"]:
            self.assertLessEqual(primitive["raw"]["bbox"][2], 100)


if __name__ == "__main__":
    unittest.main()
