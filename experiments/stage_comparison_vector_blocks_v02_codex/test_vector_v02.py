from __future__ import annotations

import copy
import tempfile
import unittest
from pathlib import Path

import fitz

from .comparator import compare_descriptions
from .extractor import PageCache, assess_text_quality, extract_block
from .gates import route_comparison
from .l3_change_only import build_l3_change_only


class VectorV02Tests(unittest.TestCase):
    def _pdf(
        self,
        path: Path,
        *,
        dash: str | None = None,
        fill: tuple[float, float, float] | None = None,
        color: tuple[float, float, float] = (0, 0, 0),
        width: float = 0.5,
        text: str = "250 A",
        geometry: bool = True,
    ) -> None:
        document = fitz.open()
        page = document.new_page(width=200, height=200)
        if geometry:
            shape = page.new_shape()
            shape.draw_rect(fitz.Rect(20, 20, 100, 100))
            shape.draw_line(fitz.Point(20, 60), fitz.Point(100, 60))
            shape.finish(color=color, fill=fill, width=width, dashes=dash)
            shape.commit()
            circle = page.new_shape()
            circle.draw_circle(fitz.Point(60, 60), 12)
            circle.finish(color=color, width=width, dashes=dash)
            circle.commit()
        if text:
            page.insert_text(fitz.Point(35, 45), text, fontsize=8)
        document.save(path)
        document.close()

    def _extract_pair(self, directory: str, **right_options):
        root = Path(directory)
        left_path, right_path = root / "left.pdf", root / "right.pdf"
        self._pdf(left_path)
        self._pdf(right_path, **right_options)
        cache = PageCache(root / "cache")
        kwargs = {"page_index": 0, "bbox_norm": (0.1, 0.1, 0.6, 0.6), "page_cache": cache}
        left = extract_block(left_path, block_id="left", **kwargs)
        right = extract_block(right_path, block_id="right", **kwargs)
        comparison = compare_descriptions(left, right)
        return left, right, comparison, route_comparison(left, right, comparison), cache

    def test_cache_hit_and_multiple_blocks_same_page(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "sample.pdf"
            self._pdf(path)
            cache = PageCache(Path(directory) / "cache")
            extract_block(path, page_index=0, bbox_norm=(0, 0, 0.7, 0.7), block_id="a", page_cache=cache)
            extract_block(path, page_index=0, bbox_norm=(0.2, 0.2, 1, 1), block_id="b", page_cache=cache)
            self.assertEqual(cache.stats["get_drawings_calls"], 1)
            self.assertEqual(cache.stats["memory_hits"], 1)
            cache.clear_memory()
            extract_block(path, page_index=0, bbox_norm=(0, 0, 1, 1), block_id="c", page_cache=cache)
            self.assertEqual(cache.stats["disk_hits"], 1)
            self.assertEqual(cache.stats["get_drawings_calls"], 1)

    def test_extractor_version_invalidates_cache(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "sample.pdf"
            self._pdf(path)
            first = PageCache(Path(directory) / "cache", extractor_version="v1")
            second = PageCache(Path(directory) / "cache", extractor_version="v2")
            self.assertNotEqual(first.cache_path(path, 0), second.cache_path(path, 0))
            first.get(path, 0); second.get(path, 0)
            self.assertEqual(first.stats["misses"], 1)
            self.assertEqual(second.stats["misses"], 1)

    def test_style_unchanged(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            _, _, comparison, _, _ = self._extract_pair(directory)
        self.assertEqual(comparison["style"]["changed_pairs"], 0)
        self.assertNotEqual(comparison["status"], "STYLE_ONLY_CHANGED")

    def test_style_only_solid_to_dashed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            _, _, comparison, _, _ = self._extract_pair(directory, dash="[3 2] 0")
        self.assertEqual(comparison["status"], "STYLE_ONLY_CHANGED")
        self.assertGreater(comparison["style"]["field_change_counts"].get("dash", 0), 0)

    def test_style_only_fill_added(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            _, _, comparison, _, _ = self._extract_pair(directory, fill=(0.8, 0.8, 0.8))
        self.assertEqual(comparison["status"], "STYLE_ONLY_CHANGED")
        self.assertGreater(comparison["style"]["field_change_counts"].get("fill", 0), 0)

    def test_style_width_and_color_changed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            _, _, comparison, _, _ = self._extract_pair(directory, color=(1, 0, 0), width=1.0)
        counts = comparison["style"]["field_change_counts"]
        self.assertGreater(counts.get("stroke", 0), 0)
        self.assertGreater(counts.get("width", 0), 0)

    def test_style_join_changed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            left, right, _, _, _ = self._extract_pair(directory)
        for primitive in right["geometry"]["primitives"]:
            primitive["style"]["line_join"] = 1
        comparison = compare_descriptions(left, right)
        self.assertEqual(comparison["status"], "STYLE_ONLY_CHANGED")
        self.assertGreater(comparison["style"]["field_change_counts"].get("line_join", 0), 0)

    def test_broken_text_routes_to_vision(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            left, right, comparison, _, _ = self._extract_pair(directory)
        broken = assess_text_quality([{"text": "\ufffd\ufffd\ufffd\x01", "font": "unknown", "font_size": 8, "bbox": [0, 0, 1, 1]}])
        self.assertEqual(broken["status"], "TEXT_BROKEN")
        right["text_quality"] = broken
        comparison = compare_descriptions(left, right)
        routing = route_comparison(left, right, comparison)
        self.assertFalse(routing["gates"]["text_ok"])
        self.assertEqual(routing["route"], "VECTOR_WITH_VISION")

    def test_capped_routes_to_vision(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory); path = root / "sample.pdf"; self._pdf(path)
            cache = PageCache(root / "cache")
            common = {"page_index": 0, "bbox_norm": (0, 0, 1, 1), "page_cache": cache, "topology_cap": 1}
            left = extract_block(path, block_id="a", **common)
            right = extract_block(path, block_id="b", **common)
            comparison = compare_descriptions(left, right)
            routing = route_comparison(left, right, comparison)
        self.assertTrue(left["cap_flags"]["topology_capped"])
        self.assertEqual(routing["route"], "VECTOR_WITH_VISION")

    def test_crop_mismatch(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory); path = root / "sample.pdf"; self._pdf(path)
            cache = PageCache(root / "cache")
            left = extract_block(path, page_index=0, bbox_norm=(0.1, 0.1, 0.6, 0.6), block_id="a", page_cache=cache)
            right = extract_block(path, page_index=0, bbox_norm=(0.0, 0.0, 1.0, 0.35), block_id="b", page_cache=cache)
            comparison = compare_descriptions(left, right)
            routing = route_comparison(left, right, comparison)
        self.assertEqual(comparison["status"], "CROP_MISMATCH")
        self.assertFalse(routing["gates"]["crop_ok"])

    def test_empty_vector_is_vision_only(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory); path = root / "text.pdf"; self._pdf(path, geometry=False)
            cache = PageCache(root / "cache")
            left = extract_block(path, page_index=0, bbox_norm=(0, 0, 1, 1), block_id="a", page_cache=cache)
            right = copy.deepcopy(left); right["block_id"] = "b"
            comparison = compare_descriptions(left, right)
            routing = route_comparison(left, right, comparison)
        self.assertEqual(routing["route"], "VISION_ONLY")

    def test_quality_fail_safe(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            left, right, comparison, routing, _ = self._extract_pair(directory)
        self.assertEqual(routing["route"], "VECTOR_OK")
        right["cap_flags"]["patterns_capped"] = True
        comparison = compare_descriptions(left, right)
        self.assertNotEqual(route_comparison(left, right, comparison)["route"], "VECTOR_OK")

    def test_l3_change_only_excludes_unchanged_and_hashes(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            left, right, comparison, routing, _ = self._extract_pair(directory, text="315 A")
        payload = build_l3_change_only(comparison, routing)
        serialized = str(payload).lower()
        self.assertNotIn("signature", serialized)
        self.assertNotIn("hash", serialized)
        self.assertNotIn("primitive", serialized)
        self.assertNotIn("unchanged", serialized)
        self.assertTrue(payload["changed_values"])


if __name__ == "__main__":
    unittest.main()
