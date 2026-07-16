"""Тесты синтеза result.json из *_blocks.json (+ тексты из *_results.md).

Синтетическая фикстура повторяет реальную схему blocks.json (schema_version 1),
плюс сверка на реальных ZIP из experiments/новая структура. (skip, если нет).
"""
from __future__ import annotations

import json
import zipfile
from pathlib import Path

import pytest

from backend.app.services.common.blocks_json import (
    SYNTH_SOURCE,
    build_result_json,
    load_blocks_json,
    synthesize_result_json_file,
)
from backend.app.services.common.results_md import parse_results_md

REPO_ROOT = Path(__file__).resolve().parents[2]
SAMPLES_DIR = REPO_ROOT / "experiments" / "новая структура."

BLOCKS_DATA = {
    "schema_version": 1,
    "document_id": "doc_test",
    "document_name": "X_V1.pdf",
    "generated_at": "2026-07-15T05:51:33.174699Z",
    "coordinate_space": "normalized_page_top_left",
    "pages": [
        {"page_index": 0, "width_px": 1000, "height_px": 2000, "rotation": 0},
        {"page_index": 1, "width_px": 2000, "height_px": 1000, "rotation": 90},
    ],
    "blocks": [
        {"block_id": "blk_" + "a" * 32, "ordinal": 1, "page_index": 0,
         "page_label": 1, "block_type": "text", "shape_type": "rectangle",
         "status": "recognized", "export_status": "recognized",
         "coords_norm": [0.1, 0.2, 0.5, 0.4], "polygon_points": None,
         "crop_url": "https://vibe.cloud-ip.cc/api/crops/tok1"},
        {"block_id": "blk_" + "b" * 32, "ordinal": 2, "page_index": 1,
         "page_label": 2, "block_type": "image", "shape_type": "polygon",
         "status": "recognized", "export_status": "recognized",
         "coords_norm": [0.0, 0.0, 0.5, 0.5],
         "polygon_points": [[0.0, 0.0], [0.5, 0.0], [0.25, 0.5]],
         "crop_url": "https://vibe.cloud-ip.cc/api/crops/tok2"},
        {"block_id": "blk_" + "c" * 32, "ordinal": None, "page_index": 1,
         "page_label": 2, "block_type": "stamp", "shape_type": "rectangle",
         "status": "recognized", "export_status": "recognized",
         "coords_norm": [0.8, 0.9, 1.0, 1.0], "polygon_points": None,
         "crop_url": None},
    ],
}

MD_SAMPLE = f"""\
# Document: X_V1.pdf

Path: АР / X / X_V1.pdf

Generated: 2026-07-15 05:51:33 UTC

**Stamp:** Code: X-1 | Stage: Р | Object: Объект | Organization: ОРГ

---

## Page 1

### BLOCK #1 [TEXT]: blk_{"a" * 32}

> **Created:** 2026-07-07 15:22:34 UTC
> **Crop:** [Crop](https://vibe.cloud-ip.cc/api/crops/tok1)
> **Stamp:** Code: X-1 | Stage: Р | Sheet:  | Object: Объект | Name:  | Organization: ОРГ | Revisions:

Текст первого блока.

## Page 2

### BLOCK #2 [IMAGE]: blk_{"b" * 32}

> **Created:** 2026-07-07 15:23:00 UTC
> **Crop:** [Crop](https://vibe.cloud-ip.cc/api/crops/tok2)
> **Stamp:** Code: X-1 | Stage: Р | Sheet: 2 | Object: Объект | Name: План этажа | Organization: ОРГ | Revisions:

**[IMAGE]** | Type: План | Level: Этаж 1

**Summary:** План.
"""


@pytest.fixture()
def md_doc():
    return parse_results_md(MD_SAMPLE)


@pytest.fixture()
def result(md_doc):
    return build_result_json(BLOCKS_DATA, md_doc, pdf_name="X_V1.pdf")


class TestLoad:
    def test_load_valid(self, tmp_path):
        p = tmp_path / "x_blocks.json"
        p.write_text(json.dumps(BLOCKS_DATA), encoding="utf-8")
        assert load_blocks_json(p) is not None

    def test_load_broken_returns_none(self, tmp_path):
        p = tmp_path / "x_blocks.json"
        p.write_text("{broken", encoding="utf-8")
        assert load_blocks_json(p) is None
        p.write_text('{"pages": {}}', encoding="utf-8")
        assert load_blocks_json(p) is None


class TestBuild:
    def test_pages_shape(self, result):
        assert [p["page_number"] for p in result["pages"]] == [1, 2]
        assert result["pages"][0]["width"] == 1000
        assert result["pages"][0]["height"] == 2000
        assert result["pages"][1]["rotation"] == 90

    def test_marker_fields(self, result):
        assert result["source"] == SYNTH_SOURCE
        assert result["synthesized_from"] == "blocks_json"
        assert result["pdf_path"] == "X_V1.pdf"

    def test_text_block_coords_and_body(self, result):
        b = result["pages"][0]["blocks"][0]
        assert b["block_type"] == "text"
        assert b["coords_px"] == [100, 400, 500, 800]
        assert b["coords_norm"] == [0.1, 0.2, 0.5, 0.4]
        assert b["ocr_text"] == "Текст первого блока."
        assert b["created_at"] == "2026-07-07 15:22:34"  # без " UTC"
        assert b["source"] == SYNTH_SOURCE

    def test_image_block_polygon_scaled(self, result):
        b = next(x for x in result["pages"][1]["blocks"] if x["block_type"] == "image"
                 and x.get("category_code") != "stamp")
        assert b["polygon_points_norm"] == [[0.0, 0.0], [0.5, 0.0], [0.25, 0.5]]
        assert b["polygon_points"] == [[0, 0], [1000, 0], [500, 500]]
        assert b["ocr_json"]["type"] == "План"
        assert b["ocr_json"]["sections"]["summary"] == "План."

    def test_stamp_becomes_image_with_category_code(self, result):
        b = next(x for x in result["pages"][1]["blocks"] if x.get("category_code") == "stamp")
        assert b["block_type"] == "image"  # как в старом result.json
        assert b["crop_url"] == ""         # у штампов crop_url = null
        assert b["ocr_json"]["sheet_number"] == "2"
        assert b["ocr_json"]["sheet_name"] == "План этажа"
        assert b["ocr_json"]["document_code"] == "X-1"
        # ocr_text — JSON-строка (как в старом формате)
        assert json.loads(b["ocr_text"])["sheet_number"] == "2"

    def test_without_md_geometry_still_full(self):
        res = build_result_json(BLOCKS_DATA, None)
        assert sum(len(p["blocks"]) for p in res["pages"]) == 3
        b = res["pages"][0]["blocks"][0]
        assert b["ocr_text"] == "" and b["coords_px"] == [100, 400, 500, 800]

    def test_crop_pipeline_view(self, result):
        """Как это увидит crop_blocks: image-блоки без category_code=stamp."""
        targets = [b for p in result["pages"] for b in p["blocks"]
                   if b["block_type"] == "image" and b.get("category_code") != "stamp"]
        assert len(targets) == 1
        assert targets[0]["crop_url"].endswith("/tok2")


class TestSynthesizeFile:
    def _write_inputs(self, tmp_path):
        bj = tmp_path / "X_V1_blocks.json"
        bj.write_text(json.dumps(BLOCKS_DATA, ensure_ascii=False), encoding="utf-8")
        md = tmp_path / "X_V1_results.md"
        md.write_text(MD_SAMPLE, encoding="utf-8")
        return bj, md

    def test_creates_result_json(self, tmp_path):
        bj, md = self._write_inputs(tmp_path)
        dst = tmp_path / "work" / "result.json"
        assert synthesize_result_json_file(bj, dst, md, pdf_name="X_V1.pdf") is True
        data = json.loads(dst.read_text(encoding="utf-8"))
        assert data["source"] == SYNTH_SOURCE
        assert sum(len(p["blocks"]) for p in data["pages"]) == 3

    def test_never_overwrites_real_result_json(self, tmp_path):
        bj, md = self._write_inputs(tmp_path)
        dst = tmp_path / "result.json"
        dst.write_text(json.dumps({"pdf_path": "real", "pages": []}), encoding="utf-8")
        assert synthesize_result_json_file(bj, dst, md) is False
        assert json.loads(dst.read_text(encoding="utf-8"))["pdf_path"] == "real"

    def test_overwrites_own_synth(self, tmp_path):
        bj, md = self._write_inputs(tmp_path)
        dst = tmp_path / "result.json"
        assert synthesize_result_json_file(bj, dst, md) is True
        assert synthesize_result_json_file(bj, dst, md) is True  # повторно — ок

    def test_fail_soft_on_broken_blocks_json(self, tmp_path):
        bj = tmp_path / "X_V1_blocks.json"
        bj.write_text("{broken", encoding="utf-8")
        assert synthesize_result_json_file(bj, tmp_path / "r.json") is False
        assert not (tmp_path / "r.json").exists()


# ─── Интеграция: _sync_v2_work_copies синтезирует 02_work/result.json ───────

class TestSyncV2WorkCopies:
    def _layout(self, tmp_path, with_blocks=True, with_real_result=False):
        vdir = tmp_path / "versions" / "v001"
        inp = vdir / "01_input"
        inp.mkdir(parents=True)
        (inp / "X_V1.pdf").write_bytes(b"%PDF-1.7 fake")
        (inp / "X_V1_results.md").write_text(MD_SAMPLE, encoding="utf-8")
        if with_blocks:
            (inp / "X_V1_blocks.json").write_text(
                json.dumps(BLOCKS_DATA, ensure_ascii=False), encoding="utf-8")
        if with_real_result:
            (inp / "X_V1_result.json").write_text(
                json.dumps({"pdf_path": "real", "pages": []}), encoding="utf-8")
        info = {"pdf_file": "X_V1.pdf", "md_file": "X_V1_results.md"}
        return vdir, info

    def test_synthesizes_work_result_json(self, tmp_path):
        from backend.app.services.common.version_service import _sync_v2_work_copies
        vdir, info = self._layout(tmp_path)
        _sync_v2_work_copies(vdir, info)
        work = vdir / "02_work"
        assert (work / "blocks.json").is_file()
        data = json.loads((work / "result.json").read_text(encoding="utf-8"))
        assert data["source"] == SYNTH_SOURCE
        # тексты подтянуты из document.md (рабочей копии MD)
        first = data["pages"][0]["blocks"][0]
        assert first["ocr_text"] == "Текст первого блока."

    def test_real_result_json_wins_over_synth(self, tmp_path):
        from backend.app.services.common.version_service import _sync_v2_work_copies
        vdir, info = self._layout(tmp_path, with_real_result=True)
        _sync_v2_work_copies(vdir, info)
        data = json.loads((vdir / "02_work" / "result.json").read_text(encoding="utf-8"))
        assert data.get("pdf_path") == "real"
        assert data.get("source") != SYNTH_SOURCE

    def test_no_blocks_json_no_result(self, tmp_path):
        from backend.app.services.common.version_service import _sync_v2_work_copies
        vdir, info = self._layout(tmp_path, with_blocks=False)
        _sync_v2_work_copies(vdir, info)
        assert not (vdir / "02_work" / "result.json").exists()


# ─── Сверка на реальных выгрузках (если ZIP лежат на диске) ──────────────────

def _real_triples():
    if not SAMPLES_DIR.is_dir():
        return []
    out = []
    for zpath in sorted(SAMPLES_DIR.glob("*.zip")):
        try:
            zf = zipfile.ZipFile(zpath)
        except zipfile.BadZipFile:
            continue
        names = zf.namelist()
        md = next((n for n in names if n.endswith("_results.md")), None)
        bj = next((n for n in names if n.endswith("_blocks.json")), None)
        if md and bj:
            out.append((zpath.name, zf.read(md).decode("utf-8"),
                        json.loads(zf.read(bj))))
    return out


@pytest.mark.parametrize("zname,md_text,blocks_data",
                         _real_triples() or [pytest.param(None, None, None,
                                             marks=pytest.mark.skip(reason="реальные ZIP не найдены"))])
def test_real_samples_synthesize(zname, md_text, blocks_data):
    md_doc = parse_results_md(md_text)
    res = build_result_json(blocks_data, md_doc)
    assert len(res["pages"]) == len(blocks_data["pages"])
    n_expected = len(blocks_data["blocks"])
    assert sum(len(p["blocks"]) for p in res["pages"]) == n_expected
    for p in res["pages"]:
        for b in p["blocks"]:
            x0, y0, x1, y1 = b["coords_px"]
            assert 0 <= x0 <= x1 <= p["width"]
            assert 0 <= y0 <= y1 <= p["height"]
            if b.get("category_code") == "stamp":
                assert b["block_type"] == "image"
            elif b["block_type"] in ("text", "image"):
                # тексты подтянуты из MD
                assert b["ocr_text"] != "" or b["block_type"] == "image"
    stamps = [b for p in res["pages"] for b in p["blocks"]
              if b.get("category_code") == "stamp"]
    assert all("ocr_json" in b for b in stamps)
