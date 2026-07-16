"""Приём нового комплекта портала (pdf + *_results.md + *_results.html
[+ *_blocks.json], с 2026-07-15 — возможно ZIP'ом).

С 2026-07-13 портал vibe отдаёт новый комплект вместо старого квартета
(pdf + *_document.md + *_result.json + *_ocr.html); с 2026-07-16 в комплекте
может присутствовать опциональный *_blocks.json с геометрией блоков, а сам
комплект приходит ZIP-архивом. Этап 1: файлы нового комплекта не должны
теряться ни в одном контуре приёма, старый метод продолжает работать без
изменений (удаление приёма старого метода — после ~2026-08-14,
см. docs/new_upload_format.md).
"""
from __future__ import annotations

import importlib.util
import io
import sys
import zipfile
from pathlib import Path

import pytest

from backend.app.services.common.project_service import (
    _classify_upload_files,
    _compute_upload_fingerprint,
    _is_new_format_bundle,
    _upload_bundle_warnings,
)
from backend.app.services.common import md_resolver
from backend.app.services.storage import projects_v2_source_resolver as src_resolver

REPO_ROOT = Path(__file__).resolve().parents[2]

NEW_BUNDLE = [
    ("ПД-00260568-ЭМ_1-1_V1.pdf", b"%PDF-1.7 fake"),
    ("ПД-00260568-ЭМ_1-1_V1_results.md", "# Document: x.pdf\n\n## Page 1\n".encode("utf-8")),
    ("ПД-00260568-ЭМ_1-1_V1_results.html", b"<!DOCTYPE html><html></html>"),
]

OLD_BUNDLE = [
    ("13AB-RD-EM-K1 V1.pdf", b"%PDF-1.7 fake"),
    ("13AB-RD-EM-K1 V1_document.md", b"## STRANICA 1\n"),
    ("13AB-RD-EM-K1 V1_result.json", b"{}"),
    ("13AB-RD-EM-K1 V1_ocr.html", b"<html></html>"),
]

BLOCKS_JSON = (
    "ПД-00260568-ЭМ_1-1_V1_blocks.json",
    b'{"schema_version": 1, "pages": [], "blocks": []}',
)
NEW_BUNDLE_WITH_BLOCKS = NEW_BUNDLE + [BLOCKS_JSON]


def _zip_bytes(files: list[tuple[str, bytes]]) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for name, data in files:
            zf.writestr(name, data)
    return buf.getvalue()


def _v2lib():
    spec = importlib.util.spec_from_file_location(
        "v2lib_under_test", REPO_ROOT / "scripts" / "projects_v2" / "v2lib.py"
    )
    mod = importlib.util.module_from_spec(spec)
    sys.modules.setdefault("v2lib_under_test", mod)
    spec.loader.exec_module(mod)
    return mod


# ─── классификатор браузерной загрузки ───────────────────────────────────────

class TestClassifyUploadFiles:
    def test_new_bundle_nothing_ignored(self):
        cls = _classify_upload_files(NEW_BUNDLE)
        assert [n for n, _ in cls["pdfs"]] == ["ПД-00260568-ЭМ_1-1_V1.pdf"]
        assert [n for n, _ in cls["mds"]] == ["ПД-00260568-ЭМ_1-1_V1_results.md"]
        # _results.html раньше молча выбрасывался — теперь роль ocr
        assert [n for n, _ in cls["ocrs"]] == ["ПД-00260568-ЭМ_1-1_V1_results.html"]
        assert cls["results"] == []
        assert cls["ignored"] == []

    def test_old_bundle_unchanged(self):
        cls = _classify_upload_files(OLD_BUNDLE)
        assert len(cls["pdfs"]) == 1 and len(cls["mds"]) == 1
        assert [n for n, _ in cls["results"]] == ["13AB-RD-EM-K1 V1_result.json"]
        assert [n for n, _ in cls["ocrs"]] == ["13AB-RD-EM-K1 V1_ocr.html"]
        assert cls["ignored"] == []

    def test_random_html_still_ignored(self):
        cls = _classify_upload_files([("readme.html", b"<html></html>")])
        assert cls["ocrs"] == [] and cls["ignored"] == ["readme.html"]


class TestNewFormatBundleWarnings:
    def test_detects_new_format(self):
        cls = _classify_upload_files(NEW_BUNDLE)
        assert _is_new_format_bundle(cls) is True
        assert _is_new_format_bundle(_classify_upload_files(OLD_BUNDLE)) is False

    def test_new_format_without_blocks_json_warns(self):
        warns = _upload_bundle_warnings(True, False, True, new_format=True)
        assert len(warns) == 1
        assert "БЕЗ *_blocks.json" in warns[0]

    def test_new_format_with_blocks_json_mentions_geometry_source(self):
        warns = _upload_bundle_warnings(True, False, True, new_format=True,
                                        has_blocks_json=True)
        assert len(warns) == 1
        assert "*_blocks.json" in warns[0] and "БЕЗ" not in warns[0]

    def test_old_format_warning_text_unchanged(self):
        warns = _upload_bundle_warnings(True, False, True, new_format=False)
        assert warns == ["Не найден *_result.json — кроп блоков потребует подготовки."]


# ─── _blocks.json: геометрия нового комплекта (2026-07-16) ───────────────────

class TestBlocksJsonIngest:
    def test_blocks_json_classified_not_ignored(self):
        cls = _classify_upload_files(NEW_BUNDLE_WITH_BLOCKS)
        assert [n for n, _ in cls["blocks"]] == [BLOCKS_JSON[0]]
        assert cls["results"] == []
        assert cls["ignored"] == []

    def test_bundle_without_blocks_json_has_empty_blocks(self):
        cls = _classify_upload_files(NEW_BUNDLE)
        assert cls["blocks"] == []

    def test_fingerprint_differs_with_and_without_blocks_json(self):
        """Комплект с геометрией НЕ дубль того же комплекта без неё —
        иначе догрузить _blocks.json версией невозможно (409)."""
        fp_without = _compute_upload_fingerprint(_classify_upload_files(NEW_BUNDLE))
        fp_with = _compute_upload_fingerprint(_classify_upload_files(NEW_BUNDLE_WITH_BLOCKS))
        assert fp_without["bundle_fingerprint"] != fp_with["bundle_fingerprint"]
        assert fp_without["pdf_sha256"] == fp_with["pdf_sha256"]

    def test_blocks_json_alone_detected_as_new_format(self):
        cls = _classify_upload_files([("x.pdf", b"%PDF"), BLOCKS_JSON])
        assert _is_new_format_bundle(cls) is True


# ─── ZIP-комплект портала (с 2026-07-15) ─────────────────────────────────────

class TestZipExpansion:
    def test_zip_bundle_expands_and_classifies(self):
        zip_name = "ПД-00260568-ЭМ_1-1_V1.zip"
        cls = _classify_upload_files([(zip_name, _zip_bytes(NEW_BUNDLE_WITH_BLOCKS))])
        assert [n for n, _ in cls["pdfs"]] == ["ПД-00260568-ЭМ_1-1_V1.pdf"]
        assert [n for n, _ in cls["mds"]] == ["ПД-00260568-ЭМ_1-1_V1_results.md"]
        assert [n for n, _ in cls["ocrs"]] == ["ПД-00260568-ЭМ_1-1_V1_results.html"]
        assert [n for n, _ in cls["blocks"]] == [BLOCKS_JSON[0]]
        assert cls["ignored"] == []

    def test_zip_mixed_with_plain_files(self):
        files = [("extra_document.md", b"## X\n"),
                 ("bundle.zip", _zip_bytes(NEW_BUNDLE))]
        cls = _classify_upload_files(files)
        assert len(cls["pdfs"]) == 1
        assert {n for n, _ in cls["mds"]} == {"extra_document.md",
                                              "ПД-00260568-ЭМ_1-1_V1_results.md"}

    def test_zip_member_paths_flattened_and_macosx_skipped(self):
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as zf:
            zf.writestr("sub/dir/doc.pdf", b"%PDF")
            zf.writestr("__MACOSX/._doc.pdf", b"junk")
            zf.writestr(".hidden", b"junk")
        cls = _classify_upload_files([("a.zip", buf.getvalue())])
        assert [n for n, _ in cls["pdfs"]] == ["doc.pdf"]
        assert cls["ignored"] == []

    def test_nested_zip_not_expanded(self):
        outer = _zip_bytes([("inner.zip", _zip_bytes(NEW_BUNDLE))])
        cls = _classify_upload_files([("outer.zip", outer)])
        assert cls["pdfs"] == []
        assert cls["ignored"] == ["inner.zip"]

    def test_bad_zip_raises_upload_error(self):
        # класс исключения и функцию берём из ЖИВОГО модуля: соседние тесты
        # делают importlib.reload(project_service), и импортированный при
        # коллекции UploadFolderError перестаёт совпадать по идентичности
        from backend.app.services.common import project_service as ps
        with pytest.raises(ps.UploadFolderError, match="распаковать"):
            ps._classify_upload_files([("broken.zip", b"not a zip at all")])

    def test_zip_bomb_member_count_guard(self):
        from backend.app.services.common import project_service as ps
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as zf:
            for i in range(501):
                zf.writestr(f"f{i}.md", b"x")
        with pytest.raises(ps.UploadFolderError, match="слишком много файлов"):
            ps._classify_upload_files([("bomb.zip", buf.getvalue())])


# ─── мигратор v2lib.find_input_quad ──────────────────────────────────────────

class TestFindInputQuad:
    def test_new_bundle_md_and_html_mapped(self, tmp_path):
        v2lib = _v2lib()
        for name, data in NEW_BUNDLE:
            (tmp_path / name).write_bytes(data)
        quad = v2lib.find_input_quad(tmp_path)
        assert quad["pdf"].name.endswith(".pdf")
        # раньше _results.md терялся → в v2 доезжал только PDF
        assert quad["document_md"].name == "ПД-00260568-ЭМ_1-1_V1_results.md"
        assert quad["ocr_html"].name == "ПД-00260568-ЭМ_1-1_V1_results.html"
        assert quad["result_json"] is None

    def test_old_bundle_unchanged(self, tmp_path):
        v2lib = _v2lib()
        for name, data in OLD_BUNDLE:
            (tmp_path / name).write_bytes(data)
        quad = v2lib.find_input_quad(tmp_path)
        assert quad["document_md"].name.endswith("_document.md")
        assert quad["ocr_html"].name.endswith("_ocr.html")
        assert quad["result_json"].name.endswith("_result.json")

    def test_old_suffix_wins_when_both_present(self, tmp_path):
        v2lib = _v2lib()
        for name, data in NEW_BUNDLE + OLD_BUNDLE:
            (tmp_path / name).write_bytes(data)
        quad = v2lib.find_input_quad(tmp_path)
        assert quad["document_md"].name.endswith("_document.md")
        assert quad["ocr_html"].name.endswith("_ocr.html")

    def test_blocks_json_mapped(self, tmp_path):
        v2lib = _v2lib()
        for name, data in NEW_BUNDLE_WITH_BLOCKS:
            (tmp_path / name).write_bytes(data)
        quad = v2lib.find_input_quad(tmp_path)
        assert quad["blocks_json"] is not None
        assert quad["blocks_json"].name.endswith("_blocks.json")
        # _blocks.json НЕ путается с ролью result_json
        assert quad["result_json"] is None
        assert v2lib.WORK_NORMALIZED["blocks_json"] == "blocks.json"

    def test_blocks_json_absent_is_none(self, tmp_path):
        v2lib = _v2lib()
        for name, data in NEW_BUNDLE:
            (tmp_path / name).write_bytes(data)
        assert v2lib.find_input_quad(tmp_path)["blocks_json"] is None


# ─── резолвер источников projects_v2 ─────────────────────────────────────────

def _make_v2_version(tmp_path: Path, files: list[tuple[str, bytes]]) -> Path:
    vdir = tmp_path / "versions" / "v001"
    (vdir / "01_input").mkdir(parents=True)
    (vdir / "02_work").mkdir(parents=True)
    for name, data in files:
        (vdir / "01_input" / name).write_bytes(data)
    return vdir


class TestSourceResolver:
    def test_v2_resolves_results_md_and_html(self, tmp_path):
        vdir = _make_v2_version(tmp_path, NEW_BUNDLE)
        res = src_resolver.resolve_version_source_files(vdir)
        assert res.layout == "projects_v2"
        assert res.md_path is not None and res.md_path.name.endswith("_results.md")
        assert res.ocr_html_path is not None
        assert res.ocr_html_path.name.endswith("_results.html")
        assert res.result_json_path is None

    def test_v2_old_bundle_unchanged(self, tmp_path):
        vdir = _make_v2_version(tmp_path, OLD_BUNDLE)
        res = src_resolver.resolve_version_source_files(vdir)
        assert res.md_path.name.endswith("_document.md")
        assert res.ocr_html_path.name.endswith("_ocr.html")
        assert res.result_json_path.name.endswith("_result.json")

    def test_v2_document_md_priority_over_results_md(self, tmp_path):
        both = NEW_BUNDLE + [("ПД-00260568-ЭМ_1-1_V1_document.md", b"## STRANICA 1\n")]
        vdir = _make_v2_version(tmp_path, both)
        res = src_resolver.resolve_version_source_files(vdir)
        assert res.md_path.name.endswith("_document.md")

    def test_legacy_layout_results_md(self, tmp_path):
        vdir = tmp_path / "proj"
        vdir.mkdir()
        for name, data in NEW_BUNDLE:
            (vdir / name).write_bytes(data)
        res = src_resolver.resolve_version_source_files(vdir)
        assert res.layout == "legacy"
        assert res.md_path is not None and res.md_path.name.endswith("_results.md")
        assert res.ocr_html_path.name.endswith("_results.html")

    def test_v2_resolves_blocks_json(self, tmp_path):
        vdir = _make_v2_version(tmp_path, NEW_BUNDLE_WITH_BLOCKS)
        res = src_resolver.resolve_version_source_files(vdir)
        assert res.blocks_json_path is not None
        assert res.blocks_json_path.name.endswith("_blocks.json")
        v2 = src_resolver.resolve_v2_source_files(vdir)
        assert v2.blocks_json_path is not None

    def test_v2_work_normalized_blocks_json_wins(self, tmp_path):
        vdir = _make_v2_version(tmp_path, NEW_BUNDLE_WITH_BLOCKS)
        (vdir / "02_work" / "blocks.json").write_bytes(b"{}")
        res = src_resolver.resolve_version_source_files(vdir)
        assert res.blocks_json_path == vdir / "02_work" / "blocks.json"

    def test_blocks_json_absent_resolves_none(self, tmp_path):
        vdir = _make_v2_version(tmp_path, NEW_BUNDLE)
        res = src_resolver.resolve_version_source_files(vdir)
        assert res.blocks_json_path is None
        assert res.blocks_json_paths == ()

    def test_legacy_layout_blocks_json(self, tmp_path):
        vdir = tmp_path / "proj"
        vdir.mkdir()
        for name, data in NEW_BUNDLE_WITH_BLOCKS:
            (vdir / name).write_bytes(data)
        res = src_resolver.resolve_version_source_files(vdir)
        assert res.layout == "legacy"
        assert res.blocks_json_path is not None
        assert res.blocks_json_path.name.endswith("_blocks.json")


# ─── md_resolver ─────────────────────────────────────────────────────────────

class TestMdResolver:
    def test_is_doc_md_accepts_results_md(self):
        assert md_resolver._is_doc_md("x_results.md") is True
        assert md_resolver._is_doc_md("x_document.md") is True
        assert md_resolver._is_doc_md("readme.md") is False

    def test_stem_no_doc_strips_results_suffix(self):
        assert md_resolver._stem_no_doc("ABC_results.md") == "ABC"
        assert md_resolver._stem_no_doc("ABC_document.md") == "ABC"


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
