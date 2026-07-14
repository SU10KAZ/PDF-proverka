"""Приём нового 3-файлового комплекта портала (pdf + *_results.md + *_results.html).

С 2026-07-13 портал vibe отдаёт 3 файла вместо старого квартета
(pdf + *_document.md + *_result.json + *_ocr.html). Этап 1: файлы нового
комплекта не должны теряться ни в одном контуре приёма, старый метод
продолжает работать без изменений (удаление приёма старого метода — после
~2026-08-14, см. docs/new_upload_format.md).
"""
from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

from backend.app.services.common.project_service import (
    _classify_upload_files,
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

    def test_new_format_warning_text(self):
        warns = _upload_bundle_warnings(True, False, True, new_format=True)
        assert len(warns) == 1
        assert "Новый 3-файловый комплект" in warns[0]

    def test_old_format_warning_text_unchanged(self):
        warns = _upload_bundle_warnings(True, False, True, new_format=False)
        assert warns == ["Не найден *_result.json — кроп блоков потребует подготовки."]


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
