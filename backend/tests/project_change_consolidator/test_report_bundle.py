"""«Классический отчёт» архивом: Excel + папки «Было»/«Стало» с PDF и MD каждого документа."""
from __future__ import annotations

import zipfile

from backend.app.services.project_change_consolidator import report_bundle as B


def _version(root, code, *, inputs=True):
    version = root / code / "versions" / "v001"
    (version / "01_input").mkdir(parents=True)
    (version / "02_work").mkdir(parents=True)
    if inputs:
        (version / "01_input" / f"{code}.pdf").write_bytes(b"%PDF-1")
        (version / "01_input" / f"{code}_results.md").write_text("# md", encoding="utf-8")
        (version / "01_input" / f"{code}_blocks.json").write_text("{}", encoding="utf-8")  # не попадает в архив
    (version / "02_work" / "document.pdf").write_bytes(b"%PDF-work")
    (version / "02_work" / "document.md").write_text("# work", encoding="utf-8")
    return version


def test_bundle_has_excel_and_one_folder_per_document_on_each_side(tmp_path):
    old = _version(tmp_path, "ИОС2.2")
    new = _version(tmp_path, "ПТ1/1", inputs=False)  # без 01_input → копии 02_work под именем шифра
    pair = {"left": {"document_code": "ИОС2.2", "pdf_path": str(old / "02_work" / "document.pdf")},
            "right": {"document_code": "ПТ1/1", "pdf_path": str(new / "02_work" / "document.pdf")}}
    path = B.build_bundle(b"xlsx", "ИОС2.2_ИТ_шаблон.xlsx", pair)
    try:
        names = set(zipfile.ZipFile(path).namelist())
    finally:
        path.unlink()
    assert names == {
        "ИОС2.2_ИТ_шаблон.xlsx",
        "Было/ИОС2.2/ИОС2.2.pdf",
        "Было/ИОС2.2/ИОС2.2_results.md",
        "Стало/ПТ1_1/ПТ1_1.md",
        "Стало/ПТ1_1/ПТ1_1.pdf",
    }


def test_missing_documents_are_listed_not_fatal(tmp_path):
    pair = {"left": {"document_code": "X", "pdf_path": str(tmp_path / "nowhere" / "02_work" / "document.pdf")},
            "right": {}}
    path = B.build_bundle(b"xlsx", "r.xlsx", pair)
    try:
        archive = zipfile.ZipFile(path)
        assert "НЕ_НАЙДЕНО.txt" in archive.namelist()
        assert "Было/X" in archive.read("НЕ_НАЙДЕНО.txt").decode("utf-8")
    finally:
        path.unlink()
