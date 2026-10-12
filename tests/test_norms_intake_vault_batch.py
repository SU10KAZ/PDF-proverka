"""Приём пакета новых MD-норм (выгрузки *_results.md) в формат vault.

build_status_index берёт код нормы только из имени файла, поэтому главное
свойство приёма — имя выходного файла разбирается parse_filename обратно
в то же обозначение, что стоит в заголовке документа.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

TOOLS = Path(__file__).resolve().parent.parent / "norms" / "tools"
if str(TOOLS) not in sys.path:
    sys.path.insert(0, str(TOOLS))

from intake_vault_batch import run  # noqa: E402
from parse_filename import parse_filename  # noqa: E402


@pytest.mark.parametrize(
    ("name", "code", "type_"),
    [
        ("ГОСТ IEC 60898-1-2020_ Межгосударственный стандарт_document.md", "ГОСТ IEC 60898-1-2020", "ГОСТ"),
        ("ГОСТ ISO 898-1-2014_document.md", "ГОСТ ISO 898-1-2014", "ГОСТ"),
        ("ГОСТ Р МЭК 62305-1-2010_ Национальный стандарт_document.md", "ГОСТ Р МЭК 62305-1-2010", "ГОСТ Р"),
        ("ГОСТ Р ИСО 17637-2024_document.md", "ГОСТ Р ИСО 17637-2024", "ГОСТ Р"),
        ("ПП РФ № 815_document.md", "ПП РФ № 815", "ПП РФ"),
        ("ФЗ 126-ФЗ_document.md", "ФЗ 126-ФЗ", "ФЗ"),
        # прежние шаблоны не задеты
        ("СП 256_1325800_2016_ Свод правил_document.md", "СП 256.1325800.2016", "СП"),
        ("ГОСТ 10180-2012_ Бетоны_document.md", "ГОСТ 10180-2012", "ГОСТ"),
    ],
)
def test_parse_filename_prefixed_designations(name, code, type_):
    parsed = parse_filename(name)
    assert parsed["code"] == code
    assert parsed["type"] == type_
    assert parsed["parse_confidence"] == "high"


def _write(folder: Path, name: str, title: str, body: str = "") -> None:
    text = (
        f"# Document: {name}\n\nPath: X / {name}\n\n## Page 1\n\n"
        "### BLOCK #1 [TEXT]: blk_1\n\n> **Created:** 2026-08-25\n"
        "> **Crop:** [Crop](https://example.invalid/c)\n\n"
        f"КонсультантПлюс\n\n{title}\n\n{body}"
    )
    (folder / name).write_text(text, encoding="utf-8")


def test_intake_names_files_by_document_designation(tmp_path):
    src, out = tmp_path / "src", tmp_path / "out"
    src.mkdir()
    _write(src, "ГОСТ_12_1_003-2014__Межгосударственный_стандарт_results.md",
           '"ГОСТ 12.1.003-2014. Межгосударственный стандарт. Шум"', "4.1 Текст пункта")
    _write(src, "Приказ_МЧС_России_от_06_04_2021_N_200_Об_утверждении_results.md",
           '"Об утверждении свода правил СП 6.13130.2021"')
    _write(src, "СНиП_12.04-2002_results.md", '"О принятии строительных норм и правил"')
    _write(src, "СП_12-04-2002_results.md", '"О принятии строительных норм и правил"')
    index = tmp_path / "status_index.json"
    index.write_text(json.dumps({"norms": [
        {"code": "ГОСТ 6133-2019", "type": "ГОСТ", "source": "vault"},
    ]}), encoding="utf-8")
    report = tmp_path / "report.json"

    summary = run(src, out, report, index)

    files = {p.name for p in out.iterdir()}
    codes = {parse_filename(n)["code"] for n in files}
    assert {"ГОСТ 12.1.003-2014", "СП 6.13130.2021"} <= {c.replace("_", ".") for c in codes}
    assert summary["files_written"] == 3  # два файла СНиП 12-04-2002 — дубль
    gost = (out / next(n for n in files if n.startswith("ГОСТ 12"))).read_text(encoding="utf-8")
    assert gost.startswith("---\ncode: ")
    assert "**Crop:**" not in gost and "**Created:**" not in gost
    assert "## СТРАНИЦА 1" in gost
    data = json.loads(report.read_text(encoding="utf-8"))
    assert data["lost_old_norms"] == ["ГОСТ 6133-2019"]


def test_intake_refuses_non_empty_output(tmp_path):
    src, out = tmp_path / "src", tmp_path / "out"
    src.mkdir()
    out.mkdir()
    (out / "old.md").write_text("x", encoding="utf-8")
    _write(src, "ГОСТ_10180-2012_results.md", '"ГОСТ 10180-2012. Бетоны"')
    with pytest.raises(SystemExit):
        run(src, out, tmp_path / "r.json", tmp_path / "missing.json")
