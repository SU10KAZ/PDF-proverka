"""
test_norm_quote_backfill.py
---------------------------
Дозаливка отсутствующих цитат норм по номеру пункта.

Почему понадобилась: Шаг 7 (`requote_norms_native`) строит поисковый запрос ИЗ
уже имеющейся цитаты и при пустой `norm_quote` молча пропускает замечание. А
пусто оно чаще всего — этапы 01/02 дают цитату лишь в 2-22% находок. Замер на
живых проектах (04.08.2026): у ЭО1-3 из 39 замечаний без цитаты 38 имели код
документа и номер пункта, то есть текст доставался точным обращением к индексу.

Run: python -m pytest tests/test_norm_quote_backfill.py -v
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import norms._native_verify as nv  # noqa: E402


class _FakeNormsApi:
    """Индекс, знающий один пункт: ГОСТ 21.602-2016 п. 5.1."""

    def __init__(self):
        self.calls = []

    def get_paragraph(self, code, paragraph, max_lines=50):
        self.calls.append((code, paragraph))
        if code == "ГОСТ 21.602-2016" and paragraph == "5.1":
            return {"found": True, "text": "5.1 В состав общих данных включают: ..."}
        return {"found": False, "text": None}


@pytest.fixture()
def fake_api(monkeypatch):
    api = _FakeNormsApi()
    monkeypatch.setattr(nv, "_import_norms_api", lambda: api)
    return api


def _write(tmp_path: Path, findings: list[dict]) -> Path:
    p = tmp_path / "03_findings.json"
    p.write_text(json.dumps({"findings": findings}, ensure_ascii=False), encoding="utf-8")
    return p


def _read(p: Path) -> list[dict]:
    return json.loads(p.read_text(encoding="utf-8"))["findings"]


def test_quote_filled_from_index(tmp_path, fake_api):
    p = _write(tmp_path, [
        {"id": "F-001", "norm": "ГОСТ 21.602-2016 (действует), п. 5.1"},
    ])
    rep = nv.backfill_missing_quotes_native(tmp_path)

    assert rep["filled"] == 1
    out = _read(p)[0]
    assert out["norm_quote"].startswith("5.1 В состав общих данных")
    assert out["norm_quote_source"] == "norms_index"


def test_reference_without_clause_is_counted_not_guessed(tmp_path, fake_api):
    """Ссылка без номера пункта — цитировать нечего; выдумывать запрещено."""
    p = _write(tmp_path, [{"id": "F-001", "norm": "СП 60.13330.2020 (действует)"}])
    rep = nv.backfill_missing_quotes_native(tmp_path)

    assert rep["filled"] == 0
    assert rep["no_paragraph"] == 1
    assert _read(p)[0].get("norm_quote") is None
    assert fake_api.calls == []


def test_existing_quote_untouched(tmp_path, fake_api):
    """Уже процитированные замечания не трогаем — это работа Шага 7."""
    p = _write(tmp_path, [
        {"id": "F-001", "norm": "ГОСТ 21.602-2016, п. 5.1", "norm_quote": "своя цитата"},
    ])
    rep = nv.backfill_missing_quotes_native(tmp_path)

    assert rep["candidates"] == 0 and rep["filled"] == 0
    assert _read(p)[0]["norm_quote"] == "своя цитата"


def test_multi_document_reference_takes_first_hit(tmp_path, fake_api):
    """В ссылке несколько документов — берём первый, где пункт реально нашёлся."""
    p = _write(tmp_path, [{
        "id": "F-001",
        "norm": "СП 999.9999.2099, п. 1.1; ГОСТ 21.602-2016 (действует), п. 5.1",
    }])
    rep = nv.backfill_missing_quotes_native(tmp_path)

    assert rep["filled"] == 1
    assert _read(p)[0]["norm_quote"].startswith("5.1 ")
    assert ("СП 999.9999.2099", "1.1") in fake_api.calls


def test_unknown_clause_leaves_quote_empty(tmp_path, fake_api):
    """Пункта нет в индексе — цитата остаётся пустой, ничего не сочиняем."""
    p = _write(tmp_path, [{"id": "F-001", "norm": "СП 999.9999.2099, п. 7.7"}])
    rep = nv.backfill_missing_quotes_native(tmp_path)

    assert rep["filled"] == 0
    assert _read(p)[0].get("norm_quote") is None


def test_missing_file_is_soft(tmp_path, fake_api):
    rep = nv.backfill_missing_quotes_native(tmp_path / "нет")
    assert rep == {"filled": 0, "candidates": 0, "no_paragraph": 0}
