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
    """Индекс, знающий два пункта: ГОСТ 21.602-2016 п. 5.1 и ГОСТ Р 21.101-2020 п. 4.1.

    `NOT_IN_INDEX` имитирует документы, которых в индексе нет вовсе — в том числе
    «ГОСТ 21.101-2020» без «Р»: именно в такой форме его пишет свод, а индекс
    хранит с «Р», и без подстановки варианта ссылка считалась бы отсутствующей.
    """

    NOT_IN_INDEX = {"ГОСТ 21.101-2020", "СП 111.11111.2011"}

    def __init__(self):
        self.calls = []
        self.status_calls = []

    def get_norm_status(self, code):
        self.status_calls.append(code)
        if code in self.NOT_IN_INDEX:
            return {"matched_code": None, "resolution_reason": "not_in_index"}
        return {"matched_code": code, "resolution_reason": "exact"}

    def get_paragraph(self, code, paragraph, max_lines=50):
        self.calls.append((code, paragraph))
        if code == "ГОСТ 21.602-2016" and paragraph == "5.1":
            return {"found": True, "text": "5.1 В состав общих данных включают: ..."}
        if code == "ГОСТ Р 21.101-2020" and paragraph == "4.1":
            return {"found": True, "text": "4.1 Рабочая документация состоит из ..."}
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


# ─── Состояние ссылки по индексу (norm_paragraph_state) ───
#
# Верификация пунктов сверяла цитату с текстом пункта, а цитаты почти никогда не
# было — на 4645 проверок пришлось 2 подтверждения. Проверка «есть ли вообще
# такой пункт в этом документе» дешёвая, детерминированная и ловит именно то,
# ради чего вводился запрет выдумывать номера.


def test_state_verified_when_clause_exists(tmp_path, fake_api):
    p = _write(tmp_path, [{"id": "F-001", "norm": "ГОСТ 21.602-2016 (действует), п. 5.1"}])
    rep = nv.backfill_missing_quotes_native(tmp_path)

    assert _read(p)[0]["norm_paragraph_state"] == "paragraph_verified"
    assert rep["states"]["paragraph_verified"] == 1


def test_state_flags_clause_absent_from_its_document(tmp_path, fake_api):
    """Документ реальный, пункта в нём нет — признак придуманного номера."""
    p = _write(tmp_path, [{"id": "F-001", "norm": "ГОСТ 21.602-2016 (действует), п. 99.9"}])
    rep = nv.backfill_missing_quotes_native(tmp_path)

    assert _read(p)[0]["norm_paragraph_state"] == "paragraph_not_found"
    assert rep["states"]["paragraph_not_found"] == 1


def test_state_marks_document_without_clause(tmp_path, fake_api):
    """Назван только документ — ссылка неполная, но не потерянная."""
    p = _write(tmp_path, [{"id": "F-001", "norm": "ГОСТ 21.602-2016 (действует)"}])
    rep = nv.backfill_missing_quotes_native(tmp_path)

    assert _read(p)[0]["norm_paragraph_state"] == "paragraph_missing"
    assert rep["no_paragraph"] == 1


def test_state_marks_document_outside_index(tmp_path, fake_api):
    p = _write(tmp_path, [{"id": "F-001", "norm": "СП 111.11111.2011, п. 1.1"}])
    nv.backfill_missing_quotes_native(tmp_path)

    assert _read(p)[0]["norm_paragraph_state"] == "document_not_in_index"


def test_gost_r_variant_is_tried(tmp_path, fake_api):
    """«ГОСТ 21.101-2020» из свода = «ГОСТ Р 21.101-2020» в индексе."""
    p = _write(tmp_path, [{"id": "F-001", "norm": "ГОСТ 21.101-2020 (действует), п. 4.1"}])
    rep = nv.backfill_missing_quotes_native(tmp_path)

    out = _read(p)[0]
    assert out["norm_paragraph_state"] == "paragraph_verified"
    assert out["norm_quote"].startswith("4.1 Рабочая документация")
    assert rep["filled"] == 1
