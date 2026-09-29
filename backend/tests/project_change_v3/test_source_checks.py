"""Deterministic source checks (source_checks.py): 0 model calls, review signals only."""
from __future__ import annotations

import json

import pytest

from backend.app.services.project_change_v3 import source_checks as sc
from backend.app.services.project_change_v3.source_checks import FLAGS, run_source_checks
from backend.tests.project_change_v3.test_v3_runtime_generic import synthetic_pair  # noqa: F401  (fixture)

ALL = {name: True for name in FLAGS}


def _page(side, n, native="", md="", tables=()):
    return {"side": side, "physical_page": n, "native_page_text": native,
            "blocks": [{"block_id": f"b{side}{n}", "modality": "TEXT", "structured_md": md, "tables": list(tables)}]}


def _card(pid="PC-1", params=(), old_state="", new_state="", evidence=(), summary="s", subject="subj"):
    return {"projectchange_id": pid, "engineering_subject": subject, "scope": "", "change_summary": summary,
            "old_state": old_state, "new_state": new_state, "changed_parameters": list(params),
            "evidence_items": list(evidence)}


def _pages(*pages):
    return {(p["side"], p["physical_page"]): p for p in pages}


def _checks(changes, pages, hints=(), **only):
    enabled = {name: bool(only.get(name)) for name in FLAGS} if only else ALL
    return run_source_checks(projectchanges=changes, unresolved_hints=list(hints), page_records=pages, enabled=enabled)


# ------------------------------------------------------------------ absence
RUKAV = {"name": "Длина пожарного рукава", "old_value": "не указана", "new_value": "30", "unit": "м", "location": "к.4"}


def test_absence_is_contradicted_across_a_wrapped_line():
    pages = _pages(_page("OLD", 8, native="Согласно СТУ длина пожарного\nкрана принята 30 м (для корпуса 4)."),
                   _page("NEW", 1, native="рукава 30 м"))
    res = _checks([_card(params=[RUKAV])], pages, absence=True)
    [finding] = res["findings"]
    assert finding["check"] == "ABSENCE_CONTRADICTED" and finding["parameter_ordinal"] == 1
    assert finding["source"][0]["physical_page"] == 8


def test_absence_holds_when_old_really_lacks_it():
    pages = _pages(_page("OLD", 8, native="Трубопроводы из стальных труб."), _page("NEW", 1, native="3В1.1 Ø20"))
    card = _card(params=[{"name": "Система улучшенной питьевой воды", "old_value": "отсутствует",
                          "new_value": "3В1.1 Ø20", "unit": "", "location": ""}])
    assert _checks([card], pages, absence=True)["findings"] == []


def test_recognition_meta_lines_are_not_document_text():
    pages = _pages(_page("OLD", 8, md="**Description:** длина пожарного крана принята 30 м по всему корпусу"))
    assert _checks([_card(params=[RUKAV])], pages, absence=True)["findings"] == []


def test_single_digit_values_never_match():
    pages = _pages(_page("OLD", 5, native="насосов второй зоны 2 шт."))
    card = _card(params=[{"name": "Насосы второй зоны", "old_value": "не предусмотрены", "new_value": "2",
                          "unit": "шт", "location": ""}])
    assert _checks([card], pages, absence=True)["findings"] == []


# ------------------------------------------------------------------ elevations
NEW29 = "Отметка 0.000 (Ур Ч.П)= 123.20\nОтметка оси трубы = -3.49 (от Ур Ч.П)\nОтметка от ур. земли 119.60 (3,6м глубина)"


def test_elevation_that_does_not_add_up_is_unclear_for_a_quiet_card():
    pages = _pages(_page("NEW", 29, native=NEW29))
    quiet = _card(new_state="Ось трубы −3,49, абсолютная 119,60.")
    [finding] = _checks([quiet], pages, numeric=True)["findings"]
    assert finding["check"] == "ELEVATION_UNCLEAR" and finding["projectchange_id"] == "PC-1"
    assert "уточнения" in finding["message_ru"] and "119.71" in finding["message_ru"]


def test_elevation_caveat_or_consistent_page_gives_nothing():
    pages = _pages(_page("NEW", 29, native=NEW29))
    careful = _card(new_state="Ось −3,49; к какой точке относится 119,60, требует уточнения.")
    assert _checks([careful], pages, numeric=True)["findings"] == []
    ok = _pages(_page("OLD", 21, native=NEW29.replace("-3.49", "-2.51").replace("119.60", "120.69")))
    assert _checks([_card(new_state="−2,51 / 120,69")], ok, numeric=True)["findings"] == []


# ------------------------------------------------------------------ table totals
TABLE = ("| № | Потребитель | Сут | Час |\n|---|---|---|---|\n| 1 | Жильё | 100,00 | 5,00 |\n"
         "| 2 | Офис | 20,00 | 1,00 |\n| | Итого | 120,00 | 7,00 |")


def test_total_that_does_not_add_up_when_another_column_does():
    pages = _pages(_page("NEW", 3, tables=[TABLE]))
    [finding] = _checks([], pages, numeric=True)["findings"]
    assert finding["check"] == "TABLE_TOTAL_MISMATCH" and "7.0" in finding["claim"] and "6.0" in finding["claim"]


def test_table_without_a_proven_sum_law_is_silent():
    pages = _pages(_page("NEW", 3, tables=[TABLE.replace("120,00", "121,00")]))
    assert _checks([], pages, numeric=True)["findings"] == []


# ------------------------------------------------------------------ option
OPTION_PAGE = "12. Модуль химической промывки SEK 28 1 1500,00\n13. Комплекс дозирования Medomat FPR 100 1 1480,00\nИТОГО ПО ОПЦИОНАЛЬНОМУ ОБОРУДОВАНИЮ: 2 980,00"


def test_option_equipment_presented_as_mandatory():
    pages = _pages(_page("NEW", 52, native=OPTION_PAGE))
    [finding] = _checks([_card(new_state="Узел BWT: Rondomat, комплекс дозирования Medomat FPR 100.")], pages, option=True)["findings"]
    assert finding["check"] == "OPTION_NOT_REFLECTED" and finding["claim"] == "Medomat"
    assert _checks([_card(new_state="Medomat FPR 100 (опция)")], pages, option=True)["findings"] == []


# ------------------------------------------------------------------ legend only
def _ev(side, text):
    return {"side": side, "physical_page": 1, "block_id": "b", "relevant_fragment": text}


def test_legend_only_card_is_documentary():
    card = _card(evidence=[_ev("OLD", "трубопровод хозяйственно-питьевого водопровода (совмещенный с противопожарным)"),
                           _ev("NEW", "Легенда: В2.1 трубопровод противопожарного водопровода 1 зона")])
    [finding] = _checks([card], {}, documentary=True)["findings"]
    assert finding["check"] == "LEGEND_ONLY"
    fitting_list = _card(evidence=[_ev("NEW", "Обратный клапан / Шаровой кран / Затвор дисковый / Задвижка с эл. приводом")])
    assert len(_checks([fitting_list], {}, documentary=True)["findings"]) == 1


def test_a_drawn_change_is_not_legend_only():
    card = _card(evidence=[_ev("OLD", "трубопровод В1"), _ev("NEW", "Стояк Ст.В2.1 Ø65 с ПК на 3 этаже")])
    assert _checks([card], {}, documentary=True)["findings"] == []


# ------------------------------------------------------------------ table rows
HEAD = "| № | Наименование | Ед. | Часов | Кол-во | Норма | Расход |\n|---|---|---|---|---|---|---|\n"
OLD_T = HEAD + "| 2 | Административные раб. | 1 рабоч. | 10 | 9 | 0,012 | 0,11 |\n| 5 | Салон красоты | 1 рабоч. | 10 | 5 | 0,056 | 0,28 |"
NEW_T = HEAD + "| 2 | Административные здания (сотрудники) | 1 работающий | 8 | 9 | 0,012 | 0,108 |\n| 5 | Салон красоты | 1 работающий | 10 | 5 | 0,056 | 0,280 |"


def _balance():
    return _pages(_page("OLD", 15, tables=["Баланс водопотребления\n" + OLD_T]),
                  _page("NEW", 23, tables=["Баланс водопотребления\n" + NEW_T]))


def test_changed_row_without_a_card_is_reported_and_format_only_rows_are_not():
    [finding] = _checks([], _balance(), table_rows=True)["findings"]
    assert finding["check"] == "UNCOVERED_TABLE_ROW"
    assert "10 → 8" in finding["claim"] and "Салон" not in finding["claim"]


def test_changed_row_named_by_a_card_or_its_numbers_is_covered():
    by_name = _card(new_state="Административные здания: 8 ч, расход 0,108 м³/сут")
    assert _checks([by_name], _balance(), table_rows=True)["findings"] == []
    hint = {"engineering_subject": "Административные сотрудники", "suspected_change": "часы 10 → 8"}
    assert _checks([], _balance(), hints=[hint], table_rows=True)["findings"] == []


# ------------------------------------------------------------------ entry point
def test_flags_default_off(monkeypatch):
    for env in FLAGS.values():
        monkeypatch.delenv(env, raising=False)
    assert sc.enabled_checks() == {name: False for name in FLAGS}
    monkeypatch.setenv(FLAGS["absence"], "1")
    assert sc.enabled_checks()["absence"] is True
    empty = run_source_checks(projectchanges=[], unresolved_hints=[], page_records={}, enabled={})
    assert empty["findings"] == [] and empty["summary"]["findings_total"] == 0


def test_finding_ids_are_sequential():
    res = _checks([], _balance(), table_rows=True)
    assert [f["finding_id"] for f in res["findings"]] == ["SC-001"]


# ------------------------------------------------------------------ engine integration
def test_engine_writes_checks_only_when_a_flag_is_on(synthetic_pair, monkeypatch, tmp_path):
    from backend.tests.project_change_v3 import test_v3_runtime_generic as generic

    for env in FLAGS.values():
        monkeypatch.setenv(env, "1")
    generic.test_fake_provider_e2e(synthetic_pair, monkeypatch, tmp_path / "on")
    [run_dir] = list((tmp_path / "on").glob("prod/*/*/production/runs/*"))
    checks = json.loads((run_dir / "project_change_v3_source_checks.json").read_text(encoding="utf-8"))
    result = json.loads((run_dir / "project_change_v3_result.json").read_text(encoding="utf-8"))
    manifest = json.loads((run_dir / "run_manifest.json").read_text(encoding="utf-8"))
    assert checks["schema"] == sc.SOURCE_CHECKS_SCHEMA and checks["run_id"] == result["run_id"]
    assert all(checks["enabled"].values())
    assert result["source_checks"] == checks["summary"]
    assert "project_change_v3_source_checks" in manifest["artifacts"]

    for env in FLAGS.values():
        monkeypatch.delenv(env, raising=False)
    generic.test_fake_provider_e2e(synthetic_pair, monkeypatch, tmp_path / "off")
    [run_dir] = list((tmp_path / "off").glob("prod/*/*/production/runs/*"))
    result = json.loads((run_dir / "project_change_v3_result.json").read_text(encoding="utf-8"))
    assert "source_checks" not in result and "dedupe_policy" not in result
    assert not (run_dir / "project_change_v3_source_checks.json").exists()
