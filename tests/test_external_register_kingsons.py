"""Тесты импорта реестра ответов заказчика (King&Sons 2026-05-27).

Покрывает:
  - CustomerResponse.from_raw("Требует внесения") и VERDICT_MAP;
  - parse_kingsons_xlsx: разбор колонок, вердикт ЗАСТРОЙЩИКА, occurrence-ключи;
  - apply_verdicts.build_plan: matched→mark vs unmatched→create;
  - apply_verdicts.apply_register(apply): создание REG-findings + expert decisions,
    идемпотентность повторного прогона.
"""
from __future__ import annotations

import json

import pytest
import openpyxl

from backend.app.services.external_register import apply_verdicts, parser
from backend.app.services.external_register.apply_verdicts import VERDICT_MAP
from backend.app.services.external_register.models import (
    CustomerResponse,
    FindingMatch,
    MatchStatus,
    RegisterEntry,
    RegisterFile,
)


FIELD_HEADER = [
    "№", "Лист/Раздел", "Проблема", "Описание", "Решение", "Категория",
    "Чем грозит", "Категория", "Комментарий", "Категория", "Комментарий",
    "Категория", "Комментарий",
]


def _make_xlsx(tmp_path):
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Замечания к отправ."
    # строка-шапка сторон
    party = [None] * 13
    party[0] = "СУ-10"; party[7] = "ОЛИМПРОЕКТ"; party[9] = "СЕВЕРИН"; party[11] = "ЗАСТРОЙЩИК"
    ws.append(party)
    ws.append(FIELD_HEADER)
    # раздел 1 (АР1, ревизия 1)
    ws.append(["133/23-ГК-АР1 (от 10.03.26)"] + [None] * 12)
    ws.append([1, "АР1 л.9", "Проблема 1", "Описание 1", "Решение 1", "Критическая",
               "Риск 1", "", "", "", "", "Отклонено", "коммент-1"])
    ws.append([2, "АР1 л.7", "Проблема 2", "Описание 2", "Решение 2", "Экономическая",
               "Риск 2", "", "", "", "", "Требует внесения", "коммент-2"])
    # work-type подзаголовок — должен игнорироваться, раздел не сбрасывается
    ws.append(["Кладка"] + [None] * 12)
    ws.append([3, "АР1 л.5", "Проблема 3", "", "", "Рекомендательная",
               "", "", "", "", "", "Внесено", ""])
    # раздел 2 (АР1, ревизия 2 — тот же код → occurrence ~2)
    ws.append(["133/23-ГК-АР1 (от 01.12.25)"] + [None] * 12)
    ws.append([1, "АР1 л.3", "Проблема 4", "", "", "Критическая",
               "", "", "", "", "", "По согласованию Заказчика", ""])
    path = tmp_path / "kingsons.xlsx"
    wb.save(path)
    return path


# ─── from_raw / VERDICT_MAP ──────────────────────────────────────────────────


def test_from_raw_trebuet_vneseniya():
    assert CustomerResponse.from_raw("Требует внесения") == CustomerResponse.TREBUET_VNESENIYA
    assert CustomerResponse.from_raw("требует") == CustomerResponse.TREBUET_VNESENIYA
    assert CustomerResponse.from_raw("Отклонено") == CustomerResponse.OTKLONENO
    assert CustomerResponse.from_raw("Внесено") == CustomerResponse.VNESENO


def test_verdict_map():
    assert VERDICT_MAP[CustomerResponse.OTKLONENO] == "rejected"
    assert VERDICT_MAP[CustomerResponse.TREBUET_VNESENIYA] == "accepted"
    assert VERDICT_MAP[CustomerResponse.VNESENO] == "accepted"
    assert VERDICT_MAP[CustomerResponse.PO_SOGLASOVANIYU] == "accepted"
    assert CustomerResponse.UNKNOWN not in VERDICT_MAP  # без вердикта → пропуск


# ─── parse_kingsons_xlsx ─────────────────────────────────────────────────────


def test_parse_kingsons_xlsx(tmp_path):
    es = parser.parse_kingsons_xlsx(_make_xlsx(tmp_path))
    assert len(es) == 4
    # все из АР1
    assert all(e.section_code == "133/23-ГК-АР1" for e in es)
    # occurrence-ключи: первый блок #1..#3, второй блок ~2#1
    keys = [e.key for e in es]
    assert keys == [
        "133/23-ГК-АР1#1", "133/23-ГК-АР1#2", "133/23-ГК-АР1#3", "133/23-ГК-АР1~2#1",
    ]
    # вердикт берётся из колонки ЗАСТРОЙЩИК
    assert es[0].customer_response == CustomerResponse.OTKLONENO
    assert es[1].customer_response == CustomerResponse.TREBUET_VNESENIYA
    assert es[2].customer_response == CustomerResponse.VNESENO
    assert es[3].customer_response == CustomerResponse.PO_SOGLASOVANIYU
    # содержимое
    assert es[0].problem == "Проблема 1"
    assert es[0].cat_su10 == "Критическая"
    assert es[0].customer_comment == "коммент-1"


def test_entry_key_to_finding_id():
    assert parser.entry_key_to_finding_id("133/23-ГК-АР1#3") == "REG-133-23-ГК-АР1-3"
    assert parser.entry_key_to_finding_id("133/23-ГК-АР1~2#1") == "REG-133-23-ГК-АР1-2-1"


# ─── build_plan: matched vs new ──────────────────────────────────────────────


def _entry(key, code, resp, match=None, status=MatchStatus.UNMATCHED, cat="Критическая"):
    return RegisterEntry(
        key=key, section_code=code, local_no=1, sheet_ref="л.1",
        problem="p", description="d", proposed_solution="s", cat_su10=cat, risk="r",
        customer_response_raw=resp.value, customer_response=resp, customer_comment="c",
        match=match, match_status=status,
    )


def test_build_plan_matched_and_new():
    code = "133/23-ГК-ЭО2"
    matched = _entry(
        "ЭО2#1", code, CustomerResponse.OTKLONENO,
        match=FindingMatch(project_id="EOM/133_23-ГК-ЭО2", finding_id="F-001", confidence=0.95),
        status=MatchStatus.AUTO_MATCHED,
    )
    new = _entry("ЭО2#2", code, CustomerResponse.TREBUET_VNESENIYA)
    skip = _entry("ЭО2#3", code, CustomerResponse.UNKNOWN)
    reg = RegisterFile(register_id="r", object_id="0b540226", entries=[matched, new, skip])

    report = apply_verdicts.build_plan(reg, "0b540226")
    t = report.totals()
    assert t["mark_existing"] == 1
    assert t["create_new"] == 1
    assert t["skipped_no_verdict"] == 1
    plan = report.projects[0]
    assert plan.mark_existing[0]["finding_id"] == "F-001"
    assert plan.mark_existing[0]["decision"] == "rejected"
    assert plan.create_new[0]["finding_id"] == "REG-ЭО2-2"
    assert plan.create_new[0]["decision"] == "accepted"


# ─── apply_register: запись + идемпотентность ────────────────────────────────


def test_apply_creates_findings_and_decisions(tmp_path, monkeypatch):
    proj_dir = tmp_path / "EOM" / "133_23-ГК-ЭО2"
    (proj_dir / "_output").mkdir(parents=True)

    monkeypatch.setattr(apply_verdicts.service, "_findings_output_dir",
                        lambda oid, pid, version_id="v1": proj_dir / "_output")

    saved_calls = []

    def fake_save(project_id, decisions, reviewer="", removed_ids=None):
        saved_calls.append((project_id, list(decisions)))
        return {"saved": len(decisions)}

    monkeypatch.setattr(apply_verdicts.kb_service, "save_expert_review", fake_save)

    e_rej = _entry("ЭО2#1", "133/23-ГК-ЭО2", CustomerResponse.OTKLONENO)
    e_acc = _entry("ЭО2#2", "133/23-ГК-ЭО2", CustomerResponse.TREBUET_VNESENIYA, cat="Экономическая")
    reg = RegisterFile(register_id="su10_2026-05-27", object_id="0b540226", entries=[e_rej, e_acc])

    report = apply_verdicts.apply_register(reg, "0b540226", dry_run=False)
    assert report.dry_run is False

    findings_file = proj_dir / "_output" / "03_findings.json"
    data = json.loads(findings_file.read_text(encoding="utf-8"))
    ids = {f["id"] for f in data["findings"]}
    assert ids == {"REG-ЭО2-1", "REG-ЭО2-2"}
    rej_f = next(f for f in data["findings"] if f["id"] == "REG-ЭО2-1")
    assert rej_f["severity"] == "КРИТИЧЕСКОЕ"
    assert rej_f["origin"] == "customer_registry"
    assert rej_f["external_register"]["customer_response"] == "Отклонено"
    acc_f = next(f for f in data["findings"] if f["id"] == "REG-ЭО2-2")
    assert acc_f["severity"] == "ЭКОНОМИЧЕСКОЕ"
    assert data["meta"]["total_findings"] == 2

    # expert decisions переданы
    assert len(saved_calls) == 1
    _, decisions = saved_calls[0]
    by_id = {d.item_id: d for d in decisions}
    assert by_id["REG-ЭО2-1"].decision == "rejected"
    assert by_id["REG-ЭО2-1"].rejection_reason == "c"  # комментарий заказчика
    assert by_id["REG-ЭО2-2"].decision == "accepted"

    # идемпотентность: повторный прогон не дублирует findings
    saved_calls.clear()
    apply_verdicts.apply_register(reg, "0b540226", dry_run=False)
    data2 = json.loads(findings_file.read_text(encoding="utf-8"))
    assert {f["id"] for f in data2["findings"]} == {"REG-ЭО2-1", "REG-ЭО2-2"}
    assert len(data2["findings"]) == 2
    # бэкап создан после первой мутации
    assert (proj_dir / "_output" / "03_findings.json.bak").exists()


def test_apply_dry_run_writes_nothing(tmp_path, monkeypatch):
    proj_dir = tmp_path / "EOM" / "133_23-ГК-ЭО2"
    (proj_dir / "_output").mkdir(parents=True)
    monkeypatch.setattr(apply_verdicts.service, "_findings_output_dir",
                        lambda oid, pid, version_id="v1": proj_dir / "_output")
    monkeypatch.setattr(apply_verdicts.kb_service, "save_expert_review",
                        lambda *a, **k: pytest.fail("save_expert_review не должен вызываться в dry-run"))

    e = _entry("ЭО2#1", "133/23-ГК-ЭО2", CustomerResponse.OTKLONENO)
    reg = RegisterFile(register_id="r", object_id="0b540226", entries=[e])
    report = apply_verdicts.apply_register(reg, "0b540226", dry_run=True)
    assert report.dry_run is True
    assert not (proj_dir / "_output" / "03_findings.json").exists()
