"""reserc.md #43 — тесты Excel-генерации (severity-нормализация + скрытый project_id).

generate_excel_report не имел тестов на построение листов. Покрываем
детерминированные части (без изменения прод-кода): нормализацию severity EN→RU и
старо-русских форм, скрытую ячейку project_id (нужна для round-trip импорта
решений), устойчивость к пустым данным.
"""
from __future__ import annotations

from openpyxl import Workbook

import backend.app.pipeline.stages.report.generate_excel_report as ger


# ─── severity нормализация ───────────────────────────────────────────────────

def test_normalize_sev_en_to_ru():
    assert ger.normalize_sev("CRITICAL") == "КРИТИЧЕСКОЕ"
    assert ger.normalize_sev("ECONOMIC") == "ЭКОНОМИЧЕСКОЕ"
    assert ger.normalize_sev("OPERATIONAL") == "ЭКСПЛУАТАЦИОННОЕ"
    assert ger.normalize_sev("CHECK_RELATED") == "ПРОВЕРИТЬ ПО СМЕЖНЫМ"


def test_normalize_sev_old_russian_to_new():
    assert ger.normalize_sev("КРИТИЧНО") == "КРИТИЧЕСКОЕ"
    assert ger.normalize_sev("ПРОВЕРИТЬ") == "ПРОВЕРИТЬ ПО СМЕЖНЫМ"


def test_normalize_sev_passthrough():
    assert ger.normalize_sev("КРИТИЧЕСКОЕ") == "КРИТИЧЕСКОЕ"   # уже канон
    assert ger.normalize_sev("нечто_иное") == "нечто_иное"      # неизвестное — как есть


def test_get_sev_cfg_known_and_fallback():
    assert ger.get_sev_cfg("CRITICAL") == ger.SEVERITY_CONFIG["КРИТИЧЕСКОЕ"]
    # неизвестная severity → безопасный fallback
    assert ger.get_sev_cfg("zzz") == ger.SEVERITY_CONFIG["ПРОВЕРИТЬ ПО СМЕЖНЫМ"]


# ─── скрытый project_id (round-trip импорта решений) ─────────────────────────

def _proj_entry(pid, findings):
    return {
        "project_id": pid,
        "project_info": {"object": "Тестовый дом"},
        "findings_json": {"findings": findings, "meta": {"total_findings": len(findings)}},
        "sheet_name": "ЭМ1",
    }


def test_build_project_sheet_hidden_project_id_cell():
    wb = Workbook()
    pid = "EOM/13АВ-ЭМ1"
    ger.build_project_sheet(wb, _proj_entry(pid, [
        {"severity": "CRITICAL", "finding": "Нет заземления", "page": 1},
    ]))
    ws = wb["ЭМ1"]
    pid_col = len(ger.PROJ_COLUMNS) + 1
    # Скрытая ячейка хранит ПОЛНЫЙ project_id для обратной загрузки решений.
    assert ws.cell(row=2, column=pid_col).value == pid
    assert ws.column_dimensions[ger.get_column_letter(pid_col)].hidden is True


def test_build_project_sheet_empty_findings_no_crash():
    wb = Workbook()
    ger.build_project_sheet(wb, _proj_entry("EOM/empty", []))
    ws = wb["ЭМ1"]
    pid_col = len(ger.PROJ_COLUMNS) + 1
    assert ws.cell(row=2, column=pid_col).value == "EOM/empty"


def test_build_optimization_project_sheet_hidden_project_id():
    wb = Workbook()
    pid = "EOM/13АВ-ЭМ1"
    entry = {
        "project_id": pid,
        "project_info": {"object": "Дом"},
        "optimization_json": {"items": [
            {"title": "Кабель дешевле", "savings_pct": 10, "spec_items": ["Поз. 5"]},
        ], "meta": {}},
        "sheet_name": "ЭМ1",
    }
    ger.build_optimization_project_sheet(wb, entry)
    ws = wb["ОПТ ЭМ1"]
    pid_col = len(ger.OPT_COLUMNS) + 1
    assert ws.cell(row=2, column=pid_col).value == pid
