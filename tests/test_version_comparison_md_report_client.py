"""PDF для заказчика над проверенным отчётом: вид без ID и реестров, распределение важных изменений, проверка."""
from __future__ import annotations

import pytest

from backend.app.services.version_comparison_ai.md_report import client, pdf


def _tr(a, b, scope, *, changes=(), main="Изменены кабели и защита.", impact="", complete=True, d=("2026-01-01", "2026-02-01")):
    return {"transition_id": f"T{a:02d}", "from_version": f"V{a}", "to_version": f"V{b}", "from_date": d[0], "to_date": d[1],
            "change_scope": scope, "significance": "Средняя", "main_changes": main, "changes": list(changes),
            "construction_impact": impact, "completeness": {"complete": complete}}


def _fact(fid, tr, text, kind="ai_change", **details):
    return {"fact_id": fid, "transition": tr, "kind": kind, "text": text, "sheet": "л.35", "details": details}


REGISTRY = {"facts": [
    _fact("Ф0001", "V2→V3", "Старая: л.36 (стр. PDF 82); новая: л.38 (стр. PDF 84). Поз. 1; Наименование Кабель "
          "нагревательный саморегулирующийся; Марка Freezstop-25К; раздел Обогрев системы К2.1; Количество: 1233 м → "
          "1233+1233 м; Примечание: не указано → осн.+рез. кабель", kind="spec_entry", change_kind="changed"),
    _fact("Ф0002", "V2→V3", "Сети кондиционеров — в A было указано «в огнезащите»; в B это условие отсутствует.",
          object="Сети кондиционеров", change_kind="требование"),
    _fact("Ф0003", "V3→V4", "Старая: —; новая: л.10 (стр. PDF 55). Добавлено: не было → Наименование Огнезащитный "
          "герметик; Марка ОГ-20; Количество 215 кг", kind="spec_entry", change_kind="added"),
]}


def _report():
    return {"project_code": "133/23-ГК-ЭМ2", "transitions": [
        _tr(1, 2, "Нет изменений содержания", d=("2025-05-30", "2025-05-30")),
        _tr(2, 3, "Существенные изменения", changes=[
            {"object": "ЯУР", "before": "Три DIN-рейки", "after": "Две DIN-рейки", "sources": ["Ф0009"]},
            {"object": "QF5.3, первое описание", "before": "10 А", "after": "16 А", "sources": ["Ф0008"]},
            {"object": "QF4.1", "before": "Ics не указана в сопоставленном фрагменте", "after": "25 кА", "sources": ["Ф0007"]}],
            impact="При выполнении по V2 потребовалась бы сверка кабелей [Ф0001]; удаление строк не доказывает исключения."),
        _tr(3, 4, "Нет изменений содержания", complete=False),
    ], "significant_changes": [{"title": "Резерв обогрева", "sources": ["Ф0001", "Ф0002", "Ф0003"]}]}


def test_build_rows_distribution_and_cleanup():
    view = client.build(_report(), REGISTRY, {"object": "Объект", "section": "Раздел"})
    rows = {r["label"]: r for r in view["rows"]}
    assert list(rows) == ["V1→V2", "V2→V3", "V3→V4"]
    # Подтверждённое отсутствие изменений — одна короткая запись без последствия.
    assert rows["V1→V2"]["lead"] == client.NO_CHANGES and rows["V1→V2"]["scale"] == client.SCOPE_NONE
    assert not rows["V1→V2"]["points"] and not rows["V1→V2"]["impact"]
    # Неполный результат не выдаётся за «изменений нет».
    assert rows["V3→V4"]["lead"] != client.NO_CHANGES and rows["V3→V4"]["scale"] != client.SCOPE_NONE
    pts = rows["V2→V3"]["points"]
    texts = " | ".join(f"{p['object']}: {p['text']}" for p in pts)
    # Важное изменение — в своём переходе; будущее (V3→V4) не уходит в ранний.
    assert "1233 м → 1233+1233 м" in texts and "ОГ-20" not in texts
    assert any("ОГ-20" in p["object"] for p in rows["V3→V4"]["points"])
    # Стороны A/B — номера выдач; снятое требование — с листом.
    assert "в V2 было указано" in texts and "в V3 это условие" in texts and "(л.35)" in texts
    # Методические оговорки и «первое описание» не попадают к заказчику; порядок — защита раньше DIN-реек.
    assert "сопоставленном" not in texts and "первое описание" not in texts
    assert [p["object"] for p in pts].index("QF5.3") < [p["object"] for p in pts].index("ЯУР")
    assert "Ф0001" not in rows["V2→V3"]["impact"] and "не доказывает" not in rows["V2→V3"]["impact"]
    assert view["period"] == "30.05.2025–01.02.2026" and view["versions"] == "V1–V4"


def test_build_is_deterministic():
    a = client.build(_report(), REGISTRY, {"object": "О", "section": "Р"})
    b = client.build(_report(), REGISTRY, {"object": "О", "section": "Р"})
    assert a == b


def test_render_and_check(tmp_path):
    pytest.importorskip("reportlab")
    pytest.importorskip("fitz")
    view = client.build(_report(), REGISTRY, {"object": "Объект", "section": "Раздел"})
    out = pdf.render_client(view, tmp_path / "c.pdf")
    chk = pdf.check_client(out, view)
    assert chk["ok"], chk["problems"]
    # Служебный ID в тексте — проверка не пропускает.
    bad = dict(view, rows=[dict(view["rows"][1], lead="Изменены кабели [Ф0001] и защита.")] + view["rows"][2:])
    out2 = pdf.render_client(bad, tmp_path / "bad.pdf")
    assert any("Ф0001" in p for p in pdf.check_client(out2, bad)["problems"])
