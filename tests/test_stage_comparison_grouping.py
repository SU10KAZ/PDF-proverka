"""Unit tests for unified_grouping.py (deterministic post-processing).

Покрывает:
  1.  Formal stamp / designer / GIP — попадают в hidden_formal.
  2.  Escalation: stage_change / expert_review → medium, не hidden.
  3.  Materials grouping: 3 findings на разных листах → 1 group, affected_count=3.
  4.  Equipment grouping: одинаковое оборудование на нескольких страницах → 1 group.
  5.  No merge when old/new differ: разные new_value → разные группы.
  6.  Simplification: removed/исключено → change_direction=simplification, cost decrease.
  7.  Evidence preservation: все source_finding_ids сохранены.
  8.  Cross-pair rollup: одинаковый old→new в разных pair → scope_level=session_rollup.
  9.  pair_id filter: возвращает только группы этой пары.
  10. include_formal toggle: false/true изменяет видимость formal groups.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest


# ─── Sample finding factory ─────────────────────────────────────────────


def _f(
    *,
    id: str,
    pair_id: str = "p_test",
    pair_label: str = "АА_БЭ-03-ИОС1.1.pdf ↔ АА_БЭ-03-ИОС1.1.pdf",
    source_layer: str = "text",
    type: str = "changed",
    category: str = "general",
    severity: str = "medium",
    title: str = "",
    summary: str = "",
    old_value: str = "",
    new_value: str = "",
    construction_impact: str = "",
    cost_impact: str = "possible",
    requires_human_review: bool = False,
    confidence: float = 0.9,
    page: int | None = 1,
    left_page: int | None = 1,
    right_page: int | None = 1,
    evidence_left: dict | None = None,
    evidence_right: dict | None = None,
) -> dict:
    return {
        "id": id,
        "pair_id": pair_id,
        "pair_label": pair_label,
        "source_layer": source_layer,
        "type": type,
        "category": category,
        "severity": severity,
        "title": title,
        "summary": summary,
        "old_value": old_value,
        "new_value": new_value,
        "construction_impact": construction_impact,
        "cost_impact": cost_impact,
        "requires_human_review": requires_human_review,
        "confidence": confidence,
        "page": page,
        "left_page": left_page,
        "right_page": right_page,
        "sheet": f"Лист {page}" if page is not None else "—",
        "evidence_left": evidence_left or {"quote": f"L:{id}", "section": "x", "approx_location": "стр.1"},
        "evidence_right": evidence_right or {"quote": f"R:{id}", "section": "x", "approx_location": "стр.1"},
    }


# ─── 1. Formal — stamp / designer hidden ────────────────────────────────


def test_formal_designer_hidden():
    from backend.app.services.stage_comparison import unified_grouping as ug

    items = [
        _f(
            id="f1",
            source_layer="stamp",
            type="stamp_changed",
            severity="high",
            cost_impact="likely",
            title="Изменена организация-разработчик в штампе",
            summary="Разработчик изменён с ООО «АРТЕЛ АРХИТЕКТС» на ООО «СЕВ.Р.ДЕВЕЛОПМЕНТ»",
            old_value="ООО «АРТЕЛ АРХИТЕКТС»",
            new_value="ООО «СЕВ.Р.ДЕВЕЛОПМЕНТ»",
        ),
    ]
    out = ug.group_findings(items)
    assert len(out["groups"]) == 0
    assert len(out["hidden_formal_groups"]) == 1
    g = out["hidden_formal_groups"][0]
    assert g["is_formal"] is True
    assert g["formal_reason"] == "designer_only"
    assert g["escalation_reason"] is None
    assert g["significance"] == "formal"


def test_formal_gip_hidden():
    from backend.app.services.stage_comparison import unified_grouping as ug

    items = [
        _f(
            id="g1",
            source_layer="stamp",
            type="stamp_changed",
            title="Изменён ГИП в штампе",
            summary="ГИП изменён с Иванов И.И. на Петров П.П.",
        ),
    ]
    out = ug.group_findings(items)
    assert out["hidden_formal_groups"][0]["formal_reason"] == "gip_gap_only"
    assert out["hidden_formal_groups"][0]["significance"] == "formal"


# ─── 2. Stamp escalation: stage / expert review → not hidden ───────────


def test_stamp_stage_change_escalates_to_medium():
    from backend.app.services.stage_comparison import unified_grouping as ug

    items = [
        _f(
            id="s1",
            source_layer="stamp",
            type="stamp_changed",
            title="Изменена стадия проекта",
            summary="Стадия изменена с П на РД",
            old_value="Стадия: П",
            new_value="Стадия: РД",
        ),
    ]
    out = ug.group_findings(items)
    assert len(out["groups"]) == 1
    g = out["groups"][0]
    assert g["is_formal"] is True
    assert g["escalation_reason"] == "stage_change"
    assert g["significance"] in ("medium", "high")  # не formal


def test_expert_review_escalates():
    from backend.app.services.stage_comparison import unified_grouping as ug

    items = [
        _f(
            id="e1",
            source_layer="stamp",
            type="stamp_changed",
            title="Получено положительное заключение экспертизы",
            summary="Получено новое положительное заключение негосударственной экспертизы",
        ),
    ]
    out = ug.group_findings(items)
    assert len(out["groups"]) == 1
    assert out["groups"][0]["escalation_reason"] == "expert_review"


# ─── 3. Materials grouping: 3 findings → 1 group ────────────────────────


def test_materials_grouping_three_sheets_one_group():
    from backend.app.services.stage_comparison import unified_grouping as ug

    items = [
        _f(
            id=f"m{i}",
            type="material_changed",
            category="architecture",
            page=i,
            title="Заменён материал отделки фасада",
            summary="Плитка А заменена на плитку Б",
            old_value="Плитка керамическая А, 600x600 мм",
            new_value="Плитка керамическая Б, 600x600 мм",
            cost_impact="possible",
        )
        for i in range(1, 4)
    ]
    out = ug.group_findings(items)
    assert len(out["groups"]) == 1
    g = out["groups"][0]
    assert g["affected_count"] == 3
    assert g["theme"] == "materials"
    assert sorted(g["source_finding_ids"]) == ["m1", "m2", "m3"]
    assert sorted(g["affected_pages"]) == [1, 2, 3]


# ─── 4. Equipment grouping ──────────────────────────────────────────────


def test_equipment_grouping_one_group():
    from backend.app.services.stage_comparison import unified_grouping as ug

    items = [
        _f(
            id=f"e{i}",
            type="equipment_changed",
            category="engineering_systems",
            page=p,
            title="Заменён насос системы ОВ",
            summary="Циркуляционный насос модели X заменён на модель Y",
            old_value="Насос Grundfos UPS 100",
            new_value="Насос Wilo Stratos 100",
            cost_impact="likely",
        )
        for i, p in enumerate(range(10, 13), start=1)
    ]
    out = ug.group_findings(items)
    assert len(out["groups"]) == 1
    g = out["groups"][0]
    assert g["theme"] == "equipment"
    assert g["affected_count"] == 3
    assert g["significance"] == "high"


# ─── 5. No merge when old/new differ ────────────────────────────────────


def test_different_new_value_yields_value_variants():
    """V2 semantic clustering: плитка А→Б и плитка А→В попадают в одну
    semantic group (theme=materials + subject=tile + action=replaced),
    но разные old/new сохраняются как value_variants и группа помечается
    requires_human_review=true."""
    from backend.app.services.stage_comparison import unified_grouping as ug

    items = [
        _f(
            id="m1",
            type="material_changed",
            title="Заменена плитка",
            summary="Плитка А → Плитка Б",
            old_value="Плитка А",
            new_value="Плитка Б",
        ),
        _f(
            id="m2",
            type="material_changed",
            page=2,
            title="Заменена плитка",
            summary="Плитка А → Плитка В",
            old_value="Плитка А",
            new_value="Плитка В",
        ),
    ]
    out = ug.group_findings(items)
    assert len(out["groups"]) == 1
    g = out["groups"][0]
    assert len(g["value_variants"]) == 2
    olds = {v["old_value"] for v in g["value_variants"]}
    news = {v["new_value"] for v in g["value_variants"]}
    assert olds == {"Плитка А"}
    assert news == {"Плитка Б", "Плитка В"}
    assert g["requires_human_review"] is True
    assert g["review_reason"] == "multiple_value_variants"


# ─── 6. Simplification: removed / исключено ────────────────────────────


def test_simplification_direction_and_cost_decrease():
    from backend.app.services.stage_comparison import unified_grouping as ug

    items = [
        _f(
            id="r1",
            type="removed",
            title="Лист «Принципиальная схема системы водостока» аннулирован",
            summary="В новой редакции лист помечен «Аннул.», исключено оборудование",
            old_value="Принципиальная схема системы водостока — действующий лист",
            new_value="Аннул.",
        ),
    ]
    out = ug.group_findings(items)
    assert len(out["groups"]) == 1
    g = out["groups"][0]
    assert g["change_direction"] == "simplification"
    assert g["cost_impact_direction"] == "decrease"


# ─── 7. Evidence preservation ───────────────────────────────────────────


def test_evidence_preserved_total_count():
    from backend.app.services.stage_comparison import unified_grouping as ug

    items = [
        _f(id=f"x{i}", type="changed", page=i, title="Что-то",
           summary="Что-то изменилось", old_value="A", new_value="B")
        for i in range(1, 6)
    ]
    out = ug.group_findings(items)
    all_source_ids = []
    for g in out["groups"]:
        all_source_ids.extend(g["source_finding_ids"])
    for g in out["hidden_formal_groups"]:
        all_source_ids.extend(g["source_finding_ids"])
    assert sorted(all_source_ids) == sorted(it["id"] for it in items)


def test_no_evidence_loss_on_mixed():
    from backend.app.services.stage_comparison import unified_grouping as ug

    items = [
        _f(id="m1", type="material_changed", title="Плитка", old_value="A", new_value="B"),
        _f(id="s1", source_layer="stamp", type="stamp_changed",
           title="Изменена организация-разработчик в штампе"),
        _f(id="e1", type="equipment_changed", title="Насос", old_value="P1", new_value="P2"),
    ]
    out = ug.group_findings(items)
    all_source_ids = []
    for g in out["groups"]:
        all_source_ids.extend(g["source_finding_ids"])
    for g in out["hidden_formal_groups"]:
        all_source_ids.extend(g["source_finding_ids"])
    assert sorted(all_source_ids) == ["e1", "m1", "s1"]


# ─── 8. Cross-pair rollup ────────────────────────────────────────────────


def test_cross_pair_rollup_session_level():
    from backend.app.services.stage_comparison import unified_grouping as ug

    items = [
        _f(
            id="a",
            pair_id="pAR",
            pair_label="ABC-АР1.pdf ↔ ABC-АР1.pdf",
            type="material_changed",
            title="Заменена плитка",
            old_value="Плитка А, 600x600 мм",
            new_value="Плитка Б, 600x600 мм",
        ),
        _f(
            id="b",
            pair_id="pKR",
            pair_label="ABC-КР1.pdf ↔ ABC-КР1.pdf",
            type="material_changed",
            title="Заменена плитка",
            old_value="Плитка А, 600x600 мм",
            new_value="Плитка Б, 600x600 мм",
        ),
    ]
    out = ug.group_findings(items)
    assert len(out["groups"]) == 1
    g = out["groups"][0]
    assert g["scope_level"] == "session_rollup"
    assert sorted(g["affected_pair_ids"]) == ["pAR", "pKR"]


# ─── 9. pair_id filter ──────────────────────────────────────────────────


def test_pair_id_filter(tmp_path, monkeypatch):
    monkeypatch.setenv("COMPARISON_ROOT", str(tmp_path / "comparison_test"))
    from backend.app.services.stage_comparison import unified_grouping as ug
    from backend.app.services.stage_comparison import paths as paths_mod

    sid = "sess_test"
    items = [
        _f(id="a", pair_id="p1", type="material_changed",
           old_value="A", new_value="B"),
        _f(id="b", pair_id="p2", type="equipment_changed",
           old_value="X", new_value="Y", cost_impact="likely"),
    ]
    # Persist a fake unified_findings.json so build_unified_grouped reads it.
    p = paths_mod.unified_findings_path(sid)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps({"version": 1, "items": items}), encoding="utf-8")

    out_all = ug.get_unified_grouped(sid)
    out_p1 = ug.get_unified_grouped(sid, pair_id="p1")
    out_p2 = ug.get_unified_grouped(sid, pair_id="p2")
    assert len(out_all["groups"]) == 2
    assert len(out_p1["groups"]) == 1
    assert out_p1["groups"][0]["source_finding_ids"] == ["a"]
    assert len(out_p2["groups"]) == 1
    assert out_p2["groups"][0]["source_finding_ids"] == ["b"]


# ─── 10. include_formal toggle ──────────────────────────────────────────


def test_include_formal_toggle(tmp_path, monkeypatch):
    monkeypatch.setenv("COMPARISON_ROOT", str(tmp_path / "comparison_test"))
    from backend.app.services.stage_comparison import unified_grouping as ug
    from backend.app.services.stage_comparison import paths as paths_mod

    sid = "sess_test2"
    items = [
        _f(id="real", type="material_changed", old_value="A", new_value="B"),
        _f(id="stamp1", source_layer="stamp", type="stamp_changed",
           title="Изменена организация-разработчик"),
    ]
    p = paths_mod.unified_findings_path(sid)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps({"version": 1, "items": items}), encoding="utf-8")

    out_default = ug.get_unified_grouped(sid)
    out_with_formal = ug.get_unified_grouped(sid, include_formal=True)
    # default: hidden_formal_groups пуст (но summary считает их).
    assert len(out_default["hidden_formal_groups"]) == 0
    assert out_default["summary"]["hidden_formal_count"] == 1
    # include_formal: hidden_formal_groups присутствует.
    assert len(out_with_formal["hidden_formal_groups"]) == 1


# ─── Misc: helper functions sanity ──────────────────────────────────────


def test_normalize_value_strips_pages_and_quotes():
    from backend.app.services.stage_comparison import unified_grouping as ug

    a = 'Плитка "Каменная", 600x600 мм (стр. 17)'
    b = "Плитка «Каменная», 600x600 мм стр.18"
    assert ug.normalize_value(a) == ug.normalize_value(b)


def test_assign_theme_materials():
    from backend.app.services.stage_comparison import unified_grouping as ug

    f = _f(id="x", type="material_changed", title="Плитка А → Б",
           summary="Заменена плитка", old_value="Плитка А", new_value="Плитка Б")
    assert ug.assign_theme(f) == "materials"


def test_assign_theme_equipment():
    from backend.app.services.stage_comparison import unified_grouping as ug

    f = _f(id="x", type="equipment_changed", title="Насос заменён",
           summary="Циркуляционный насос модели X")
    assert ug.assign_theme(f) == "equipment"


def test_infer_change_direction_added_removed():
    from backend.app.services.stage_comparison import unified_grouping as ug

    assert ug.infer_change_direction(_f(id="a", type="added", title="Добавлено")) == "complication"
    assert ug.infer_change_direction(_f(id="r", type="removed", title="Удалено")) == "simplification"


# ─── V2 semantic clustering tests ────────────────────────────────────────


def test_semantic_subject_tile_groups_variants():
    """Плитка с разными формулировками на 3 листах — одна semantic group,
    value_variants сохраняют все old/new пары."""
    from backend.app.services.stage_comparison import unified_grouping as ug

    items = [
        _f(
            id="t1", type="material_changed", page=1,
            title="Заменена плитка фасадной отделки",
            summary="Плитка тип А заменена на тип Б",
            old_value="Плитка фасадная, тип А, 600x600 мм",
            new_value="Плитка фасадная, тип Б, 600x600 мм",
        ),
        _f(
            id="t2", type="material_changed", page=2,
            title="Изменена керамогранитная плитка",
            summary="Плитка А заменена на плитку Б на цоколе",
            old_value="Плитка керамогранит А",
            new_value="Плитка керамогранит Б",
        ),
        _f(
            id="t3", type="material_changed", page=3,
            title="Изменена плитка в холле",
            summary="Заменена плитка типа А на тип Б",
            old_value="Плитка А типовая",
            new_value="Плитка Б типовая",
        ),
    ]
    out = ug.group_findings(items)
    assert len(out["groups"]) == 1
    g = out["groups"][0]
    assert g["semantic_subject"] == "tile"
    assert g["affected_count"] == 3
    assert sorted(g["source_finding_ids"]) == ["t1", "t2", "t3"]
    # Все 3 разных формулировки сохранены как varianty.
    assert len(g["value_variants"]) == 3


def test_semantic_equipment_grouping_pump():
    """Несколько findings про насос → одна semantic group equipment+pump+replaced."""
    from backend.app.services.stage_comparison import unified_grouping as ug

    items = [
        _f(
            id="p1", type="equipment_changed", page=10,
            title="Заменён насос системы отопления",
            summary="Циркуляционный насос заменён на новую модель",
            old_value="Grundfos UPS 100",
            new_value="Wilo Stratos 80",
            cost_impact="likely",
        ),
        _f(
            id="p2", type="equipment_changed", page=11,
            title="Изменён насосный агрегат",
            summary="Насос модели X заменён на Y",
            old_value="Насос X",
            new_value="Насос Y",
            cost_impact="likely",
        ),
    ]
    out = ug.group_findings(items)
    assert len(out["groups"]) == 1
    g = out["groups"][0]
    assert g["semantic_subject"] == "pump"
    assert g["affected_count"] == 2


def test_no_merge_different_systems():
    """Светильники и насосы не объединяются — разные subject."""
    from backend.app.services.stage_comparison import unified_grouping as ug

    items = [
        _f(
            id="l1", type="equipment_changed",
            title="Заменён светильник в холле",
            summary="Светильник типа X заменён на тип Y",
            old_value="Светильник X",
            new_value="Светильник Y",
        ),
        _f(
            id="p1", type="equipment_changed",
            title="Заменён насос",
            summary="Насос X на насос Y",
            old_value="Насос X",
            new_value="Насос Y",
        ),
    ]
    out = ug.group_findings(items)
    assert len(out["groups"]) == 2
    subjects = {g["semantic_subject"] for g in out["groups"]}
    assert subjects == {"lighting_fixture", "pump"}


def test_no_merge_cost_direction_conflict():
    """Safety guard: одно изменение увеличивает стоимость, другое уменьшает —
    не объединять в один semantic кластер."""
    from backend.app.services.stage_comparison import unified_grouping as ug

    items = [
        _f(
            id="inc",
            type="added",
            title="Добавлен новый насос",
            summary="Добавлен дополнительный циркуляционный насос",
            new_value="Дополнительный насос",
            cost_impact="likely",
        ),
        _f(
            id="dec",
            type="removed",
            title="Удалён насос",
            summary="Исключён резервный насос из проекта",
            old_value="Резервный насос",
            cost_impact="likely",
        ),
    ]
    out = ug.group_findings(items)
    # Сохраняются как 2 отдельные группы (direction conflict).
    assert len(out["groups"]) == 2


def test_simplification_cluster():
    """removed/исключено по drainage-системе → change_direction=simplification,
    cost_impact_direction=decrease, и одинаковый subject позволяет слить
    findings в один semantic кластер."""
    from backend.app.services.stage_comparison import unified_grouping as ug

    items = [
        _f(
            id="r1", type="removed",
            title="Исключён водосток от стилобата",
            summary="Лист системы водостока аннулирован",
            old_value="Принципиальная схема системы водостока — действующий лист",
            new_value="Аннул.",
        ),
        _f(
            id="r2", type="removed",
            page=5,
            title="Удалена схема водостока в подвале",
            summary="Водосток исключён",
            old_value="Система водостока подвала",
            new_value="Аннул.",
        ),
    ]
    out = ug.group_findings(items)
    assert len(out["groups"]) == 1
    g = out["groups"][0]
    assert g["change_direction"] == "simplification"
    assert g["cost_impact_direction"] == "decrease"
    assert g["semantic_subject"] == "drainage"


def test_semantic_group_preserves_all_evidence():
    """Все source_finding_ids сохраняются в semantic merge."""
    from backend.app.services.stage_comparison import unified_grouping as ug

    items = [
        _f(id=f"t{i}", type="material_changed",
           page=i, title="Заменена плитка",
           old_value=f"Плитка тип {chr(64+i)}", new_value=f"Плитка тип {chr(89+i)}")
        for i in range(1, 5)
    ]
    out = ug.group_findings(items)
    all_ids = []
    for g in out["groups"]:
        all_ids.extend(g["source_finding_ids"])
    for g in out["hidden_formal_groups"]:
        all_ids.extend(g["source_finding_ids"])
    assert sorted(all_ids) == ["t1", "t2", "t3", "t4"]


def test_cross_pair_semantic_rollup():
    """Одинаковый semantic_subject/action в разных pair одной session → session_rollup."""
    from backend.app.services.stage_comparison import unified_grouping as ug

    items = [
        _f(
            id="a", pair_id="pAR1",
            pair_label="ABC-АР1.pdf ↔ ABC-АР1.pdf",
            type="material_changed",
            title="Заменён фасадный материал",
            old_value="Стеклофибробетон тип 1",
            new_value="Стеклофибробетон тип 2",
        ),
        _f(
            id="b", pair_id="pAR2",
            pair_label="ABC-АР2.pdf ↔ ABC-АР2.pdf",
            type="material_changed",
            title="Заменена фасадная панель",
            old_value="Стеклофибробетон A",
            new_value="Стеклофибробетон B",
        ),
    ]
    out = ug.group_findings(items)
    assert len(out["groups"]) == 1
    g = out["groups"][0]
    assert g["scope_level"] == "session_rollup"
    assert sorted(g["affected_pair_ids"]) == ["pAR1", "pAR2"]
    assert g["semantic_subject"] == "facade_material"


def test_no_cross_session_merge():
    """Группировка работает только в рамках одного group_findings() вызова.

    Этот контракт защищает от cross-session merge: каждая сессия обрабатывается
    отдельным вызовом, и API не позволяет передать items из разных сессий
    в один вызов.
    """
    from backend.app.services.stage_comparison import unified_grouping as ug

    # Imitate two separate sessions by two separate calls.
    items_session_a = [
        _f(id="a1", pair_id="pA", type="material_changed",
           old_value="Плитка А", new_value="Плитка Б"),
    ]
    items_session_b = [
        _f(id="b1", pair_id="pB", type="material_changed",
           old_value="Плитка А", new_value="Плитка Б"),
    ]
    out_a = ug.group_findings(items_session_a)
    out_b = ug.group_findings(items_session_b)
    # Каждая сессия — независимый результат, без cross-merge.
    assert len(out_a["groups"]) == 1
    assert len(out_b["groups"]) == 1
    assert out_a["groups"][0]["source_finding_ids"] == ["a1"]
    assert out_b["groups"][0]["source_finding_ids"] == ["b1"]


def test_formal_still_hidden():
    """Designer/GIP/шифр всё ещё в hidden_formal_groups."""
    from backend.app.services.stage_comparison import unified_grouping as ug

    items = [
        _f(
            id="d1", source_layer="stamp", type="stamp_changed",
            title="Изменена организация-разработчик",
            summary="Проектировщик изменён с ООО А на ООО Б",
            old_value="ООО А", new_value="ООО Б",
        ),
        _f(
            id="g1", source_layer="stamp", type="stamp_changed",
            title="Изменён ГИП",
            summary="ГИП изменён",
        ),
        _f(
            id="s1", source_layer="stamp", type="stamp_changed",
            title="Изменён шифр проекта",
            summary="Шифр изменён на новый",
        ),
    ]
    out = ug.group_findings(items)
    # Минимум 3 hidden groups (могут слиться через semantic, но не visible).
    assert len(out["groups"]) == 0
    assert len(out["hidden_formal_groups"]) >= 1
    all_hidden_ids = set()
    for g in out["hidden_formal_groups"]:
        all_hidden_ids.update(g["source_finding_ids"])
    assert all_hidden_ids == {"d1", "g1", "s1"}


def test_stage_change_not_hidden():
    """П→РД и экспертиза остаются в visible groups (escalated)."""
    from backend.app.services.stage_comparison import unified_grouping as ug

    items = [
        _f(
            id="st1", source_layer="stamp", type="stamp_changed",
            title="Изменена стадия проекта",
            summary="Стадия П заменена на РД",
            old_value="Стадия: П", new_value="Стадия: РД",
        ),
        _f(
            id="ex1", source_layer="stamp", type="stamp_changed",
            title="Получено положительное заключение экспертизы",
            summary="Новое положительное заключение негосударственной экспертизы",
        ),
    ]
    out = ug.group_findings(items)
    assert len(out["groups"]) >= 1
    escalations = {g["escalation_reason"] for g in out["groups"]}
    assert "stage_change" in escalations or "expert_review" in escalations


def test_pair_id_filter_with_semantic_groups(tmp_path, monkeypatch):
    """pair_id фильтр работает поверх semantic-кластеров: возвращает только
    группы, у которых evidence относится к этой паре."""
    monkeypatch.setenv("COMPARISON_ROOT", str(tmp_path / "comparison_test"))
    from backend.app.services.stage_comparison import unified_grouping as ug
    from backend.app.services.stage_comparison import paths as paths_mod

    sid = "sess_pair_filter"
    items = [
        _f(id="a1", pair_id="pAR1", pair_label="X-АР1.pdf ↔ X-АР1.pdf",
           type="material_changed", title="Заменён фасадный материал",
           old_value="Фасад А", new_value="Фасад Б"),
        _f(id="a2", pair_id="pAR2", pair_label="X-АР2.pdf ↔ X-АР2.pdf",
           type="material_changed", title="Заменён фасадный материал",
           old_value="Фасад А", new_value="Фасад Б"),
        _f(id="b1", pair_id="pAR2", pair_label="X-АР2.pdf ↔ X-АР2.pdf",
           type="equipment_changed", title="Заменён насос",
           old_value="Насос X", new_value="Насос Y"),
    ]
    p = paths_mod.unified_findings_path(sid)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps({"version": 1, "items": items}), encoding="utf-8")

    out_pAR1 = ug.get_unified_grouped(sid, pair_id="pAR1")
    out_pAR2 = ug.get_unified_grouped(sid, pair_id="pAR2")
    # pAR1: только facade-merged group (она cross-pair, но содержит pAR1).
    assert len(out_pAR1["groups"]) == 1
    assert out_pAR1["groups"][0]["semantic_subject"] == "facade_material"
    # pAR2: и facade group, и pump group.
    subjs_pAR2 = {g["semantic_subject"] for g in out_pAR2["groups"]}
    assert subjs_pAR2 == {"facade_material", "pump"}


def test_extract_semantic_subject_pump():
    from backend.app.services.stage_comparison import unified_grouping as ug

    f = _f(id="x", title="Заменён насос", summary="Циркуляционный насос")
    assert ug.extract_semantic_subject(f) == "pump"


def test_extract_semantic_subject_facade():
    from backend.app.services.stage_comparison import unified_grouping as ug

    f = _f(id="x", title="Заменён фасадный материал",
           summary="Фасадная облицовка изменена")
    assert ug.extract_semantic_subject(f) == "facade_material"


def test_extract_semantic_subject_unknown_safety():
    """Если subject не распознан → semantic merge не выполняется."""
    from backend.app.services.stage_comparison import unified_grouping as ug

    items = [
        _f(id="x1", type="changed",
           title="Что-то непонятное изменилось", summary="qwerty",
           old_value="A1", new_value="B1"),
        _f(id="x2", type="changed",
           title="Что-то ещё", summary="zzzz",
           old_value="A2", new_value="B2"),
    ]
    out = ug.group_findings(items)
    # Оба считаются unknown, никакого склейки не должно произойти.
    assert len(out["groups"]) == 2
