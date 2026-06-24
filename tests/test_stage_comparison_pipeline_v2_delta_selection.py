# -*- coding: utf-8 -*-
"""Тесты engineering_first selection strategy (Pipeline V2 delta explanation).

Synthetic deltas, без LLM/сети. Покрытие по spec-пунктам:
  1.  changed_only работает как раньше;
  2.  engineering_first выбирает cable/equipment/power/scheme раньше stamp;
  3.  stamp_field не занимает весь max_deltas;
  4.  contents/navigation ограничены quota;
  5.  weak/artifact идут в конец;
  6.  max_deltas строго соблюдается;
  7.  порядок стабильный (deterministic);
  8.  per_subject_cap ограничивает дробление подписи;
  9.  мало инженерных → остаток добирается stamp/navigation;
 10.  нет инженерных вовсе → стратегия не падает, выборка полезная;
 11.  include_high_confidence не ломает quotas;
 12.  llm_runner=None не вызывает LLM;
 13.  существующие стратегии не изменились.
"""
from __future__ import annotations

import pytest

from backend.app.services.stage_comparison import (
    pipeline_v2_delta_explanation as de,
)


# ─── builders ────────────────────────────────────────────────────────────────


_SEQ = {"n": 0}


def _delta(entity_type, *, delta_type="changed", subject="", field="value",
           confidence=0.85, flags=None, page_left=5, page_right=5, did=None):
    _SEQ["n"] += 1
    return {
        "delta_id": did or f"d{_SEQ['n']:03d}_{entity_type}_{delta_type}",
        "delta_type": delta_type, "entity_type": entity_type,
        "semantic_group": "x", "subject": subject, "field": field,
        "old_value": "old", "new_value": "new", "confidence": confidence,
        "page_numbers": {"left": page_left, "right": page_right},
        "evidence": {"left": {"quote": "old"}, "right": {"quote": "new"}},
        "quality_flags": list(flags or []),
    }


def _report(deltas):
    return {"version": 1, "kind": "stage_comparison_pipeline_v2_entity_diff",
            "summary": {"deltas_total": len(deltas)}, "deltas": deltas,
            "warnings": []}


def _opts(strategy="engineering_first", max_deltas=20, include_high=True,
          ef=None):
    o = {"selection_strategy": strategy, "max_deltas": max_deltas,
         "include_high_confidence": include_high}
    if ef is not None:
        o["engineering_first"] = ef
    return o


def _mixed_deltas():
    """Реалистичный микс по образу ИОС1.1: штампы доминируют по количеству."""
    deltas = []
    # 8 инженерных: 2 changed power, cable/equipment/scheme added/removed
    deltas.append(_delta("power_supply", delta_type="changed", subject="ввод 1"))
    deltas.append(_delta("power_supply", delta_type="changed", subject="ввод 2"))
    deltas.append(_delta("cable", delta_type="added", subject="ВВГнг 3x2.5"))
    deltas.append(_delta("cable", delta_type="removed", subject="ВВГнг 3x1.5"))
    deltas.append(_delta("equipment", delta_type="added", subject="ИБП"))
    deltas.append(_delta("scheme_component", delta_type="added", subject="QF1"))
    deltas.append(_delta("table_row", delta_type="added", subject="поз. 5"))
    deltas.append(_delta("norm_reference", delta_type="removed", subject="СП 256"))
    # 12 штампов на разных страницах (high-confidence)
    for p in range(1, 13):
        deltas.append(_delta("stamp_field", subject="document_code",
                             confidence=0.9, page_left=p, page_right=p))
    # 4 навигации
    for p in range(1, 5):
        deltas.append(_delta("contents_item", subject=f"строка {p}",
                             page_left=4, page_right=4))
    # 3 weak: unknown + ocr_noise + low_match_score
    deltas.append(_delta("unknown", subject="мусор"))
    deltas.append(_delta("stamp_field", subject="project_name",
                         flags=["possible_ocr_noise"]))
    deltas.append(_delta("cable", subject="КГ 3х4", flags=["low_match_score"]))
    return deltas


def _select(deltas, **kw):
    return de.select_deltas_for_explanation(_report(deltas), _opts(**kw))


def _types(selected):
    return [d["entity_type"] for d in selected]


# ─── 1, 13: старые стратегии не изменились ───────────────────────────────────


def test_1_changed_only_unchanged():
    deltas = _mixed_deltas()
    sel = _select(deltas, strategy="changed_only", max_deltas=50)
    assert all(d["delta_type"] == "changed" for d in sel)
    # порядок исходный (по списку дельт), без переранжирования
    ids = [d["delta_id"] for d in sel]
    src = [d["delta_id"] for d in deltas if d["delta_type"] == "changed"]
    assert ids == src[:len(ids)]


def test_13_other_strategies_unchanged():
    deltas = _mixed_deltas()
    all_sel = _select(deltas, strategy="all", max_deltas=1000)
    assert len(all_sel) == len(deltas)
    nhr = _select(deltas, strategy="needs_human_review", max_deltas=1000)
    assert nhr == []  # ни у одной дельты нет needs_human_review флага
    prio = _select(deltas, strategy="priority_only", max_deltas=1000,
                   include_high=False)
    # added/removed + low-confidence changed + flagged — как раньше
    assert all(d["delta_type"] in ("added", "removed")
               or float(d["confidence"]) < 0.75
               or set(d["quality_flags"]) & de._PRIORITY_FLAGS
               or d["confidence"] < 0.9
               for d in prio)


# ─── 2-5: приоритет групп и квоты ────────────────────────────────────────────


def test_2_engineering_selected_before_stamp():
    sel = _select(_mixed_deltas(), max_deltas=20)
    types = _types(sel)
    # все 8 чистых инженерных вошли и идут ПЕРЕД первым штампом
    first_stamp = types.index("stamp_field")
    eng_before = [t for t in types[:first_stamp]
                  if t in de._SELECTION_ENGINEERING_TYPES]
    assert len(eng_before) == 8
    for t in ("cable", "equipment", "power_supply", "scheme_component"):
        assert t in types


def test_3_stamp_cannot_take_all():
    # только штампы есть в избытке: инженерных 8 < квоты 12 → штампы добирают,
    # но при наличии инженерных НЕ вытесняют их
    sel = _select(_mixed_deltas(), max_deltas=20)
    stamps = [t for t in _types(sel) if t == "stamp_field"]
    assert 0 < len(stamps) < 20
    # инженерные заняли свои места полностью
    assert sum(1 for t in _types(sel)
               if t in de._SELECTION_ENGINEERING_TYPES) >= 8


def test_4_navigation_quota_limited():
    sel = _select(_mixed_deltas(), max_deltas=20)
    nav_in_quota_zone = [d for d in sel
                         if d["entity_type"] == "contents_item"]
    # навигации в выборке не больше, чем quota + возможный добор ПОСЛЕ
    # leftovers штампов; в данном миксе слотов хватает штампам → ровно quota
    assert len(nav_in_quota_zone) == 2


def test_5_weak_goes_last():
    # гарантируемое свойство: КВОТНАЯ зона инженерных идёт раньше первого
    # weak (добор leftovers прохода 2 МОЖЕТ стоять после weak-квоты — это
    # задокументированное перераспределение, не нарушение приоритета)
    sel = _select(_mixed_deltas(), max_deltas=20)
    groups = [de.classify_selection_group(d) for d in sel]
    assert "weak_or_artifact" in groups
    first_weak = groups.index("weak_or_artifact")
    eng_total = groups.count("engineering")
    quota_zone = min(eng_total, de._ENGINEERING_FIRST_DEFAULTS["engineering_quota"])
    assert groups[:first_weak].count("engineering") >= quota_zone
    # weak не превышает квоту, пока есть другие кандидаты
    assert groups.count("weak_or_artifact") <= \
        de._ENGINEERING_FIRST_DEFAULTS["weak_quota"]


def test_5b_redistribution_order_documented():
    """>12 инженерных: leftovers добираются ПОСЛЕ weak-квоты (проход 2)."""
    deltas = [_delta("cable", delta_type="added", subject=f"каб {i}",
                     page_left=i, page_right=i) for i in range(14)]
    deltas.append(_delta("unknown", subject="мусор1"))
    deltas.append(_delta("unknown", subject="мусор2"))
    sel = _select(deltas, max_deltas=16)
    groups = [de.classify_selection_group(d) for d in sel]
    assert groups == ["engineering"] * 12 + ["weak_or_artifact"] * 2 \
        + ["engineering"] * 2


# ─── 6-7: строгий лимит и детерминизм ────────────────────────────────────────


def test_6_max_deltas_strict():
    for n in (1, 5, 20, 100):
        sel = _select(_mixed_deltas(), max_deltas=n)
        assert len(sel) <= n
    assert len(_select(_mixed_deltas(), max_deltas=0)) == 0


def test_7_deterministic_order():
    _SEQ["n"] = 0  # одинаковые id при каждой генерации микса
    a = _select(_mixed_deltas(), max_deltas=20)
    _SEQ["n"] = 0
    b = _select(_mixed_deltas(), max_deltas=20)
    assert [d["delta_id"] for d in a] == [d["delta_id"] for d in b]
    # перетасованный вход даёт ту же выборку (сортировка внутри групп)
    _SEQ["n"] = 0
    deltas = _mixed_deltas()
    sel_rev = de.select_deltas_for_explanation(
        _report(list(reversed(deltas))), _opts(max_deltas=20))
    assert sorted(d["delta_id"] for d in sel_rev) == \
           sorted(d["delta_id"] for d in a)


# ─── 8: per_subject_cap ──────────────────────────────────────────────────────


def test_8_per_subject_cap_limits_signature_split():
    # подпись штампа: 4 атомарные дельты одного события (subject=signature,
    # одна пара страниц) + 1 другой штамп
    deltas = [
        _delta("stamp_field", subject="signature", did="sig_composite"),
        _delta("stamp_field", subject="signature", did="sig_role"),
        _delta("stamp_field", subject="signature", did="sig_surname"),
        _delta("stamp_field", subject="signature", did="sig_date"),
        _delta("stamp_field", subject="organization", did="org"),
    ]
    sel = _select(deltas, max_deltas=3)
    subjects = [d["subject"] for d in sel]
    # cap=2: максимум 2 дельты подписи в основной выборке, organization вошла
    assert subjects.count("signature") == 2
    assert "organization" in subjects
    # при свободных слотах overflow добирается ПОСЛЕДНИМ и НЕ теряется
    sel5 = _select(deltas, max_deltas=5)
    subjects5 = [d["subject"] for d in sel5]
    assert len(sel5) == 5                       # излишки cap'а добраны
    assert subjects5.count("signature") == 4    # 2 kept + 2 overflow
    assert subjects5[-2:] == ["signature", "signature"]


def test_8b_group_key_shape():
    d = _delta("stamp_field", subject="Signature", page_left=4, page_right=4)
    assert de.build_selection_group_key(d) == "stamp_field|signature|4|4"
    # без subject — fallback на field
    d2 = _delta("cable", subject="", field="presence")
    assert "cable|presence|" in de.build_selection_group_key(d2)


# ─── 9-10: деградация при дефиците инженерных ────────────────────────────────


def test_9_few_engineering_backfilled_by_stamp():
    deltas = [_delta("power_supply", subject="ввод")]
    deltas += [_delta("stamp_field", subject="document_code",
                      page_left=p, page_right=p) for p in range(1, 30)]
    sel = _select(deltas, max_deltas=20)
    assert len(sel) == 20
    assert _types(sel)[0] == "power_supply"
    assert _types(sel).count("stamp_field") == 19


def test_10_no_engineering_at_all_still_useful():
    deltas = [_delta("stamp_field", subject="document_code",
                     page_left=p, page_right=p) for p in range(1, 6)]
    deltas += [_delta("contents_item", subject=f"s{i}") for i in range(3)]
    sel = _select(deltas, max_deltas=20)
    assert len(sel) == 8  # всё, что есть
    assert set(_types(sel)) == {"stamp_field", "contents_item"}
    # пустой вход не падает
    assert _select([], max_deltas=20) == []


# ─── 11: include_high_confidence ─────────────────────────────────────────────


def test_11_include_high_confidence_respects_quotas():
    deltas = _mixed_deltas()
    sel_true = _select(deltas, max_deltas=20, include_high=True)
    sel_false = _select(deltas, max_deltas=20, include_high=False)
    assert len(sel_true) == 20
    # include_high=False: семантика priority_only-фильтра запинена точно —
    # кандидаты = 6 added/removed инженерных + 2 priority-flagged weak;
    # ВСЕ high-confidence чистые changed (штампы, contents, power) выпали
    assert len(sel_false) == 8
    assert all(d["subject"] != "document_code" for d in sel_false)
    assert sum(1 for t in _types(sel_false)
               if t in de._SELECTION_ENGINEERING_TYPES) >= 6
    # квоты не «ломаются»: weak не вытесняет инженерные в обоих режимах
    for sel in (sel_true, sel_false):
        groups = [de.classify_selection_group(d) for d in sel]
        if "weak_or_artifact" in groups and "engineering" in groups:
            assert groups.index("engineering") < groups.index("weak_or_artifact")


# ─── 12: llm_runner=None не вызывает LLM ─────────────────────────────────────


def test_12_no_llm_with_engineering_first(monkeypatch):
    import socket

    def _boom(*a, **k):  # pragma: no cover
        raise AssertionError("network in selection test")

    monkeypatch.setattr(socket, "socket", _boom)
    rep = de.explain_entity_diff_report(
        _report(_mixed_deltas()), None, _opts(max_deltas=10), llm_runner=None)
    assert rep["summary"]["selected_total"] == 10
    assert rep["summary"]["skipped_total"] == 10
    assert rep["selection"]["strategy"] == "engineering_first"


# ─── дополнительные контракты ────────────────────────────────────────────────


def test_quotas_override_via_options():
    sel = _select(_mixed_deltas(), max_deltas=10,
                  ef={"engineering_quota": 2, "admin_stamp_quota": 1,
                      "navigation_quota": 1, "weak_quota": 0,
                      "per_subject_cap": 1})
    types = _types(sel)
    assert len(sel) == 10
    # квоты прохода 1: 2 eng + 1 stamp + 1 nav + 0 weak; добор прохода 2 —
    # из leftovers в порядке приоритета → ВСЕ оставшиеся инженерные первыми.
    # Точный состав (отличим от defaults 12/4/2/2 — kill-тест ревью):
    assert types[0] in de._SELECTION_ENGINEERING_TYPES
    assert sum(1 for t in types if t in de._SELECTION_ENGINEERING_TYPES) == 8
    assert types.count("stamp_field") == 1
    assert types.count("contents_item") == 1


def test_engineering_first_config_parses_overrides():
    cfg = de._engineering_first_config({"engineering_first": {
        "engineering_quota": 3, "weak_quota": 7, "per_subject_cap": 0}})
    assert cfg["engineering_quota"] == 3
    assert cfg["weak_quota"] == 7
    assert cfg["per_subject_cap"] == 0
    # незатронутые ключи — defaults
    assert cfg["admin_stamp_quota"] == \
        de._ENGINEERING_FIRST_DEFAULTS["admin_stamp_quota"]
    # отрицательные клампятся в 0
    assert de._engineering_first_config(
        {"engineering_first": {"navigation_quota": -5}})["navigation_quota"] == 0


def test_junk_quota_values_fall_back_to_defaults():
    sel = _select(_mixed_deltas(), max_deltas=20,
                  ef={"engineering_quota": "abc", "per_subject_cap": None})
    assert len(sel) == 20  # не падает, defaults применены


def test_confidence_ordering_within_group():
    deltas = [
        _delta("cable", delta_type="added", subject="c-low", confidence=0.5),
        _delta("cable", delta_type="added", subject="c-high", confidence=0.9),
        _delta("cable", delta_type="added", subject="c-mid", confidence=0.7),
    ]
    sel = _select(deltas, max_deltas=10)
    assert [d["subject"] for d in sel] == ["c-high", "c-mid", "c-low"]
    # при max_deltas=1 выживает самый уверенный
    assert _select(deltas, max_deltas=1)[0]["subject"] == "c-high"


def test_delta_type_rank_changed_beats_added():
    # равные confidence, у added лексикографически МЕНЬШИЙ id —
    # порядок обязан определяться рангом delta_type, не id
    deltas = [
        _delta("cable", delta_type="added", subject="a", confidence=0.8,
               did="a_added"),
        _delta("cable", delta_type="changed", subject="z", confidence=0.8,
               did="z_changed"),
    ]
    sel = _select(deltas, max_deltas=1)
    assert sel[0]["delta_id"] == "z_changed"


def test_navigation_before_weak_in_group_order():
    sel = _select(_mixed_deltas(), max_deltas=20)
    groups = [de.classify_selection_group(d) for d in sel]
    assert "navigation_contents" in groups and "weak_or_artifact" in groups
    assert groups.index("navigation_contents") < groups.index("weak_or_artifact")


def test_per_subject_cap_is_per_group():
    """Одинаковый group key в РАЗНЫХ selection-группах — у каждой свой cap."""
    deltas = [
        # admin_stamp: 2 чистые подписи (cap=2 → обе в kept)
        _delta("stamp_field", subject="signature", did="s1"),
        _delta("stamp_field", subject="signature", did="s2"),
        # weak: те же entity/subject/pages, но с ocr_noise-флагом
        _delta("stamp_field", subject="signature", did="w1",
               flags=["possible_ocr_noise"]),
        _delta("stamp_field", subject="signature", did="w2",
               flags=["possible_ocr_noise"]),
        # наполнители navigation
        _delta("contents_item", subject="n1"),
        _delta("contents_item", subject="n2"),
    ]
    sel = _select(deltas, max_deltas=6)
    ids = [d["delta_id"] for d in sel]
    # глобальный cap съел бы w1/w2 (3-я/4-я с тем же ключом) до overflow;
    # по-групповой cap держит обе weak-дельты в kept своей группы
    assert len(sel) == 6
    assert {"s1", "s2", "w1", "w2"} <= set(ids)
    groups = [de.classify_selection_group(d) for d in sel]
    assert groups.count("weak_or_artifact") == 2


def test_classify_groups():
    assert de.classify_selection_group(_delta("cable")) == "engineering"
    assert de.classify_selection_group(_delta("stamp_field")) == "admin_stamp"
    assert de.classify_selection_group(_delta("contents_item")) == "navigation_contents"
    assert de.classify_selection_group(_delta("unknown")) == "weak_or_artifact"
    assert de.classify_selection_group(
        _delta("cable", flags=["possible_ocr_noise"])) == "weak_or_artifact"
    # нераспознанный НЕ-weak тип → engineering (forward-compat)
    assert de.classify_selection_group(_delta("future_type")) == "engineering"
    # one-sided evidence-флаги НЕ делают дельту weak
    assert de.classify_selection_group(
        _delta("cable", flags=["left_evidence_missing", "one_sided_entity"])
    ) == "engineering"