"""Тесты усиленного штамп-матчинга (Stage 2-8): canonicalize, признаки листа,
hard-gates, candidate-matrix + mutual-best, безопасный LLM-adjudicator.

Дополняет tests/test_stage_comparison_stamp_matching.py (exact/forward-fill/
distinctive-offset/in-order dup/ambiguous-suppression остаются там).
"""
from __future__ import annotations

import json
from dataclasses import dataclass

from backend.app.services.stage_comparison import stamp_matching as sm
from backend.app.services.stage_comparison import stamp_llm_match as slm


# ─── helpers ────────────────────────────────────────────────────────────────

def _md(pages: list[tuple[int, object, object]]) -> str:
    out: list[str] = []
    for pn, sno, snm in pages:
        out.append(f"## СТРАНИЦА {pn}")
        if sno is not None:
            out.append(f"**Лист:** {sno}")
        if snm is not None:
            out.append(f"**Наименование листа:** {snm}")
        out.append(f"текст страницы {pn}")
        out.append("")
    return "\n".join(out)


def _idx(pages, **kw):
    return sm.build_sheet_index(_md(pages), **kw)


def _feat(name: str, *, page: int = 1, source: str = "md") -> sm.SheetFeatures:
    rec = sm.SheetRec(page=page, sheet_no=str(page), sheet_name=name,
                      norm_name=sm.normalize_sheet_name(name),
                      section_class="structural", is_graphic=True,
                      name_source=source)
    return sm.extract_sheet_features(rec)


def _matched(res):
    return {(it["left_page"], it["right_page"], it["match_type"])
            for it in res["suggested_items"] if it["match"]}


# ═══ Stage 3 — canonicalize_sheet_name ════════════════════════════════════

def test_canonicalize_safe_aliases():
    c = sm.canonicalize_sheet_name
    assert c(sm.normalize_sheet_name("Однолинейная расчетная схема ГРЩ")) == \
        c(sm.normalize_sheet_name("Однолинейная схема ГРЩ"))
    assert c(sm.normalize_sheet_name("План расположения ВРУ")) == \
        c(sm.normalize_sheet_name("План ВРУ"))
    assert c("") == ""
    # идемпотентность
    once = c(sm.normalize_sheet_name("Однолинейная расчетная схема ГРЩ"))
    assert c(once) == once


def test_canonicalize_does_not_merge_different_scheme_types():
    """расчетная/принципиальная/структурная схемы НЕ сливаются вслепую."""
    c = sm.canonicalize_sheet_name
    assert c(sm.normalize_sheet_name("Принципиальная схема АВР")) != \
        c(sm.normalize_sheet_name("Структурная схема АВР"))


# ═══ Stage 2 — извлечение признаков ═══════════════════════════════════════

def test_extract_features_kind_system_equipment():
    f = _feat("Однолинейная схема ВРУ-1")
    assert f.sheet_kind == "схема"
    assert "вру" in f.system_tokens
    assert "вру-1" in f.equipment_ids


def test_extract_features_floor_building():
    f = _feat("План 2 этажа корпус 3 секция 1")
    assert f.sheet_kind == "план"
    assert "этаж:2" in f.floor_tokens
    assert "корпус:3" in f.building_tokens
    assert "секция:1" in f.building_tokens


def test_extract_equipment_skips_voltage_rating():
    """«ГРЩ-0,4кВ» — это напряжение, а не номер единицы → не equipment_id."""
    f = _feat("Однолинейная схема ГРЩ-0,4кВ")
    assert f.equipment_ids == set()
    assert "грщ" in f.system_tokens


# ═══ Stage 4 — get_hard_conflict ═══════════════════════════════════════════

def test_hard_conflict_equipment_unit():
    assert sm.get_hard_conflict(_feat("Схема ВРУ-1"), _feat("Схема ВРУ-2"))
    assert sm.get_hard_conflict(_feat("Схема ЩО-1"), _feat("Схема ЩО-2"))
    # одинаковая единица — конфликта нет
    assert sm.get_hard_conflict(_feat("Схема ВРУ-1"), _feat("Схема ВРУ-1 ввод")) is None


def test_hard_conflict_system_family():
    assert sm.get_hard_conflict(_feat("Однолинейная схема ГРЩ"),
                                _feat("Однолинейная схема ВРУ")) == "system_conflict"


def test_hard_conflict_floor_building():
    assert sm.get_hard_conflict(_feat("План 1 этажа"), _feat("План 2 этажа")) == "floor_conflict"
    assert sm.get_hard_conflict(_feat("План корпус 1"), _feat("План корпус 2")) == "building_conflict"
    assert sm.get_hard_conflict(_feat("План секция 1"), _feat("План секция 2")) == "building_conflict"


def test_hard_conflict_kind():
    assert sm.get_hard_conflict(_feat("План ВРУ"), _feat("Спецификация ВРУ")) == "kind_conflict"
    assert sm.get_hard_conflict(_feat("План ВРУ"), _feat("Ведомость ВРУ")) == "kind_conflict"
    # схема vs спецификация — конфликт ТОЛЬКО без общего оборудования
    assert sm.get_hard_conflict(_feat("Схема ВРУ"), _feat("Спецификация ВРУ")) == "kind_conflict"
    assert sm.get_hard_conflict(_feat("Схема ВРУ-1"), _feat("Спецификация ВРУ-1")) is None


def test_hard_conflict_none_when_one_side_lacks_feature():
    # у одной стороны нет этажа → нельзя утверждать конфликт
    assert sm.get_hard_conflict(_feat("План 1 этажа"), _feat("План ВРУ")) is None


# ═══ Stage 3 — exact_canonical_name в матчере ═════════════════════════════

def test_canonical_match_without_llm():
    left = _idx([(1, "1", "Содержание тома"),
                 (2, "2", "Однолинейная расчетная схема ГРЩ")])
    right = _idx([(1, "1", "Содержание тома"),
                  (2, "2", "Однолинейная схема ГРЩ")])
    res = sm.match_sheet_indexes(left, right)  # без LLM
    assert (2, 2, "exact_canonical_name") in _matched(res)
    it = next(it for it in res["suggested_items"]
              if it["match"] and it["left_page"] == 2)
    assert it["needs_review"] is False  # канонический алиас — безопасно


# ═══ Stage 4/5 — hard-gates подавляют ложные пары в матчере ═══════════════

def test_vru1_vru2_not_matched():
    res = sm.match_sheet_indexes(_idx([(1, "1", "Однолинейная схема ВРУ-1")]),
                                 _idx([(1, "1", "Однолинейная схема ВРУ-2")]))
    assert res["matched_count"] == 0
    assert any(r["rejected_reason"].startswith("equipment_conflict")
               for r in res["rejected"])


def test_grsh_vru_not_matched():
    res = sm.match_sheet_indexes(_idx([(1, "1", "Однолинейная схема ГРЩ")]),
                                 _idx([(1, "1", "Однолинейная схема ВРУ")]))
    assert res["matched_count"] == 0
    assert any(r["rejected_reason"] == "system_conflict" for r in res["rejected"])


def test_floor_plans_not_matched():
    res = sm.match_sheet_indexes(_idx([(1, "1", "План 1 этажа")]),
                                 _idx([(1, "1", "План 2 этажа")]))
    assert res["matched_count"] == 0


def test_korpus_not_matched():
    res = sm.match_sheet_indexes(_idx([(1, "1", "План корпус 1")]),
                                 _idx([(1, "1", "План корпус 2")]))
    assert res["matched_count"] == 0


def test_sekcia_not_matched():
    res = sm.match_sheet_indexes(_idx([(1, "1", "План секция 1")]),
                                 _idx([(1, "1", "План секция 2")]))
    assert res["matched_count"] == 0


def test_plan_vs_specification_not_matched():
    # Сильно совпадающие имена, но разный вид листа (план vs спецификация) —
    # hard-gate блокирует «соблазнительную» пару и фиксирует её в rejected.
    res = sm.match_sheet_indexes(
        _idx([(1, "1", "План оборудования ВРУ-1 этажные щиты распределение")]),
        _idx([(1, "1", "Спецификация оборудования ВРУ-1 этажные щиты распределение")]))
    assert res["matched_count"] == 0
    assert any(r["rejected_reason"] == "kind_conflict" for r in res["rejected"])


def test_same_kind_different_building_cross_matches_correctly():
    """Дубликаты по виду листа, различающиеся корпусом, матчатся по имени
    и НЕ скремблируются (корпус1↔корпус2 запрещён hard-gate)."""
    left = _idx([(1, "1", "План корпус 1"), (2, "2", "План корпус 2")])
    right = _idx([(1, "1", "План корпус 2"), (2, "2", "План корпус 1")])
    res = sm.match_sheet_indexes(left, right)
    m = {(it["left_page"], it["right_page"]) for it in res["suggested_items"] if it["match"]}
    assert m == {(1, 2), (2, 1)}


# ═══ Stage 5 — mutual-best ════════════════════════════════════════════════

def test_mutual_best_prevents_weaker_left_grabbing_right():
    """Слабый левый лист не уводит правый, который сильнее тянется к другому."""
    left = _idx([(1, "1", "Схема ВРУ"),
                 (2, "2", "Схема ВРУ ИТП теплоснабжение узел распределение")])
    right = _idx([(1, "1", "Схема ВРУ ИТП теплоснабжение узел распределение ввод резерв")])
    res = sm.match_sheet_indexes(left, right)
    m = {(it["left_page"], it["right_page"]) for it in res["suggested_items"] if it["match"]}
    assert (2, 1) in m       # сильная пара
    assert (1, 1) not in m   # слабый левый p1 не перехватил


def test_fuzzy_match_has_diagnostics():
    left = _idx([(1, "1", "Схема ВРУ ИТП расчетная")])
    right = _idx([(1, "1", "Содержание тома"),
                  (2, "2", "Схема ВРУ ИТП расчетная однолинейная")])
    res = sm.match_sheet_indexes(left, right)
    it = next(it for it in res["suggested_items"] if it["match"] and it["left_page"] == 1)
    assert it["match_type"] in ("fuzzy_name", "fuzzy_structural")
    assert it["match_diag"]["mutual_best"] is True
    assert "margin" in it["match_diag"]
    assert it["positive_evidence"]  # хоть один признак


# ═══ Stage 8 — text_layer риск и более строгий порог ══════════════════════

def test_text_layer_match_has_risk_flag():
    left = _idx([(1, None, None)], extra_text_by_page={1: "Схема ВРУ ИТП однолинейная"})
    right = _idx([(1, None, None)], extra_text_by_page={1: "Схема ВРУ ИТП однолинейная"})
    res = sm.match_sheet_indexes(left, right)
    it = next(it for it in res["suggested_items"] if it["match"])
    assert it["match_type"] == "text_layer"
    assert "text_layer_fallback" in it["risk_flags"]


def test_text_layer_uses_stricter_threshold():
    """Тот же fuzzy-кейс: как имя из MD матчится (низкий порог), как текст-слой —
    нет (строгий порог)."""
    name_l = "Схема ВРУ ИТП однолинейная вводная"
    name_r = "Схема ВРУ ИТП расчетная распределительная"
    md_res = sm.match_sheet_indexes(_idx([(1, "1", name_l)]), _idx([(1, "1", name_r)]),
                                    min_score=0.1, fallback_min_score=0.99)
    tl_res = sm.match_sheet_indexes(
        _idx([(1, None, None)], extra_text_by_page={1: name_l}),
        _idx([(1, None, None)], extra_text_by_page={1: name_r}),
        min_score=0.1, fallback_min_score=0.99)
    assert md_res["matched_count"] == 1     # как имя — проходит низкий порог
    assert tl_res["matched_count"] == 0     # как текст-слой — держим строгий порог


# ═══ Stage 7 — безопасный LLM-adjudicator ═════════════════════════════════

def test_llm_cannot_choose_outside_candidates():
    left = _idx([(1, "1", "Содержание тома"), (2, "2", "Схема ГРЩ")])
    right = _idx([
        (1, "1", "Содержание тома"),
        (2, "2", "Схема ГРЩ распределение этажные щиты ввод резерв магистрали стояки"),
        (3, "3", "Пожарная сигнализация план эвакуации текстовый совсем другой")])

    # LLM пытается выбрать p3 (НЕ кандидат для p2) — должно игнорироваться.
    def fn(rem_left, rem_right, tasks=None):
        return [(2, 3, 0.95, "llm_semantic")]

    res = sm.match_sheet_indexes(left, right, llm_match_fn=fn)
    m = {(it["left_page"], it["right_page"]) for it in res["suggested_items"] if it["match"]}
    assert (2, 3) not in m
    assert res["llm_match_count"] == 0


def test_llm_hard_gate_blocks_vru1_vru2_even_if_forced():
    left = _idx([(1, "1", "Схема ВРУ-1")])
    right = _idx([(1, "1", "Схема ВРУ-2")])

    def fn(rem_left, rem_right, tasks=None):
        return [(1, 1, 0.99, "llm_semantic")]  # форсим конфликтную пару

    res = sm.match_sheet_indexes(left, right, llm_match_fn=fn)
    assert res["matched_count"] == 0
    assert res["llm_match_count"] == 0


def test_llm_adjudicate_accepts_valid_candidate():
    left = _idx([(1, "1", "Содержание тома"), (2, "2", "Однолинейная расчетная схема ГРЩ")])
    right = _idx([
        (1, "1", "Содержание тома"),
        (2, "2", "Схема ГРЩ распределение этажные щиты корпус 5 секция 2 ввод резерв магистрали стояки")])

    def fn(rem_left, rem_right, tasks=None):
        # tasks должен прийти из matcher с кандидатом p2
        assert tasks and tasks[0]["candidates"]
        return [(2, 2, 0.86, "llm_semantic")]

    res = sm.match_sheet_indexes(left, right, llm_match_fn=fn)
    assert (2, 2, "llm_semantic") in _matched(res)
    assert res["llm_match_count"] == 1


# ─── llm_adjudicate_candidates (мок-provider) ──────────────────────────────

@dataclass
class _PR:
    status: str
    raw_response: str = ""
    error: str | None = None
    duration_sec: float = 0.1
    model: str = "haiku"


class _Provider:
    def __init__(self, pr):
        self._pr = pr
        self.calls = []

    def invoke(self, *, system_prompt, user_prompt, model, timeout_sec, work_dir=None):
        self.calls.append({"system": system_prompt, "user": user_prompt})
        return self._pr


def _task():
    return {"left_page": 51, "left_name": "Однолинейная расчетная схема ГРЩ",
            "left_kind": "схема", "left_systems": ["грщ"],
            "candidates": [{"new_page": 32, "name": "Однолинейная схема ГРЩ",
                            "deterministic_score": 0.4, "kind": "схема",
                            "systems": ["грщ"]}]}


def test_build_candidate_prompt_lists_candidates():
    system, user, meta = slm.build_candidate_match_prompt([_task()])
    assert "page 51" in user and "page 32" in user
    assert "кандидат" in user.lower()
    assert meta["n_tasks"] == 1 and meta["n_candidates"] == 1


def test_adjudicate_enforces_candidate_membership():
    # модель вернула p99 (нет в кандидатах) и p32 (есть) — остаётся только p32
    pr = _PR(status="done", raw_response=json.dumps({"pairs": [
        {"old_page": 51, "new_page": 99, "confidence": 0.9},
        {"old_page": 51, "new_page": 32, "confidence": 0.9}]}))
    rep = slm.llm_adjudicate_candidates([_task()], provider=_Provider(pr), min_confidence=0.6)
    # дедуп по old_page оставит первую (p99) на этапе parse, но membership-фильтр
    # её выкинет → пар нет (p99 не кандидат, p32 после дедупа old_page уже занят)
    assert all(p["new_page"] in (32,) for p in rep["pairs"])
    assert all(p["old_page"] == 51 for p in rep["pairs"])


def test_adjudicate_failsoft_on_provider_error():
    rep = slm.llm_adjudicate_candidates(
        [_task()], provider=_Provider(_PR(status="error", error="rc=1")))
    assert rep["status"] == "error"
    assert rep["pairs"] == []


def test_adjudicate_no_candidates_skips_call():
    prov = _Provider(_PR(status="done", raw_response="{}"))
    rep = slm.llm_adjudicate_candidates(
        [{"left_page": 1, "candidates": []}], provider=prov)
    assert rep["status"] == "no_unmatched"
    assert prov.calls == []
