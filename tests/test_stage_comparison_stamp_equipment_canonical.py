"""Тесты сопоставления однолинейных схем оборудования по panel identity.

Реальный кейс ИОС 1.1 (Балчуг): имя листа в штампе несёт длинный
дисциплинарный ПРЕФИКС, который МЕНЯЕТСЯ между стадиями, а различающий лист
«хвост» (ВРУ-1, ГРЩ) спрятан в конце:

    старая: «Часть 1. Внутреннее электроснабжение и освещение. Молниезащита и
             заземление. (в т.ч.ОЗДС). Однолинейная расчетная схема ГРЩ»
    новая:  «Внутреннее электроснабжение и освещение. (втч ОЗДС) Однолинейная
             схема ГРЩ»

Различающийся префикс ломал и exact_canonical (canonical_name мимо), и
multipart-проход (sheet_group_key из всего имени мимо). Фикс — извлечь
panel identity «однолинейная схема|<panel>» и матчить по ней.
"""
from __future__ import annotations

from backend.app.services.stage_comparison import stamp_matching as sm
from backend.app.services.stage_comparison import stamp_auto_apply as saa


# Реальные префиксы штампа из ИОС 1.1 (различаются между стадиями).
LPRE = ("Часть 1. Внутреннее электроснабжение и освещение. Молниезащита и "
        "заземление. (в т.ч.ОЗДС). ")
RPRE = "Внутреннее электроснабжение и освещение. (втч ОЗДС) "


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


def _matched(res):
    return {(it["left_page"], it["right_page"]): it
            for it in res["suggested_items"] if it["match"]}


# ═══ panel identity / designation ═════════════════════════════════════════

def test_panel_designation_basic():
    p = sm._extract_panel_designation
    assert p("однолинейная схема вру-1") == "вру-1"
    assert p("однолинейная схема вру2") == "вру-2"
    assert p("однолинейная схема грщ") == "грщ"
    assert p("однолинейная схема щр-1а") == "щр-1а"
    assert p("однолинейная схема що-2") == "що-2"
    assert p("однолинейная схема щита квартирного як1") == "як-1"


def test_panel_designation_skips_voltage_rating():
    # «0,4кВ» — напряжение, а не номер единицы → bare «грщ» (как equipment_ids).
    assert sm._extract_panel_designation("однолинейная схема грщ-0,4кв") == "грщ"


def test_panel_designation_letter_and_word_suffix():
    # «ВРУа» и «ВРУ-А» дают одинаковую марку; «ВРУ.ИТП» / «ВРУ-ИТП» — тоже.
    assert sm._extract_panel_designation("однолинейная схема вруа") == "вру-а"
    assert sm._extract_panel_designation("однолинейная схема вру-а") == "вру-а"
    assert sm._extract_panel_designation("однолинейная схема вру.итп") == "вру-итп"
    assert sm._extract_panel_designation("однолинейная схема вру-итп") == "вру-итп"


def test_scheme_identity_requires_oneline_marker():
    # «План ВРУ-1» — не однолинейная схема → нет panel identity (не сольётся
    # со схемой того же оборудования).
    assert sm._extract_scheme_identity("план расположения вру-1") == ""
    assert sm._extract_scheme_identity("однолинейная схема вру-1") == \
        "однолинейная схема|вру-1"


# ═══ Test 5 — group key / scheme identity канонизируется одинаково ═════════

def test_scheme_identity_canonical_group_key():
    """Три формы одного логического листа дают ОДИН panel identity / group key,
    несмотря на разный префикс штампа и «начало/конец»."""
    def feat(name):
        rec = sm.SheetRec(page=1, sheet_no="1", sheet_name=name,
                          norm_name=sm.normalize_sheet_name(name),
                          section_class="structural", is_graphic=True,
                          name_source="md")
        return sm.extract_sheet_features(rec)

    a = feat(LPRE + "Однолинейная расчетная схема ВРУ-1")
    b = feat(RPRE + "Однолинейная схема ВРУ-1 (начало)")
    c = feat(RPRE + "Однолинейная схема ВРУ-1 (конец)")
    assert a.scheme_identity_key == "однолинейная схема|вру-1"
    assert a.scheme_identity_key == b.scheme_identity_key == c.scheme_identity_key
    # group_key для multipart тоже одинаков (это panel identity).
    assert a.sheet_group_key == b.sheet_group_key == c.sheet_group_key


# ═══ Test 1 — ВРУ-1 implicit multipart (две страницы ↔ начало/конец) ═══════

def test_vru1_two_pages_vs_start_end():
    left = _idx([(24, "24", LPRE + "Однолинейная расчетная схема ВРУ-1"),
                 (25, "25", LPRE + "Однолинейная расчетная схема ВРУ-1")])
    right = _idx([(24, "24", RPRE + "Однолинейная схема ВРУ-1 (начало)"),
                  (25, "25", RPRE + "Однолинейная схема ВРУ-1 (конец)")])
    res = sm.match_sheet_indexes(left, right)
    m = _matched(res)
    assert (24, 24) in m, sorted(m)
    assert (25, 25) in m, sorted(m)
    # обе стороны заполнены, тип — multipart-группа.
    assert m[(24, 24)]["match_type"] in ("exact_multipart_group", "multipart_group")
    assert m[(25, 25)]["match_type"] in ("exact_multipart_group", "multipart_group")
    # листы не остались односторонними
    assert res["left_only_count"] == 0
    assert res["right_only_count"] == 0


# ═══ Test 2 — ГРЩ переименование + сдвиг страниц (1↔1) ═════════════════════

def test_grsh_renamed_shifted_equipment_canonical():
    left = _idx([(52, "52", LPRE + "Однолинейная расчетная схема ГРЩ")])
    right = _idx([(21, "21", RPRE + "Однолинейная схема ГРЩ")])
    res = sm.match_sheet_indexes(left, right)
    m = _matched(res)
    assert (52, 21) in m, sorted(m)
    assert m[(52, 21)]["match_type"] == "equipment_canonical_match"


def test_grsh_one_to_many_multipart():
    """Реальная раскладка ИОС 1.1: ГРЩ — 1 страница слева, 3 справа (имя + два
    forward-fill продолжения). Якорь 52↔21, продолжения — односторонние."""
    left = _idx([(52, "52", LPRE + "Однолинейная расчетная схема ГРЩ")])
    right = _idx([(21, "21", RPRE + "Однолинейная схема ГРЩ"),
                  (22, "22", None), (23, "23", None)])  # forward-fill
    res = sm.match_sheet_indexes(left, right)
    m = _matched(res)
    assert (52, 21) in m, sorted(m)
    # продолжения 22/23 — односторонние (не теряются, но и не дублируют якорь)
    cont = [it for it in res["suggested_items"]
            if it["match_type"] == "multipart_continuation"]
    assert {it["right_page"] for it in cont} == {22, 23}


# ═══ Test 3/4 — hard-gates защищают от неправильных матчей ═════════════════

def test_vru1_not_matched_with_vru2():
    res = sm.match_sheet_indexes(
        _idx([(1, "1", LPRE + "Однолинейная расчетная схема ВРУ-1")]),
        _idx([(1, "1", RPRE + "Однолинейная схема ВРУ-2")]))
    assert res["matched_count"] == 0
    assert any(r["rejected_reason"].startswith("equipment_conflict")
               for r in res["rejected"])


def test_grsh_not_matched_with_vru():
    res = sm.match_sheet_indexes(
        _idx([(1, "1", LPRE + "Однолинейная расчетная схема ГРЩ")]),
        _idx([(1, "1", RPRE + "Однолинейная схема ВРУ")]))
    assert res["matched_count"] == 0
    assert any(r["rejected_reason"] == "system_conflict" for r in res["rejected"])


def test_vru1_and_vru2_dont_cross_in_mixed_set():
    """В смешанном наборе ВРУ-1 и ВРУ-2 матчатся каждый со своим, без скремблинга."""
    left = _idx([(1, "1", LPRE + "Однолинейная расчетная схема ВРУ-1"),
                 (2, "2", LPRE + "Однолинейная расчетная схема ВРУ-2")])
    right = _idx([(1, "1", RPRE + "Однолинейная схема ВРУ-2"),
                  (2, "2", RPRE + "Однолинейная схема ВРУ-1")])
    m = _matched(sm.match_sheet_indexes(left, right))
    assert (1, 2) in m   # ВРУ-1 слева p1 ↔ ВРУ-1 справа p2
    assert (2, 1) in m   # ВРУ-2 слева p2 ↔ ВРУ-2 справа p1


# ═══ auto-apply: equipment_canonical безопасен для пакетного применения ════

def test_equipment_canonical_is_safe_to_auto_apply():
    item = {"match": True, "match_type": "equipment_canonical_match",
            "score": 0.93, "confidence": 0.93, "risk_flags": [],
            "positive_evidence": ["однолинейная схема|грщ"]}
    ok, reason = saa.should_auto_apply_stamp_match(item)
    assert ok is True
    assert reason == "exact"
