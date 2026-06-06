"""Тесты позиционного выравнивания нераспознанных листов + презентации сторон.

Покрывает:
  * positional_alignment для начальных/межанкорных unmatched-прогонов
    (titульные/вводные листы без рамок) — Case 1..5 + trailing;
  * презентацию: suggested_items несут ОТДЕЛЬНЫЕ названия старого/нового листа
    (matched / one-sided / duplicate);
  * build_auto_apply_items: positional сохраняется как пара (не split), считается
    отдельно (positional_alignment), не как applied.

Сеть/Qwen/crop_url НЕ задействованы (чистый матчинг по именам листов из MD).
"""
from __future__ import annotations

from backend.app.services.stage_comparison import stamp_matching as sm
from backend.app.services.stage_comparison import stamp_auto_apply as aa


# ─── helpers ────────────────────────────────────────────────────────────────

def _md(pages):
    """pages: list of (page_no, sheet_name | None)."""
    out = []
    for pn, nm in pages:
        out.append(f"## СТРАНИЦА {pn}")
        out.append(f"**Лист:** {pn}")
        if nm is not None:
            out.append(f"**Наименование листа:** {nm}")
        out.append(f"содержимое листа {pn}")
        out.append("")
    return "\n".join(out)


def _match(left, right):
    return sm.match_sheet_indexes(sm.build_sheet_index(_md(left)),
                                  sm.build_sheet_index(_md(right)))


def _pairs(res):
    return [(it["left_page"], it["right_page"], it["match_type"]) for it in res["suggested_items"]]


def _has(res, lp, rp):
    return any(it["left_page"] == lp and it["right_page"] == rp for it in res["suggested_items"])


# ═══ B. Positional alignment ══════════════════════════════════════════════

def test_case1_leading_equal_run_positional():
    """2 непарных титульных до anchor 3↔5 → позиционно 1↔1, 2↔2 (без съезда)."""
    res = _match(
        [(1, "Обложка тома"), (2, "Состав проекта"), (3, "Схема ВРУ")],
        [(1, "Зарегистрировано"), (2, "Перечень изменений"), (5, "Схема ВРУ")])
    seq = _pairs(res)
    assert (1, 1, "positional_alignment") in seq
    assert (2, 2, "positional_alignment") in seq
    assert (3, 5, "exact_name") in seq
    # никакого искусственного съезда
    assert not _has(res, 1, None) and not _has(res, None, 1)
    assert not _has(res, 2, None) and not _has(res, None, 2)
    assert res["matched_count"] == 1          # positional НЕ считается matched
    assert res["positional_alignment_count"] == 2


def test_case2_leading_unequal_run():
    """Разная длина начального прогона: общая часть позиционно, хвост — right_only."""
    res = _match(
        [(1, "Обложка тома"), (2, "Состав проекта"), (3, "Схема ВРУ")],
        [(1, "Зарегистрировано"), (2, "Перечень изменений"),
         (3, "Письмо согласования"), (5, "Схема ВРУ")])
    seq = _pairs(res)
    assert (1, 1, "positional_alignment") in seq
    assert (2, 2, "positional_alignment") in seq
    assert (None, 3, "right_only") in seq      # лишний правый — честный right_only
    assert (3, 5, "exact_name") in seq
    assert res["positional_alignment_count"] == 2
    assert res["right_only_count"] == 1


def test_case3_one_sided_leading_no_positional():
    """До anchor непарные только слева → НЕ выдумывать positional pair."""
    res = _match(
        [(1, "Обложка тома"), (2, "Состав проекта"), (3, "Схема ВРУ")],
        [(5, "Схема ВРУ")])
    seq = _pairs(res)
    assert (1, None, "left_only") in seq
    assert (2, None, "left_only") in seq
    assert (3, 5, "exact_name") in seq
    assert res["positional_alignment_count"] == 0


def test_case4_multipart_not_broken_by_positional():
    """multipart 1↔N остаётся, positional не вмешивается."""
    res = _match(
        [(1, "Чертеж 1")],
        [(10, "Чертеж 1 (начало)"), (11, "Чертеж 1 (продолжение)"),
         (12, "Чертеж 1 (конец)")])
    seq = _pairs(res)
    assert (1, 10, "exact_multipart_group") in seq
    assert (None, 11, "multipart_continuation") in seq
    assert (None, 12, "multipart_continuation") in seq
    assert res["positional_alignment_count"] == 0


def test_case5_page_used_at_most_once():
    """Ни одна страница не участвует и в matched, и в positional одновременно."""
    res = _match(
        [(1, "Титульный"), (2, "Содержание X"), (3, "Схема ВРУ")],
        [(1, "Иное"), (2, "Содержание Y"), (5, "Схема ВРУ")])
    left_pages = [it["left_page"] for it in res["suggested_items"] if it["left_page"] is not None]
    right_pages = [it["right_page"] for it in res["suggested_items"] if it["right_page"] is not None]
    assert len(left_pages) == len(set(left_pages))     # без дублей слева
    assert len(right_pages) == len(set(right_pages))   # без дублей справа


def test_trailing_run_not_zipped():
    """Непарные ПОСЛЕ последнего anchor'а остаются односторонними (не позиционно)."""
    res = _match(
        [(1, "Схема ВРУ"), (2, "Удалённый лист")],
        [(1, "Схема ВРУ"), (2, "Добавленный лист")])
    seq = _pairs(res)
    assert (1, 1, "exact_name") in seq
    assert (2, None, "left_only") in seq
    assert (None, 2, "right_only") in seq
    assert res["positional_alignment_count"] == 0


def test_no_anchor_no_positional():
    """Если нет ни одного уверенного anchor'а — ничего не выравниваем позиционно."""
    res = _match([(1, "АБВ"), (2, "ГДЕ")], [(1, "ЁЖЗ"), (2, "ИЙК")])
    assert res["positional_alignment_count"] == 0
    # всё осталось односторонним (как было) — никаких positional
    assert all(it["match_type"] != "positional_alignment" for it in res["suggested_items"])


def test_positional_item_has_both_sides_and_risk():
    res = _match(
        [(1, "Обложка"), (2, "Схема ВРУ")],
        [(1, "Лист регистрации"), (2, "Схема ВРУ")])
    pos = [it for it in res["suggested_items"] if it["match_type"] == "positional_alignment"]
    assert len(pos) == 1
    p = pos[0]
    assert p["left_page"] == 1 and p["right_page"] == 1
    assert p["match"] is False                                   # не уверенный матч
    assert "unconfirmed_alignment" in (p.get("risk_flags") or [])
    assert p.get("needs_review") is True
    assert p["left_sheet_name"] == "Обложка"
    assert p["right_sheet_name"] == "Лист регистрации"           # обе стороны видны


# ═══ A. Presentation — обе стороны в suggested_items ══════════════════════

def test_matched_pair_has_both_sheet_names():
    res = _match([(7, "Текстовая часть")], [(4, "Текстовая часть")])
    it = next(i for i in res["suggested_items"] if i["match"])
    assert it["left_page"] == 7 and it["right_page"] == 4
    assert it["left_sheet_name"] == "Текстовая часть"
    assert it["right_sheet_name"] == "Текстовая часть"


def test_one_sided_keeps_present_side_name():
    res = _match([(1, "Только старый лист"), (2, "Общий")], [(2, "Общий")])
    lo = next(i for i in res["suggested_items"]
              if i["match_type"] == "left_only" and i["left_page"] == 1)
    assert lo["left_sheet_name"] == "Только старый лист"
    assert lo["right_page"] is None
    assert (lo.get("right_sheet_name") or "") == ""             # пустая сторона


def test_canonical_pair_shows_differing_names():
    """Каноническое совпадение разных формулировок — обе стороны показаны раздельно."""
    res = _match([(25, "Однолинейная расчетная схема ГРЩ")],
                 [(18, "Однолинейная схема ГРЩ")])
    it = next(i for i in res["suggested_items"] if i["match"])
    assert it["match_type"] in ("exact_canonical_name", "fuzzy_name", "fuzzy_structural")
    assert it["left_sheet_name"] == "Однолинейная расчетная схема ГРЩ"
    assert it["right_sheet_name"] == "Однолинейная схема ГРЩ"


def test_duplicate_names_keep_both_sides():
    """Повторяющиеся имена (Текстовая часть × N) не теряют отображение сторон."""
    res = _match(
        [(1, "Текстовая часть"), (2, "Текстовая часть"), (3, "Текстовая часть")],
        [(1, "Текстовая часть"), (2, "Текстовая часть"), (3, "Текстовая часть")])
    matched = [i for i in res["suggested_items"] if i["match"]]
    assert len(matched) == 3
    for it in matched:
        assert it["left_sheet_name"] == "Текстовая часть"
        assert it["right_sheet_name"] == "Текстовая часть"
        assert it["left_page"] is not None and it["right_page"] is not None


# ═══ build_auto_apply_items: positional сохраняется как пара, считается отдельно ═

def test_build_items_positional_saved_as_pair_not_split():
    suggested = [
        {"match": True, "match_type": "exact_name", "left_page": 3, "right_page": 5,
         "score": 1.0},
        {"match": False, "match_type": "positional_alignment", "left_page": 1,
         "right_page": 1, "score": 0.0, "risk_flags": ["unconfirmed_alignment"]},
        {"match": False, "match_type": "positional_alignment", "left_page": 2,
         "right_page": 2, "score": 0.0, "risk_flags": ["unconfirmed_alignment"]},
    ]
    built = aa.build_auto_apply_items(suggested)
    assert built["applied"] == 1                       # только exact считается applied
    assert built["positional_alignment"] == 2
    pages = {(it["left_page"], it["right_page"]) for it in built["items"]}
    assert (1, 1) in pages and (2, 2) in pages         # позиционно сохранено парами
    assert (3, 5) in pages
    # никакого расцепления positional
    assert (1, None) not in pages and (None, 1) not in pages
    assert (2, None) not in pages and (None, 2) not in pages
    # сохранённые items — только канонические поля
    for it in built["items"]:
        assert set(it.keys()) == {"slot", "left_page", "right_page", "mode", "note"}
