"""Тесты derived/weak извлечения заголовка листа + умного ведущего positional.

Покрывает:
  * derived title из содержимого (Календарный план), при обрезанном/ином штампе;
  * weak derived_name_match (низкая уверенность, risk derived_name, needs_review);
  * derived НЕ становится полноценным exact-матчем;
  * случайное упоминание заголовка в теле листа (глубоко) НЕ даёт derived;
  * умная остановка ведущего positional по divergence (осмысленный блок с одной
    стороны) + целевой кейс пользователя без съезда;
  * отображение: suggested_items несут derived-имена и name_source.

Без сети/Qwen — чистый матчинг по MD.
"""
from __future__ import annotations

from backend.app.services.stage_comparison import stamp_matching as sm


def _md(pages):
    """pages: (page_no, sheet_name|None, body|None)."""
    out = []
    for tup in pages:
        pn, nm = tup[0], tup[1]
        body = tup[2] if len(tup) > 2 else None
        out.append(f"## СТРАНИЦА {pn}")
        out.append(f"**Лист:** {pn}")
        if nm is not None:
            out.append(f"**Наименование листа:** {nm}")
        out.append(body if body is not None else f"содержимое листа {pn}")
        out.append("")
    return "\n".join(out)


def _match(left, right):
    return sm.match_sheet_indexes(sm.build_sheet_index(_md(left)),
                                  sm.build_sheet_index(_md(right)))


def _row(res, lp, rp):
    return next((it for it in res["suggested_items"]
                 if it["left_page"] == lp and it["right_page"] == rp), None)


# ═══ derived title extraction (helper) ════════════════════════════════════

def test_derive_title_from_content_heading():
    assert sm._derive_title_from_text("BLOCK ELXF Календарный план\n| N | работы |") == "Календарный план"
    assert sm._derive_title_from_text("Проект организации строительства корпуса") \
        == "Проект организации строительства"
    assert sm._derive_title_from_text("просто текст без заголовка") == ""


def test_derive_title_skips_deep_mention():
    """Упоминание заголовка ГЛУБОКО в теле листа (не вверху) → НЕ derived."""
    deep = "Разрешение на корректировку. " + ("x" * 400) + " в календарный план добавлено"
    assert sm._derive_title_from_text(deep) == ""


def test_derive_title_excludes_own_known_title():
    """Если штамп-имя — известный заголовок (Календарный план), derived его НЕ
    повторяет (иначе лист «Календарный план» дал бы derived самого себя)."""
    excl = {sm.normalize_sheet_name("Календарный план")}
    assert sm._derive_title_from_text("Календарный план график работ", exclude_norms=excl) == ""
    # без exclude — извлёкся бы
    assert sm._derive_title_from_text("Календарный план график работ") == "Календарный план"


# ═══ Календарный план — derived match ═════════════════════════════════════

def test_kalendarny_plan_derived_match():
    """Лист «Проект организации строительства» с блоком «Календарный план» в
    содержимом матчится со стадией-2 листом «Календарный план» (weak)."""
    res = _match(
        [(48, "Текстовая часть"),
         (49, "Проект организации строительства", "BLOCK ELXF Календарный план\n| N | работы |")],
        [(45, "Текстовая часть"),
         (46, "Календарный план", "Календарный план\nграфик производства работ")])
    it = _row(res, 49, 46)
    assert it is not None
    assert it["match_type"] == "derived_name_match"
    assert it["score"] <= 0.7 and it["needs_review"] is True
    assert "derived_name" in (it["risk_flags"] or [])
    assert it["left_derived_sheet_name"] == "Календарный план"
    assert res["derived_match_count"] == 1


def test_derived_does_not_become_exact_match():
    """Безымянный лист, лишь УПОМИНАЮЩИЙ заголовок вверху, не даёт exact-матч на
    полной уверенности — derived работает только как weak-кандидат."""
    res = _match(
        [(1, None, "Календарный план\nтаблица")],
        [(1, "Календарный план", "Календарный план график")])
    it = _row(res, 1, 1)
    assert it is not None
    # матч есть, но это weak derived, НЕ exact_name на 1.0
    assert it["match_type"] == "derived_name_match"
    assert it["score"] < 1.0


def test_incidental_mention_does_not_false_match():
    """Лист «Разрешение», глубоко упоминающий «календарный план», НЕ должен
    перехватывать матч у настоящего листа «Календарный план»."""
    res = _match(
        [(1, "Текстовая часть"),
         (2, "Разрешение на корректировку",
          "Разрешение. " + ("y" * 400) + " в календарный план добавлено устройство"),
         (3, "Проект организации строительства", "BLOCK Календарный план\n| N |")],
        [(1, "Текстовая часть"),
         (2, "Календарный план", "Календарный план график")])
    # стр.2 (Разрешение) НЕ должна стать derived-матчем
    r2 = _row(res, 2, 2)
    assert r2 is None or r2["match_type"] != "derived_name_match"
    # настоящий лист (стр.3) матчится с Календарным планом
    it = _row(res, 3, 2)
    assert it is not None and it["match_type"] == "derived_name_match"


# ═══ Display fields ═══════════════════════════════════════════════════════

def test_display_carries_derived_name_and_source():
    res = _match(
        [(1, "Проект организации строительства", "BLOCK Календарный план\n| N |")],
        [(1, "Календарный план", "Календарный план график")])
    it = _row(res, 1, 1)
    assert it["left_derived_sheet_name"] == "Календарный план"
    assert it.get("left_name_source") == "md"          # штамп-имя есть
    assert it.get("right_name_source") == "md"


# ═══ Умный ведущий positional ═════════════════════════════════════════════

def test_leading_target_case_no_sjezd():
    """Целевой кейс пользователя: ведущие безымянные листы выровнены, без съезда.

    Для полностью безымянных страниц позиционно выравнивается общий префикс
    (1↔1, 2↔2, …), реальный anchor «Текстовая часть» остаётся парой, именованный
    «Содержание тома» НЕ мис-парится, и НЕТ раскладки 1→None / None→1.
    """
    res = _match(
        [(1, None), (2, None), (3, None), (4, None),
         (5, "Содержание тома"), (6, None), (7, "Текстовая часть")],
        [(1, None), (2, None), (3, None), (4, "Текстовая часть")])
    seq = [(it["left_page"], it["right_page"], it["match_type"]) for it in res["suggested_items"]]
    assert (1, 1, "positional_alignment") in seq
    assert (2, 2, "positional_alignment") in seq
    assert (7, 4, "exact_name") in seq
    # именованный «Содержание тома» (стр.5) не спарен позиционно
    assert (5, None, "left_only") in seq
    # НЕТ старого съезда: левый и правый титульник не висят раздельными слотами
    assert not any(lp == 1 and rp is None for lp, rp, _ in seq)
    assert not any(lp is None and rp == 1 for lp, rp, _ in seq)


def test_leading_stops_on_divergence():
    """Как только у одной стороны начинается осмысленный блок (derived-заголовок),
    а у другой нет — позиционный zip останавливается."""
    res = _match(
        [(1, None), (2, None),
         (3, None, "Ведомость объемов работ по корпусу 1\nтаблица"),
         (4, None), (5, "Текстовая часть")],
        [(1, None), (2, None), (3, None), (4, "Текстовая часть")])
    seq = [(it["left_page"], it["right_page"], it["match_type"]) for it in res["suggested_items"]]
    assert (1, 1, "positional_alignment") in seq
    assert (2, 2, "positional_alignment") in seq
    # divergence на 3-й позиции (left3 = Ведомость) → НЕ позиционно
    assert not any(lp == 3 and it_t == "positional_alignment" for lp, rp, it_t in seq)
    assert (5, 4, "exact_name") in seq
    assert res["positional_alignment_count"] == 2


def test_leading_named_frontmatter_still_aligns():
    """Front-matter с РАЗНЫМИ подписями (Обложка ↔ Лист регистрации), но
    осмысленными с обеих сторон, выравнивается позиционно (не съезд)."""
    res = _match(
        [(1, "Обложка тома"), (2, "Состав проекта"), (3, "Схема ВРУ")],
        [(1, "Лист регистрации"), (2, "Перечень изменений"), (5, "Схема ВРУ")])
    seq = [(it["left_page"], it["right_page"], it["match_type"]) for it in res["suggested_items"]]
    assert (1, 1, "positional_alignment") in seq
    assert (2, 2, "positional_alignment") in seq
    assert (3, 5, "exact_name") in seq
