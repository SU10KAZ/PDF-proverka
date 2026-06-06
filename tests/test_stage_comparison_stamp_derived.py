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

def test_leading_target_case_exact_layout():
    """РЕАЛЬНЫЙ целевой кейс пользователя (acceptance): сильно неравные ведущие
    прогоны (left 1..6 безымянных, right 1..3 безымянных, anchor 7↔4) дают РОВНО:

        1 ↔ 1  positional
        2 ↔ 2  positional
        3 ↔ —  left_only   (None ↔ 3 — справа отдельным right_only рядом)
        4 ↔ —  left_only
        5 ↔ —  left_only
        6 ↔ —  left_only
        7 ↔ 4  matched

    Boundary-buffer не приклеивает 3-ю правую к 3-й левой → НЕТ 3↔3.
    """
    res = _match(
        [(1, None), (2, None), (3, None), (4, None),
         (5, "Содержание тома"), (6, None), (7, "Текстовая часть")],
        [(1, None), (2, None), (3, None), (4, "Текстовая часть")])
    seq = [(it["left_page"], it["right_page"], it["match_type"]) for it in res["suggested_items"]]
    # первые две пары выровнены
    assert (1, 1, "positional_alignment") in seq
    assert (2, 2, "positional_alignment") in seq
    assert res["positional_alignment_count"] == 2          # ровно 2, не 3
    # КЛЮЧЕВОЕ: нет 3↔3 (правую стр.3 не притянули к левой стр.3)
    assert _row(res, 3, 3) is None
    # левые 3..6 — односторонние
    for lp in (3, 4, 5, 6):
        assert (lp, None, "left_only") in seq
    # правая стр.3 — честный right_only (не висит как 3↔3 и не теряется)
    assert (None, 3, "right_only") in seq
    # реальный anchor сохранён
    assert (7, 4, "exact_name") in seq
    # НЕТ старого съезда для первых двух страниц
    assert _row(res, 1, None) is None and _row(res, None, 1) is None
    assert _row(res, 2, None) is None and _row(res, None, 2) is None


def test_leading_stops_on_divergence():
    """Как только у одной стороны начинается осмысленный ШТАМП-блок, а у другой
    нет — позиционный zip останавливается (обложки с derived-именем НЕ считаются
    divergence, см. test_leading_cover_pages_still_align)."""
    res = _match(
        [(1, None), (2, None),
         (3, "Ведомость объемов работ"),
         (4, None), (5, "Текстовая часть")],
        [(1, None), (2, None), (3, None), (4, "Текстовая часть")])
    seq = [(it["left_page"], it["right_page"], it["match_type"]) for it in res["suggested_items"]]
    assert (1, 1, "positional_alignment") in seq
    assert (2, 2, "positional_alignment") in seq
    # divergence на 3-й позиции (left3 = штамп «Ведомость») → НЕ позиционно
    assert not any(lp == 3 and it_t == "positional_alignment" for lp, rp, it_t in seq)
    assert (5, 4, "exact_name") in seq
    assert res["positional_alignment_count"] == 2


def test_leading_cover_pages_still_align():
    """Обложечные листы без штампа, но с derived-именем РАЗДЕЛА/ТОМА в шапке
    («Проект организации строительства»), считаются front-matter и выравниваются
    позиционно — derived НЕ ломает ведущее выравнивание (реальный кейс pac34250b
    right стр.2,3)."""
    cover = ("ОБЩЕСТВО С ОГРАНИЧЕННОЙ ОТВЕТСТВЕННОСТЬЮ\nПРОЕКТНАЯ ДОКУМЕНТАЦИЯ\n"
             "Раздел 7 «Проект организации строительства»\nТом 7")
    res = _match(
        [(1, None), (2, None), (3, "Текстовая часть")],
        [(1, None, cover), (2, None, cover), (3, "Текстовая часть")])
    seq = [(it["left_page"], it["right_page"], it["match_type"]) for it in res["suggested_items"]]
    # обложки выровнены позиционно, несмотря на derived «Проект организации…»
    assert (1, 1, "positional_alignment") in seq
    assert (2, 2, "positional_alignment") in seq
    assert (3, 3, "exact_name") in seq


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


# ═══ Регрессия: derived (Pass 2.6) не затирает матрицу кандидатов Pass 3 ═════

def test_derived_match_with_llm_pass_does_not_crash():
    """Pass 2.6 (derived) использовал локальную переменную `cand` как номер
    страницы (int), затирая матрицу кандидатов `cand` (dict (lp,rp)->score),
    которую читает Pass 3 (LLM adjudication). Итог — `TypeError: argument of
    type 'int' is not iterable` на suggest-by-stamp с включённым ИИ-доматчингом
    (HTTP 500). Здесь: derived-матч (49↔46) клобберил `cand`, а остаток
    (50 / 47) уводил исполнение в Pass 3 → падение ДО вызова LLM."""
    left = sm.build_sheet_index(_md([
        (48, "Текстовая часть"),
        (49, "Проект организации строительства",
         "BLOCK ELXF Календарный план\n| N | работы |"),   # derived → 46
        (50, "Схема ВРУ-7"),                                 # остаток (hard-gate vs ГРЩ)
    ]))
    right = sm.build_sheet_index(_md([
        (45, "Текстовая часть"),
        (46, "Календарный план", "Календарный план\nграфик производства работ"),
        (47, "Схема ГРЩ-9"),                                 # остаток
    ]))
    def fn(rem_left, rem_right, tasks):  # new-style llm_match_fn (3 args)
        return []  # fail-soft; важно лишь, что Pass 3 ДОШЁЛ сюда без TypeError

    # До фикса этот вызов поднимал TypeError внутри match_sheet_indexes:
    # цикл построения кандидатов Pass 3 (`if (lp, rp) not in cand`) падал,
    # т.к. `cand` был затёрт в int derived-проходом.
    res = sm.match_sheet_indexes(left, right, llm_match_fn=fn)

    # derived-матч сохранён
    it = _row(res, 49, 46)
    assert it is not None and it["match_type"] == "derived_name_match"
    # остаток с обеих сторон ПРОШЁЛ через Pass 3 (цикл кандидатов отработал)
    # и не упал; hard-gate ВРУ≠ГРЩ → 50 и 47 односторонние, ложной пары нет
    assert _row(res, 50, 47) is None
    assert _row(res, 50, None) is not None
    assert _row(res, None, 47) is not None
