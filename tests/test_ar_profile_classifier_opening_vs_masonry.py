# Регресс классификатора АР: «план проёмов» не должен красть кладочные планы.
# Боевой кейс: блок 9UN7-PPMM-9CG (13АВ-РД-АР1.2-К5 v3) — кладочный план, чьё
# Chandra-описание упоминает оконные проёмы (ОК-2.1к), уезжал в ar_opening_plan.
from backend.app.pipeline.stages.block_grounding.architecture_geometry import (
    PROFILE_MASONRY_PLAN,
    PROFILE_OPENING_PLAN,
    classify_ar_profile,
)

# Реальный контекст классификации боевого блока (block_title + block_type +
# short_description + description из пакета block_vector_graphs/9UN7-PPMM-9CG.json).
REAL_MASONRY_CONTEXT = (
    "Кладочный план этажа с экспликацией помещений и маркировкой конструктивных элементов. "
    "План этажа | Оси: 5.А, 5.Б, 5.В, 5.Е, 5.Ж, 5.И, 5.Л, 5.П, 5.С, 5.Т, 5.1, 5.2, 5.3, 5.4, 5.5 "
    "Кладочный план этажа с экспликацией помещений и маркировкой конструктивных элементов. "
    "На чертеже представлен план этажа с сеткой осей от 5.А до 5.Т и от 5.1 до 5.5. "
    "Изображены контуры помещений с нумерацией (например, 5.404.8, 5.397.1), указанием "
    "площадей и типов перегородок (Д-17, Л-3л). Присутствуют отметки уровней пола "
    "(+5,250, +3,900, +5,330) и обозначения оконных проемов (ОК-2.1к, ОК-5). Стены "
    "показаны двойными линиями с различной штриховкой, обозначающей материалы конструкций."
)

# Реальный заголовок истинного плана отверстий из каталога эталонов (4DQQ-CMAK-ATQ).
REAL_OPENING_TITLE = (
    "Архитектурные решения. Кладочные планы. Подземная автостоянка в осях П.1-П.24. "
    "План с маркировкой отверстий -1 этажа подземной автостоянки в осях П.1-П.12. "
    "Фрагмент плана этажа с монолитными конструкциями и ведомостью отверстий."
)


def test_masonry_plan_mentioning_openings_stays_masonry():
    assert classify_ar_profile(REAL_MASONRY_CONTEXT) == PROFILE_MASONRY_PLAN


def test_plan_with_opening_markup_stays_opening_plan():
    assert classify_ar_profile(REAL_OPENING_TITLE) == PROFILE_OPENING_PLAN


def test_explicit_opening_plan_phrase_wins():
    assert classify_ar_profile(
        "План проёмов 3-го этажа с марками дверей Д-1, Д-2 и окон ОК-1."
    ) == PROFILE_OPENING_PLAN


def test_masonry_plan_with_incidental_door_marks_stays_masonry():
    assert classify_ar_profile(
        "Кладочный план 14-го этажа. Показаны перегородки, дверные проемы Д-3, Д-5л."
    ) == PROFILE_MASONRY_PLAN
