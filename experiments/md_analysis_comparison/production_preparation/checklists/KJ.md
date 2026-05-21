# Checklist — KJ (Конструкции железобетонные)

Применимость по `document_type`: см. `checklist_applicability_matrix.md`.

## Mandatory required sections / artifacts

- [problem_class=missing_mandatory_section, severity=ПРОВЕРИТЬ_ПО_СМЕЖНЫМ, applies=full_rd]
  Общие данные раздела КЖ: классы бетона, классы арматуры,
  морозостойкость F, водонепроницаемость W
  (СП 63.13330.2018, разделы 6 и 10).
- [problem_class=missing_mandatory_section, severity=ПРОВЕРИТЬ_ПО_СМЕЖНЫМ, applies=full_rd]
  Схема расположения элементов: фундаменты, плиты, балки, колонны,
  диафрагмы жёсткости (ГОСТ 21.501-2018, п. 5.4).
- [problem_class=missing_mandatory_section, severity=ПРОВЕРИТЬ_ПО_СМЕЖНЫМ, applies=full_rd]
  Раздел "Антикоррозионная защита бетонных и арматурных конструкций"
  (СП 28.13330.2017).
- [problem_class=missing_mandatory_section, severity=ПРОВЕРИТЬ_ПО_СМЕЖНЫМ, applies=full_rd]
  Требования к производству работ (опалубка, бетонирование, уход)
  (СП 70.13330.2012).

## Mandatory required calculations

- [problem_class=missing_calculation_basis, severity=КРИТИЧЕСКОЕ, applies=full_rd]
  Расчётная схема несущего каркаса с указанием нагрузок
  (СП 20.13330.2016, СП 63.13330.2018, п. 6).
- [problem_class=missing_calculation_basis, severity=КРИТИЧЕСКОЕ, applies=full_rd]
  Сбор нагрузок (постоянные, временные, снеговые, ветровые, особые)
  (СП 20.13330.2016).
- [problem_class=missing_calculation_basis, severity=КРИТИЧЕСКОЕ, applies=full_rd]
  Расчёт по I группе предельных состояний (несущая способность)
  основных элементов (СП 63.13330.2018, п. 8).
- [problem_class=missing_calculation_basis, severity=ЭКСПЛУАТАЦИОННОЕ, applies=full_rd]
  Расчёт по II группе предельных состояний (трещиностойкость, прогибы),
  включая продавливание плит перекрытия в зоне колонн
  (СП 63.13330.2018, п. 8.1.46, 8.2).
- [problem_class=missing_calculation_basis, severity=ЭКСПЛУАТАЦИОННОЕ, applies=full_rd]
  Геотехнический расчёт оснований фундаментов с привязкой к ИГИ
  (СП 22.13330.2016).

## Mandatory required specifications / schedules

- [problem_class=missing_mandatory_schedule, severity=ЭКСПЛУАТАЦИОННОЕ, applies=full_rd|specification_only]
  Спецификация арматуры с указанием класса, диаметра, длины
  (ГОСТ 21.501-2018, СП 63.13330.2018).
- [problem_class=missing_mandatory_schedule, severity=ЭКСПЛУАТАЦИОННОЕ, applies=full_rd|specification_only]
  Ведомость расхода стали (арматурные изделия, закладные)
  (ГОСТ 21.501-2018).
- [problem_class=missing_mandatory_schedule, severity=ЭКСПЛУАТАЦИОННОЕ, applies=full_rd|specification_only]
  Спецификация закладных деталей с привязкой к ГОСТ 14098-2014
  (ГОСТ 21.502-2016).

## Mandatory required diagrams / schemes

- [problem_class=missing_diagram, severity=ЭКСПЛУАТАЦИОННОЕ, applies=full_rd]
  Конструктивные узлы армирования (сопряжения колонна–плита,
  балка–колонна, плита–стена) с указанием шагов и диаметров
  (ГОСТ 21.501-2018, СП 63.13330.2018, п. 10).
- [problem_class=missing_diagram, severity=ЭКСПЛУАТАЦИОННОЕ, applies=full_rd]
  Узлы сварных соединений арматуры
  (ГОСТ 14098-2014).

## Mandatory required parameters (per element)

- [problem_class=missing_mandatory_parameter, severity=ЭКСПЛУАТАЦИОННОЕ, applies=full_rd|specification_only]
  На каждом армированном элементе: класс арматуры (A400 / A500C),
  диаметр, шаг расстановки, защитный слой
  (СП 63.13330.2018, п. 10.3).
- [problem_class=missing_mandatory_parameter, severity=РЕКОМЕНДАТЕЛЬНОЕ, applies=full_rd|specification_only]
  Длина анкеровки / нахлёста стыков арматуры
  (СП 63.13330.2018, п. 10.3.5).

## Mandatory coordination requirements (cross-discipline)

- [problem_class=missing_mandatory_section, severity=ПРОВЕРИТЬ_ПО_СМЕЖНЫМ, applies=full_rd]
  Закладные детали для крепления конструкций КМ / ЭОМ / ОВ
  (координация с соответствующими разделами).
- [problem_class=missing_mandatory_section, severity=ПРОВЕРИТЬ_ПО_СМЕЖНЫМ, applies=full_rd]
  Отверстия в плитах и стенах для ОВ / ВК (с указанием размеров
  и привязок), обоснование армирования вокруг отверстий.
- [problem_class=missing_mandatory_section, severity=ПРОВЕРИТЬ_ПО_СМЕЖНЫМ, applies=full_rd]
  Привязка фундаментов к данным инженерно-геологических изысканий
  (СП 22.13330.2016).

## Mandatory testing / commissioning requirements

- [problem_class=missing_mandatory_section, severity=РЕКОМЕНДАТЕЛЬНОЕ, applies=full_rd]
  Контроль качества бетонирования и приёмка скрытых работ по
  армированию (СП 70.13330.2012).

## Recommended items

- [problem_class=missing_calculation_basis, severity=РЕКОМЕНДАТЕЛЬНОЕ, applies=full_rd]
  Расчёт прогибов и раскрытия трещин с привязкой к категории
  трещиностойкости (СП 63.13330.2018).
- [problem_class=missing_mandatory_section, severity=РЕКОМЕНДАТЕЛЬНОЕ, applies=full_rd]
  Технологическая последовательность бетонных работ
  (СП 70.13330.2012).

## Conditional items (by object type / document_type)

- [problem_class=missing_calculation_basis, severity=ЭКСПЛУАТАЦИОННОЕ, applies=full_rd]
  Применяется для зданий выше 75 м: расчёт на ветровую нагрузку
  с учётом пульсаций (СП 20.13330.2016).
- [problem_class=missing_calculation_basis, severity=ЭКСПЛУАТАЦИОННОЕ, applies=full_rd]
  Применяется в сейсмических районах: расчёт на сейсмические
  воздействия (СП 14.13330.2018).
- [problem_class=missing_mandatory_section, severity=ЭКСПЛУАТАЦИОННОЕ, applies=full_rd]
  Применяется при наличии подземной части: гидроизоляция и
  расчёт водонепроницаемости (СП 250.1325800.2016).

## Anti-patterns — DO NOT flag these as findings

- Конкретный производитель арматуры или бетона не указан — не
  нарушение, выбор по результатам торгов.
- "Расчёт на продавливание без учёта момента" как completeness —
  это домен calculations lens, не completeness; не дублировать.
- "Спецификация арматуры неполна" без цитирования конкретной строки
  с отсутствующим параметром — спекулятивно, выбросить.
- Фантомные пункты СП без проверки номера — выбросить.
- "Следует уточнить защитный слой" — non-actionable, выбросить
  или переформулировать с указанием конкретной позиции.
- РЕКОМЕНДАТЕЛЬНОЕ для отсутствия расчётной схемы — это инверсия;
  отсутствие основы расчёта = КРИТИЧЕСКОЕ.
- Перечислять каждую недостающую характеристику арматуры отдельной
  находкой — это один класс `rebar_spec_incomplete`.
