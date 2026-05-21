# Checklist — OV (Отопление, вентиляция и кондиционирование)

Применимость по `document_type`: см. `checklist_applicability_matrix.md`.

## Mandatory required sections / artifacts

- [problem_class=missing_mandatory_section, severity=ПРОВЕРИТЬ_ПО_СМЕЖНЫМ, applies=full_rd]
  Общие данные раздела ОВ: климатические параметры, температурный
  график, исходные данные (СП 60.13330.2020, п. 5).
- [problem_class=missing_mandatory_section, severity=ПРОВЕРИТЬ_ПО_СМЕЖНЫМ, applies=full_rd]
  Раздел "Отопление" с описанием типа системы, теплоносителя
  и регулирования (СП 60.13330.2020).
- [problem_class=missing_mandatory_section, severity=ПРОВЕРИТЬ_ПО_СМЕЖНЫМ, applies=full_rd]
  Раздел "Вентиляция" с описанием систем приточной и вытяжной
  (СП 60.13330.2020, п. 7).
- [problem_class=missing_mandatory_section, severity=ПРОВЕРИТЬ_ПО_СМЕЖНЫМ, applies=full_rd]
  Раздел "Противодымная вентиляция" для МКД и общественных
  зданий (СП 7.13130.2013).

## Mandatory required calculations

- [problem_class=missing_calculation_basis, severity=КРИТИЧЕСКОЕ, applies=full_rd]
  Расчёт воздухообмена по помещениям (приток / вытяжка / баланс)
  (СП 60.13330.2020, п. 7).
- [problem_class=missing_calculation_basis, severity=ЭКСПЛУАТАЦИОННОЕ, applies=full_rd]
  Расчёт тепловых нагрузок и теплопотерь помещений
  (СП 60.13330.2020).
- [problem_class=missing_calculation_basis, severity=ЭКСПЛУАТАЦИОННОЕ, applies=full_rd]
  Расчёт потерь давления и подбор вентустановок (рабочая точка
  на характеристике вентилятора) (СП 60.13330.2020).
- [problem_class=missing_calculation_basis, severity=ЭКСПЛУАТАЦИОННОЕ, applies=full_rd]
  Проверка кратности воздухообмена по нормативам помещений
  (СП 60.13330.2020, СанПиН 1.2.3685-21).
- [problem_class=missing_calculation_basis, severity=РЕКОМЕНДАТЕЛЬНОЕ, applies=full_rd]
  Гидравлический расчёт системы отопления и акустический расчёт
  вентсистем (СП 60.13330.2020, СП 51.13330.2011).

## Mandatory required specifications / schedules

- [problem_class=missing_mandatory_schedule, severity=ЭКСПЛУАТАЦИОННОЕ, applies=full_rd|specification_only]
  Спецификация оборудования и материалов
  (ГОСТ 21.602-2016).
- [problem_class=missing_mandatory_schedule, severity=ЭКСПЛУАТАЦИОННОЕ, applies=full_rd|specification_only]
  Ведомость воздуховодов с указанием сечений, материалов,
  способа крепления.

## Mandatory required diagrams / schemes

- [problem_class=missing_diagram, severity=ЭКСПЛУАТАЦИОННОЕ, applies=full_rd]
  Аксонометрические / принципиальные схемы вентсистем и планы
  вентиляции с маркировкой систем (П1, П2, В1, В2)
  (ГОСТ 21.602-2016).
- [problem_class=missing_diagram, severity=ЭКСПЛУАТАЦИОННОЕ, applies=full_rd]
  Узлы прохода воздуховодов через ограждения с указанием
  огнезадерживающих клапанов (СП 7.13130.2013).

## Mandatory required parameters (per ventilation unit / pump)

- [problem_class=missing_mandatory_parameter, severity=ЭКСПЛУАТАЦИОННОЕ, applies=full_rd|specification_only]
  На каждой установке: производительность L (м³/ч), полное
  давление P (Па) (СП 60.13330.2020).
- [problem_class=missing_mandatory_parameter, severity=ЭКСПЛУАТАЦИОННОЕ, applies=full_rd|specification_only]
  На каждой установке: электрическая мощность P_уст (кВт),
  напряжение и число фаз (для передачи в раздел ЭОМ).
- [problem_class=missing_mandatory_parameter, severity=РЕКОМЕНДАТЕЛЬНОЕ, applies=full_rd|specification_only]
  КПД и класс энергоэффективности установок.

## Mandatory coordination requirements (cross-discipline)

- [problem_class=missing_mandatory_section, severity=ПРОВЕРИТЬ_ПО_СМЕЖНЫМ, applies=full_rd]
  Передача в ЭОМ перечня электроприёмников ОВ (P_уст, K_с,
  число фаз) (ГОСТ Р 21.101-2020).
- [problem_class=missing_mandatory_section, severity=ПРОВЕРИТЬ_ПО_СМЕЖНЫМ, applies=full_rd]
  Координация с АПС по управлению вентиляцией при пожаре
  (СП 7.13130.2013) — обязательно для МКД.
- [problem_class=missing_mandatory_section, severity=ПРОВЕРИТЬ_ПО_СМЕЖНЫМ, applies=full_rd]
  Координация с АР / КЖ по проходкам и отверстиям для
  воздуховодов в перекрытиях и стенах.
- [problem_class=missing_mandatory_section, severity=ПРОВЕРИТЬ_ПО_СМЕЖНЫМ, applies=full_rd]
  Координация с разделом газоснабжения (для кухонь с газом —
  отдельный учёт вытяжки).

## Mandatory testing / commissioning requirements

- [problem_class=missing_mandatory_section, severity=РЕКОМЕНДАТЕЛЬНОЕ, applies=full_rd]
  Программа пуско-наладочных работ вентсистем (балансировка,
  измерение параметров) и испытаний противодымной вентиляции
  (СП 7.13130.2013).

## Recommended items

- [problem_class=missing_calculation_basis, severity=РЕКОМЕНДАТЕЛЬНОЕ, applies=full_rd]
  Расчёт тепловой изоляции трубопроводов и воздуховодов, расчёт
  тепловой мощности отопительных приборов
  (СП 60.13330.2020, СП 61.13330.2012).

## Conditional items (by object type / document_type)

- [problem_class=missing_mandatory_section, severity=ЭКСПЛУАТАЦИОННОЕ, applies=full_rd]
  Применяется для МКД и общественных: огнестойкость
  воздуховодов EI 30 / 60 / 90 / 150 в зависимости от схемы
  (СП 7.13130.2013).
- [problem_class=missing_mandatory_section, severity=ЭКСПЛУАТАЦИОННОЕ, applies=full_rd]
  Применяется при наличии АХЗ / ИТП: схема узла и привязка
  к теплоносителю.
- [problem_class=missing_diagram, severity=ЭКСПЛУАТАЦИОННОЕ, applies=full_rd]
  Применяется для жилых МКД: вентиляция канализационных стояков
  на стыке OV / VK (избегать двойного учёта с разделом ВК).

## Anti-patterns — DO NOT flag these as findings

- Конкретный производитель вентоборудования (Systemair / Wolf /
  ВЕНТС) не указан — не нарушение.
- "Спецификация оборудования может быть неполной" без цитирования
  строки — спекулятивно, выбросить.
- "Скорость воздуха 0,55 м/с в одной точке системы" повторяется
  под разными формулировками — это один класс
  `air_velocity_excess` (collapse).
- "Необходимо уточнить шумовые характеристики" без явного
  отсутствия — non-actionable, выбросить.
- Фантомные пункты СП 60 без проверки номера — выбросить.
- На `document_type=audit_comparison` флаг "отсутствует
  аксонометрическая схема" — out-of-scope.
- РЕКОМЕНДАТЕЛЬНОЕ для отсутствия расчёта воздухообмена — это
  инверсия, минимум КРИТИЧЕСКОЕ.
- Перечислять отсутствие огнестойкости для каждого воздуховода
  отдельно — один класс `fire_resistance_air_duct` (collapse).
