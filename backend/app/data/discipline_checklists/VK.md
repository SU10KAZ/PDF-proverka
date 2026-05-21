# Checklist — VK (Водоснабжение и канализация)

Применимость по `document_type`: см. `checklist_applicability_matrix.md`.

## Mandatory required sections / artifacts

- [problem_class=missing_mandatory_section, severity=ПРОВЕРИТЬ_ПО_СМЕЖНЫМ, applies=full_rd]
  Общие данные раздела ВК: источник водоснабжения, исходное
  давление на вводе, нормативный сток
  (СП 30.13330.2020, п. 5).
- [problem_class=missing_mandatory_section, severity=ПРОВЕРИТЬ_ПО_СМЕЖНЫМ, applies=full_rd]
  Раздел "Хозяйственно-питьевое водоснабжение В1"
  (СП 30.13330.2020).
- [problem_class=missing_mandatory_section, severity=ПРОВЕРИТЬ_ПО_СМЕЖНЫМ, applies=full_rd]
  Раздел "Горячее водоснабжение Т3 / Т4" с описанием схемы
  циркуляции (СП 30.13330.2020).
- [problem_class=missing_mandatory_section, severity=ПРОВЕРИТЬ_ПО_СМЕЖНЫМ, applies=full_rd]
  Раздел "Бытовая К1 и ливневая К2 канализация"
  (СП 30.13330.2020).

## Mandatory required calculations

- [problem_class=missing_calculation_basis, severity=КРИТИЧЕСКОЕ, applies=full_rd]
  Расчётные расходы воды: Q_сут, Q_час, Q_сек
  (СП 30.13330.2020).
- [problem_class=missing_calculation_basis, severity=ЭКСПЛУАТАЦИОННОЕ, applies=full_rd]
  Гидравлический расчёт сетей (диаметры, потери, давление
  на вводе и у потребителей) (СП 30.13330.2020).
- [problem_class=missing_calculation_basis, severity=ЭКСПЛУАТАЦИОННОЕ, applies=full_rd]
  Расчёт стоков бытовой и ливневой канализации (диаметры, уклоны,
  наполняемость, водосборные площади)
  (СП 30.13330.2020, СП 32.13330.2018).
- [problem_class=missing_calculation_basis, severity=РЕКОМЕНДАТЕЛЬНОЕ, applies=full_rd]
  Расчёт гидроудара / защиты от перенапряжения
  (СП 30.13330.2020).

## Mandatory required specifications / schedules

- [problem_class=missing_mandatory_schedule, severity=ЭКСПЛУАТАЦИОННОЕ, applies=full_rd|specification_only]
  Спецификация оборудования и материалов
  (ГОСТ 21.601-2011).
- [problem_class=missing_mandatory_schedule, severity=ЭКСПЛУАТАЦИОННОЕ, applies=full_rd|specification_only]
  Спецификация сантехнического оборудования с указанием типов
  и расходов (ГОСТ 21.601-2011).

## Mandatory required diagrams / schemes

- [problem_class=missing_diagram, severity=ЭКСПЛУАТАЦИОННОЕ, applies=full_rd]
  Аксонометрические / принципиальные схемы систем В1, Т3/Т4, К1, К2
  и планы сетей по этажам (ГОСТ 21.601-2011).
- [problem_class=missing_diagram, severity=ЭКСПЛУАТАЦИОННОЕ, applies=full_rd]
  Узлы прохода трубопроводов через ограждения с указанием
  гильз и противопожарных муфт (СП 30.13330.2020).

## Mandatory required parameters (per pump / equipment)

- [problem_class=missing_mandatory_parameter, severity=ЭКСПЛУАТАЦИОННОЕ, applies=full_rd|specification_only]
  На каждом насосе: производительность Q (м³/ч), напор
  H (м вод. ст.) (СП 30.13330.2020).
- [problem_class=missing_mandatory_parameter, severity=ЭКСПЛУАТАЦИОННОЕ, applies=full_rd|specification_only]
  На каждом насосе: электрическая мощность P_уст (кВт),
  напряжение и число фаз (для передачи в ЭОМ).
- [problem_class=missing_mandatory_parameter, severity=РЕКОМЕНДАТЕЛЬНОЕ, applies=full_rd|specification_only]
  КПД, класс защиты IP насосов.

## Mandatory coordination requirements (cross-discipline)

- [problem_class=missing_mandatory_section, severity=ПРОВЕРИТЬ_ПО_СМЕЖНЫМ, applies=full_rd]
  Передача в ЭОМ перечня электроприёмников насосного
  оборудования (ГОСТ Р 21.101-2020).
- [problem_class=missing_mandatory_section, severity=ПРОВЕРИТЬ_ПО_СМЕЖНЫМ, applies=full_rd]
  Координация с АПС для внутреннего противопожарного
  водопровода (СП 10.13130.2020).
- [problem_class=missing_mandatory_section, severity=ПРОВЕРИТЬ_ПО_СМЕЖНЫМ, applies=full_rd]
  Координация с ИТП для горячего водоснабжения (узлы ввода,
  параметры теплоносителя).
- [problem_class=missing_mandatory_section, severity=ПРОВЕРИТЬ_ПО_СМЕЖНЫМ, applies=full_rd]
  Отверстия / гильзы в АР и КЖ для прокладки трубопроводов.

## Mandatory testing / commissioning requirements

- [problem_class=missing_mandatory_section, severity=РЕКОМЕНДАТЕЛЬНОЕ, applies=full_rd]
  Программа гидравлических испытаний трубопроводов, дезинфекции
  и промывки систем питьевого водоснабжения
  (СП 30.13330.2020, СП 73.13330.2016, СанПиН 2.1.4.1074-01).

## Recommended items

- [problem_class=missing_calculation_basis, severity=РЕКОМЕНДАТЕЛЬНОЕ, applies=full_rd]
  Расчёт тепловой изоляции трубопроводов ГВС и ХВС в неотапливаемых
  зонах и схема узла учёта воды (СП 61.13330.2012).

## Conditional items (by object type / document_type)

- [problem_class=missing_mandatory_section, severity=ЭКСПЛУАТАЦИОННОЕ, applies=full_rd]
  Применяется при наличии повысительной насосной: схема насосной
  станции, резервирование, режимы (СП 30.13330.2020).
- [problem_class=missing_mandatory_parameter, severity=ЭКСПЛУАТАЦИОННОЕ, applies=full_rd]
  Применяется при наличии бака-аккумулятора ГВС: расчёт ёмкости
  и температуры (СП 30.13330.2020).
- [problem_class=missing_mandatory_section, severity=ЭКСПЛУАТАЦИОННОЕ, applies=full_rd]
  Применяется при наличии внутреннего пожарного водопровода:
  расчёт числа струй и расхода (СП 10.13130.2020).
- [problem_class=missing_diagram, severity=ЭКСПЛУАТАЦИОННОЕ, applies=full_rd]
  Применяется при наличии жироуловителей (общепит): схема
  отведения стоков (СП 30.13330.2020).

## Anti-patterns — DO NOT flag these as findings

- Конкретный производитель насосов / арматуры (Grundfos / Wilo /
  Danfoss) не указан — не нарушение.
- "Спецификация сантехоборудования может быть неполной" без
  цитирования строки — спекулятивно, выбросить.
- "Недостаточный объём резерва" отдельно по каждому насосу —
  один класс `pump_redundancy` (collapse).
- "Необходимо уточнить циркуляцию ГВС" без явного отсутствия —
  non-actionable, выбросить.
- Фантомные пункты СП 30 / СП 32 без проверки — выбросить.
- На `document_type=tz_vs_rd` флаг "отсутствует циркуляция ГВС"
  трактовать как несоответствие ТЗ, а не как нарушение нормы.
- РЕКОМЕНДАТЕЛЬНОЕ для отсутствия расчёта расходов — это
  инверсия, минимум КРИТИЧЕСКОЕ.
- Перечислять каждый стояк без диаметра — один класс
  `pipe_diameter_missing` (collapse).
