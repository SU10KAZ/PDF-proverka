# Checklist — AR (Архитектурные решения)

Применимость по `document_type`: см. `checklist_applicability_matrix.md`.
Любой пункт включается в выдачу только если соответствующий `applies=`
покрывает текущий `document_type`.

## Mandatory required sections / artifacts

- [problem_class=missing_mandatory_section, severity=ПРОВЕРИТЬ_ПО_СМЕЖНЫМ, applies=full_rd]
  Объяснительная (пояснительная) записка раздела АР с описанием
  принятых архитектурных и объёмно-планировочных решений
  (ГОСТ Р 21.1101-2013, п. 4.2).
- [problem_class=missing_mandatory_section, severity=ПРОВЕРИТЬ_ПО_СМЕЖНЫМ, applies=full_rd]
  Поэтажные планы с экспликацией помещений
  (ГОСТ 21.501-2018, п. 5.3).
- [problem_class=missing_mandatory_section, severity=ПРОВЕРИТЬ_ПО_СМЕЖНЫМ, applies=full_rd]
  Разрезы здания (продольный и поперечный) с отметками уровней
  (ГОСТ 21.501-2018, п. 5.5).
- [problem_class=missing_mandatory_section, severity=ПРОВЕРИТЬ_ПО_СМЕЖНЫМ, applies=full_rd]
  Фасады с указанием отметок, материалов отделки и узлов примыканий
  (ГОСТ 21.501-2018, п. 5.6).
- [problem_class=missing_mandatory_section, severity=ПРОВЕРИТЬ_ПО_СМЕЖНЫМ, applies=full_rd]
  Раздел "Пожарная безопасность" / огнестойкость конструкций и класс
  пожарной опасности (ФЗ-123, СП 2.13130.2020).
- [problem_class=missing_mandatory_section, severity=ПРОВЕРИТЬ_ПО_СМЕЖНЫМ, applies=full_rd]
  Раздел "Эвакуация" — пути эвакуации, ширина, протяжённость, число
  эвакуационных выходов (СП 1.13130.2020).
- [problem_class=missing_mandatory_section, severity=ПРОВЕРИТЬ_ПО_СМЕЖНЫМ, applies=full_rd]
  Раздел нормоконтроля / штамп с указанием стадии РД
  (ГОСТ Р 21.1101-2013, п. 5).

## Mandatory required calculations

- [problem_class=missing_mandatory_parameter, severity=ЭКСПЛУАТАЦИОННОЕ, applies=full_rd]
  Расчёт инсоляции жилых помещений (для жилых МКД)
  (СП 50.13330.2012, пункт уточнить — см. также СанПиН 1.2.3685-21).
- [problem_class=missing_calculation_basis, severity=ЭКСПЛУАТАЦИОННОЕ, applies=full_rd]
  Технико-экономические показатели объекта (ТЭП): этажность,
  площади, строительный объём (ГОСТ Р 21.1101-2013, п. 4.2.2).

## Mandatory required specifications / schedules

- [problem_class=missing_mandatory_schedule, severity=ЭКСПЛУАТАЦИОННОЕ, applies=full_rd|specification_only]
  Спецификация заполнения оконных и дверных проёмов
  (ГОСТ 21.501-2018, п. 5.10).
- [problem_class=missing_mandatory_schedule, severity=ЭКСПЛУАТАЦИОННОЕ, applies=full_rd|specification_only]
  Ведомость отделки помещений
  (ГОСТ 21.501-2018, п. 5.11).
- [problem_class=missing_mandatory_schedule, severity=ЭКСПЛУАТАЦИОННОЕ, applies=full_rd]
  Экспликация помещений с привязкой к этажным планам
  (ГОСТ 21.501-2018, п. 5.3).

## Mandatory required diagrams / schemes

- [problem_class=missing_diagram, severity=ЭКСПЛУАТАЦИОННОЕ, applies=full_rd]
  Узлы характерные (примыкание кровли, парапета, цоколя)
  (ГОСТ 21.501-2018, п. 5.9).
- [problem_class=missing_diagram, severity=ЭКСПЛУАТАЦИОННОЕ, applies=full_rd]
  Схема расположения эвакуационных путей и выходов на планах
  (СП 1.13130.2020).

## Mandatory coordination requirements (cross-discipline)

- [problem_class=missing_mandatory_section, severity=ПРОВЕРИТЬ_ПО_СМЕЖНЫМ, applies=full_rd]
  Координация проходов / отверстий для смежных разделов
  ОВ / ВК / ЭОМ через перекрытия и стены (ГОСТ 21.501-2018, п. 5.8).
- [problem_class=missing_mandatory_section, severity=ПРОВЕРИТЬ_ПО_СМЕЖНЫМ, applies=full_rd]
  Координация с КЖ / КМ по закладным деталям и опиранию конструкций
  (ГОСТ 21.501-2018).

## Mandatory testing / commissioning requirements

- [problem_class=missing_mandatory_section, severity=РЕКОМЕНДАТЕЛЬНОЕ, applies=full_rd]
  Указания по приёмке отделочных работ
  (СП 71.13330.2017).

## Recommended items

- [problem_class=incomplete_specification, severity=РЕКОМЕНДАТЕЛЬНОЕ, applies=full_rd|specification_only]
  Раздел отделки помещений с привязкой типов отделки к экспликации
  (ГОСТ 21.501-2018, п. 5.11).
- [problem_class=missing_mandatory_section, severity=РЕКОМЕНДАТЕЛЬНОЕ, applies=full_rd]
  Решение по доступности для МГН
  (СП 59.13330.2020).
- [problem_class=missing_calculation_basis, severity=РЕКОМЕНДАТЕЛЬНОЕ, applies=full_rd]
  Звукоизоляционные характеристики ограждающих конструкций
  (СП 51.13330.2011).

## Conditional items (by object type / document_type)

- [problem_class=missing_mandatory_parameter, severity=ЭКСПЛУАТАЦИОННОЕ, applies=full_rd]
  Применяется для жилых МКД: расчёт КЕО (естественная освещённость)
  (СП 52.13330.2016).
- [problem_class=missing_mandatory_section, severity=РЕКОМЕНДАТЕЛЬНОЕ, applies=full_rd]
  Применяется только при наличии витражей / СПФ-фасадов: узлы
  крепления светопрозрачных конструкций (СП 426.1325800.2020).
- [problem_class=missing_diagram, severity=ЭКСПЛУАТАЦИОННОЕ, applies=full_rd]
  Применяется при наличии кровли с выходом на эксплуатацию:
  план кровли и узлы парапета (ГОСТ 21.501-2018).

## Anti-patterns — DO NOT flag these as findings

- Конкретный производитель окон / дверей / отделочных материалов
  не указан — это не нарушение, рынок производителей открытый.
- "Возможно, спецификация неполна" без цитирования конкретной строки —
  спекулятивно, выбросить.
- "Необходимо уточнить класс пожарной опасности" без явного указания,
  что класс отсутствует или противоречит ФЗ-123 — недопустимая
  формулировка, нет действия.
- Фантомные номера пунктов норм без проверки (например,
  "СП XX, п. Y.Y.Y" с непроверяемым номером) — выбросить.
- На `document_type=audit_comparison` флаг "отсутствует пояснительная
  записка" — раздел заведомо out-of-scope для аудита сравнения.
- РЕКОМЕНДАТЕЛЬНОЕ для отсутствия эвакуационных выходов — это инверсия,
  такой дефект всегда минимум ПРОВЕРИТЬ_ПО_СМЕЖНЫМ, а при явном
  противоречии норме — КРИТИЧЕСКОЕ.
- Перечислять каждое не указанное помещение в экспликации отдельной
  находкой — это один класс `missing_room_exposition` (collapse).
