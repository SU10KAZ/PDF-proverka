# Checklist — MULTI (Междисциплинарный / cross-section / TZ vs RD)

Используется когда дисциплина не сужена до одной (`document_type` =
`audit_comparison` или `tz_vs_rd` или MD-документ охватывает несколько
разделов одновременно). Фокус — на согласованность между разделами и
соответствие РД и ТЗ.

Применимость по `document_type`: см. `checklist_applicability_matrix.md`.

## Mandatory required sections / artifacts

- [problem_class=missing_mandatory_section, severity=ПРОВЕРИТЬ_ПО_СМЕЖНЫМ, applies=full_rd|audit_comparison]
  Сводный перечень разделов проекта (ЭОМ / ОВ / ВК / АР / КЖ / КМ /
  СС) с указанием стадии и шифров (ГОСТ Р 21.1101-2013).
- [problem_class=missing_mandatory_section, severity=ПРОВЕРИТЬ_ПО_СМЕЖНЫМ, applies=tz_vs_rd]
  Каждое требование ТЗ имеет соответствующее решение в РД
  (ГОСТ Р 21.1101-2013).
- [problem_class=missing_mandatory_section, severity=ПРОВЕРИТЬ_ПО_СМЕЖНЫМ, applies=tz_vs_rd]
  Отклонения от ТЗ обозначены явно (через лист изменений или
  согласованный протокол) (ГОСТ Р 21.1101-2013).
- [problem_class=missing_mandatory_section, severity=ПРОВЕРИТЬ_ПО_СМЕЖНЫМ, applies=audit_comparison]
  Сводная ведомость замечаний и их статуса (учтено / частично /
  отклонено).

## Mandatory cross-section consistency (ЭОМ ↔ ОВ ↔ ВК ↔ СС)

- [problem_class=missing_mandatory_section, severity=ПРОВЕРИТЬ_ПО_СМЕЖНЫМ, applies=full_rd]
  ЭОМ ↔ ОВ: значения P_уст вентиляторов в ЭОМ совпадают с
  заявленными в ОВ (ГОСТ Р 21.1101-2013).
- [problem_class=missing_mandatory_section, severity=ПРОВЕРИТЬ_ПО_СМЕЖНЫМ, applies=full_rd]
  ЭОМ ↔ ВК: значения P_уст насосов в ЭОМ совпадают с заявленными
  в ВК.
- [problem_class=missing_mandatory_section, severity=ПРОВЕРИТЬ_ПО_СМЕЖНЫМ, applies=full_rd]
  ЭОМ ↔ СС: мощность СС-стойки и адресации совпадает с
  электробалансом.
- [problem_class=missing_mandatory_section, severity=ПРОВЕРИТЬ_ПО_СМЕЖНЫМ, applies=full_rd]
  ОВ ↔ АПС (СС): алгоритм управления противодымной вентиляцией
  и отключение общеобменной вентиляции при пожаре
  (СП 7.13130.2013, СП 484.1311500.2020).
- [problem_class=missing_mandatory_section, severity=ПРОВЕРИТЬ_ПО_СМЕЖНЫМ, applies=full_rd]
  ВК ↔ АПС: запуск противопожарной насосной по сигналу
  от АПС (СП 10.13130.2020).

## Mandatory cross-section consistency (АР ↔ КЖ ↔ КМ)

- [problem_class=missing_mandatory_section, severity=ПРОВЕРИТЬ_ПО_СМЕЖНЫМ, applies=full_rd]
  АР ↔ КЖ: габариты и привязки конструктивных элементов
  совпадают на планах АР и КЖ.
- [problem_class=missing_mandatory_section, severity=ПРОВЕРИТЬ_ПО_СМЕЖНЫМ, applies=full_rd]
  АР ↔ КМ: координация узлов сопряжения металлокаркаса с
  фасадом и кровлей.
- [problem_class=missing_mandatory_section, severity=ПРОВЕРИТЬ_ПО_СМЕЖНЫМ, applies=full_rd]
  КЖ ↔ КМ: закладные детали для опирания металлоконструкций
  на железобетон.
- [problem_class=missing_mandatory_section, severity=ПРОВЕРИТЬ_ПО_СМЕЖНЫМ, applies=full_rd]
  АР ↔ ОВ / ВК / ЭОМ: проходы и отверстия в перекрытиях и
  стенах для инженерных систем.

## Mandatory parameter consistency

- [problem_class=missing_mandatory_parameter, severity=ЭКСПЛУАТАЦИОННОЕ, applies=full_rd|audit_comparison|tz_vs_rd]
  Единицы измерения одинаковые во всех разделах: P в кВт, L в
  м³/ч, Q в м³/ч / м³/сут, диаметры в мм.
- [problem_class=missing_mandatory_parameter, severity=ЭКСПЛУАТАЦИОННОЕ, applies=tz_vs_rd]
  Параметры РД количественно совпадают с ТЗ (или есть письменное
  согласование изменения).
- [problem_class=missing_mandatory_parameter, severity=РЕКОМЕНДАТЕЛЬНОЕ, applies=full_rd|specification_only]
  Спецификация оборудования всех разделов соответствует составу
  ТЗ (ГОСТ 21.110-2013).

## Mandatory required diagrams / schemes

- [problem_class=missing_diagram, severity=РЕКОМЕНДАТЕЛЬНОЕ, applies=full_rd]
  Сводная схема взаимодействия инженерных систем (для
  ответственных объектов).

## Mandatory testing / commissioning requirements

- [problem_class=missing_mandatory_section, severity=РЕКОМЕНДАТЕЛЬНОЕ, applies=full_rd]
  Программа комплексных испытаний (взаимодействие АПС, ОВ, ЭОМ,
  ВК при тревоге).

## Recommended items

- [problem_class=missing_mandatory_section, severity=РЕКОМЕНДАТЕЛЬНОЕ, applies=full_rd]
  Журнал согласований между смежниками с фиксацией точек
  передачи данных.
- [problem_class=incomplete_specification, severity=РЕКОМЕНДАТЕЛЬНОЕ, applies=audit_comparison]
  Каждое замечание из исходного аудита прокомментировано
  (учтено / отклонено / в работе).

## Conditional items (by object type / document_type)

- [problem_class=missing_mandatory_section, severity=ЭКСПЛУАТАЦИОННОЕ, applies=tz_vs_rd]
  Применяется когда ТЗ содержит специфические требования
  (зональность, BIM-модель, LEED/BREEAM): отражение в РД.
- [problem_class=missing_mandatory_section, severity=ЭКСПЛУАТАЦИОННОЕ, applies=audit_comparison]
  Применяется когда исходный аудит содержал КРИТИЧЕСКИЕ
  замечания: статус устранения каждого критического пункта.

## Anti-patterns — DO NOT flag these as findings

- Не флагать "отсутствует ЭОМ / ОВ / ВК как раздел" на
  `document_type=audit_comparison` или `specification_only` —
  отсутствие смежного раздела вне scope сравнения.
- "Возможно, есть расхождение между разделами" без указания
  конкретного параметра — спекулятивно, выбросить.
- "Необходимо проверить согласованность с ЭОМ" без указания
  конкретной строки — non-actionable, выбросить.
- Фантомные ссылки на пункты ГОСТ Р 21.1101 без проверки номера —
  выбросить.
- Каждое расхождение значения параметра — отдельный finding,
  `tz_rd_mismatch_<parameter>`; не дублировать через "ещё одно
  расхождение по этому же оборудованию".
- Перечислять каждый параметр интерфейса между разделами как
  отдельный finding — collapse к одному на (discipline_pair ×
  interface_type).
- РЕКОМЕНДАТЕЛЬНОЕ для отсутствия согласования критических
  параметров (например, мощности ввода) — это инверсия, минимум
  ПРОВЕРИТЬ_ПО_СМЕЖНЫМ.
- На `document_type=audit_comparison` cross-discipline phantom
  flag (например, "отсутствует координация ВК с АПС" при сравнении
  только электрических разделов) — drop.
