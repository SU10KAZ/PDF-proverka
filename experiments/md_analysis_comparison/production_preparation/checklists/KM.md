# Checklist — KM (Конструкции металлические)

Применимость по `document_type`: см. `checklist_applicability_matrix.md`.
Структура зеркалит EOM (M / R / conditional + параметры + cross + anti-pattern).

## Mandatory required sections / artifacts

- [problem_class=missing_mandatory_section, severity=ПРОВЕРИТЬ_ПО_СМЕЖНЫМ, applies=full_rd]
  Общие данные раздела КМ: классы стали по ГОСТ 27772-2015,
  обозначения сварных швов, требования к точности
  (ГОСТ 21.502-2016, п. 4).
- [problem_class=missing_mandatory_section, severity=ПРОВЕРИТЬ_ПО_СМЕЖНЫМ, applies=full_rd]
  Раздел "Антикоррозионная защита" с описанием системы покрытия
  (СП 28.13330.2017).
- [problem_class=missing_mandatory_section, severity=ПРОВЕРИТЬ_ПО_СМЕЖНЫМ, applies=full_rd]
  Раздел "Огнезащита несущих стальных элементов" с указанием
  предела огнестойкости (СП 2.13130.2020).
- [problem_class=missing_mandatory_section, severity=ПРОВЕРИТЬ_ПО_СМЕЖНЫМ, applies=full_rd]
  Требования к контролю качества сварных соединений
  (ГОСТ Р ИСО 17637, ГОСТ 7512-82).

## Mandatory required calculations

- [problem_class=missing_calculation_basis, severity=КРИТИЧЕСКОЕ, applies=full_rd]
  Расчётная схема каркаса с расчётными длинами l_ef стержней
  (СП 16.13330.2017, п. 7-8).
- [problem_class=missing_calculation_basis, severity=КРИТИЧЕСКОЕ, applies=full_rd]
  Подбор сечений с проверкой на прочность И устойчивость
  центрально-сжатых элементов (СП 16.13330.2017, п. 7.1).
- [problem_class=missing_calculation_basis, severity=ЭКСПЛУАТАЦИОННОЕ, applies=full_rd]
  Подбор сечений по двум группам предельных состояний (прочность
  + прогиб) (СП 20.13330.2016, СП 16.13330.2017, п. 6).
- [problem_class=missing_calculation_basis, severity=КРИТИЧЕСКОЕ, applies=full_rd]
  Расчёт узлов и соединений (болтовых и сварных)
  (СП 16.13330.2017, п. 14).

## Mandatory required specifications / schedules

- [problem_class=missing_mandatory_schedule, severity=ЭКСПЛУАТАЦИОННОЕ, applies=full_rd|specification_only]
  Ведомость элементов / отправочных марок
  (ГОСТ 21.502-2016).
- [problem_class=missing_mandatory_schedule, severity=ЭКСПЛУАТАЦИОННОЕ, applies=full_rd|specification_only]
  Спецификация металлопроката по сортаменту (ГОСТ 8239 / 8240 / 26020
  / 30245 — в зависимости от профиля).
- [problem_class=missing_mandatory_schedule, severity=ЭКСПЛУАТАЦИОННОЕ, applies=full_rd|specification_only]
  Ведомость метизов (болтовых, заклёпочных) с классом прочности.

## Mandatory required diagrams / schemes

- [problem_class=missing_diagram, severity=ЭКСПЛУАТАЦИОННОЕ, applies=full_rd]
  Узлы характерных соединений (опорные, монтажные стыки, узлы
  ферм) (ГОСТ 21.502-2016).
- [problem_class=missing_diagram, severity=ЭКСПЛУАТАЦИОННОЕ, applies=full_rd]
  Монтажные схемы с заводской и монтажной маркировкой
  отправочных элементов (ГОСТ 21.502-2016).

## Mandatory required parameters (per element / connection)

- [problem_class=missing_mandatory_parameter, severity=ЭКСПЛУАТАЦИОННОЕ, applies=full_rd|specification_only]
  На каждом элементе: сечение профиля (для прокатных — обозначение
  по ГОСТ) и класс стали (ГОСТ 27772-2015).
- [problem_class=missing_mandatory_parameter, severity=ЭКСПЛУАТАЦИОННОЕ, applies=full_rd|specification_only]
  На каждом болтовом соединении: класс прочности, количество,
  шаг, коэффициент трения μ для фрикционных соединений
  (СП 16.13330.2017, п. 14).
- [problem_class=missing_mandatory_parameter, severity=ЭКСПЛУАТАЦИОННОЕ, applies=full_rd|specification_only]
  На каждом сварном соединении: тип шва, катет k_f, длина шва,
  марка электродов / сварочной проволоки.

## Mandatory coordination requirements (cross-discipline)

- [problem_class=missing_mandatory_section, severity=ПРОВЕРИТЬ_ПО_СМЕЖНЫМ, applies=full_rd]
  Нагрузки от оборудования ОВ / ВК на покрытие и фермы
  (координация с соответствующими разделами).
- [problem_class=missing_mandatory_section, severity=ПРОВЕРИТЬ_ПО_СМЕЖНЫМ, applies=full_rd]
  Опирание перекрытий КЖ на металлические колонны
  (координация с разделом КЖ, закладные детали).
- [problem_class=missing_mandatory_section, severity=РЕКОМЕНДАТЕЛЬНОЕ, applies=full_rd]
  Координация с АР по узлам сопряжения металлокаркаса с фасадом.

## Mandatory testing / commissioning requirements

- [problem_class=missing_mandatory_section, severity=РЕКОМЕНДАТЕЛЬНОЕ, applies=full_rd]
  Указания по контролю сварных швов (РК, УЗД) и моменту затяжки
  высокопрочных болтов (ГОСТ Р ИСО 17637, СП 70.13330.2012).

## Recommended items

- [problem_class=missing_mandatory_parameter, severity=РЕКОМЕНДАТЕЛЬНОЕ, applies=full_rd]
  Явное указание коэффициентов γ_c, γ_n, γ_f
  (СП 16.13330.2017, п. 1.5).
- [problem_class=incomplete_specification, severity=РЕКОМЕНДАТЕЛЬНОЕ, applies=full_rd|specification_only]
  Указание сертификата соответствия проката для ответственных
  конструкций.

## Conditional items (by object type / document_type)

- [problem_class=missing_mandatory_section, severity=ЭКСПЛУАТАЦИОННОЕ, applies=full_rd]
  Применяется в условиях агрессивной среды: расширенный раздел
  АКЗ с группой агрессивности (СП 28.13330.2017).
- [problem_class=missing_calculation_basis, severity=ЭКСПЛУАТАЦИОННОЕ, applies=full_rd]
  Применяется для высотных и большепролётных объектов: расчёт
  на динамические воздействия (СП 20.13330.2016).
- [problem_class=missing_diagram, severity=ЭКСПЛУАТАЦИОННОЕ, applies=full_rd]
  Применяется при ферменном решении покрытия: расчёт устойчивости
  поясов и раскосов фермы (СП 16.13330.2017).

## Anti-patterns — DO NOT flag these as findings

- Указание конкретного производителя профилей / электродов само
  по себе не нарушение — выбор по торгам.
- Применение электродов Э42 / Э50А — стандартные марки по
  ГОСТ 9467-75; "не та марка" без обоснования не является дефектом.
- Расчётный пролёт без явного указания γ_n при наличии всех
  нагрузок и стандартных γ_n = 0,95...1,0 — не повод для
  КРИТИЧЕСКОГО.
- "Спецификация металлопроката может быть неполной" без цитирования
  конкретной строки — спекулятивно, выбросить.
- На `document_type=audit_comparison` флаг "отсутствует ведомость
  отправочных марок" — раздел out-of-scope.
- РЕКОМЕНДАТЕЛЬНОЕ для отсутствия расчёта устойчивости — это
  инверсия, минимум ЭКСПЛУАТАЦИОННОЕ, при явном противоречии
  КРИТИЧЕСКОЕ.
- Перечислять каждый узел без указания катета сварного шва
  отдельной находкой — это один класс `weld_spec_incomplete`.
