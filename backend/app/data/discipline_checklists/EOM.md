# Checklist — EOM (Электроснабжение и силовое электрооборудование)

Применимость по `document_type`: см. `checklist_applicability_matrix.md`.

## Mandatory required sections / artifacts

- [problem_class=missing_mandatory_section, severity=ПРОВЕРИТЬ_ПО_СМЕЖНЫМ, applies=full_rd]
  Общие данные раздела ЭОМ: категория надёжности электроснабжения,
  система заземления (TN-C-S / TN-S / TT), напряжение питания
  (ПУЭ-7, п. 1.2.18; параллельно — СП 256.1325800.2016, п. 5).
- [problem_class=missing_mandatory_section, severity=ПРОВЕРИТЬ_ПО_СМЕЖНЫМ, applies=full_rd]
  Раздел "Защита от поражения электрическим током и заземление"
  с указанием системы заземления и СУП
  (ПУЭ-7, гл. 1.7; СП 256.1325800.2016).
- [problem_class=missing_mandatory_section, severity=ПРОВЕРИТЬ_ПО_СМЕЖНЫМ, applies=full_rd]
  Раздел "Освещение": рабочее, аварийное (эвакуационное и
  резервное) (СП 52.13330.2016, СП 256.1325800.2016).

## Mandatory required calculations

- [problem_class=missing_calculation_basis, severity=КРИТИЧЕСКОЕ, applies=full_rd]
  Расчёт электрических нагрузок (электробаланс) с разделением
  по группам потребителей (СП 256.1325800.2016, п. 6.1).
- [problem_class=missing_calculation_basis, severity=ЭКСПЛУАТАЦИОННОЕ, applies=full_rd]
  Перечень всех электроприёмников с указанием P_уст, K_с, P_расч
  (СП 256.1325800.2016, раздел 7 «Нагрузки», п. 7.1 — жилые,
  п. 7.2 — общественные).
- [problem_class=missing_calculation_basis, severity=ЭКСПЛУАТАЦИОННОЕ, applies=full_rd]
  Расчёт токов короткого замыкания на шинах ВРУ / ГРЩ и проверка
  селективности защит между вышестоящими и нижестоящими аппаратами
  (ГОСТ Р 50571.5.52-2011, СП 256.1325800.2016).
- [problem_class=missing_calculation_basis, severity=РЕКОМЕНДАТЕЛЬНОЕ, applies=full_rd]
  Расчёт потерь напряжения в магистральных и групповых линиях
  (СП 256.1325800.2016, п. 12.6; ГОСТ Р 50571.5.52).

## Mandatory required specifications / schedules

- [problem_class=missing_mandatory_schedule, severity=ЭКСПЛУАТАЦИОННОЕ, applies=full_rd|specification_only]
  Кабельный журнал отходящих линий
  (ГОСТ 21.613-2014, п. 5.5).
- [problem_class=missing_mandatory_schedule, severity=ЭКСПЛУАТАЦИОННОЕ, applies=full_rd|specification_only]
  Спецификация оборудования, изделий и материалов
  (ГОСТ 21.110-2013).

## Mandatory required diagrams / schemes

- [problem_class=missing_diagram, severity=ЭКСПЛУАТАЦИОННОЕ, applies=full_rd]
  Однолинейная схема ВРУ / ВРУ-1 / ГРЩ с типами аппаратов и
  кабелей (ГОСТ 21.613-2014, п. 5.4).
- [problem_class=missing_diagram, severity=ЭКСПЛУАТАЦИОННОЕ, applies=full_rd]
  Схемы распределительных щитов (ЩР / ЩО / ЩЭ) с группами
  потребителей (ГОСТ 21.613-2014).
- [problem_class=missing_diagram, severity=ЭКСПЛУАТАЦИОННОЕ, applies=full_rd]
  План электрических сетей этажей с трассировкой кабелей,
  расположением щитов и светильников (ГОСТ 21.613-2014).

## Mandatory required parameters (per cable / breaker)

- [problem_class=missing_mandatory_parameter, severity=ЭКСПЛУАТАЦИОННОЕ, applies=full_rd|specification_only]
  На каждом кабеле: сечение и марка кабеля (тип изоляции, FRLS
  для пожарных систем) (ГОСТ 31996-2012).
- [problem_class=missing_mandatory_parameter, severity=ЭКСПЛУАТАЦИОННОЕ, applies=full_rd|specification_only]
  На каждом автоматическом выключателе: номинальный ток и
  характеристика расцепителя (B / C / D), отключающая
  способность I_откл (ГОСТ Р 50345-2010).
- [problem_class=missing_mandatory_parameter, severity=ЭКСПЛУАТАЦИОННОЕ, applies=full_rd|specification_only]
  Длина участка кабеля (для расчёта потерь напряжения и КЗ).
## Mandatory coordination requirements (cross-discipline)

- [problem_class=missing_mandatory_section, severity=ПРОВЕРИТЬ_ПО_СМЕЖНЫМ, applies=full_rd]
  Перечень электроприёмников ОВ / ВК с P_уст и K_с от смежников.
- [problem_class=missing_mandatory_section, severity=ПРОВЕРИТЬ_ПО_СМЕЖНЫМ, applies=full_rd]
  Координация с АПС по управлению вентиляцией и противопожарным
  оборудованием при пожаре (для МКД обязательно).
- [problem_class=missing_mandatory_section, severity=РЕКОМЕНДАТЕЛЬНОЕ, applies=full_rd]
  Закладные / отверстия в перекрытиях и стенах от АР / КЖ
  для прокладки кабельных трасс.

## Mandatory testing / commissioning requirements

- [problem_class=missing_mandatory_section, severity=РЕКОМЕНДАТЕЛЬНОЕ, applies=full_rd]
  Указания по приёмо-сдаточным испытаниям электроустановок,
  включая программу испытаний СУП и заземляющих устройств
  (ПУЭ-7, гл. 1.8; ГОСТ Р 50571.16-2019; СП 76.13330.2016).

## Recommended items

- [problem_class=missing_calculation_basis, severity=РЕКОМЕНДАТЕЛЬНОЕ, applies=full_rd]
  Расчёт компенсации реактивной мощности
  (для объектов с большой нагрузкой двигателей).
- [problem_class=missing_diagram, severity=РЕКОМЕНДАТЕЛЬНОЕ, applies=full_rd]
  Схема АВР для потребителей I категории
  (СП 256.1325800.2016).

## Conditional items (by object type / document_type)

- [problem_class=missing_calculation_basis, severity=ЭКСПЛУАТАЦИОННОЕ, applies=full_rd]
  Применяется для отдельно стоящих / высоких объектов и объектов
  с категорией по СО-153: молниезащита с расчётом класса
  по СО-153-34.21.122-2003.
- [problem_class=missing_mandatory_section, severity=ЭКСПЛУАТАЦИОННОЕ, applies=full_rd]
  Применяется для I и I-особой категорий: схема резервного питания
  (АВР / ДГУ / ИБП) и обоснование времени переключения
  (ПУЭ-7, п. 1.2).
- [problem_class=missing_mandatory_parameter, severity=ЭКСПЛУАТАЦИОННОЕ, applies=full_rd|specification_only]
  Применяется для розеточных групп и санузлов: указание УЗО
  (30 мА для розеточных, 10 мА для ванных)
  (ПУЭ-7, п. 7.1.71-7.1.83).
- [problem_class=missing_mandatory_section, severity=ЭКСПЛУАТАЦИОННОЕ, applies=full_rd]
  Применяется при наличии лифтов: координация с разделом ВТ
  по электропитанию.

## Anti-patterns — DO NOT flag these as findings

- Производитель / марка автомата (например, IEK ВА47-29 vs ABB)
  само по себе НЕ нарушение — маркетинговая категория.
- ПУЭ-7 без параллельной ссылки на СП — это задача нормативной
  верификации, не completeness; не дублировать в completeness lens.
- "Спецификация электрооборудования может быть неполной" без
  цитирования конкретной строки — спекулятивно, выбросить.
- "Необходимо уточнить категорию надёжности" без явного отсутствия
  категории в МД — non-actionable, выбросить.
- Фантомные номера пунктов ПУЭ / СП — выбросить.
- На `document_type=audit_comparison` флаг "отсутствует однолинейная
  схема ВРУ" — раздел заведомо out-of-scope для аудита сравнения.
- РЕКОМЕНДАТЕЛЬНОЕ для отсутствия расчёта нагрузок — это инверсия,
  минимум КРИТИЧЕСКОЕ.
- Перечислять каждый кабель без сечения отдельной находкой — это
  один класс `cable_journal_incomplete` (collapse).
