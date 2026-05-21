# Checklist — SS (Слаботочные системы / сети связи)

Применимость по `document_type`: см. `checklist_applicability_matrix.md`.

## Mandatory required sections / artifacts

- [problem_class=missing_mandatory_section, severity=ПРОВЕРИТЬ_ПО_СМЕЖНЫМ, applies=full_rd]
  Общие данные раздела СС с перечнем подсистем: СКС, СОТС
  (видеонаблюдение), СКУД, АПС, СОУЭ, домофония
  (ГОСТ 21.602-2016).
- [problem_class=missing_mandatory_section, severity=ПРОВЕРИТЬ_ПО_СМЕЖНЫМ, applies=full_rd]
  Раздел "Автоматическая пожарная сигнализация (АПС)"
  (СП 484.1311500.2020, СП 486.1311500.2020).
- [problem_class=missing_mandatory_section, severity=ПРОВЕРИТЬ_ПО_СМЕЖНЫМ, applies=full_rd]
  Раздел "Система оповещения и управления эвакуацией (СОУЭ)"
  с указанием типа по СП 3.13130.2009 (СП 3.13130.2009).
- [problem_class=missing_mandatory_section, severity=ПРОВЕРИТЬ_ПО_СМЕЖНЫМ, applies=full_rd]
  Раздел "Заземление слаботочного оборудования"
  (ПУЭ-7, гл. 1.7).

## Mandatory required calculations

- [problem_class=missing_calculation_basis, severity=ЭКСПЛУАТАЦИОННОЕ, applies=full_rd]
  Расчёт нагрузок и ёмкости батарей резервного питания (ИБП /
  АКБ) для активного оборудования (СП 134.13330.2012,
  СП 484.1311500.2020).
- [problem_class=missing_calculation_basis, severity=ЭКСПЛУАТАЦИОННОЕ, applies=full_rd]
  Расчёт количества и расстановки извещателей АПС
  (СП 484.1311500.2020).
- [problem_class=missing_calculation_basis, severity=ЭКСПЛУАТАЦИОННОЕ, applies=full_rd]
  Акустический расчёт СОУЭ (звуковое давление в эвакуационных
  путях) (СП 3.13130.2009).

## Mandatory required specifications / schedules

- [problem_class=missing_mandatory_schedule, severity=ЭКСПЛУАТАЦИОННОЕ, applies=full_rd|specification_only]
  Спецификация оборудования слаботочных систем
  (ГОСТ 21.110-2013).
- [problem_class=missing_mandatory_schedule, severity=ЭКСПЛУАТАЦИОННОЕ, applies=full_rd|specification_only]
  Кабельный журнал слаботочных кабелей
  (ГОСТ 21.602-2016).
- [problem_class=missing_mandatory_schedule, severity=РЕКОМЕНДАТЕЛЬНОЕ, applies=full_rd]
  Адресация устройств АПС / СКУД / СОУЭ
  (ГОСТ 21.602-2016).

## Mandatory required diagrams / schemes

- [problem_class=missing_diagram, severity=ЭКСПЛУАТАЦИОННОЕ, applies=full_rd]
  Структурная схема каждой подсистемы (СКС, СОТС, АПС, СОУЭ,
  СКУД, ВН) (ГОСТ 21.602-2016).
- [problem_class=missing_diagram, severity=ЭКСПЛУАТАЦИОННОЕ, applies=full_rd]
  Схемы расположения оборудования на этажных планах
  (ГОСТ 21.602-2016).
- [problem_class=missing_diagram, severity=ЭКСПЛУАТАЦИОННОЕ, applies=full_rd]
  Узлы прохода кабелей через ограждения с указанием
  противопожарных уплотнений (СП 6.13130.2021).

## Mandatory required parameters (per cable / device)

- [problem_class=missing_mandatory_parameter, severity=ЭКСПЛУАТАЦИОННОЕ, applies=full_rd|specification_only]
  На каждом кабеле: тип / категория (для СКС — Cat.5e / Cat.6;
  для АПС — FRLS / FRHF) (СП 6.13130.2021).
- [problem_class=missing_mandatory_parameter, severity=ЭКСПЛУАТАЦИОННОЕ, applies=full_rd|specification_only]
  На каждом устройстве: тип, напряжение питания, потребляемая
  мощность.

## Mandatory coordination requirements (cross-discipline)

- [problem_class=missing_mandatory_section, severity=ПРОВЕРИТЬ_ПО_СМЕЖНЫМ, applies=full_rd]
  Передача в ЭОМ перечня электроприёмников СС с указанием
  мощности, числа фаз, категории надёжности
  (ГОСТ Р 21.101-2020).
- [problem_class=missing_mandatory_section, severity=ПРОВЕРИТЬ_ПО_СМЕЖНЫМ, applies=full_rd]
  Координация АПС с ОВ по управлению противодымной вентиляцией
  (СП 7.13130.2013, СП 484.1311500.2020).
- [problem_class=missing_mandatory_section, severity=ПРОВЕРИТЬ_ПО_СМЕЖНЫМ, applies=full_rd]
  Координация АПС с лифтовым хозяйством ВТ (опускание лифтов
  на первый этаж при пожаре).
- [problem_class=missing_mandatory_section, severity=ПРОВЕРИТЬ_ПО_СМЕЖНЫМ, applies=full_rd]
  Координация с ВК для пожарного водопровода (запуск насосной
  по сигналу от АПС).

## Mandatory testing / commissioning requirements

- [problem_class=missing_mandatory_section, severity=РЕКОМЕНДАТЕЛЬНОЕ, applies=full_rd]
  Программа приёмо-сдаточных испытаний АПС / СОУЭ и указания по
  сертификации оборудования
  (СП 484.1311500.2020, ФЗ-123, ТР ТС 043/2017).

## Recommended items

- [problem_class=missing_mandatory_section, severity=РЕКОМЕНДАТЕЛЬНОЕ, applies=full_rd]
  Описание интерфейсов интеграции подсистем (SDK / OPC / БДД).
- [problem_class=missing_calculation_basis, severity=РЕКОМЕНДАТЕЛЬНОЕ, applies=full_rd]
  Расчёт сегментов СКС с проверкой длины линии
  (ГОСТ Р 53246-2008).

## Conditional items (by object type / document_type)

- [problem_class=missing_mandatory_section, severity=ЭКСПЛУАТАЦИОННОЕ, applies=full_rd]
  Применяется для МКД: домофония и СКУД входных групп
  (СП 134.13330.2012).
- [problem_class=missing_mandatory_section, severity=ЭКСПЛУАТАЦИОННОЕ, applies=full_rd]
  Применяется для объектов с массовым пребыванием: СОУЭ
  3-5 типа и автоматическое управление (СП 3.13130.2009).
- [problem_class=missing_mandatory_section, severity=ЭКСПЛУАТАЦИОННОЕ, applies=full_rd]
  Применяется для подземных автостоянок: подсистема СОТС
  и газоанализаторы CO (СП 154.13130.2013).

## Anti-patterns — DO NOT flag these as findings

- Конкретный производитель оборудования (Bolid / Rubezh /
  Siemens / Honeywell) не указан — не нарушение.
- "Спецификация СС может быть неполной" без цитирования строки —
  спекулятивно, выбросить.
- Дублировать "кабельный журнал отсутствует" для каждой
  подсистемы — один класс `cable_journal_missing` (max 1 на проект).
- "Необходимо проверить взаимодействие с АПС" без явного
  отсутствия — non-actionable, выбросить.
- Фантомные пункты СП 484 / СП 486 без проверки — выбросить.
- На `document_type=audit_comparison` флаг "отсутствует структурная
  схема СКС" — out-of-scope для сравнения.
- РЕКОМЕНДАТЕЛЬНОЕ для отсутствия АПС в МКД — это инверсия,
  АПС обязательна, минимум КРИТИЧЕСКОЕ.
- Перечислять каждый извещатель без типа — один класс
  `aps_device_spec_incomplete` (collapse).
