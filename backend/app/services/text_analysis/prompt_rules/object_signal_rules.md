# Object signal rules — completeness lens prompt block

**Назначение:** условные items (`conditionally_mandatory`) генерируют
missing findings ТОЛЬКО при наличии соответствующего сигнала в тексте MD.

«Сигнал» — это явное упоминание характеристики объекта в MD: тип здания,
наличие системы, категория надёжности, высота, сейсмика, и т.д.

Если сигнал отсутствует — finding **drop**.
Если сигнал есть и пункт отсутствует — finding по базовой severity дисциплины.
Если сигнал есть и пункт частично есть — severity **РЕКОМЕНДАТЕЛЬНОЕ** +
формулировка «дополнить».

## Таблица обязательных сигналов

### AR
- AR-08 «Расчёт инсоляции» → сигнал: `residential_building`
  («жилой», «квартиры», «МКД»).
- AR-13 «Узлы кровли» → `roof_operated` («эксплуатируемая кровля», «выход
  на кровлю»).
- AR-19 «Решение по МГН» → `public_building` ИЛИ `residential_building`.
- AR-21 «Расчёт КЕО» → `residential_building` ИЛИ `public_building`
  (детсад, школа).
- AR-22 «Узлы крепления витражей» → `facade_present` («витраж», «навесной
  фасад», «СПФ»).
- AR-23 «План эксплуатируемой кровли» → `roof_operated`.

### EOM
- EOM-17 «Координация АПС-ОВ» → `fire_system_present` ИЛИ
  `smoke_ventilation_required`.
- EOM-20 «Компенсация реактивной мощности» → `motors_present` («двигатели»,
  «cos φ», «промышленные нагрузки»).
- EOM-21 «АВР» → `category_1_power` («I категория», «I-особая», явное
  «АВР»).
- EOM-22 «Молниезащита» → `lightning_protection_required` («молниезащита»,
  «СО-153», «категория молниезащиты»).
- EOM-23 «Резервное питание» → `category_1_power`.
- EOM-24 «УЗО для розеток/ванных» → `wet_zone_present` («ванная»,
  «санузел», «розеточная группа»).
- EOM-25 «Электропитание лифтов» → `elevators_present`.

### KJ
- KJ-23 «Ветровые пульсации» → `high_rise` («≥ 75 м», «высотное»).
- KJ-24 «Сейсмика» → `seismic_region` («сейсмо», «балл по ОСР»).
- KJ-25 «Гидроизоляция подземной части» → `underground_structure`
  («подвал», «подземная часть», «цокольный этаж»).

### KM
- KM-03 «Огнезащита» → `fire_system_present`.
- KM-24 «Динамические расчёты» → `high_rise`.

### OV
- OV-04 «Противодымная вентиляция» → `high_rise` AND
  `smoke_ventilation_required` (для МКД ≥ 28 м).
- OV-11 «Ведомость воздуховодов» → `ventilation_system_present`.
- OV-13 «Узлы прохода воздуховодов с ОЗК» → `fire_system_present` AND
  `ventilation_system_present`.
- OV-18 «Координация с АПС» → `fire_system_present`.
- OV-23 «Огнестойкость воздуховодов» → `ventilation_system_present`.
- OV-24 «АХЗ/ИТП» → `automation_present`.

### VK
- VK-12 «Узлы прохода с противопожарными муфтами» → `fire_system_present`.
- VK-13/14 «Параметры насосов» → `pumps_present`.
- VK-17 «Координация с АПС» → `fire_system_present`.
- VK-22 «Повысительная насосная» → `pumps_present`.
- VK-23 «Бак ГВС» → `pumps_present` (использует pumps как proxy для
  «накопительная схема»).
- VK-24 «Расчёт ВПВ» → `fire_system_present`.

### SS
- SS-02 «АПС» → `fire_system_present`.
- SS-03 «СОУЭ» → `fire_system_present`.
- SS-05 «Расчёт АКБ/ИБП» → `fire_system_present`.
- SS-06 «Расчёт извещателей АПС» → `fire_system_present`.
- SS-07 «Акустический расчёт СОУЭ» → `fire_system_present`.
- SS-13 «Узлы прохода кабелей» → `fire_system_present`.
- SS-17 «Координация АПС-ОВ» → `fire_system_present` AND
  `smoke_ventilation_required`.
- SS-18 «Координация АПС-лифты» → `elevators_present`.
- SS-19 «Координация АПС-ВК» → `fire_system_present`.
- SS-23 «Домофония/СКУД» → `residential_building` AND `automation_present`.
- SS-24 «СОУЭ 3-5 тип» → `public_building`.
- SS-25 «СОТС/CO» → `underground_structure`.

## Source

`experiments/md_analysis_comparison/normative_checklist_research/final_report.md`,
вопрос 8 «conditional без сигнала»; `recommendations/prompt_rules_update.md` §5.
Машино-читаемое представление — поле `object_signals` в
`backend/app/data/discipline_checklists_metadata/<DISC>.json`.
