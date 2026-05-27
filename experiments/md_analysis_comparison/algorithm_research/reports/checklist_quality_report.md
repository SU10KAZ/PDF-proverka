# Checklist Quality Report

**Date:** 2026-05-20
**Scope:** Per-discipline review of `prompt_optimization/checklists/*.md`.
**Method:** Static analysis (no LLM) + mapping to dataset trap/recommendation
patterns + comparison with `document_type` routing impact.

## Files reviewed

```
algorithm_research/prompt_optimization/checklists/
  AR.md          — Архитектурные решения
  EOM.md         — Электроснабжение
  KJ.md          — Конструкции железобетонные
  KM.md          — Конструкции металлические   (NEW, добавлен в этом этапе)
  OV.md          — Отопление, вентиляция, кондиционирование
  SS.md          — Слаботочные системы / связь
  VK.md          — Внутреннее водоснабжение и канализация
  MULTI.md       — Междисциплинарный
  cross_discipline.md  — fallback
```

Все checklists используют tier-схему **M / R / O** (Mandatory / Recommended /
Optional). Pattern `## Anti-pattern (don't flag)` присутствует во всех.

## Per-discipline review

### EOM
**Сильные стороны:**
- Чёткое разделение на «обязательные секции», «параметры кабеля/автомата»,
  «cross-discipline ссылки» и «anti-pattern».
- Anti-pattern явно перечисляет производителей / марки автоматов как
  НЕ-нарушение — снижает FP по trap'у вроде «IEK ВА47-29».

**Риски FP без document_type:**
- На audit_comparison (cross_01) checklist мог выписать «отсутствует
  однолинейная схема», «отсутствует кабельный журнал». **Document_type
  routing v2 это устраняет** — см. валидацию на cross_01.

**Рекомендации:**
- Добавить tier-маркер в anti-pattern (`O` для маркетинговой информации).
- Добавить условный пункт для специализированных систем (АПС, СОУЭ) —
  они должны идти со ссылкой на смежный раздел.

### OV
**Сильные стороны:**
- Cross-discipline блок: координация с ЭОМ по питанию вентиляторов /
  АПС по управлению при пожаре.
- Anti-pattern явный.

**Риски FP без document_type:**
- На specification_only (ov_03_heating_calc) checklist мог выписать
  «отсутствует принципиальная схема» / «нет тепловых узлов». Routing v2
  должен это блокировать.

**Рекомендации:**
- Уточнить пункт «вентиляция канализационных стояков» — он на стыке OV/VK,
  легко даёт двойной счёт.

### KM (NEW)
Создан в этом этапе под km_01/km_02/km_03 кейсы. Структура зеркалит EOM
(M/R/O + параметры + cross + anti-pattern).

**Что включено:**
- Расчётная схема + расчётные длины (M)
- Подбор сечений с проверкой устойчивости центрально-сжатых (M)
- Расчёт узлов и соединений (болтовых / сварных) (M)
- Ведомость элементов / отправочных марок (M)
- Класс стали и марка проката (M)
- γ_c / γ_n / γ_f явно (R)
- Антикоррозионная защита (R)
- Огнезащита (R)

**Anti-pattern:**
- Производитель профилей / электродов само по себе не нарушение.
- Применение Э42/Э50А — стандартные марки.
- Отсутствие явного γ_n при стандартных нагрузках — не критическое.

### AR
**Сильные стороны:**
- Acceptable scope для жилья (эвакуация, пожарка, инсоляция).

**Риски FP без document_type:**
- На specification_only (ar_03_balcony_glazing) мог сработать пункт о
  пояснительной записке. Routing должен глушить.

### KJ
**Сильные стороны:**
- Cross-discipline блок (нагрузки от ОВ/ВК, опирание от КМ).

**Риски:**
- Может неверно классифицировать «расчёт на продавливание без учёта момента»
  как completeness вместо calculations — это calculations lens / current_method
  должны ловить.

### SS
**Сильные стороны:**
- Cross-discipline ссылки на ЭОМ (питание), АПС (взаимодействие).

**Риски:**
- Перекрытие с АПС/СОУЭ — checklist должен явно указывать, что АПС / СОУЭ
  отдельные подразделы.

### VK
**Сильные стороны:**
- Cross-discipline с ОВ (узлы ИТП), АПС (внутренний противопожарный).

**Риски:**
- На tz_vs_rd (vk_03) пункты про «отсутствие циркуляции ГВС» — это
  completeness, но может попасть в нарушение нормы вместо несоответствия ТЗ.

### MULTI / cross_discipline (fallback)
Используется как fallback для дисциплин без специфического checklist.
Тестов на нём не сильно — рекомендуется не доводить до этого fallback'а
в production.

## Пункты, потенциально провоцирующие FP

Cross-discipline impact analysis:

| Checklist item | Discipline | Document_type, где может выстрелить | Routing fix |
|---|---|---|---|
| «Однолинейная схема ВРУ» | EOM | audit_comparison, specification_only | v2: запрещено |
| «Пояснительная записка» | AR | specification_only | v2: запрещено |
| «Кабельный журнал» | EOM, SS | specification_only | v2: запрещено |
| «Расчёт по II гр. ПС (трещины, прогиб)» | KJ | specification_only | v2: только параметр-уровень |
| «Ведомость отправочных марок» | KM | specification_only | v2: запрещено |
| «Аксонометрические схемы» | OV, VK | specification_only | v2: запрещено |

После внедрения document_type routing v2 (см. completeness.md v2 / v1) ВСЕ
эти пункты автоматически глушатся вне `full_rd`.

## Пункты, реально полезные (cross-проверка на кейсах)

- **EOM**: «Категория надёжности электроснабжения» — ловит eom_02
  (молниезащита без указания категории) и ss_02 (АПС от II категории).
- **EOM cross**: «Перечень электроприёмников ОВ/ВК» — ловит cross_01.
- **OV cross**: «Координация с АПС» — ловит ov_02 (smoke protection без
  взаимодействия с АПС).
- **VK cross**: «Узлы ИТП» — может ловить vk_03 (рециркуляция).
- **KM**: «Устойчивость центрально-сжатых» — ловит km_01 (truss без
  проверки устойчивости).

## Пункты, которые надо сделать conditional

| Пункт | Условие | Текущее tier | Желаемое |
|---|---|---|---|
| «Молниезащита» (EOM) | Только если в МД указана категория / есть отдельно стоящий объект | R | conditional R |
| «Перечень всех электроприёмников» (EOM) | Только full_rd, не для audit_comparison | M | conditional M |
| «Аксонометрические схемы» (OV/VK) | Только full_rd | M | conditional M |
| «Антикоррозионная защита» (KM) | Только если объект с агрессивной средой | R | conditional R |

Conditional realization сейчас сделана через document_type routing (M ставится
только в full_rd). Дополнительно условия по типу объекта пока не реализованы.

## Что зависит от document_type

- **full_rd** — checklist полностью применим; absence = M-class finding.
- **audit_comparison** — применим только к системам/интерфейсам, заявленным
  в документе как предмет сравнения. Не флагать отсутствие unrelated секций.
- **tz_vs_rd** — применим только к пунктам, заявленным в ТЗ И отвеченным в
  РД. Не «изобретать» gaps из пунктов, ТЗ не упоминает.
- **specification_only** — применим только parameter-level часть (например,
  таблица «Параметры, которые ДОЛЖНЫ быть на каждом кабеле/автомате»).

## Acceptance / Action Items

| Item | Action | Owner | Status |
|---|---|---|---|
| KM checklist создан | added in this stage | research stand | done |
| document_type routing в v1 + v2 completeness prompts | done | research stand | done |
| Conditional checklist для категории объекта | планируется в Phase 2 | TBD | deferred |
| Inter-rater validation checklist'ов с инженерами проектного отдела | требуется до production | TBD | pending |
| Anti-pattern блок везде | done | research stand | done |

## Открытые риски

1. **specification_only** только что добавлен в routing — не проверен
   end-to-end на ar_03 / ov_03 / km_02 (требует A1-v2 прогона на этих кейсах).
2. **tz_vs_rd** — vk_03 кейс свежий, не валидирован prompt routing полностью.
3. **cross_discipline** в checklist EOM может вызвать FP-конфликт с
   `cross_discipline` lens — рекомендуется явно разводить «completeness
   фактов из смежного раздела» vs «cross-discipline incongruence» в
   prompt'е, а не дублировать.
