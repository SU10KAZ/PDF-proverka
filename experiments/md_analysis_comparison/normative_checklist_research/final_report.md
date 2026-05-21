# Normative Checklist Research — Final Report

**Дата:** 2026-05-21
**Scope:** research-only. Никаких изменений в backend, в чек-листах, в
production / staging, в pipeline. Все правки — proposed-changes для
последующего human-review.

---

## Summary

Проверены **195 checklist items** из 8 дисциплинарных чек-листов
`backend/app/data/discipline_checklists/{AR,EOM,KJ,KM,MULTI,OV,SS,VK}.md`
против действующей нормативной базы РФ (ПП РФ 87, ГОСТ Р 21.101-2020,
дисциплинарные СП и ГОСТ СПДС).

Главные результаты:

| Метрика | Значение |
|---|---:|
| Всего items проверено | 195 |
| Items с issue в нормативной ссылке | **18** (устаревшие/спорные/требующие верификации) |
| Items, которые НЕЛЬЗЯ использовать как `missing_finding` | **46** (24%) |
| Items, требующие conditional gate (stage / object_signal / equipment) | **71** (36%) |
| Items, готовых к использованию как unconditional `mandatory` | **71** (36%) |
| Items с recommended-severity (низкий приоритет) | **53** (27%) |

**Главный вывод:** в текущем виде запускать completeness_runner **нельзя**.
Из 195 items только ~71 действительно безопасны для unconditional
mandatory-detection; остальные требуют либо downgrade, либо stage-gate,
либо отметки `cannot_be_reported_as_missing=true`. Без этих правок FP-rate
будет высоким, особенно на `full_rd` (РД-марках) и `specification_only`.

---

## Ответы на 10 вопросов /goal

### 1. Какие checklist-пункты реально mandatory

**71 items.** Это дисциплинарно-универсальные требования СПДС-ГОСТов,
которые применимы независимо от условий объекта:

- **AR:** поэтажные планы, разрезы, фасады, экспликация, ведомость отделки,
  спецификация проёмов, эвакуационные пути (КРИТИЧЕСКОЕ).
- **EOM:** кабельный журнал, спецификация, однолинейная схема ВРУ/ГРЩ,
  схемы ЩР/ЩО, план этажей с трассировкой, сечение и марка кабеля, I_ном
  и x-ка АВ.
- **KJ:** общие данные (классы бетона/арматуры), схема расположения,
  расчётная схема, сбор нагрузок, I ГПС, спецификация арматуры, ведомость
  расхода стали, узлы армирования, класс/диаметр/шаг/защитный слой.
- **KM:** общие данные (классы стали), расчётная схема с l_ef, подбор
  сечений, II ГПС, узлы соединений, ведомость отправочных марок,
  спецификация проката, сечение/класс стали, болты, швы.
- **OV:** «Вентиляция», расчёт воздухообмена, теплопотери, потери
  давления, кратность, спецификация оборудования, аксонометрии, L/P/P_уст
  установки.
- **VK:** В1, К1+К2, Q_расч, гидравлический расчёт, расчёт стоков,
  спецификация оборудования и сантехники, аксонометрии/планы.
- **SS:** спецификация, кабельный журнал, структурная схема,
  расположение на планах, тип кабеля, параметры устройств.
- **MULTI:** только 3 item'а с условием доступности `tz_vs_rd` или
  `audit_comparison` — единицы измерения, явные отклонения от ТЗ,
  ведомость замечаний.

Полный список — в `matrix/completeness_requirements_matrix.csv` по
`normative_status=mandatory`.

### 2. Какие должны быть conditional

**71 items.** Группы условий:

- **Stage gate (ПД vs РД):** AR-01, AR-05, AR-09, EOM-01, EOM-04, EOM-06,
  KJ-05-08, KM-05-08, OV-01, OV-05, VK-01, VK-05, SS-01, SS-05-07.
  Применимы целиком только в ПД-томе; в РД-марке либо drop, либо понижение
  severity.
- **Document_type gate:** AR-05 (ПБ — отдельный том ПД), AR-19 (ОДИ),
  все MULTI items (кроме применимых к tz_vs_rd / audit_comparison).
- **Object_signal gate:** AR-08 (жилой), AR-21 (жилой/социальный), AR-13
  (кровля), AR-22 (витражи), AR-23 (экспл. кровля), EOM-20 (двигатели),
  EOM-21/23 (I категория), EOM-22 (молниезащита), EOM-24 (розетки/ванные),
  KJ-23 (≥ 75 м), KJ-24 (сейсмо), KJ-25 (подз. часть), KM-02/03/23/24/25,
  OV-04 (МКД ≥ 28 м), OV-11/13/23/24, VK-03/13/14/22/23/24/25, SS-02/03/05/06/07/13/23/24/25.

Полный список в matrix по `normative_status=conditionally_mandatory`.

### 3. Какие checklist-пункты нужно удалить

**1 item на удаление** + кандидаты на удаление по причине дублирования
с anti-pattern:

| Item | Причина |
|---|---|
| **OV-25** Вентиляция канализационных стояков | Дубль с разделом VK; в OV не должно быть |

Кандидаты на удаление в зависимости от стратегии (если предпочтительно
полностью убрать вместо `cannot_be_reported_as_missing=true`):

- Все 9 MULTI cross-section items (MULTI-05..MULTI-13) — невозможно
  проверить на одном MD.
- ~15 coordination items по всем дисциплинам.

### 4. Какие checklist-пункты нужно downgrade

Severity-downgrade (см. `recommendations/checklist_update_plan.md` §2):

- 13 items: ПРОВЕРИТЬ_ПО_СМЕЖНЫМ → РЕКОМЕНДАТЕЛЬНОЕ или drop (formal
  attributes, coordination)
- 11 items: КРИТИЧЕСКОЕ → ПРОВЕРИТЬ_ПО_СМЕЖНЫМ при отсутствии stage
  info (электробаланс, расчёт воздухообмена, Q_расч в РД-марке)
- 2 items: ЭКСПЛУАТАЦИОННОЕ → РЕКОМЕНДАТЕЛЬНОЕ (штамп, формальные)

**Из обратного — 1 upgrade:** AR-14 (эвакуация на планах) — текущая
ЭКСПЛУАТАЦИОННОЕ → **КРИТИЧЕСКОЕ** для МКД и общественных (life safety).

### 5. Какие checklist-пункты нельзя использовать для missing findings

**46 items с `can_be_reported_as_missing=false`** (24% от всех 195).

Категории:

| Категория | Items | Причина |
|---|---|---|
| Cross-section consistency | 9 (MULTI-05..MULTI-13) | Нужен другой раздел |
| Coordination artifacts | 15 (AR-15, AR-16, EOM-16-18, KJ-17-18, KM-17-19, OV-17-20, VK-16-19, SS-16-19) | Часто задание смежнику отдельным письмом |
| Programs of testing / commissioning | 6 (AR-17, EOM-19, KJ-20, KJ-22, KM-04, KM-20, OV-21, VK-20, SS-20, MULTI-18) | Часто отдельный документ (ПНР, ППР) |
| Specifications & sertificates of compliance | 1 (KM-22) | Атрибут исполнительной документации |
| Project-level summary attributes | 4 (MULTI-01, MULTI-17, MULTI-18, MULTI-19) | Атрибуты тома 1 ПЗ |
| Recommended interfaces / соглашения | 1 (SS-21) | Атрибут спец. ТЗ |
| Stage-level attributes when stage unclear | ~10 | Понижение в conditional |

Их не убираем из чек-листа (lens должен их видеть для context), но
runner НЕ должен генерировать finding'и с `problem_class=missing_*` на
их основе. Можно использовать для информационных fields или для
diagnostics.

### 6. Какие missing findings допустимы

**Допустимые missing findings** — те, что:

1. Имеют `can_be_reported_as_missing=true` в matrix
2. Их `applicable_document_types` включает текущий document_type
3. Их `applicable_stages` совместим со стадией документа (если известна)
4. Их `applicability_conditions` выполнено (есть сигнал в MD)
5. MD не содержит явной декларации отсутствия («не разрабатывается»)

Примеры допустимых:

- **AR/full_rd:** «Спецификация заполнения оконных и дверных проёмов
  отсутствует» (AR-10) — высокая confidence, mandatory СПДС.
- **EOM/full_rd:** «Кабельный журнал отсутствует» (EOM-08) — mandatory.
- **EOM/specification_only:** «Не указано сечение кабеля на позиции X»
  (EOM-13) — mandatory параметр.
- **OV/full_rd при МКД ≥ 28 м:** «Раздел противодымной вентиляции
  отсутствует» (OV-04) — conditionally_mandatory с выполненным условием.
- **VK/full_rd при наличии насосов:** «Не указаны параметры насоса H/Q»
  (VK-13) — conditionally_mandatory.
- **SS/full_rd при обязательной АПС:** «АПС отсутствует» (SS-02) —
  КРИТИЧЕСКОЕ.
- **MULTI/tz_vs_rd:** «Требование ТЗ X не имеет соответствующего решения
  в РД» (MULTI-02) — mandatory для tz_vs_rd.

### 7. Какие missing findings недопустимы

**Недопустимые missing findings:**

1. **Cross-section на одном MD:** «параметры в ЭОМ не совпадают с ОВ» —
   мы видим только один MD.
2. **Items с `can_be_reported_as_missing=false`** (46 items).
3. **Stage-mismatch:** «отсутствует ПЗ» в РД-марке — ПЗ может быть в томе ПД.
4. **Document_type-mismatch:**
   - «отсутствует кабельный журнал» в audit_comparison
   - «отсутствует расчёт» в specification_only
   - «отсутствует ПЗ» в specification_only
5. **Условные без сигнала:**
   - «отсутствует расчёт инсоляции» в нежилом объекте
   - «отсутствует молниезащита» в низком здании
   - «отсутствует АВР» без I категории
6. **Coordination as missing:** «отсутствует координация с разделом X»
   как самостоятельное замечание.
7. **Phantom-clause:** «СП X, п. Y.Y.Y» с неподтверждённой подпунктовкой
   или ссылкой на ГОСТ Р 21.1101-2013 (заменён).
8. **Speculative:** «возможно, спецификация неполна» / «следует
   уточнить класс» без указания конкретной строки.

### 8. Где высокий риск FP

Места с наивысшим FP-риском при запуске completeness_runner в текущем
состоянии:

| Зона | FP-vector | Mitigation |
|---|---|---|
| **РД-марки (большинство production-MD)** | items, считающие, что ПЗ/ТЭП/расчёты обязательны в каждой марке | Stage gate: для РД-марок понижать ПД-only items до ПРОВЕРИТЬ_ПО_СМЕЖНЫМ или drop |
| **specification_only** | requirement of ПЗ, расчётов, схем | Document_type gate: для specification_only оставить только spec-атрибуты |
| **audit_comparison** | requirement полного состава марки | Document_type gate: только формальные атрибуты |
| **MULTI cross-section** | 9 items проверяют согласованность двух разделов, которых одновременно нет | Mark `cannot_be_reported_as_missing=true` |
| **Coordination items** | 15 items флагают отсутствие координационных артефактов, которые живут вне марки | Mark `cannot_be_reported_as_missing=true` |
| **Conditional без сигнала** | расчёт инсоляции в нежилом, АВР без I категории, молниезащита в низком здании, противодымная в малом МКД, расчёт ВПВ без ВПВ, СОТС/CO без подземной автостоянки, … | Object_signal gates в prompt (см. `prompt_rules_update.md` §5) |
| **Stage-неопределённость** | если detector не различает ПД vs РД, баланс «строгий» runner будет flage все ПД-атрибуты | Понижать severity всех ПД-only до ПРОВЕРИТЬ_ПО_СМЕЖНЫМ |
| **Phantom-clause** | LLM может галлюцинировать номера пунктов (видно в текущих ссылках типа «п. 6.4», «п. 8.5», «п. 8.1.46») | Prompt-rule: запрет на неподтверждённые подпункты + параллельные ссылки на ПУЭ-7 |
| **Устаревшие ГОСТы** | Сейчас в backend-checklist 7 ссылок на ГОСТ Р 21.1101-2013 (заменён) | Normative refresh — Phase A правок |

### 9. Какие правила должен соблюдать completeness_runner

Минимальный список правил, **обязательных** перед запуском lens'а
(прежде чем запускать sub-task completeness_runner):

1. **Загружает checklist согласно discipline + document_type.** На
   `audit_comparison` — только MULTI (избранные items), на
   `specification_only` — только spec-атрибуты дисциплины.
2. **Применяет stage gate.** Если `stage` в `project_info.json` не задан
   или неоднозначен — все «ПД-only» items понижает severity до
   ПРОВЕРИТЬ_ПО_СМЕЖНЫМ с явной пометкой «стадия не определена».
3. **Применяет object_signal gate.** Для каждого conditional item — ищет
   сигнал в MD (по списку из `prompt_rules_update.md` §5). Без сигнала
   item исключается из generation pool.
4. **Не генерирует findings на items с `can_be_reported_as_missing=false`.**
   Эти 46 items могут быть в context, но не в output.
5. **Применяет cap.** `STAGE01_COMPLETENESS_MAX_FINDINGS` (10 для full_rd,
   6 для full_rd с STAGE01_COMPLETENESS_MAX_FINDINGS_FULL_RD) — жёсткое
   ограничение.
6. **Запрещает phantom-clause.** Не цитировать подпункты без верификации;
   обязательная параллель СП к ПУЭ-7.
7. **Запрещает coordination-as-missing** и **cross-section на одном MD**.
8. **Fail-open.** На любой ошибке lens'а — fallback на A0 (existing
   prompt), без catastrophic finding'а.
9. **Использует normative refresh.** При генерации finding ссылается
   только на действующие нормы (`ГОСТ Р 21.101-2020`, не `21.1101-2013`).
10. **Не дублирует с другими lens'ами.** Numeric errors, normative
    correctness, contradictions, cross-discipline — это другие lens'ы.
    Completeness lens отвечает ТОЛЬКО за наличие.

### 10. Какие prompt-ограничения обязательны

См. `recommendations/prompt_rules_update.md` целиком. Минимальный
обязательный набор для production lens prompt:

1. **Stage gate block** (§1) — стадия документа.
2. **Document_type gate block** (§2) — применимость по типу.
3. **Coordination запрет** (§3) — не флагать координацию как missing.
4. **Cross-section запрет** (§4) — не флагать cross-section на одном MD.
5. **Object_signal table** (§5) — таблица «item → требуемый сигнал в MD».
6. **Severity calibration update** (§6) — drop при отсутствии сигнала.
7. **Phantom-clause guard** (§7) — запрет на неподтверждённые подпункты.
8. **ПУЭ-7 обязательная параллель** (§8) — параллельная ссылка на СП.
9. **Anti-pattern enrichment** — дополнения к anti-pattern блоку.

Без этих ограничений lens prompt не имеет права быть включенным даже
в shadow-mode.

---

## Основные риски

1. **FP-spike на РД-марках.** Большинство production-проектов в
   `projects/` — это РД-марки, не ПД-тома. Без stage gate ожидаем
   значительный FP-rate на ПД-атрибутах (ПЗ, ТЭП, общие данные, общий
   электробаланс).
2. **Cross-section in single-MD.** 9 MULTI items, как написано в исходном
   чек-листе, **не должны** быть основой для missing-findings.
3. **Outdated GOST.** 7 items ссылаются на отменённый `ГОСТ Р 21.1101-2013` —
   это не приведёт к catastrophic failure, но снизит качество цитирования.
4. **Phantom clauses.** Несколько items указывают подпункты («п. 6.4»,
   «п. 8.5», «п. 8.1.46») без явной верификации — LLM в lens может
   усилить эту проблему, придумав ещё больше подпунктов.
5. **ПУЭ-7 в одиночку.** 5 items без параллельной ссылки на СП — нарушение
   собственной политики проекта (CLAUDE.md).
6. **Coordination as missing.** 15 coordination items — частая категория
   FP в legacy реализациях. Mitigation выше.
7. **Specification_only без guard'а.** Если detector неверно классифицирует
   тип, и lens начнёт требовать расчёты в spec'е — мгновенный FP-spike.
8. **Object_signal heuristic.** Эвристики «текст содержит слово X» хрупкие.
   Нужна валидация на 20+ реальных MD до production.

---

## Checklist items requiring human engineer validation

Категории, где **обязательно** нужна валидация человеком до того, как
lens пойдёт в shadow:

### Подтвердить точные номера пунктов в действующих редакциях норм

| Item | Текущая ссылка | Кто валидирует |
|---|---|---|
| EOM-05 | СП 256.1325800.2016, п. 6.4 | Электрик |
| EOM-07 | СП 256.1325800.2016, п. 8.5 | Электрик |
| KJ-08 | СП 63.13330.2018, п. 8.1.46 | Конструктор-ЖБ |
| KJ-16 | СП 63.13330.2018, п. 10.3.5 | Конструктор-ЖБ |
| AR-08 | СП 50.13330.2012 | Архитектор + санитарный гигиенист |
| VK-20 | СанПиН 2.1.4.1074-01 | ВК-инженер |

### Подтвердить пороги для условных items

| Item | Условие | Кто валидирует |
|---|---|---|
| OV-04 | «МКД ≥ 28 м» — для противодымной вентиляции | Пожарный инженер |
| SS-02 | Перечень обязательных к АПС объектов | Пожарный инженер |
| EOM-22 | Категории по СО-153-34.21.122-2003 | Электрик |
| KJ-23 | «≥ 75 м» для пульсаций | Конструктор |
| KJ-24 | Сейсмо-районы | Конструктор |

### Подтвердить, что coordination items не флагаются как missing

15 items (см. вопрос 5). Каждая дисциплина — отдельный инженер.

### Stage-detector

Текущий `document_type_detector` НЕ различает ПД vs РД. **Требуется
human-engineer review** для расширения detector'а на стадии — либо
руками заполнять `project_info.stage`, либо детектор по шифру.

---

## Рекомендации перед completeness_runner

Перед тем как создавать `completeness_runner` и подключать его к Stage 01,
нужно сделать **в порядке приоритета**:

### P0 — обязательно (без этого lens нельзя запускать даже в shadow)

1. **Применить Phase A правок** из `checklist_update_plan.md`: normative
   refresh (заменить `ГОСТ Р 21.1101-2013`).
2. **Применить Phase C правок:** маркировать 46 items как
   `cannot_be_reported_as_missing` через формат тагов в backend
   чек-листах (например, `[..., severity=..., applies=..., cannot_report=true]`).
3. **Обновить lens prompt** добавками из `prompt_rules_update.md` §1-4
   (stage gate, document_type gate, coordination запрет, cross-section
   запрет).
4. **Расширить `project_info.json` schema** для поля `stage` (П / Р / КМД)
   и/или детектор по шифру.

### P1 — настоятельно рекомендуется до shadow-mode

5. **Phase B правок:** добавить новые anti-patterns в каждый чек-лист.
6. **Phase D правок:** добавить explicit object_signal gates в prompt.
7. **Phase E:** удалить OV-25 (дубль с VK).
8. **Подтвердить точные номера пунктов** для 6+ items через
   `mcp__norms__get_paragraph_json` или WebSearch.

### P2 — желательно до production-rollout

9. **Cross-MD pipeline** для MULTI items (отдельный sub-task).
10. **Validation на 20+ real MD** с human grading FP-rate.
11. **Согласовать с экспертами по дисциплинам** (электрик, конструктор,
    ВК, ОВ, СС, архитектор, пожарный) — sign-off по `discipline_reports/`.

### Не делать (out of scope)

- Не пытаться auto-apply правки чек-листов из этого research'а — это
  human-review артефакт.
- Не запускать completeness_runner без P0-правок.
- Не убирать существующие чек-листы — структура нужна как context для
  lens'а.

---

## Что лежит в этой папке

```
normative_checklist_research/
├── README.md                                       # навигация
├── sources/
│   ├── pp_rf_87.md                                 # ПП РФ 87 — ПД
│   ├── gost_r_21_101_2020.md                       # СПДС
│   ├── disciplinary_norms.md                       # СП по дисциплинам
│   ├── stages_pd_rd.md                             # ПД vs РД vs КМД
│   └── document_type_normative_mapping.md          # detector → нормы
├── matrix/
│   ├── _data.py                                    # source of truth (195 items)
│   ├── build_matrix.py                             # генератор
│   ├── completeness_requirements_matrix.json
│   └── completeness_requirements_matrix.csv
├── discipline_reports/
│   ├── AR.md, KJ.md, KM.md, EOM.md, OV.md, VK.md, SS.md, MULTI.md
├── recommendations/
│   ├── checklist_update_plan.md
│   └── prompt_rules_update.md
└── final_report.md                                 # этот файл
```
