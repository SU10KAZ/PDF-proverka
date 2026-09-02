# Hierarchical Function Lineage Matching — ИОС 2.1

## Verdict

Hierarchical matching conceptually устраняет структурные ошибки ИОС 2.1, но не сам по себе решает расхождение с saved engineer RIGHT 7/8/9. Эти pages — authoritative documentary links на журнал изменений, а не функциональные аналоги. Нужны одновременно:

1. явный relation contract (`DOCUMENT_LINK` отдельно от `FUNCTIONAL_ANALOGUE`);
2. Stage A Function Lineage Map;
3. Stage B Sheet Map только внутри установленной lineage;
4. function-level, а не sheet-global conflict checking.

Поэтому итоговый выбор — **C: Function Passport/prompt + hierarchical matcher**.

## Почему текущая архитектура ломается

Текущий pipeline фактически решает:

```text
LEFT physical sheet
  → bounded page/group candidates
  → local option
  → global sheet map
  → reject any overlapping RIGHT page
```

Такая модель предполагает, что физический RIGHT sheet — атомарный и эксклюзивный ресурс. В ИОС 2.1 это неверно:

- один старый sheet может содержать несколько functions;
- одна старая function может быть распределена по нескольким новым sheets;
- один новый sheet может одновременно продолжать functions из нескольких старых sheets;
- change-register page может быть authoritative document link, не являясь функциональным аналогом.

Конкретный контрпример:

```text
LEFT 16 corpus-1 combined risers  → RIGHT 26 domestic/hot + RIGHT 28 fire
LEFT 20 old common pump sheet     → RIGHT 26 domestic boost + RIGHT 28 fire boost + RIGHT 29 metering
LEFT 21 old metering detail       → RIGHT 29 metering detail
```

На physical-page уровне это пересечение. На function-lineage уровне это три совместимых набора компонентов.

## Предлагаемая архитектура

### Stage 0: relation namespace

До functional matching нужно определить тип запроса:

- `DOCUMENT_LINK`: authoritative linkage, change-register correspondence, review/navigation relation;
- `FUNCTIONAL_ANALOGUE`: продолжение инженерной функции между версиями;
- при необходимости `BOTH`, но с двумя раздельными outputs и evidence policies.

Saved engineer mappings 17→7, 18→8, 19→9, 20→10 относятся к `DOCUMENT_LINK`. Функциональные relations 17→27, 18→24, 19→[25,30], 20→[26,28,29] относятся к `FUNCTIONAL_ANALOGUE`. Смешивать их в одну accuracy metric нельзя.

### Stage A: FUNCTION LINEAGE MAP

Единица решения — normalized engineering function, не physical sheet.

Input passport должен содержать минимум:

- `function_class`;
- `serviced_object` / corpus;
- `serviced_zone` / floors;
- systems;
- consumers;
- equipment roles;
- upstream/downstream;
- stable entities;
- topology;
- source sheet refs;
- evidence refs и explicit contradictions.

Output Stage A:

```json
{
  "lineage_id": "...",
  "old_function_ids": ["..."],
  "new_function_ids": ["..."],
  "relation": "ONE_TO_ONE | MERGED | SPLIT | DISTRIBUTED",
  "confidence": "PROVEN | AMBIGUOUS | ABSENT",
  "evidence_refs": ["..."],
  "contradictions": []
}
```

Stage A допускает many-to-many graph. Capacity key — function identity/component, а не page number.

### Stage B: SHEET MAP WITHIN LINEAGE

Для каждой proven lineage открываются только sheets, содержащие функции этой lineage. Затем определяется physical presentation:

- old sheet → new sheet;
- old combined sheet → несколько new sheets;
- несколько old sheets → consolidated new sheet;
- один new sheet используется несколькими lineages, если его функции различны.

Допустимый reuse:

```text
(RIGHT 26, CORPUS_1_DOMESTIC_HOT)
(RIGHT 26, COMMON_DOMESTIC_PRESSURE_BOOST)
```

Недопустимый conflict:

```text
один и тот же atomic new function component
одновременно назначен двум несовместимым old lineages
без declared merge/split relation
```

## ИОС 2.1 simulation

Research-only simulation, без materialization:

| Function lineage | LEFT | RIGHT | Result |
|---|---:|---:|---|
| CORPUS_1_DOMESTIC_HOT | 16 | 26 | compatible |
| CORPUS_1_FIRE_RISERS | 16 | 28 | compatible |
| COMMON_DOMESTIC_PRESSURE_BOOST | 20 | 26 | compatible reuse of RIGHT 26 |
| COMMON_FIRE_PRESSURE_BOOST | 20 | 28 | compatible reuse of RIGHT 28 |
| COMMON_INCOMING_METERING | 20,21 | 29 | overview/detail merge on one lineage |
| CORPUS_2_RISERS | 17 | 27 | direct functional continuation |
| CORPUS_3_RISERS | 18 | 24 | continuation with scope expansion to 3.1 |
| CORPUS_4_DOMESTIC_HOT | 19 | 25 | non-fire component |
| CORPUS_4_FIRE | 19 | 30 | separated fire component |

После Stage A LEFT 20 получает target [26,28,29], а LEFT 16 и LEFT 21 не нужно удалять. RIGHT reuse объяснён различными lineages.

## Какие ошибки устраняются

| Case | Hierarchy effect |
|---|---|
| 17 | Стабильно даёт functional RIGHT 27. Не должна давать documentary RIGHT 7 без отдельного `DOCUMENT_LINK` objective. |
| 18 | Стабильно даёт functional RIGHT 24. RIGHT 8 остаётся отдельной documentary relation. |
| 19 | Убирает ложный выбор между sheet 25 и group [25,30]: сначала фиксирует domestic/hot и fire lineages, затем получает distributed presentation. |
| 20 | Устраняет ложный RIGHT-page conflict с LEFT 16/21 и допускает [26,28,29]. |

Итого: hierarchy conceptually устраняет LEFT 19/20 functional errors. Для 17/18 она подтверждает, что текущий AI выбирает правильный functional analogue, а несовпадение с engineer mapping вызвано relation-contract mismatch.

## Что изменить в Function Passport

Фактическая selector projection сейчас удаляет из Function Passport восемь ключевых полей и заменяет их повторяющимися `fragment_text`. Это создаёт две проблемы:

- модель вынуждена связывать function с object/zone через отдельный sheet pointer;
- repeated prose занимает больше tokens, чем компактная structured relation.

Рекомендуемый bounded view:

```json
{
  "function_id": "...",
  "function_class": "...",
  "serviced_object": ["..."],
  "zone": ["..."],
  "floors": ["..."],
  "systems": ["..."],
  "consumers": ["..."],
  "equipment_roles": ["..."],
  "upstream_function_ids": ["..."],
  "downstream_function_ids": ["..."],
  "stable_entities": ["..."],
  "topology_signature": ["..."],
  "source_sheet_refs": ["..."],
  "evidence_refs": ["..."]
}
```

Для group candidate нужен явный component matrix:

```text
LEFT function component → RIGHT function component → RIGHT sheet
```

Class coverage без role binding недостаточен: именно поэтому ablation B/C/D предпочёл ложную группу [25,26,28], которая покрывает 7/7 classes, но подмешивает corpus-4 riser sheet 25 вместо common meter sheet 29.

## Constraints и verifier

Новый verifier должен проверять:

1. every selected function/component bound to bounded candidate IDs;
2. complete declared lineage group;
3. evidence-page and function binding;
4. no duplicate ownership одного atomic function component;
5. shared physical RIGHT допустим, если `lineage_id` различается или declared merge объединяет old components;
6. `DOCUMENT_LINK` не материализуется как `FUNCTIONAL_ANALOGUE` и наоборот;
7. human priority остаётся финальным gate.

Сохранять нынешний unconditional `set(option_a.right_pages) ∩ set(option_b.right_pages) == ∅` нельзя: он и есть root cause distributed conflict.

## Cost expectation

Фактический full TEXT input — 161 989 mean input tokens/call. Function-core ablation — 116 042, то есть на 28.4% меньше input tokens, при stable 17/18/19 functional choices. Hierarchy должна дополнительно сократить candidate universe Stage B, потому что sheet candidates фильтруются внутри lineage.

Vision не является архитектурной заменой: B+vision вырос до 174 123 mean input tokens/call и не сохранил target LEFT 20 на map-stage.

Ожидаемый выигрыш hierarchy нужно проверять отдельным будущим bounded pilot после изменения representation; в этой работе production prompt/code не менялись и результат не материализовался.
