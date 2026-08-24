# Stage 5.3 — High-level project changes

## Короткий ответ

1. **Получилось ли перейти от atomic diff к проектным решениям?** Да. Новый аддитивный слой сворачивает Stage 5 evidence в несколько трассируемых выводов, нейтральную детализацию, material review и скрытый low-value хвост.
2. **Сколько atomic evidence стало high-level changes?** На АР: `261 → 3`; на ИОС1.1: `96 → 0`; на третьем АР1: `482 → 1`. Числа справа — количество записей, не потеря evidence: исходные IDs сохранены в раскрываемых details.
3. **Сколько REVIEW реально material?** АР: 3 semantic groups / 4 evidence; ИОС: 4 / 4; АР1: 15 / 109. Массовый REVIEW не получил поголовного AI-pass.
4. **Есть ли false project changes?** В controlled benchmark — 0. В real-run ручной аудит нашёл две опасные попытки (исправление опечатки и одностороннее отсутствие материала); обе закрыты validator/guards и больше не публикуются.
5. **Отличает ли система детализацию РД от реального изменения?** Да. Например, добавленная ведомость материалов кладочных стен показана нейтрально как `DETAIL_LEVEL_INCREASED`, а не как замена материалов.
6. **Ловит ли изменение принципа работы?** Да, controlled calibration и fresh holdout проходят как `SYSTEM_OPERATION_CHANGED`.
7. **Ловит ли изменение расчётного подхода?** Да, формула, метод, предпосылки и коэффициент проверены отдельными cases как `CALCULATION_APPROACH_CHANGED`.
8. **Что получилось на АР?** 261 evidence сведены к 3 выводам, 1 detail-only группе и 3 material-review группам; ложные удаления утепления не опубликованы.
9. **Что получилось на ИОС?** Сильных выводов не опубликовано; спорная связь листов остаётся `SOURCE_LINK_UNCERTAIN`.
10. **Готов ли текстовый контур к будущему объединению с графикой?** Да. Контракт хранит `evidence_sources: ["TEXT"]`; в будущем допустимы `GRAPHIC` и `BOTH`, без изменения старых Stage 2–5 artifacts.

## Что реализовано

Stage 5.3 читает только существующий `project_change_summary.json`, который уже трассируется до Stage 4 evidence. PDF, OCR, Vision, raster/vector graphics и table parser в синтезе не участвуют.

Поток данных:

```text
project_change_summary.json (immutable)
        ↓
stable evidence index + same-version counterpart guard
        ↓
deterministic semantic pre-grouping (sheet/entity/system/parameter/subject)
        ↓
SERVICE / NON_MATERIAL / CONFIRMED / DETAIL / MATERIAL / AI_REVIEW
        ↓                    только coherent material candidates → AI
strict backend validation + fail-closed fallback
        ↓
high_level_project_changes.json (additive)
```

Новый artifact содержит:

- `high_level_changes`;
- `detail_level_increased`;
- `material_review`;
- `non_material_review`;
- `unresolved`;
- collapsed `service_structure_summary`;
- compact `semantic_groups` audit trail;
- `evidence_sources`, source signature, prompt/model/validator provenance и usage.

Каждая опубликованная запись имеет `change_id`, type, title, `CONFIRMED/REVIEW_REQUIRED`, evidence IDs, sheet groups, before/after, fragment IDs, pages и anchors.

## Fail-closed guards

Validator и post-build validator проверяют:

- существование и полное покрытие evidence IDs;
- отсутствие повторной публикации одного evidence;
- числа и designations только из evidence; backend aggregate count разрешён только как число выбранных IDs;
- совместимость high-level type с semantic family;
- запрет promotion `SERVICE_STRUCTURE → project change`;
- запрет strong publish при `PAIR_REVIEW_REQUIRED`;
- запрет `REAL_CHANGE` для чисто односторонней `ADDED` или `REMOVED` группы;
- запрет утверждения о новом объекте в `DETAIL_LEVEL_INCREASED`;
- отсутствие audit language (`ошибка проекта`, нормативность, критичность);
- повторную проверку готового artifact независимо от model response.

Один невалидный AI-group не уничтожает валидные соседние ответы batch: они перепроверяются поштучно, а только отвергнутый group уходит в fail-closed material review.

## Detail vs real change

Отдельно различаются:

- подтверждённое изменение решения;
- детализация того же решения;
- служебная/структурная информация документа;
- недостаточный контекст.

Высокая token similarity при неизменных числах и параметрах подавляет буквенные исправления. При этом explicit signals формулы, режима работы, назначения помещения, типа оборудования или материала не подавляются.

## Same-version и cross-sheet evidence

Односторонний fragment проверяется против before/after evidence других листов той же comparison pair. Совпадение требует равных чисел и консервативного token overlap.

Это сработало на известных АР случаях:

- два удаления утепления У1/У2 получили `CROSS_SHEET_COUNTERPART` и не опубликованы;
- третье одностороннее отсутствие заполнения деформационного шва оставлено material review: одного отсутствия строки недостаточно;
- технологическая лестница 7.ПОН.2 найдена в обеих версиях на разных sheet groups и не объявлена новым объектом;
- исправление `антипиррированными → антипирированными` не стало material change.

## Review triage и AI budget

AI вызывается только для coherent `AI_REVIEW` semantic groups. Служебные, same-version, буквенные и явно low-value review группы не отправляются модели.

| Комплект | Stage 5 model calls | Stage 5.3 model calls | Atomic REVIEW evidence | Material review evidence |
|---|---:|---:|---:|---:|
| АР `p570d156f57` | 7 | 2 | 246 | 4 |
| ИОС1.1 `p26c08b83a6` | 4 | 0 | 57 | 4 |
| АР1 `p16b108b9f5` | 4 | 0 | 429 | 109 |

Большой material review АР1 вызван не содержанием параметров, а восемью сомнительными sheet pairings. Они намеренно не превращаются в strong publish.

## Controlled benchmark

Ground truth задан отдельно в `benchmarks/stage_5_3_high_level_ground_truth.json`; текущий Stage 5 output не используется как expected result.

- 48 cases;
- calibration: 22;
- fresh holdout: 26;
- у каждого case отдельно заданы expected type/count/title meaning и expected evidence routing (`PUBLISH/DETAIL/REVIEW/SUPPRESS`);
- присутствуют parameter aggregation, formula/method/assumption/coefficient, paraphrase, detail, operation, room program, equipment/material, service, ambiguous/weak review, uncertain link, cross-sheet counterpart, over-merge и over-fragmentation cases.

Итог `benchmarks/results/stage_5_3_high_level_benchmark.json`:

| Metric | Calibration | Holdout | Overall |
|---|---:|---:|---:|
| cases passed | 22/22 | 26/26 | 48/48 |
| high-level precision | 1.000 | 1.000 | 1.000 |
| high-level recall | 1.000 | 1.000 | 1.000 |
| material-review precision | 1.000 | 1.000 | 1.000 |
| evidence-route accuracy | 1.000 | 1.000 | 1.000 |
| false project change | 0 | 0 | 0 |
| missed project change | 0 | 0 | 0 |
| over-fragmentation | 0 | 0 | 0 |
| over-merge | 0 | 0 | 0 |
| detail-as-change | 0 | 0 | 0 |
| service-as-project | 0 | 0 | 0 |
| unsupported claims | 0 | 0 | 0 |

Это controlled benchmark правил и контрактов, а не заявление о 100% точности на произвольной документации.

## Real runs: before / after

### АР — `p570d156f57`

Before (`project_change_summary.json`):

- 261 atomic evidence;
- 4 Stage 5 project items;
- 8 service items;
- 13 review items / 246 review evidence;
- 1 uncertain sheet pair.

After (`high_level_project_changes.json`):

- 3 high-level changes / 26 supporting evidence;
- 1 neutral detail group;
- 3 material-review groups / 4 evidence;
- 184 non-material review evidence;
- 46 service evidence;
- status `partial`, потому что один AI proposal для количества проёмов отвергнут type validator и оставлен review.

Выводы:

- `PARAMETER_SET_CHANGED` — 12 evidence свернуты в одну запись;
- `SPACE_PROGRAM_CHANGED` — 11 evidence;
- `SPACE_PROGRAM_CHANGED` — 3 evidence.

### ИОС1.1 — `p26c08b83a6`

Before:

- 96 atomic evidence;
- 0 Stage 5 project items;
- 4 service items;
- 7 review items / 57 review evidence;
- спорная sheet pair.

After:

- 0 high-level changes;
- 4 material-review groups / 4 evidence;
- 15 non-material review evidence;
- 77 service evidence;
- status `completed`;
- сомнительная П31 ↔ РД29 не создаёт strong conclusion и помечена `SOURCE_LINK_UNCERTAIN`.

Обязательный пример review triage: **57 REVIEW evidence → 0 high-level changes**.

### АР1, комплект с параметрами — `p16b108b9f5`

Для полного real run использованы существующие structured fragments и Stage 2–5 pipeline; OCR/Vision/graphics не запускались.

Before:

- 482 Stage 5 atomic evidence;
- 1 Stage 5 project item / 43 evidence;
- 11 review items / 429 evidence;
- 8 uncertain sheet pairs.

After:

- **43 parameter evidence → 1 `PARAMETER_SET_CHANGED`**;
- 15 material-review groups / 109 evidence, почти все под sheet-link guard;
- 216 non-material review evidence;
- 114 service evidence;
- status `completed`;
- исправление написания материала не опубликовано.

## UI и backward compatibility

В «Расхождениях» новый глобальный блок показывается первым:

- подтверждённые high-level changes;
- только `MATERIAL_REVIEW` в заметном warning-блоке;
- neutral collapsed `Увеличена детализация РД`;
- collapsed service/structure;
- collapsed low-value REVIEW/debug;
- before/after и переход на исходные листы внутри evidence details.

Если `high_level_project_changes.json` отсутствует, UI показывает прежний Stage 5 экран. GET pair не запускает AI. Старые `project_change_summary.json`, `text_ai_review.json` и `text_final_comparison.json` не меняются при Stage 5.3 run.

## Tests

- `python -m pytest -q tests/test_stage_comparison_*.py` — **300 passed**;
- dedicated Stage 5.3 backend/contract suite — **27 passed**;
- `npx vitest run tests/stage_comparison_*.test.js` — **89 passed**;
- frontend production build — success;
- full frontend suite — **400 passed, 7 failed** в четырёх известных несвязанных legacy test files (`section_optimization_card`, `stage_algorithm_guide`, `opt_norm_badge`, `md_page_alignment`). Те же ожидаемые строки отсутствуют/расходятся уже в `HEAD` Stage 5.2; Stage Comparison tests проходят полностью.

## Scope

Не изменялись:

- Stage 4 evidence и старые Stage 5 artifacts;
- sheet-link fingerprint, thresholds, title/content repair, swap/3-cycle;
- Graphic Router, MODE 1/MODE 2, GraphicChangeLedger, vector extraction и Вектограф;
- provider abstraction и model selection policy.

Push и release не выполнялись.
