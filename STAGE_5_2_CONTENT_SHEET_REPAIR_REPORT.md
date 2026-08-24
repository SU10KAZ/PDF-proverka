# STAGE 5.2 — Content-based sheet-link repair

## Короткий итог

1. **Работает ли content-based repair?** Да. Детерминированный matcher восстанавливает только доказанные one-to-one связи и сохраняет объяснимые компоненты решения.
2. **Сколько automatic repairs?** В полном benchmark — 14, все 14 корректны. В реальных ИОС/АР run — 0: доказательств для изменения связи нет.
3. **Сколько false repairs?** В финальном алгоритме — 0 из 257 benchmark cases; на финальном holdout — 0 из 62.
4. **Каков holdout precision?** 1.000000 (8 корректных auto repairs из 8 выполненных); recall 0.205128 намеренно низкий из-за политики precision > recall.
5. **Восстанавливает ли swap?** Да. Atomic swap восстановлен unit-тестом, strong-anchor control и одним ранее не использованным holdout control.
6. **Восстанавливает ли 3-cycle?** Да. Atomic 3-cycle восстановлен unit-тестом, strong-anchor control и одним holdout control.
7. **Что произошло с ИОС?** П31 ↔ РД29 не изменена автоматически. Лучший новый кандидат РД31 слишком слабый и неоднозначный.
8. **Что произошло с АР?** П14 ↔ РД13 не изменена: содержание уверенно подтверждает именно текущий РД13 как лучший counterpart.
9. **Что специально оставлено REVIEW?** Слабые/не-mutual кандидаты, low-margin, small-improvement, purpose conflicts, ambiguous content, split/merge и many-to-many.
10. **Можно ли переходить к Stage 5.3?** Да. Stage 5.2 достиг safety-цели с нулём false auto repairs на свежем holdout. Низкий recall является сознательным ограничением, а очистка остальных REVIEW относится к Stage 5.3.

## Что реализовано

Stage 5.1 title layer остался первым и авторитетным уровнем:

- `TITLE_EXACT` — уникальный exact title;
- `TITLE_MUTUAL_FUZZY` — mutual-best fuzzy title с прежними `0.94` threshold и `0.02` margin.

Content layer запускается только когда title layer не построил repair. Он использует только уже подготовленные structured Markdown `Summary`/`Entities` и сохраняет компактный `content_fingerprint`, а не текст страницы.

Fingerprint содержит:

- `purpose_terms`;
- `system_names`;
- `unique_designations`;
- `equipment_codes`;
- `node_names`;
- `section_names`;
- `rare_terms`;
- `structural_tokens`;
- SHA-256 исходной компактной семантики.

Из признаков исключаются service fields, контакты, даты, организация, ГИП/ГАП, подписи, общая лексика и номера координатных осей. Rarity рассчитывается отдельно внутри каждого сравниваемого комплекта.

Для каждого кандидата считаются и записываются отдельно:

- `title_similarity`;
- `purpose_similarity`;
- `rare_term_overlap`;
- `designation_overlap`;
- `system_overlap`;
- `equipment_overlap`;
- `structural_token_overlap`;
- `cross_sheet_confirmation`.

Auto repair разрешён только одновременно при:

- mutual-best в направлениях П→РД и РД→П;
- `score >= 0.44`;
- direct margin `>= 0.14`;
- reverse margin `>= 0.10`;
- improvement over current `>= 0.18`;
- совместимом и не ухудшившемся purpose;
- минимум двух независимых content components;
- уникальных anchors, среди которых есть сильный designation/system/node anchor либо минимум три независимых уникальных anchors;
- безопасной one-to-one операции.

Cross-sheet evidence может добавить только малое подтверждение. Оно не участвует в mutual-best ranking и не способно самостоятельно пройти anchor gates.

## Atomic repair и audit trail

Поддерживаются:

- single one-to-one replacement на свободный RIGHT sheet;
- `CONTENT_SWAP`;
- `CONTENT_3_CYCLE`.

Цепочки более трёх листов, partial displacement, N→1, 1→N и N→M автоматически не изменяются.

Каждая applied content change в `sheet_link_repairs.json` содержит:

- before/after snapshots;
- П, старый РД и новый РД;
- titles;
- `CONTENT_UNIQUE_ANCHORS` или `CONTENT_MUTUAL_BEST`;
- operation (`CONTENT_SWAP`/`CONTENT_3_CYCLE`, если применимо);
- matched feature groups и основные anchors;
- все score components;
- current/best/second scores, margins и improvement;
- purpose before/after;
- cross-sheet confirmation;
- fingerprint source signatures;
- `confidence=HIGH`.

Stage 5.1 Undo применяется без изменений и возвращает точный `before_snapshot`. Existing recompute path перестраивает Stage 2, Stage 3, Stage 4 и Stage 5 с `_allow_sheet_link_repair=False`, поэтому за один Stage 5 run возможен максимум один automatic cycle.

## Benchmark

Benchmark использует реальные prepared sheet indexes и structured Markdown. Ground truth берётся независимо из уникальных exact titles и high-confidence Stage 1 links; перед content evaluation titles удаляются либо заменяются дублирующимся `Общие данные`.

Controlled corruption включает correct existing links, wrong links, missing/ambiguous titles, swaps, 3-cycles, ambiguous duplicate content, split/merge и many-to-many. Дополнительно есть discipline-neutral strong-anchor controls для полного покрытия atomic operations.

| Набор | Cases | Auto | Correct | False | Precision | Recall |
|---|---:|---:|---:|---:|---:|---:|
| Calibration | 80 | 4 | 4 | 0 | 1.000000 | 0.075472 |
| Falsification regression | 115 | 2 | 2 | 0 | 1.000000 | 0.027778 |
| Fresh holdout | 62 | 8 | 8 | 0 | 1.000000 | 0.205128 |
| Всего | 257 | 14 | 14 | 0 | 1.000000 | 0.085366 |

На свежем holdout:

- wrong-link/generic-title recovery: 6 из 28;
- swap recovery: 1 из 8;
- 3-cycle recovery: 1 из 3;
- ambiguous content untouched: да;
- many-to-many/split-merge untouched: да;
- false auto repairs: 0.

Невосстановленные controlled links — это abstentions, а не неверные repairs: они не прошли margin, mutual-best, improvement или anchor gates.

### Falsification history

Первый отложенный прогон ранней версии обнаружил 3 false auto repairs из 11 automatic decisions (precision 0.727273). Все три относились к одному фасадному случаю: координаты осей `1.А`, `1.Б`, `3.К` были ошибочно приняты за сильные designations.

Исправление было reason-first: координатные оси исключены из strong designations и оставлены только как structural context. Повторный falsification-набор из 115 cases дал 2 корректных repairs и 0 false. После этого выполнен отдельный свежий holdout на ранее не использованных ground-truth sheets 7–12; его precision — 1.0.

Полный трассируемый результат: `experiments/stage_5_2_content_sheet_repair/artifacts/benchmark_results.json`.

## Реальный run: ИОС

### Before

- П page 31;
- текущий РД page 29 (`Однолинейная схема ВРУ-А`).

### Content candidates

- лучший candidate: РД page 31 (`Внутреннее электроснабжение и освещение. (втч ОЗДС)`);
- current score: `0.101647`;
- best score: `0.106585`;
- second score: `0.101647`;
- margin: `0.004938`;
- reverse margin: `0.078602`;
- improvement: `0.004938`;
- unique anchors: отсутствуют;
- mutual-best: нет.

### Decision / After

`LOW / REVIEW`. Причины: low score, low margin, non-mutual best, нет material improvement и content anchors. Связь П31 ↔ РД29 не изменена. Это не утверждение, что текущая связь правильна; это отказ делать недоказанный automatic repair.

## Реальный run: АР

### Before

- П page 14 — `Узел устройства стойки фахверка`;
- текущий РД page 13.

### Content candidates

Текущий РД13 является mutual-best:

- current/best score: `0.675157`;
- second score: `0.089505`;
- margin: `0.585652`;
- reverse margin: `0.543217`;
- improvement: `0.0`.

Совпавшие anchors включают `D600`, `БСР`, `М12`, `90кг/м3`, `высота стойки`, `газобетонные блоки`, `минеральная вата`, `80x4` в structural evidence.

### Decision / After

`MEDIUM / REVIEW`, auto repair запрещён правилами `CURRENT_LINK_BEST` и `NOT_MATERIALLY_BETTER`. Связь П14 ↔ РД13 сохранена. Title-only purpose precheck здесь дал ложный конфликт из-за общего title РД, а содержание подтверждает существующий counterpart; изменение Stage 5 REVIEW classification не входит в Stage 5.2.

## Regression и safety

- Stage 5.1 unique exact, mutual fuzzy, ambiguity, swap и 3-cycle tests сохранены.
- Content tests покрывают unique anchor, mutual-best, low margin, non-mutual, purpose conflict, swap, 3-cycle, many-to-many, missing/generic title, cross-sheet confirmation, cross-sheet-only rejection, material improvement, MEDIUM rejection и package rarity.
- Store tests покрывают artifact persistence, exact Undo, changed-after-repair guard, downstream recompute и one-cycle protection.
- UI переиспользует существующий banner и Undo; для content repair показывает старый/новый РД, anchors, reason и confidence.
- LLM, OCR, PDF parser, table parser, Vision и graphics pipeline не используются.
- Graphic Comparison G1, MODE 1/MODE 2, vector extraction и graphic ledger не изменялись.

### Выполненные проверки

- все backend tests `test_stage_comparison*.py`, кроме отдельного незавершённого G1 test: **241 passed**;
- все frontend Stage Comparison tests: **85 passed**;
- целевой Stage 5.1/5.2 backend-набор после последней правки: **69 passed**;
- frontend Stage 5 banner/Undo test: **6 passed**;
- `py_compile` production matcher/fingerprint/planner и benchmark script: успешно;
- benchmark: **257 cases**, 14 correct repairs, 0 false; fresh holdout: **62 cases**, 8 correct repairs, 0 false.

Полный repository-wide `pytest` не дошёл до выполнения из-за отсутствующих в окружении optional packages `grpc` и `google.protobuf` (5 collection errors вне Stage Comparison). Полный frontend suite выполнил 396 tests и показал 7 существующих несвязанных failures в `section_optimization_card`, `stage_algorithm_guide`, `opt_norm_badge` и `md_page_alignment`; все 85 Stage Comparison frontend tests прошли.

## Изменённые файлы

### Production backend

- `backend/app/services/stage_comparison/sheet_content_fingerprint.py` — compact fingerprint и noise filtering;
- `backend/app/services/stage_comparison/content_sheet_link_repair.py` — content ranking, guards и atomic planning;
- `backend/app/services/stage_comparison/sheet_matching.py` — извлечение всех готовых Summary/Entities и сохранение fingerprint вместо текста;
- `backend/app/services/stage_comparison/sheet_link_repair.py` — title-first/content-second orchestration и новые reason codes;
- `backend/app/services/stage_comparison/store.py` — только два Stage 5.2 hunk: передача Stage 4 evidence и сохранение plan reason.

### UI

- `frontend/index.html`;
- `frontend/static/js/app.js`.

### Tests

- `tests/test_stage_comparison_content_sheet_link_repair.py`;
- `tests/test_stage_comparison_sheet_link_repair.py`;
- `tests/test_stage_comparison_sheet_link_repair_store.py`;
- `frontend/tests/stage_comparison_project_change_summary.test.js`.

### Benchmark и документация

- `scripts/benchmark_stage_5_2_content_sheet_repair.py`;
- `experiments/stage_5_2_content_sheet_repair/artifacts/benchmark_results.json`;
- `STAGE_5_2_CONTENT_SHEET_REPAIR_REPORT.md`.

### Schema / migrations

Отдельная migration не требуется. `sheet_link_repairs.json` version 1 расширен backward-compatible полями внутри content changes; существующие Stage 5.1 artifacts и Undo остаются читаемыми.
