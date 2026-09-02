# AI Sheet Matcher repeat with Candidate Generator v4

## Итог

Вердикт: **B**. Safety gate: **PASS**. Unsupported auto matches: **0**.

Повтор выполнен изолированно на том же 36-LEFT benchmark. Selector, JSON output shape, local→Document Map Review, Pass A/B, three-cold-run unanimity, human-priority gate и fail-closed поведение сохранены; заменён только источник bounded candidates на Candidate Generator v4. Production, UI, pipeline, engineer mappings и deploy не изменялись.

## A/B по проектам

| Проект | Baseline H/P/U | old TEXT auto/review/unresolved | v4 TEXT auto/review/unresolved | v4 TEXT+VISION fallback auto/review/unresolved |
|---|---:|---:|---:|---:|
| ИОС 1.1 | 1/11/70 | 10/2/12 | 14/0/10 | 14/0/10 |
| ИОС 3.1 | 0/5/44 | 4/0/1 | 4/0/1 | 4/0/1 |
| ИОС 2.1 | 0/7/94 | 0/5/2 | 0/2/5 | 0/2/5 |

Stable relation counts (unique bounded relation IDs):

- TEXT: `{'FUNCTION_DISTRIBUTED': 0, 'MATCH_1_TO_1': 16, 'MERGED_N_TO_1': 2, 'SPLIT_1_TO_N': 0}`
- TEXT + VISION fallback: `{'FUNCTION_DISTRIBUTED': 0, 'MATCH_1_TO_1': 16, 'MERGED_N_TO_1': 2, 'SPLIT_1_TO_N': 0}`

## ИОС 2.1 critical cases

| LEFT | Engineer RIGHT | v4 rank | TEXT | Final / VISION fallback | Cold stability |
|---:|---:|---:|---|---|---|
| 17 | 7 | 5 | MATCH_1_TO_1 [17]→[27] (vcand_669a951378d5d1605601) | VISION: MATCH_1_TO_1 [17]→[27] (vcand_669a951378d5d1605601); engineer=NO | YES |
| 18 | 8 | 2 | MATCH_1_TO_1 [18]→[24] (vcand_f023727e170f4dcf7bf7) | TEXT retained: MATCH_1_TO_1 [18]→[24] (vcand_f023727e170f4dcf7bf7); engineer=NO | YES |
| 19 | 9 | 4 | UNRESOLVED | VISION: UNRESOLVED; engineer=NO | NO |

Pass-level trace:

- LEFT 17 TEXT: `A[r1=MATCH_1_TO_1→[27], r2=MATCH_1_TO_1→[27], r3=MATCH_1_TO_1→[27]]; B[r1=MATCH_1_TO_1→[27], r2=MATCH_1_TO_1→[27], r3=MATCH_1_TO_1→[27]]`
- LEFT 17 VISION: `A[r1=MATCH_1_TO_1→[27], r2=MATCH_1_TO_1→[27], r3=MATCH_1_TO_1→[27]]; B[r1=MATCH_1_TO_1→[27], r2=MATCH_1_TO_1→[27], r3=MATCH_1_TO_1→[27]]`
- LEFT 18 TEXT: `A[r1=MATCH_1_TO_1→[24], r2=MATCH_1_TO_1→[24], r3=MATCH_1_TO_1→[24]]; B[r1=MATCH_1_TO_1→[24], r2=MATCH_1_TO_1→[24], r3=MATCH_1_TO_1→[24]]`
- LEFT 18 VISION: не запускался.
- LEFT 19 TEXT: `A[r1=FUNCTION_DISTRIBUTED→[25, 30], r2=MATCH_1_TO_1→[25], r3=MATCH_1_TO_1→[25]]; B[r1=MATCH_1_TO_1→[25], r2=MATCH_1_TO_1→[25], r3=FUNCTION_DISTRIBUTED→[25, 30]]`
- LEFT 19 VISION: `A[r1=MATCH_1_TO_1→[25], r2=MATCH_1_TO_1→[25], r3=MATCH_1_TO_1→[25]]; B[r1=MATCH_1_TO_1→[25], r2=MATCH_1_TO_1→[25], r3=FUNCTION_DISTRIBUTED→[25, 30]]`

### Графический лист 5 (physical LEFT 20)

Target `fcand_6294159aac7851a636dd` (`FUNCTION_DISTRIBUTED`, RIGHT `[26,28,29]`) был в shortlist с rank 10. TEXT: `UNRESOLVED`. Final: `UNRESOLVED`. Stable target selected: **False**.
TEXT pass trace: `A[r1=NEED_MORE_EVIDENCE, r2=NEED_MORE_EVIDENCE, r3=NEED_MORE_EVIDENCE]; B[r1=NEED_MORE_EVIDENCE, r2=FUNCTION_DISTRIBUTED→[26, 28, 29]!map-rejected, r3=FUNCTION_DISTRIBUTED→[26, 28, 29]!map-rejected]`. VISION pass trace: `A[r1=NEED_MORE_EVIDENCE, r2=NEED_MORE_EVIDENCE, r3=NEED_MORE_EVIDENCE]; B[r1=NEED_MORE_EVIDENCE, r2=FUNCTION_DISTRIBUTED→[26, 28, 29]!map-rejected, r3=FUNCTION_DISTRIBUTED→[26, 28, 29]!map-rejected]`. Группа появлялась в отдельных Pass B, но конфликтовала с другими uses RIGHT и была fail-closed; стабильно заменить NO_MATCH/одиночный лист не смогла.

## Group shortlist

Передавалось не более 16 групп на LEFT из 1046 generated groups. Group Recall after shortlist: **100.0%** (7/7). Reference map не участвовала в построении или ranking shortlist и использовалась только для evaluation.

## Vision fallback

TEXT calls: **18**; VISION fallback calls: **18**; full-vision calls avoided: **0**.
Vision получал только renders fallback LEFT и их bounded RIGHT pages. Уже materialization-safe TEXT cases не переоткрывались.

- ИОС 1.1: fallback LEFT [24, 25, 31, 39, 41, 43, 45, 47, 49, 52]; reason trace сохранён в `decisions.jsonl`.
- ИОС 3.1: fallback LEFT [15]; reason trace сохранён в `decisions.jsonl`.
- ИОС 2.1: fallback LEFT [16, 17, 19, 20, 21, 51]; reason trace сохранён в `decisions.jsonl`.

## Manual audit and safety

Auto-resolved: 18; supported: 18; partial: 0; unsupported: 0. Complex auto relations manually gated: 2 unique (4 LEFT-task occurrences).

Document Map Review не применял безусловное 1→1 assignment: legal MERGED/SPLIT/DISTRIBUTED доступны только как atomic prebuilt v4 groups. Конкурирующие undeclared uses одного RIGHT блокируются; NEW/REMOVED sheet не материализуется как NEW/REMOVED function. Конфликт с engineer mapping всегда остаётся HUMAN_REVIEW.

## Stability

Три независимых cold runs, в каждом byte-identical Pass A и Pass B. Автоматическое решение требует совпадения всех шести verified map selections. Exact overlap, map overlap, stable core, disagreement count и unstable relations находятся в `stability.json`.

| Проект/mode | Stable core | Disagreement | Exact overlap range | Map overlap range |
|---|---:|---:|---:|---:|
| ИОС 1.1 TEXT | 21 | 3 | 87.5–95.8% | 84.0–95.5% |
| ИОС 1.1 TEXT_VISION_FALLBACK | 14 | 10 | 58.3–79.2% | 58.3–79.2% |
| ИОС 3.1 TEXT | 5 | 0 | 100.0–100.0% | 100.0–100.0% |
| ИОС 3.1 TEXT_VISION_FALLBACK | 5 | 0 | 100.0–100.0% | 100.0–100.0% |
| ИОС 2.1 TEXT | 3 | 4 | 42.9–85.7% | 42.9–75.0% |
| ИОС 2.1 TEXT_VISION_FALLBACK | 2 | 5 | 28.6–85.7% | 28.6–66.7% |

## Cost

Calls TEXT/VISION/total: **18/18/36**. Tokens TEXT/VISION/total: **3912337/3403226/7315563**. Wall time: **768.7s**. Tokens per stable auto decision: **406420.167**.

Previous experiment: 36 calls, 6,261,720 tokens, 721.0s. Call delta: +0; token delta: 1053843.
Vision scope: 17/36 LEFT; vision-token reduction: 9.2%; tokens/stable-auto change vs old best arm: +23.3%.
Model responses succeeded: 36/36; whole-map verifier accepted 31/36 and rejected 5 maps fail-closed.

## Acceptance gates

- PASS — `unsupported_auto_matches_zero`
- PASS — `engineer_conflict_materializations_zero`
- PASS — `closed_engineer_mappings_all_supported`
- PASS — `stable_auto_substantially_above_old_text`
- FAIL — `stable_auto_substantially_above_old_best_arm`
- PASS — `at_least_one_stable_safe_complex_relation`
- PASS — `text_solves_majority_of_ordinary_engineer_cases`
- PASS — `group_recall_after_shortlist_100_percent`
- FAIL — `vision_tokens_reduced_at_least_50_percent`
- FAIL — `total_tokens_not_above_previous`

## Production safety

Frozen source artifacts unchanged: **True**. Запись выполнялась только в research package/tests и `comparison/ai_sheet_matcher/20260902_v4_ai_repeat/`. Deploy и push не выполнялись.
