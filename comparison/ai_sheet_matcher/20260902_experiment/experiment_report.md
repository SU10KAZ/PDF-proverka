# Research spike — AI Sheet Matcher

## Итог

Вердикт: **B**. Safety gate: **PASS**; unsupported automatic matches: **0**.

Эксперимент изолирован и выполнен на frozen-артефактах. Production matcher, пороги, UI, исходные прогоны и deployment не изменялись. Reference hypotheses использованы только как аудит-кейсы. Сохранённое решение инженера имеет безусловный приоритет над моделью.

AI-часть намеренно ограничена сложными листами: 24 LEFT для ИОС 1.1, 5 для ИОС 3.1 и 7 для ИОС 2.1. Candidate recall измерен шире — на всех 70 уникальных human-confirmed/reference кейсах.

## Метрики benchmark

| Проект | Baseline H/P/U | Human confirmations | Recall@5 | Recall@10 | TEXT auto/review/unresolved/unsupported | VISION+TEXT auto/review/unresolved/unsupported |
|---|---:|---:|---:|---:|---:|---:|
| ИОС 1.1 | 1/11/70 | 37 relations / 40 LEFT cases | 72.5% | 87.5% | 10/2/12/2 | 15/3/6/3 |
| ИОС 3.1 | 0/5/44 | 8 relations / 8 LEFT cases | 75.0% | 100.0% | 4/0/1/0 | 4/0/1/0 |
| ИОС 2.1 | 0/7/94 | 15 relations / 15 LEFT cases | 22.7% | 40.9% | 0/5/2/5 | 0/4/3/4 |

## Ответы на десять вопросов исследования

### 1. Где проблема была в candidate generation

В 18 из 70 уникальных human/reference кейсов обязательный RIGHT-лист или вся обязательная группа не попали в top-10 конкретного LEFT. Примеры:

- ИОС 1.1 LEFT 24 → RIGHT [24, 25] (CANDIDATE_GENERATION_PROBLEM)
- ИОС 1.1 LEFT 25 → RIGHT [24, 25] (CANDIDATE_GENERATION_PROBLEM)
- ИОС 1.1 LEFT 30 → RIGHT [28] (CANDIDATE_GENERATION_PROBLEM)
- ИОС 1.1 LEFT 41 → RIGHT [34] (CANDIDATE_GENERATION_PROBLEM)
- ИОС 1.1 LEFT 52 → RIGHT [21, 22, 23] (CANDIDATE_GENERATION_PROBLEM)
- ИОС 2.1 LEFT 6 → RIGHT [12] (CANDIDATE_GENERATION_PROBLEM)
- ИОС 2.1 LEFT 7 → RIGHT [13] (CANDIDATE_GENERATION_PROBLEM)
- ИОС 2.1 LEFT 8 → RIGHT [14] (CANDIDATE_GENERATION_PROBLEM)
- ИОС 2.1 LEFT 9 → RIGHT [15] (CANDIDATE_GENERATION_PROBLEM)
- ИОС 2.1 LEFT 10 → RIGHT [16] (CANDIDATE_GENERATION_PROBLEM)
- ИОС 2.1 LEFT 11 → RIGHT [17] (CANDIDATE_GENERATION_PROBLEM)
- ИОС 2.1 LEFT 12 → RIGHT [18] (CANDIDATE_GENERATION_PROBLEM)

Для одиночного LEFT селектор такие пропуски исправить не может. Document Map Review иногда может собрать группу из кандидатов соседних LEFT, но это не помогло ГРЩ LEFT 52 → RIGHT 21/22/23 (22 и 23 отсутствовали) и ВРУ-4 LEFT 29/30 → RIGHT 28 (RIGHT 28 отсутствовал у LEFT 30).

### 2. Где кандидат был найден, но deterministic matcher его не выбрал уверенно

В 44 human-confirmed кейсах нужные страницы уже были в top-10, но exact HIGH production relation не было. Примеры:

- ИОС 1.1 LEFT 31 → RIGHT [29] (CANDIDATE_SELECTION_PROBLEM)
- ИОС 1.1 LEFT 6 → RIGHT [5] (CANDIDATE_SELECTION_PROBLEM)
- ИОС 1.1 LEFT 7 → RIGHT [6] (CANDIDATE_SELECTION_PROBLEM)
- ИОС 1.1 LEFT 8 → RIGHT [7] (CANDIDATE_SELECTION_PROBLEM)
- ИОС 1.1 LEFT 9 → RIGHT [8] (CANDIDATE_SELECTION_PROBLEM)
- ИОС 1.1 LEFT 10 → RIGHT [9] (CANDIDATE_SELECTION_PROBLEM)
- ИОС 1.1 LEFT 11 → RIGHT [10] (CANDIDATE_SELECTION_PROBLEM)
- ИОС 1.1 LEFT 12 → RIGHT [11] (CANDIDATE_SELECTION_PROBLEM)
- ИОС 1.1 LEFT 13 → RIGHT [12] (CANDIDATE_SELECTION_PROBLEM)
- ИОС 1.1 LEFT 14 → RIGHT [13] (CANDIDATE_SELECTION_PROBLEM)
- ИОС 1.1 LEFT 15 → RIGHT [14] (CANDIDATE_SELECTION_PROBLEM)
- ИОС 1.1 LEFT 16 → RIGHT [15] (CANDIDATE_SELECTION_PROBLEM)

### 3. Результат TEXT AI

- ИОС 1.1: 10 retrospectively supported stable auto, 2 human review, 12 unresolved, 2 stable-but-blocked unsupported.
- ИОС 3.1: 4 retrospectively supported stable auto, 0 human review, 1 unresolved, 0 stable-but-blocked unsupported.
- ИОС 2.1: 0 retrospectively supported stable auto, 5 human review, 2 unresolved, 5 stable-but-blocked unsupported.

Всего TEXT дал 14 поддержанных стабильных LEFT-решений из 36 исследованных задач. Это retrospective score: human mappings не передавались модели, но использовались после ответа как safety gate, поэтому результат не равен готовой production-автоматизации.

### 4. Прирост VISION+TEXT относительно TEXT

- ИОС 1.1: 15 против 10 (+5); unresolved 6 против 12.
- ИОС 3.1: 4 против 4 (+0); unresolved 1 против 1.
- ИОС 2.1: 0 против 0 (+0); unresolved 3 против 2.

Vision дал измеримый прирост только на переименованной серии ИОС 1.1: +5 supported stable LEFT. На ИОС 3.1 прирост нулевой, на ИОС 2.1 — нулевой и стабильность карты стала хуже. При этом TEXT уже содержит сохранённые OCR/image-description артефакты, поэтому это инкремент vision поверх vision-enriched текста.

### 5. Обнаруженные 1→1 / 1→N / N→1 / FUNCTION_DISTRIBUTED

- ИОС 1.1: TEXT {'FUNCTION_DISTRIBUTED': 0, 'MATCH_1_TO_1': 12, 'MERGED_N_TO_1': 0, 'SPLIT_1_TO_N': 0}; VISION+TEXT {'FUNCTION_DISTRIBUTED': 0, 'MATCH_1_TO_1': 18, 'MERGED_N_TO_1': 0, 'SPLIT_1_TO_N': 0}. Это уникальные стабильные bounded option IDs, не ground truth.
- ИОС 3.1: TEXT {'FUNCTION_DISTRIBUTED': 0, 'MATCH_1_TO_1': 4, 'MERGED_N_TO_1': 0, 'SPLIT_1_TO_N': 0}; VISION+TEXT {'FUNCTION_DISTRIBUTED': 0, 'MATCH_1_TO_1': 4, 'MERGED_N_TO_1': 0, 'SPLIT_1_TO_N': 0}. Это уникальные стабильные bounded option IDs, не ground truth.
- ИОС 2.1: TEXT {'FUNCTION_DISTRIBUTED': 0, 'MATCH_1_TO_1': 5, 'MERGED_N_TO_1': 0, 'SPLIT_1_TO_N': 0}; VISION+TEXT {'FUNCTION_DISTRIBUTED': 0, 'MATCH_1_TO_1': 4, 'MERGED_N_TO_1': 0, 'SPLIT_1_TO_N': 0}. Это уникальные стабильные bounded option IDs, не ground truth.

Стабильных 1→N, N→1 или FUNCTION_DISTRIBUTED не обнаружено. Для ВРУ-3 локальные проходы устойчиво предлагали LEFT 27→RIGHT 27 и LEFT 28→RIGHT 27, но Document Map Review не оформил prebuilt MERGED option: один из листов переводился в NEED_MORE_EVIDENCE. Для ВРУ-4 один vision-pass предложил нестабильный FUNCTION_DISTRIBUTED LEFT 29/30→RIGHT 28/29; он не материализован. ГРЩ не имел полной bounded-группы.

### 6. Стабильность во всех трёх cold runs

- ИОС 1.1 TEXT: 16 стабильных задач, 8 расхождений/отказов; pairwise map overlap 75.0%, 75.0%, 100.0%.
- ИОС 1.1 VISION_TEXT: 20 стабильных задач, 4 расхождений/отказов; pairwise map overlap 83.3%, 83.3%, 95.8%.
- ИОС 3.1 TEXT: 4 стабильных задач, 1 расхождений/отказов; pairwise map overlap 100.0%, 80.0%, 80.0%.
- ИОС 3.1 VISION_TEXT: 4 стабильных задач, 1 расхождений/отказов; pairwise map overlap 80.0%, 100.0%, 80.0%.
- ИОС 2.1 TEXT: 5 стабильных задач, 2 расхождений/отказов; pairwise map overlap 71.4%, 100.0%, 71.4%.
- ИОС 2.1 VISION_TEXT: 4 стабильных задач, 3 расхождений/отказов; pairwise map overlap 85.7%, 71.4%, 57.1%.

35 из 36 model calls прошли verifier целиком. Один VISION_TEXT pass ИОС 2.1 был детерминированно отклонён из-за конфликта использования RIGHT 26 двумя LEFT-задачами; оба решения этого pass закрыты fail-closed.

### 7. Сколько решений осталось человеку

TEXT оставил человеку 22 из 36 задач; VISION+TEXT — 17 из 36. В число входят нестабильные/неразрешённые задачи и стабильные выборы, которые конфликтуют с human mapping либо не имеют human confirmation.

Ключевой кандидат на ручную перепроверку: ИОС 2.1. TEXT и VISION+TEXT во всех шести проходах выбрали функциональные пары LEFT 17→RIGHT 27, LEFT 18→RIGHT 24 и LEFT 19→RIGHT 25, тогда как сохранённые engineer-accepted links указывают RIGHT 7/8/9. AI не переопределил человека: все три пары заблокированы.

LEFT 20 и LEFT 51 ИОС 2.1 остались нестабильными; vision не разрешил конкуренцию RIGHT 29/30 и RIGHT 29/63. LEFT 21→RIGHT 29 был стабилен в обоих режимах, но оставлен человеку из-за отсутствия подтверждённой карты.

### 8. Unsupported automatic matches

Unsupported automatic matches: **0**. Safety gate: **PASS**. Стабильные, но неподдержанные выборы посчитаны отдельно и заблокированы до materialization.

### 9. Стоимость

Model calls: 36 (36 attempts including retries); wall runtime: 721.0s; summed call runtime: 1401.5s; tokens: 6261720.

### 10. Рекомендация

**B — перспективно, нужны доработки; controlled rollout пока не рекомендован.**

До rollout нужны: discipline-agnostic candidate generation с устойчивыми структурными сдвигами и групповыми кандидатами; независимый ручной functional ground truth для конфликтов со старыми page-number links; более надёжное group reasoning; повтор того же six-pass safety gate.

## Очередь ручного аудита

Полная трассировка находится в `decisions.jsonl`. Ниже стабильные конкретные выборы, заблокированные human-priority gate:

| Проект | Режим | LEFT | Выбор LEFT→RIGHT | Тип | Verifier | Confidence |
|---|---|---:|---|---|---|---|
| ИОС 1.1 | TEXT | 25 | [25]→[25] | MATCH_1_TO_1 | BLOCKED_HUMAN_DECISION_CONFLICT | UNANIMOUS_6_OF_6 |
| ИОС 1.1 | TEXT | 52 | [52]→[21] | MATCH_1_TO_1 | BLOCKED_HUMAN_DECISION_CONFLICT | UNANIMOUS_6_OF_6 |
| ИОС 1.1 | VISION_TEXT | 24 | [24]→[24] | MATCH_1_TO_1 | BLOCKED_HUMAN_DECISION_CONFLICT | UNANIMOUS_6_OF_6 |
| ИОС 1.1 | VISION_TEXT | 25 | [25]→[25] | MATCH_1_TO_1 | BLOCKED_HUMAN_DECISION_CONFLICT | UNANIMOUS_6_OF_6 |
| ИОС 1.1 | VISION_TEXT | 52 | [52]→[21] | MATCH_1_TO_1 | BLOCKED_HUMAN_DECISION_CONFLICT | UNANIMOUS_6_OF_6 |
| ИОС 2.1 | TEXT | 16 | [16]→[26] | MATCH_1_TO_1 | BLOCKED_HUMAN_SUPPORT_MISSING | UNANIMOUS_6_OF_6 |
| ИОС 2.1 | TEXT | 17 | [17]→[27] | MATCH_1_TO_1 | BLOCKED_HUMAN_DECISION_CONFLICT | UNANIMOUS_6_OF_6 |
| ИОС 2.1 | TEXT | 18 | [18]→[24] | MATCH_1_TO_1 | BLOCKED_HUMAN_DECISION_CONFLICT | UNANIMOUS_6_OF_6 |
| ИОС 2.1 | TEXT | 19 | [19]→[25] | MATCH_1_TO_1 | BLOCKED_HUMAN_DECISION_CONFLICT | UNANIMOUS_6_OF_6 |
| ИОС 2.1 | TEXT | 21 | [21]→[29] | MATCH_1_TO_1 | BLOCKED_HUMAN_SUPPORT_MISSING | UNANIMOUS_6_OF_6 |
| ИОС 2.1 | VISION_TEXT | 17 | [17]→[27] | MATCH_1_TO_1 | BLOCKED_HUMAN_DECISION_CONFLICT | UNANIMOUS_6_OF_6 |
| ИОС 2.1 | VISION_TEXT | 18 | [18]→[24] | MATCH_1_TO_1 | BLOCKED_HUMAN_DECISION_CONFLICT | UNANIMOUS_6_OF_6 |
| ИОС 2.1 | VISION_TEXT | 19 | [19]→[25] | MATCH_1_TO_1 | BLOCKED_HUMAN_DECISION_CONFLICT | UNANIMOUS_6_OF_6 |
| ИОС 2.1 | VISION_TEXT | 21 | [21]→[29] | MATCH_1_TO_1 | BLOCKED_HUMAN_SUPPORT_MISSING | UNANIMOUS_6_OF_6 |

## Ограничения интерпретации

- `stable_auto_decisions` — решения, retrospectively совпавшие с сохранённой human map и прошедшие six-pass gate; это не новая независимая ground truth.
- Старые `sheet_links` с `user_accepted` считаются human decisions и не переопределяются, даже когда functional evidence указывает на другую страницу.
- AI-метрики относятся к 36 приоритетным задачам, не ко всем LEFT-листам документов.
- NO_ANALOG никогда не материализуется: top-10 не доказывает полноту отсутствия.

## Доказательство изоляции

Frozen source artifacts unchanged: **True**. Запись выполнялась только в папку эксперимента. Deploy не выполнялся.
