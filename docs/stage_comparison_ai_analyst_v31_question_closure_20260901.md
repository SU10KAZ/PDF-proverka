# AI Analyst v3.1 — HRO Question Closure

Дата: 2026-09-01. Acceptance-пара: `p11c797af90` (ГРЩ). Экспериментальный слой добавлен над frozen FAST/HRO и существующими результатами AI Analyst v3. Production-последовательность `FAST → HRO → Engineer Review → Final Report`, production artifacts, Candidate Factory v3, Verifier v3, materialization, нормы и UI не изменялись. Push, deploy и release не выполнялись.

## 1. HEAD before

Исходный HEAD: `f9fcab8adf7e326fcf3a531bc14d328315019dcc` (`Document AI analyst v3 experiment`). Исходное дерево было чистым, `main...origin/main [ahead 2]`.

## 2. Commits

Реализация и тесты: `585189f7` (`Add HRO question closure experiment`). Документационный отчёт фиксируется отдельным коммитом; его hash приводится в финальном handoff.

## 3. Шесть QuestionClosureContracts

| HRO question | Question ID | Status после gate | Closure result |
|---|---|---|---|
| Сопоставимость расчётных режимов | `hrg_mode_33434a66cf174adbf52396e7` | `BLOCKED_POLICY` | Остаётся human authority |
| Изменение резервных линий | `hquestion_d5afdbd0b2d3a0f7bd92f7f7` | `BLOCKED_MISSING_EVIDENCE` | LEFT absence не доказано |
| Неоднозначное соответствие элементов схемы | `hquestion_0b54ffcaad72a4f448356627` | `PARTIALLY_RESOLVED` | Stable core покрывает лишь часть исходного inventory |
| Эквивалентность требования по измерительным приборам | `hquestion_9578e26b546217f2293e5278` | `BLOCKED_MISSING_EVIDENCE` | «Мультиметр» не доказывает полный требуемый состав измерений |
| Эквивалентность требования по шинам N и PE | `hquestion_a863b0dee0043c9d9413d10c` | `CLOSED_AI_STABLE` | `DIFFERENT_REQUIREMENT`, strict gate 6/6 passes |
| Выберите строку ВРУ3 | `hquestion_a96a7c297d1edce07edbe806` | `BLOCKED_AMBIGUOUS_EVIDENCE` | Две LEFT rows остаются правдоподобны |

Каждый contract содержит обязательные поля `question_id`, `question_type`, `required_subproblems[]`, `required_evidence[]`, `closure_conditions[]`, `blocking_conditions[]`, `affected_atomic_ids[]`, `current_status`. Exact accounting: 6 contracts = 6 baseline human interactions; ни один вопрос не удалён молча.

## 4. Какие вопросы разрешимы на текущем evidence

| Question | Closure possible with current evidence | Reason | AI needed |
|---|---:|---|---:|
| Mode group | NO | Решение о соответствии `Рабочий/Пожарный ↔ Аварийный/ПП` относится к полномочиям инженера | NO |
| Reserve lines | NO | Нет bounded LEFT absence и нет доказанного существующего эквивалента | NO |
| Graph correspondence | NO | После stable-core reuse остаются 10 LEFT и 16 RIGHT source nodes без конечных candidates | NO |
| Measurement | NO | Все шесть прежних passes стабильно вернули `INSUFFICIENT_EVIDENCE` | NO |
| N/PE | YES | Существуют три непересекающихся bounded варианта: SAME, DIFFERENT, INSUFFICIENT | YES |
| ВРУ3 | NO | Frozen evidence не различает две LEFT rows | NO |

Итого теоретически автоматически разрешим на текущих данных ровно 1 из 6 вопросов.

## 5. Graph correspondence closure

Исходная meta-находка `chg_ae7c3f788f2d` содержит не семь, а 13 unresolved LEFT и 19 unresolved RIGHT engineering nodes. Из 14 all-run stable v3 решений семь относятся к graph identity, но напрямую покрывают исходный inventory только тремя парами:

- `OUT:1QF1@460 ↔ OUT:1QF10@1220` — ДР1-ХМ1;
- `OUT:2QF12@1952 ↔ OUT:2QF10@2524` — ДР2-ХМ2;
- `OUT:1QF13@1206 ↔ OUT:1QF13@1471` — ШУ-ХВС/ЩНО, canonical ШНО.

Остальные четыре stable graph relations (`LOAD:1QF13`, две compensation groups и aggregate `SERVICE_GROUP:BUS2`) являются diagnostic/aggregate relations относительно точного source inventory этой HRO-находки и не считаются закрытием инженерных узлов.

После reuse остаются 10 LEFT и 16 RIGHT source nodes. Среди них отходящие аппараты, сервисные элементы ОПН/FU/УЗИП и два `UNKNOWN_NODE:QF*`; для них Candidate Factory v3 не создал конечных correspondence candidates. Слой сформировал 26 blocked selector subproblems с `NO_BOUNDED_CORRESPONDENCE_CANDIDATES`, не отправил их модели и оставил вопрос `PARTIALLY_RESOLVED`.

Ответ benchmark: существующие 14 stable-core resolutions не закрывают graph HRO question полностью.

## 6. N/PE closure

V3.1 исключил пересекающийся `REQUIREMENT_CHANGED` и invalid `REQUIREMENT_ADDED`, оставив минимальный набор:

- `SAME_REQUIREMENT`;
- `DIFFERENT_REQUIREMENT`;
- `INSUFFICIENT_EVIDENCE`.

Во всех Runs 1–3 оба passes выбрали `aiv3cand_d1e177883cc2cf17e0a91f2a / DIFFERENT_REQUIREMENT`. Verifier v3 во всех трёх runs вернул `VERIFIED_SELECTION`.

Ручная сверка PDF-растра подтвердила: LEFT показывает PE-шину и подписи подключений `К РЕ-шине ГРЩ2` / `К РЕ-шине ГРЩ3`, но не содержит требования предусмотреть N; RIGHT явно устанавливает `В панелях предусмотреть шины N и РЕ`. Эти фрагменты неэквивалентны. Решение не утверждает недоказанное отсутствие требования на всём LEFT и поэтому не подменяет invalid bounded-absence candidate.

Question status: `CLOSED_AI_STABLE`.

## 7. Measurement closure

Во всех прежних V3 Runs 1–3 Pass 1 и Pass 2 выбрали один и тот же `INSUFFICIENT_EVIDENCE`. Короткая подпись `Мультиметр` не доказывает, что устройство выполняет полный перечень функций RIGHT: ток, напряжение, частота, мощность и гармоники. Stable fail-closed evidence переиспользован, новых model calls не было. Status: `BLOCKED_MISSING_EVIDENCE`.

## 8. Reserve closure

Для `0 → 2` LEFT absence по-прежнему не ограничено доказанной полнотой поиска. `SUPPORTED_CHANGE_0_TO_2` в v3 имеет `INVALID / LEFT_ABSENCE_NOT_PROVEN`; варианты существующих несопоставленных линий не доказаны. Модель не вызывалась. Status: `BLOCKED_MISSING_EVIDENCE`.

## 9. ВРУ3 closure

Две LEFT rows `2х(5х95)` и `3х(5х120)` конкурируют за одну RIGHT row `2х…5х150`. Все три прежних v3 runs безопасно оставили выбор человеку; stable fail-closed candidate переиспользован, новых calls нет. Status: `BLOCKED_AMBIGUOUS_EVIDENCE`.

## 10. Mode closure

Сопоставление `Рабочий/Пожарный ↔ Аварийный/ПП` не маршрутизировалось AI. Status: `BLOCKED_POLICY`.

## 11. Model calls

Реально понадобилось 6 calls: 1 N/PE task × 2 unanimity passes × 3 independent cold runs. Stable v3 core из 14 решений не пересчитывался. Graph, measurement, reserve, ВРУ3 и mode group не отправлялись модели. Для сравнения, финальные v3 runs требовали 8 calls каждый и 24 calls суммарно.

## 12. HRO before

Baseline HRO = 6 mandatory interactions: один mode group и пять standalone questions. Frozen FAST signature до и после V3.1: `364709637f75bf15d4bd4623c41176f0d178ed58f4560cb6dbd667fdecf39ce8`.

## 13. HRO Run 1

HRO `6 → 5`. Closed IDs: `hquestion_a863b0dee0043c9d9413d10c`. Passes: DIFFERENT / DIFFERENT. Calls: 2. Runtime: 19,170 s. Cache disabled, hits 0, rejected 0.

## 14. HRO Run 2

HRO `6 → 5`. Closed IDs: `hquestion_a863b0dee0043c9d9413d10c`. Passes: DIFFERENT / DIFFERENT. Calls: 2. Runtime: 20,036 s. Cache disabled, hits 0, rejected 0.

## 15. HRO Run 3

HRO `6 → 5`. Closed IDs: `hquestion_a863b0dee0043c9d9413d10c`. Passes: DIFFERENT / DIFFERENT. Calls: 2. Runtime: 20,669 s. Cache disabled, hits 0, rejected 0.

## 16. Stable closed questions

Cross-run closed-question set совпал 3/3. Candidate совпал во всех шести passes. Стабильно закрыт 1 question ID: `hquestion_a863b0dee0043c9d9413d10c`. Stable HRO after = 5.

## 17. Manual audit

Для единственного автоматически закрытого вопроса проверены все девять closure conditions: полное решение required subproblem, grounding всех refs, отсутствие blocking condition, human priority, сохранение affected atomic ID, unanimity внутри каждого run, VERIFIED во всех runs, один candidate между runs и независимая семантическая сверка с растром. Engineer decision для `ureview_c89c1ab0ad4a650fbb22` остаётся `PENDING_REVIEW`, принятого human answer нет. Verdict: `SAFE_TO_CLOSE`.

## 18. Unsupported closures

Unsupported closures = 0. `UNSAFE_TO_CLOSE` = 0. Verifier rejects = 0. Ни один partial, missing-evidence, ambiguous-evidence или policy question не закрыт.

## 19. Runtime

Runs: 19,170 s + 20,036 s + 20,669 s = 59,875 s. Seconds per stable CLOSED HRO QUESTION = 59,875. Это примерно в 17,4 раза меньше суммарного runtime трёх v3 runs (1 043,774 s), поскольку stable core не пересчитывался.

## 20. Product verdict

Verdict A:

- одинаковый HRO result 5/5/5;
- один stable question closed;
- unsupported closures = 0;
- 6 narrowly scoped calls и 59,875 s total;
- manual audit = `SAFE_TO_CLOSE`;
- production artifacts unchanged.

Question Closure Layer удовлетворяет условиям controlled production rollout. Это не разрешение включать прежний атомарный AI v3 целиком: production должен использовать именно closure-first routing и строгий 2-pass × 3-run gate. В рамках задачи push/deploy/release не выполнялись, AI v2 и AI v3 остаются OFF.

## 21. Tests, artifacts и git status

Новые V3.1 tests: 12 passed. Связанная регрессия v31/v3/v2/materialization/reproducibility/HRO/production orchestrator: 198 passed. Полный `python -m pytest -q` остановился на collection пяти несвязанных gateway/PKI/transport suites: в окружении отсутствуют optional `grpc` и `google.protobuf`; тесты не запускались. `git diff --check` прошёл; `ruff` в окружении не установлен.

Локальные ignored artifacts находятся в `comparison/ai_analyst_v31/20260901_grsh_question_closure/`:

- `question_closure_contracts.json`;
- `closure_analysis.json`;
- `closure_ai_tasks.json`;
- `closure_run_1.json`, `closure_run_2.json`, `closure_run_3.json`;
- `closure_gate.json`;
- `manual_closure_audit.json`.

После документационного коммита ожидается чистый `main...origin/main [ahead 4]`. Push, deploy и release не выполнялись.

Из 6 вопросов инженера на текущих данных вообще можно автоматически решить 1.

V3.1 стабильно закрыл 1.

Для остальных не хватает не интеллекта модели, а доказательств/полномочий.

Можно ли после этого включать AI в production: ДА — только Question Closure Layer с описанными fail-closed gates; production в рамках этого эксперимента не включался.
