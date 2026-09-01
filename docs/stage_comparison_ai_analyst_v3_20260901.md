# AI Analyst v3 — bounded selector: отчёт эксперимента

Дата: 2026-09-01. Acceptance-пара: `p11c797af90` (ГРЩ). Финальный режим эксперимента: `unanimity`, cache disabled. Production FAST, HRO, UI и normative pipeline не изменялись.

## 1. HEAD before

Исходный HEAD: `512de39b570484899204d6f4d8506ac3a68e5d30` (`Make GRSH provenance test release-safe`).

## 2. Commits

Реализация и тесты: `cb464d77` (`Add bounded AI analyst v3 experiment`). Отчёт фиксируется отдельным документационным коммитом; его hash приводится в финальном handoff.

## 3. Candidate Factory architecture

Backend читает frozen production artifacts, строит компактный Evidence Catalog, формирует конечные selector tasks, детерминированно создаёт и сортирует candidates, вычисляет `candidate_id`, `candidate_signature`, `task_signature` и общий `candidate_set_signature`. Модель не создаёт факты и не расширяет множество вариантов. На acceptance-паре получена идентичная во всех runs фабрика: 61 task, 259 candidates, signature `d6587a56c41efbce75ecd5612fb5c97341605e8f756aa2cc650d831d5a43133a`.

## 4. Candidate schemas

Candidate содержит `candidate_id`, `task_id`, `candidate_type`, `summary`, `left_refs`, `right_refs`, `values`, `units`, `entity_refs`, `graph_refs`, `table_refs`, `text_refs`, `deterministic_features`, `proof_requirements`, `eligibility`, `prefilter_reasons`, `resolution_effect`, `materialization` и `candidate_signature`. Eligibility имеет только три состояния: `ELIGIBLE_FOR_AUTO_RESOLUTION`, `ADVISORY_ONLY`, `INVALID`.

## 5. Типы selector tasks

Фабрика построила: 27 `TABLE_ROW_IDENTITY`, 14 advisory-only `MODE_MAPPING`, 10 `CHANGE_INTERPRETATION`, 7 `ENTITY_IDENTITY`, 2 `TEXT_EQUIVALENCE`, 1 `LABEL_CONFLICT`. До модели дошли 43 задачи; mode mapping и задачи без auto-resolvable semantic choice остались backend/human-only.

## 6. Candidate Evidence Bundle

Каждый вариант несёт только заранее привязанные refs и необходимые данные: строки и соседей таблицы, узлы и инцидентные рёбра графа, точные текстовые фрагменты, значения и единицы, deterministic features, proof obligations и immutable signature. Полный Evidence Catalog в prompt не отправляется. В acceptance-фабрике: 188 eligible, 60 advisory-only и 11 invalid candidates.

## 7. Model output schema

Ответ строго ограничен массивом `selections`; в каждой записи допустимы только `task_id`, enum `selected_candidate_id`, `confidence_bucket` и диагностический `optional_short_reason` до 240 символов. Полей для evidence refs, claims, значений или единиц нет. Модель: `gpt-5.6-sol`, reasoning `low`, только subscription Codex CLI gateway.

## 8. Verifier v3

Verifier проверяет принадлежность candidate задаче, selectable binding, неизменность signature, существование всех refs, grounding кабельных значений, grounding единиц, proof obligations, frozen input signature и приоритет решений инженера. Ответ с чужим/придуманным ID, изменённым input, неполным proof или human-protected target отклоняется. Materialization использует только prebuilt candidate и существующий детерминированный pipeline; prose модели не является доказательством.

## 9. Deterministic prefilter

До selector отсекаются несовместимые стороны, секции, типы строк и сущностей, failed proof obligations и любые bounded-absence утверждения без доказанной полноты поиска. Mode mapping остаётся advisory-only. Для `0 → 2` резервных линий auto-candidate создан для трассируемости, но имеет `INVALID / LEFT_ABSENCE_NOT_PROVEN` и не может быть выбран для публикации.

## 10. Deterministic winners

Если после prefilter остаётся один доказанный несмысловой вариант, backend выбирает его без вызова модели. Механизм покрыт тестом. На этой acceptance-паре deterministic winners = 0: все auto-resolvable решения действительно требовали semantic ranking.

## 11. Batching

Задачи группируются по `graph_entity_identity`, `table_feeder_identity`, `text_change_interpretation`; table-группа детерминированно разделена на два чанка. На pass получилось четыре batch: 8 graph tasks (53 450 bytes), 14 table tasks (468 712), 13 table tasks (453 998), 8 text/change tasks (87 599). Shared compact Sheet Context передаётся один раз на batch. Лимит task payload = 480 000 bytes; абсолютный prompt limit = 800 000 bytes, превышение fail-closed останавливается до model call.

## 12. Single vs two-pass result

Controlled single: 27 verified, после аудита 15 supported и 12 partially supported, 15 product decisions, HRO reduction 1, 4 calls, 185,938 s, unsafe 0. Controlled unanimity: 14 verified/supported, 14 product decisions, HRO reduction 0, 18 disagreements, 8 calls, 348,830 s, unsafe 0. Unanimity выбран из-за fail-closed поведения и отсечения 12 слабых table-пар single-pass.

Один дополнительный unanimity-контроль с двумя исчерпанными `TRANSIENT` ответами сохранён только как диагностика и не включён в acceptance: система корректно вернула затронутые задачи человеку.

## 13. Old v2 unstable cable cases

V3 построил конечные варианты для всех шести forensic cases:

- `uchg_1bf23921e9dbc1df9612`: `FORMATTING_ONLY_3_TO_3`, стабилен 3/3 runs;
- `uchg_79d5e6aa07c1e8df4ffe`: `FORMATTING_ONLY_2_TO_2`, стабилен 3/3;
- `uchg_86dd32aa72abda4b7af5`: `REAL_CHANGE_2_TO_3`, стабилен 3/3;
- `uchg_882a967353baf40a13d1`: конкурирующие `FORMATTING_ONLY_2_TO_2` и `REAL_CHANGE_3_TO_2`; selector стабильно выбрал `INSUFFICIENT_EVIDENCE`, оставив вопрос человеку;
- `uchg_d1d843ad9e789eeabb13`: `REAL_CHANGE_2_TO_3`, стабилен 3/3;
- `uchg_dc6ba52a9e78520fd415`: `REAL_CHANGE_2_TO_3`, стабилен 3/3.

## 14. ВРУ3 benchmark

Для двух LEFT rows фабрика построила две конкретные пары с одной RIGHT row: `2х(5х95) ↔ 2х...5х150` и `3х(5х120) ↔ 2х...5х150`, плюс `NONE` и `INSUFFICIENT_EVIDENCE`. Runs 1–2 единогласно оставили `INSUFFICIENT`; Run 3 разошёлся между `INSUFFICIENT` и `NONE`. Во всех трёх runs задача осталась инженеру. Отдельный кабельный case ВРУ3 РП1 `2 → 2` стабилен и доказан.

## 15. N/PE benchmark

Backend предложил `SAME_REQUIREMENT`, `DIFFERENT_REQUIREMENT`, `REQUIREMENT_CHANGED`, invalid bounded-absence `REQUIREMENT_ADDED` и `INSUFFICIENT`. Runs 1–2 разошлись между `DIFFERENT_REQUIREMENT` и `INSUFFICIENT`; Run 3 дважды выбрал фактически поддержанный `DIFFERENT_REQUIREMENT`. Из-за межзапусковой нестабильности HRO стал 6/6/5 — это единственная причина verdict B.

## 16. Measurement requirement benchmark

Для «Мультиметр» против требования к многофункциональным измерительным приборам построено то же конечное семейство text candidates. Все шесть passes выбрали `INSUFFICIENT_EVIDENCE`: короткая подпись не доказывает полный состав измеряемых показателей. Вопрос стабильно остался человеку.

## 17. Reserve-lines benchmark

Для `0 → 2` созданы `SUPPORTED_CHANGE_0_TO_2`, `EXISTING_LINES_NOT_MATCHED`, `DIFFERENT_ENTITY`, `INSUFFICIENT_EVIDENCE`. Первый вариант invalid из-за `LEFT_ABSENCE_NOT_PROVEN`; задача не отправлялась модели как auto-resolvable и во всех runs осталась человеку.

## 18. Graph correspondence benchmark

Общий расплывчатый вопрос разложен на 7 entity tasks. Все семь точных соответствий стабильны во всех трёх runs и вручную supported: ДР1-ХМ1, ДР2-ХМ2, две пары ШУ-ХВС/ЩНО с общей canonical identity ШНО и QF13/20A, две compensation groups и service group BUS2. Конфликт исходных подписей ШУ-ХВС/ЩНО не скрывается и отдельно остаётся документным фактом.

## 19. FAST baseline

Frozen FAST signature: `364709637f75bf15d4bd4623c41176f0d178ed58f4560cb6dbd667fdecf39ce8`. Baseline: status `COMPLETED`, automatic findings 41, engineering review 6, document inconsistencies 11, insufficient evidence 19, deterministic changes 54, review items 23, Stage 7 rows 77, runtime 3,161 s, model calls 0.

## 20. HRO baseline

Baseline HRO = 6 mandatory interactions: 1 mode group и 5 standalone questions (резервные линии, graph correspondence, measurement, N/PE, ВРУ3). Atomic Stage 7 targets = 77. Production HRO остаётся эталоном и экспериментом не перезаписан.

## 21. V3 Run 1

8 successful calls, cache 0/0/0, runtime 357,404 s. Raw: 18 verified, 43 human-required, 14 disagreements. Audit: 14 supported, 4 partially supported, 0 unsupported. После gate: 14 product decisions, 5 materialized findings, Stage 7 = 76, HRO = 6, unsafe = 0.

## 22. V3 Run 2

8 successful calls, cache 0/0/0, runtime 347,244 s. Raw: 14 verified, 47 human-required, 18 disagreements. Audit: 14 supported, 0 partial, 0 unsupported. После gate: 14 product decisions, 5 materialized findings, Stage 7 = 76, HRO = 6, unsafe = 0.

## 23. V3 Run 3

8 successful calls, cache 0/0/0, runtime 339,126 s. Raw: 19 verified, 42 human-required, 17 disagreements. Audit: 15 supported, 4 partially supported, 0 unsupported. После gate: 15 product decisions, 5 materialized findings, Stage 7 = 76, HRO = 5, unsafe = 0.

## 24. Pairwise product overlap

Run1/Run2: 14/14 = 100%. Run1/Run3: 14/15 = 93,3333%. Run2/Run3: 14/15 = 93,3333%. Minimum = 93,3333%, что выше целевых 90%. Prose модели не учитывался.

## 25. All-run stable core

Точное ядро содержит 14 task/candidate/product triples: 7 graph relations (`aiv2_graph093...`, `aiv2_graph1b...`, `aiv2_graph77...`, `aiv2_graph901...`, `aiv2_graphae...`, `aiv2_graphb09...`, `aiv2_graphdc...`), document conflict `dinc_4aa7caeb4bbc`, row relation `etrow_73e5b5173ec6` и 5 cable interpretations (`uchg_1bf...`, `uchg_79d...`, `uchg_86dd...`, `uchg_d1d...`, `uchg_dc6...`). N/PE в stable core не входит.

## 26. Human interactions before/after

HRO по runs: 6→6, 6→6, 6→5. Стабильно снято 0 из 6 взаимодействий; обязательное условие одинакового HRO count не выполнено. При этом Stage 7 стабильно 77→76 за счёт безопасной детерминированной переработки доказанных findings, а не за счёт скрытия вопросов.

## 27. Model calls

Финальные runs: 8 + 8 + 8 = 24 реальных model calls, cache disabled. Controlled comparison: single 4, unanimity 8. Runtime check подтвердил subscription `codex-cli 0.151.0-alpha.7.2`, structured output, sandbox/isolation, отключённые tools/plugins/browser и отсутствие утечки secret environment names.

## 28. Runtime

Runs: 357,404 s; 347,244 s; 339,126 s. Всего 1 043,774 s (17 мин 23,774 с), среднее 347,925 s. Controlled single: 185,938 s; controlled unanimity: 348,830 s.

## 29. Manual audit

Проверены все 51 raw auto-resolved result трёх финальных runs: 43 `SUPPORTED`, 8 `PARTIALLY_SUPPORTED`, 0 `UNSUPPORTED`. Partial table-pairs не материализованы и не снимают human interaction. Audit files имеют status `COMPLETE`.

## 30. Unsupported

`unsupported_materialized = 0` во всех трёх runs; verifier rejected selections = 0; FAST provenance/status violations = 0; production FAST artifacts mutated = false.

## 31. Tests

Релевантная регрессия: 163 passed (`ai_v3`, неизменённые `ai_v2`, v2 materialization/reproducibility и production orchestrator). Отдельно v3: 20 passed. Полный `python -m pytest -q` остановился на collection пяти несвязанных gateway/PKI suites из-за отсутствующих в окружении `grpc` и `google.protobuf`; тесты не начинались, дефекта v3 этот запуск не показал. `git diff --check` прошёл.

## 32. Product verdict

Вердикт B. Архитектура безопасна: candidate space закрыт, evidence только backend, verifier fail-closed, human priority сохранён, unsafe = 0, product overlap 93,33%. Но обязательная стабильность HRO не достигнута (6/6/5), а стабильная экономия из шести вопросов равна нулю. Controlled production rollout сейчас не рекомендован. Следующий эксперимент должен стабилизировать класс text-equivalence/N/PE без хардкода task IDs и повторить тот же gate.

## 33. Git status и артефакты

После документационного коммита ожидается чистый `main...origin/main [ahead 2]`. Push, deploy и release не выполнялись. V3 по умолчанию выключен отдельным `STAGE_COMPARISON_AI_ANALYST_V3=false`; v2 и production orchestrator не изменены. Основные локальные артефакты: `comparison/ai_analyst_v3/20260901_grsh_v3_cold_run{1,2,3}/`, `comparison/ai_analyst_v3/20260901_grsh_reproducibility_gate.json`, `comparison/ai_analyst_v3/20260901_grsh_controlled_single_vs_unanimity.json`.

В AI Analyst v3 модель теперь не придумывает доказательства, а выбирает только из вариантов, подготовленных системой.

Из 6 текущих вопросов инженера V3 стабильно снял 0.

Совпадение продуктовых решений между тремя холодными запусками: 93,33%.

Можно ли выкатывать AI Analyst v3: НЕТ.
