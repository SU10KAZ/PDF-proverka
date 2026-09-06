# Sheet Matcher v4 за флагом — чистая production-интеграция

**Дата:** 2026-09-06. **Статус:** кандидат, флаг по умолчанию выключен, в прод не выкатывался.

## Что это

Sheet Matcher v4 — четыре доказанных исправления одного алгоритма сопоставления
листов (`sheet_matcher.py`), которые исследование программы качества сравнения
V002 (audit root `corpus-audits/20260906_v002_comparison_quality`, вердикт A)
проверило на 23 парах объекта 272 и на замороженном holdout:

| # | Исправление | Где | Дефект v3, который оно закрывает |
|---|---|---|---|
| 1 | Паспорт листа из **тела страницы Markdown** (`sheet_passport.py`, режим MERGE, общедокументные термины удаляются по частоте) | `production_orchestrator._production_sheet_indexes` | паспорт строился только из строк `**Summary:**/**Entities:**` (847 из 3039 страниц корпуса) → 782 из 1165 левых листов DEV без единого факта, лист UNKNOWN при любом кандидате |
| 2 | Окно pass-1 = deep top-K: те же сигналы и веса, что у deep; кандидат без наблюдаемых фактов не занимает место в окне | `sheet_matcher._pass1_v4`, `_pass1_sort_key_v4` | pass-1 не считал topology (вес 0,24 в deep); страница без фактов набирала 1,0 на близости номеров и занимала 83 % окна |
| 3 | Ось фасада/разреза: «Фасад **в осях** 3.К-1.А» — ось `3.к-1.а`, а не предлог «в» | `sheet_identity._FACADE_RE_V4 / _SECTION_RE_V4` | 26 страниц АР2 получали ось «в», сливались в один ключ штампа и уходили в неоднозначность |
| 4 | Страж неоднозначности HIGH: недоминируемая HIGH-альтернатива понижает пару до POSSIBLE (вопрос, называющий обоих кандидатов) | `sheet_matcher.match_sheets` (блок `_undominated_high_alternatives`) | без стража паспорт давал 10 ложных HIGH на почти одинаковых таблицах |

Числа исследования (23 пары, FAST, 0 обращений к модели): UNKNOWN 2137→132,
HIGH 53→296 (эффективные группы 53→288), ложных HIGH 0, ссылки уровня A
(ручные `user_corrected`) поддержаны 0→10 DEV / 0→8 holdout.
**Но** вопросов 121→383 и «на проверку» 722→2234 (изменений 841→841): листов
сопоставлено больше, а значит больше несвязанных фрагментов текста попадает в
сравнение. Это не ошибка матчера, но глобально включать v4 нельзя, пока
downstream-анализ (TEXT REVIEW NOISE V1) не проведён на теневых артефактах.

## Флаги (читаются в момент вызова, `sheet_matcher_flags.py`)

| Переменная | Default | Смысл |
|---|---|---|
| `STAGE_COMPARISON_SHEET_MATCHER_V4_ENABLED` | `false` | боевой алгоритм. `false` → v3, результат **побайтово** равен базе (`algorithm_version = production-sheet-matcher.v3`, тот же `input_signature`, никаких новых ключей). `true` → замороженная v4 целиком (четыре исправления вместе; по отдельности они не проверялись) |
| `STAGE_COMPARISON_SHEET_MATCHER_V4_SHADOW_ENABLED` | `false` | тень: после боевого v3 для allowlist-пары считается ещё и v4 (только индекс + матчер, секунды, без второго сравнения) и пишется артефактом `sheet_matcher_v4_shadow.json` + заметкой `state.sheet_matcher_v4_shadow` |
| `..._SHADOW_PAIR_ALLOWLIST` / `..._SHADOW_RUN_ALLOWLIST` | пусто | точные `pair_id` / `run_id` через запятую. **Пустые списки = никто**; одним флагом тень на все пары не включить |

Тень не меняет ни `sheet_relations`, ни область листов, ни вопросы, ни синтез,
ни отчёт. Тень не считается, если боевой алгоритм уже v4 (`V4_IS_PRODUCTION`), и
в PAGE-режиме (область выбрана пользователем). Любой сбой тени пишется как
`SHADOW_FAILED` и проглатывается.

Явный аргумент важнее флага: `match_sheets(..., algorithm=ALGORITHM_V4)`,
`_run_sheet_matcher(pair, algorithm=...)`, `_production_sheet_indexes(pair,
sheet_matcher_v4=True)`, `parse_stamp_title(text, axis_preposition=True)`.
Индекс и матчер всегда выбираются ОДНИМ алгоритмом (`_run_sheet_matcher`),
иначе паспорт v4 попал бы в v3 или наоборот.

## Контракт артефактов

- `sheet_relations.algorithm_version` = `production-sheet-matcher.v3` | `.v4`;
  `input_signature` включает версию → при переключении флага все пары
  считаются устаревшими и пересчитываются при следующем запуске (~10 с/пару в
  FAST). Исторические сессии не трогаются.
- v4 добавляет в `diagnostics` ключ `ambiguous_high_demoted`, в pass-1 —
  `substantive_observed` и сигнал `topology`, в relation — reason code
  `high_candidate_ambiguous` и `conflicting_evidence[].kind =
  UNDOMINATED_HIGH_ALTERNATIVE`. При v3 этих ключей нет.
- `sheet_matcher_v4_shadow.json`: `production{algorithm_version,
  input_signature, relation_counts}`, `shadow{...}`,
  `left_page_status_transitions` («HIGH->POSSIBLE»: n), полный v4
  `sheet_relations` для офлайн-анализа, `gate`, `affects_production=false`,
  `uses_model=false`.

## Регресс (чистая интеграция, worktree от `origin/main` = `f4ab15fc`)

Отчёты — `corpus-audits/20260906_v002_clean_integration/reports/`:
флаг OFF — 23/23 пары побайтово (без `generated_at`) равны Golden Baseline;
флаг ON — 23/23 побайтово равны замороженному S3; holdout — тот же вердикт A;
полный replay конвейера — см. `CLEAN_INTEGRATION_REPORT.md` там же.

## Откат

- Флаг выключен по умолчанию → откат не нужен: прод считает v3.
- Если флаг включали: `STAGE_COMPARISON_SHEET_MATCHER_V4_ENABLED=false` +
  рестарт бэкенда; артефакты, посчитанные v4, станут устаревшими по
  `input_signature` и пересчитаются v3. Потерь данных нет.
- Полный откат кода — переключение `current` на прежний релиз
  (`deploy_center_release.py --release <прежний>`).

## Что НЕ входит

- Опция `NATIVE_PDF_TEXT` паспорта (есть в модуле, не подключена — на DEV слабее MD).
- Изменение окна `top_k` (остаётся 5).
- Показ альтернатив стража в SHEET-вопросе, вопрос по односторонним «вытесненным» HIGH.
- Правки TEXT-стадии/синтеза (review-шум) — отдельный трек TEXT REVIEW NOISE V1.
- Entity Matcher v3 — отдельный коммит (`docs/stage_comparison_entity_matcher_v3.md`).
