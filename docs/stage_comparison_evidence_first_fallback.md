# evidence_first_s2_fallback — fallback для больших enriched MD пар

**Дата:** 2026-05-29
**Статус:** controlled / shadow (по умолчанию ВЫКЛЮЧЕНО)
**Модуль:** [backend/app/services/stage_comparison/evidence_first_fallback.py](../backend/app/services/stage_comparison/evidence_first_fallback.py)

## Проблема

`run_enriched_comparison` сравнивает `left_enriched.md` + `right_enriched.md`
через Opus. Если суммарный объём превышает
`STAGE_COMPARISON_ENRICHED_COMPARE_MAX_CHARS` (default **600 000**), пара
отдаёт `status=too_large`, `changes=[]` — выпадает из сравнения целиком.

Пример (проект КР2, пара `p2ef68719`): left 586K + right 279K = **865K** →
`too_large`. В сессии Балчуг таких пар три: ИОС-4.2 (639K), КР2 (865K),
ООС1 (1.19M).

## Почему именно эта стратегия (research 2026-05-29)

На КР2 прогнаны три fallback-стратегии, метрика — `confirmed_unique_changes`
(не raw count), каждое изменение верифицировано против исходного MD:

| Стратегия | raw | **confirmed_unique** |
|---|---:|---:|
| naive full (поднять лимит, скормить 865K одним куском) | 7 | 7 |
| compact single-pass (выкинуть прозу) | 9 | 8 |
| **scope-aware section split + evidence verification** | 38 | **13** |

Naive/compact теряют per-sheet структурный diff (lost-in-the-middle на 865K).
raw=38 у section-split обманчив — ~половина это description-variance из
low-confidence Qwen-блоков; её снимает evidence verification + дедуп. Подробный
разбор — в research-отчёте задачи.

**Ключевой факт:** для КР2 проблема не в прозе (чистый compact = 649K, всё ещё
над лимитом), а в **287K left-only разделов** (ПЗ 74K + АР 215K), у которых нет
правого аналога. Это packaging/scope-разница, а не инженерные изменения.
Структурная зона КР↔КР = 570K — сама помещается под лимит.

## Pipeline

```text
raw enriched MD
  → build_fact_index (left/right)          # детерминированный парсинг страниц/штампа/классов
  → build_scope_map                        # left_only / right_only / common scope_keys
  → deterministic_fact_diff                # штамп + scope-only разделы + состав листов (без LLM)
  → scope_aware_section_split              # выровненные чанки ≤ chunk budget
  → build_shared_header                    # штамп + материальная сводка в КАЖДЫЙ чанк
  → compare_chunk (per chunk, Opus)        # LLM сравнивает один раздел
  → verify_change_evidence                 # каждый quote сверяется с raw MD
  → merge_and_dedup                        # детерминированные + LLM, дедуп по сигнатуре
  → comparison_result.json (status=done, strategy=evidence_first_s2_fallback)
```

### 1. Canonical fact index

Парсит `## СТРАНИЦА N`, `**Лист:**`, `**Наименование листа:**`, `**Штамп:**`,
`block_id:` из QWEN-блоков. Каждая страница получает `section_class` и
`building_part`.

**Классификация (`_classify_section`)** — ключевой сигнал это **наличие
image-блоков**: текстовые/ПЗ-листы их не имеют (`img=0`), чертёжные имеют
(`img≥1`). Это устойчивее, чем ловить маркеры в прозе ПЗ (которая полна слов
«конструктивные», «монолитных»).

- `img=0` + длинный текст → `pz`; короткий → `other`;
- чертёж: `архитектурн/фасад` → `architectural`;
  `схема расположения / монолитных / плиты перекрытия` → `structural`
  (проверяется раньше «план», чтобы КР не утекали в АР);
  `разрез/сечение/узел/детали` → `sections_details`;
  `план N этажа / план кровли` → `architectural`; иначе → `structural`.

`building_part` извлекается из имени листа (`Корпус[аы] N` → нормализованный
`1,2` / `3,3.1` / `4`), применяется к structural и sections_details.

`scope_key = section_class|building_part`.

### 2. Scope map

`left_only` / `right_only` / `common` множества scope_keys (с сохранением
порядка появления для детерминированности).

### 3. Deterministic fact diff (без LLM)

Покрывает то, что LLM при section-split мис-фреймит:

- **штамп** — `stamp_changed`, если штампы различаются;
- **scope-only разделы** — раздел существует только с одной стороны → ОДИН
  grouped `section_changed` на раздел (не по листам — это убирает фрагментацию
  «24 удалённых листа»);
- **состав листов внутри общего scope** — added/removed sheet-names → grouped
  change.

Детерминированные изменения считаются grounded по построению (quote берётся из
реальных имён листов / штампа).

### 4. Scope-aware section split

Для каждого `common` scope_key выравнивает страницы left↔right и режет на чанки
`≤ chunk_max_chars` (default 200K). Если bucket больше бюджета —
`_split_pages_by_budget` делит на N≈ceil(total/budget) частей по объёму. Жёсткий
cap `max_chunks` (default 16) на число LLM-вызовов. **Каждый чанк помещается под
общий лимит 600K — safety-guard не отменяется.**

### 5. Shared global header

Компактный заголовок (`<SHARED_GLOBAL_HEADER>`): штамп OLD/NEW + материальная
сводка (строки с маркерами `класс бетона / арматура / W6 / F200 / А500` …),
прокидывается в КАЖДЫЙ чанк. Закрывает единственную дыру section-split:
cross-section факты (класс бетона из ПЗ, легенда) иначе теряются и change
мис-фреймится как «added». Cap `header_max_chars` (default 12K).

### 6. Per-chunk LLM compare

Тот же `SYSTEM_PROMPT` из `enriched_comparison.py` + chunk-specific user-prompt
(раздел + страницы + shared header + OLD/NEW). Provider — `ClaudeCodeProvider`
(`claude -p`, subscription). Парсинг/нормализация инжектятся из
`enriched_comparison` (`_extract_model_payload`, `_parse_model_json`,
`_normalize_change`), чтобы не дублировать логику. Каждый change получает
`provenance=llm_chunk` + `chunk_id`.

### 7. Original MD evidence verification

Каждый LLM-change сверяется с raw MD: `evidence_left/right.quote` и `evidence[]`
нормализуются (NFKC, ё→е, collapse whitespace, lower) и ищутся в исходном тексте
стороны — сначала exact-substring, затем token-overlap ≥ `fuzzy_threshold`
(default 0.6). Если ни одна сторона/якорь не подтверждены:

- `drop_ungrounded=true` (default) → change выкидывается (это снимает
  hallucination/description-variance);
- `drop_ungrounded=false` → остаётся, но `requires_human_review=true`.

Добавляет `evidence_verified` (bool) и `evidence_scores`.

### 8. Merge + dedup

Объединяет детерминированные + проверенные LLM. Сигнатура дедупа =
`type | топ-токены title | номера листов из approx_location`. Детерминированные
имеют приоритет (заменяют LLM-дубль). Кросс-чанковые дубли (один и тот же факт
из shared header в нескольких чанках) схлопываются.

## Включение (controlled rollout)

По умолчанию **выключено** — `too_large` ведёт себя как раньше.

```env
# Включить fallback (иначе too_large → changes=[] как прежде)
STAGE_COMPARISON_EVIDENCE_FIRST_FALLBACK_ENABLED=true

# Тюнинг (необязательно, показаны defaults)
STAGE_COMPARISON_EVIDENCE_FIRST_CHUNK_MAX_CHARS=200000
STAGE_COMPARISON_EVIDENCE_FIRST_MAX_CHUNKS=16
STAGE_COMPARISON_EVIDENCE_FIRST_HEADER_MAX_CHARS=12000
STAGE_COMPARISON_EVIDENCE_FIRST_MIN_QUOTE_LEN=8
STAGE_COMPARISON_EVIDENCE_FIRST_FUZZY_THRESHOLD=0.6
STAGE_COMPARISON_EVIDENCE_FIRST_DROP_UNGROUNDED=true
```

Срабатывает только в too_large-ветке `run_enriched_comparison`, только если
флаг включён И provider (Claude Code) доступен. Иначе — обычный `too_large`.

## Контракт результата

`comparison_result.json` пишется как обычно, но с дополнительными полями:

- `status=done`, `strategy=evidence_first_s2_fallback`, `fallback=true`;
- `diagnostics` — scope_map, список чанков, per-chunk статусы, счётчики
  (`deterministic_changes`, `llm_changes_raw`, `llm_changes_dropped_ungrounded`,
  `duplicates_removed`, `final_changes`);
- каждый change: `provenance` (`deterministic` | `llm_chunk`),
  `evidence_verified`, `evidence_scores`, `chunk_id` (для LLM).

Downstream (`unified_findings`, UI) видит обычный `status=done` со списком
`changes` — формат полностью совместим.

## Безопасность

- pipeline никогда не бросает наружу: при ошибке provider'а чанк помечается
  warning, детерминированные изменения всё равно возвращаются; при фатальной
  ошибке — `status=error` с диагностикой, пара не падает;
- shadow по умолчанию: без флага — поведение идентично прежнему `too_large`;
- LM Studio / Qwen не трогаются (это Opus-фаза, локальные модели не
  задействованы).

## Operator runbook

1. Включить флаг в `.env`, перезапустить backend.
2. Запустить unified-analysis на паре с `too_large` (force_compare=true).
3. Проверить `comparison_result.json`:
   ```bash
   jq '{status, strategy, n: (.changes|length), diag: .diagnostics |
        {det: .deterministic_changes, raw: .llm_changes_raw,
         dropped: .llm_changes_dropped_ungrounded, dups: .duplicates_removed,
         final: .final_changes}}' \
     comparison/sessions/<sid>/pairs/<pid>/enriched_comparison/comparison_result.json
   ```
4. Если `llm_changes_dropped_ungrounded` подозрительно велик — проверить, что
   enriched MD не битый и Opus получает читаемый вход; при необходимости
   снизить `FUZZY_THRESHOLD` до 0.5.
5. Откат — снять флаг, перезапустить backend.

## Связанные файлы

- [backend/app/services/stage_comparison/evidence_first_fallback.py](../backend/app/services/stage_comparison/evidence_first_fallback.py)
- [backend/app/services/stage_comparison/enriched_comparison.py](../backend/app/services/stage_comparison/enriched_comparison.py) — too_large-ветка
- [tests/test_stage_comparison_evidence_first_fallback.py](../tests/test_stage_comparison_evidence_first_fallback.py)
