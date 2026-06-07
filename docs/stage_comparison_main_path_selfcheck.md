# Stage Comparison — r6 self-check на основном пути сравнения

**Дата:** 2026-06-05
**Статус:** controlled (по умолчанию **ВЫКЛЮЧЕНО**, один флаг)
**Модуль:** [backend/app/services/stage_comparison/enriched_comparison.py](../backend/app/services/stage_comparison/enriched_comparison.py)

## Проблема

`run_enriched_comparison` отдаёт Opus два enriched MD и пишет вернувшиеся
`changes` в `comparison_result.json` **как есть** — без сверки с исходным
текстом. Галлюцинированная дельта («убрали ЩО-7», которого Qwen просто не
распознал, или придуманный номинал) попадает в отчёт наравне с реальной.

Полноценная evidence-верификация (`verify_change_evidence` + `_quote_grounded`
+ drop) существовала только в
[evidence_first_fallback.py](../backend/app/services/stage_comparison/evidence_first_fallback.py)
и срабатывала **лишь** для огромных пар (`too_large`, >600K символов) и только
при включённом `STAGE_COMPARISON_EVIDENCE_FIRST_FALLBACK_ENABLED`. Для
подавляющего большинства пар самопроверки не было.

Это реализация рекомендации **r6** («второй проход: процитируй значение из A и
B по каждой дельте, не смог — выкинь/пометь») и практичной версии **r3**
(сверка чисел) для проектов, где векторный текст-слой PDF недоступен (CAD-шрифты
ISOCPEUR/GOST) и текст берётся из Chandra MD.

## Что делает

После нормализации `changes` в done-ветке `run_enriched_comparison`, если
`STAGE_COMPARISON_SELFCHECK_ENABLED=true`, каждый change прогоняется через
`_apply_selfcheck`:

```text
change от Opus
  → verify_change_evidence(change, _norm_text(left_md), _norm_text(right_md), fb_cfg)
      (переиспользуется из evidence_first_fallback: exact-substring → token-overlap ≥ fuzzy_threshold)
  → evidence_verified == true?  → да: evidence_verified_by="quote"
                                → нет: числовой re-cite _numeric_grounded(...)
      → значение change (old/new_value + evidence quotes) есть в MD нужной стороны?
          → да: evidence_verified=true, evidence_verified_by="number"  (rescue)
          → нет: ungrounded
  → ungrounded changes:
      мягкий режим (default): requires_human_review=true + evidence_verified=false + selfcheck_note
      strict-режим (SELFCHECK_DROP_UNGROUNDED=true): change выкидывается
```

### Числовой re-cite (`_salient_numbers` / `_numeric_grounded`)

`_salient_numbers(text)` извлекает «значимые» числовые токены после канонизации
(`,`→`.`, `×`/`х`(кир)/`x`→`x`, NFKC+ё→е+lower): `5x10`, `5x185`, `0.5s`,
`160а`, `1000а`. Токены короче 3 символов отбрасываются как шум (номера пунктов,
позиции, единичные счётчики).

`_numeric_grounded` сверяет значение change со стороной MD по смыслу:
`old_value` / `evidence_left` → **left** (старая стадия), `new_value` /
`evidence_right` → **right** (новая). Это «процитируй конкретное значение»
рекомендации, специализированное под числа/сечения/номиналы, которые
token-overlap по тексту ловит слабо.

Re-cite только **спасает** дельту (делает проверку мягче), поэтому ложных
дропов он не добавляет — лишь снижает false-negative grounding'а.

## Режимы

| Режим | Флаг | Поведение с ungrounded |
|---|---|---|
| мягкий (рекомендуется для старта) | `SELFCHECK_DROP_UNGROUNDED=false` (default) | `requires_human_review=true` + `selfcheck_note`, change остаётся |
| строгий | `SELFCHECK_DROP_UNGROUNDED=true` | change удаляется |

Старт в мягком режиме осознан: `_quote_grounded` на коротких числовых дельтах
может давать false-negative, поэтому сначала помечаем (инженер видит флаг), а не
дропаем молча.

## Флаги (`.env`)

| Переменная | Default | Назначение |
|---|---|---|
| `STAGE_COMPARISON_SELFCHECK_ENABLED` | `false` | главный включатель self-check на основном пути |
| `STAGE_COMPARISON_SELFCHECK_DROP_UNGROUNDED` | `false` | `true` → дропать негрунтованные; `false` → помечать |

Пороги grounding (`fuzzy_threshold`, `min_quote_len`, low-conf gate)
переиспользуются из `FallbackConfig` (`load_fallback_config()`), чтобы
основной путь и too_large-ветка верифицировали одинаково. Их env-тюнинг —
`STAGE_COMPARISON_EVIDENCE_FIRST_*` (см.
[stage_comparison_evidence_first_fallback.md](stage_comparison_evidence_first_fallback.md)).

## Контракт результата

`comparison_result.json` (status=done) получает:

- поле `selfcheck` (или `null`, если выключено):
  ```json
  {"enabled": true, "mode": "mark|drop", "total": N, "verified": k,
   "rescued_by_number": r, "ungrounded": m, "dropped": d, "marked_review": x,
   "fuzzy_threshold": 0.6, "min_quote_len": 8}
  ```
- per-change: `evidence_verified` (bool), `evidence_verified_by`
  (`quote|number`), `evidence_scores`; для негрунтованных в мягком режиме —
  `requires_human_review=true` + `selfcheck_note`;
- при наличии ungrounded — warning в `warnings[]`.

Downstream совместим: `evidence_verified` уже читал `derive_quality_label`
([v2_review.py](../backend/app/services/stage_comparison/v2_review.py)) — теперь
ungrounded-дельты основного пути автоматически получают `quality_label=questionable`.

## Безопасность

- по умолчанию OFF → поведение идентично прежнему;
- fail-soft: исключение в верификации одного change → он считается verified и
  не теряется; исключение в `_apply_selfcheck` целиком → ловится, сравнение
  отдаёт changes без self-check + `selfcheck.error`;
- LM Studio / Qwen не задействованы (это Opus-фаза);
- логика верификации переиспользуется из fallback — дублирования нет.

## Деплой

uvicorn без `--reload` держит модуль в памяти — после правки нужен рестарт
backend, иначе running-сравнения используют старый код:

```bash
pkill -f "uvicorn backend.app.main"
uvicorn backend.app.main:app --host 0.0.0.0 --port 8081 --reload &
```

## Тесты

- [tests/test_stage_comparison_selfcheck.py](../tests/test_stage_comparison_selfcheck.py)
  — `_salient_numbers` / `_numeric_grounded` / `_apply_selfcheck` (mark/drop/empty);
- интеграция `run_enriched_comparison` (mark / drop / default-off) —
  [tests/test_stage_comparison_unified_analysis.py](../tests/test_stage_comparison_unified_analysis.py).

## Связанные файлы

- [backend/app/services/stage_comparison/enriched_comparison.py](../backend/app/services/stage_comparison/enriched_comparison.py) — `_apply_selfcheck`, `_salient_numbers`, `_numeric_grounded`, врезка в `run_enriched_comparison`
- [backend/app/services/stage_comparison/evidence_first_fallback.py](../backend/app/services/stage_comparison/evidence_first_fallback.py) — переиспользуемые `verify_change_evidence` / `_quote_grounded` / `_norm_text`
