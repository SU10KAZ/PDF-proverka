# Консолидатор ProjectChange V1 — этап «Сведение дублей»

Консолидатор собирает раздробленные карточки ProjectChange V3 в человеческие инженерные события. Одно проектное решение часто попадает в несколько регионов Mapper: разные здания, листы, текст и чертёж. Консолидатор — последний этап прогона сравнения пары: идёт сразу после завершённого V3 и пишет отдельный результат «Итоговые».

Он ничего не меняет:
- результат V3 и Dedupe остаются авторитетными;
- `runs/`, `current_run.json`, Human Mapping и каталог не трогаются;
- автоматического продвижения нет.

Код: `backend/app/services/project_change_consolidator/`. Тесты: `backend/tests/project_change_consolidator/`.

## Конвейер

```
завершённый прогон V3 (или замороженный пакет с явными sha)
→ канонические идентичности: карточка = projectchange_id, подсказка = {region_id, hint_id}
→ префильтр prefilter/1 (детерминированный, высокая полнота, рёбра только между регионами, кластеры 2–8)
→ один вызов на кластер + пакет одиночек (stage CONSOLIDATE, images=[], 0 повторов)
→ сырой ответ пишется ДО проверки
→ валидатор S/R/C/K/P/M (провал → исходные карточки кластера или группы) → раскрытие ссылок
→ X1–X6 → неизменяемый прогон тени
```

**Модель решает только:**
- MERGE / KEEP_SEPARATE / UNCERTAIN;
- канал: ENGINEERING / DOCUMENTARY / REVIEW;
- существенность подсказок;
- текст пересборки.

**Все факты берутся детерминированно из источника:** значения параметров (через плейсхолдеры `{{Pn.old|new|unit}}`), доказательства, места и провенанс конфликтов.

Каждое число и обозначение в новой прозе должно встречаться у членов группы или в привязанных подсказках (C4). Составная карточка — барьер: не сливается и уходит в REVIEW.

## Хранилище тени

`<production_dir>/projectchange_consolidator_shadow/<source_run_id>/<consolidator_run_id>/` (`storage.py`):
- только добавление;
- атомарная запись с чтением обратно;
- манифест пишется последним;
- после завершения файлы только для чтения.

Результат выдаётся только COMPLETED-прогоном, привязанным к точному sha исходного результата (`ShadowStore.load_completed`).

Источник, которого нет среди прогонов пары (замороженный исследовательский прогон), кладётся рядом явным импортом (`source_import.py`) в `.../<source_run_id>/SOURCE_IMPORT/`:
- копии байт в байт с квитанцией;
- привязка по sha PDF текущей пары.

## Этап прогона «Сведение дублей»

`stage.py`, вызов — `production_orchestrator.run_production_comparison` сразу после V3, под тем же замком пары и с тем же токеном отмены («Остановить анализ» останавливает и этап).

- Запускается только для V3 со статусом COMPLETED/REVIEW и `reason_code=v3_completed`; сбой этапа прогон V3 не меняет.
- Модель — `PROJECTCHANGE_CONSOLIDATOR_PROVIDER` (по умолчанию Opus 5.5 xhigh) через открытый шлюз V3; шлюз закрыт → `SKIPPED/provider_unavailable`, вызовов нет.
- Ход этапа — `production/projectchange_consolidation_stage.json` (`RUNNING` → `COMPLETED` / `FAILED` / `SKIPPED` / `CANCELLED`, `calls_done` из `calls_total`); `GET .../production/state` отдаёт его как `consolidation`. `RUNNING` без живого потока в процессе портала — `INTERRUPTED`.
- Повтор для текущего прогона: `POST .../production/consolidation` (фон, замок пары; идущий анализ — 409). В портале — кнопка «Свести дубли» в баннере вкладки «Изменения проекта».
- Если тестовый провайдер V3 задан, этап не идёт (тесты прогона не вызывают модель).

## Флаги

| Переменная | По умолчанию | Смысл |
|---|---|---|
| `PROJECTCHANGE_CONSOLIDATION_STAGE` | включено | `0` — не выполнять этап «Сведение дублей» после прогона V3 |
| `PROJECTCHANGE_CONSOLIDATOR_PROVIDER` | `claude_code_cli:claude-opus-5-5:xhigh` | `claude_code_cli:<модель>:<усилие>`; другой транспорт — отказ (`bad_provider_config`) |
| `PROJECTCHANGE_CONSOLIDATOR_MAX_CALLS` | 40 | предел вызовов одного этапа; выше — `SKIPPED/call_plan_exceeded` до первой отправки |

Модель вызывается только при открытых воротах инференса V3.

## UI «Исходные | Итоговые»

Вкладка изменений пары, компонент `frontend/static/js/project-change-consolidation.js`.

Эндпоинты только для чтения, 0 вызовов модели:

```
GET /api/stage-comparison/sessions/{s}/pairs/{p}/consolidated
GET /api/stage-comparison/sessions/{s}/pairs/{p}/consolidated/{source_run_id}/{consolidator_run_id}
GET /api/stage-comparison/sessions/{s}/pairs/{p}/consolidated/{source_run_id}/evidence/{evidence_id}/crop
```

- «Исходные» по умолчанию — обычный список текущего прогона.
- Если прогон-источник консолидации не текущий, «Исходные» показывают карточки этого источника.
- Id доказательств — схема V3 с R-04 (`hint:<region_id>/<hint_id>`), поэтому сводная карточка открывает ту же страницу, bbox и блок, что и карточка-член.

## Офлайн-запуск по замороженному пакету

```bash
python scripts/consolidator_shadow_frozen.py --bundle SPEC.json --out DIR --plan-only --max-calls 12
python scripts/consolidator_shadow_frozen.py --bundle SPEC.json --out DIR --provider fake:keep --max-calls 12
# живой: только явно, с sha замороженного входа из --plan-only
python scripts/consolidator_shadow_frozen.py --bundle SPEC.json --out DIR \
    --provider claude_code_cli:claude-opus-5:xhigh --allow-live --expect-freeze <sha> --max-calls 12
```

Первый живой эксперимент и его оценка: `/home/coder/auditmanager/corpus-audits/20260924_projectchange_consolidator_v1_execution/`.
