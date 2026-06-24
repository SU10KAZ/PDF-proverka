# Stage Comparison Pipeline V2 — read-only UI payload endpoint

**Дата:** 2026-06-10
**Статус:** controlled integration, шаг 1 — **только read-only endpoint**,
без frontend, без запуска анализа.
**Модули:**
- [backend/app/services/stage_comparison/pipeline_v2_payload_service.py](../backend/app/services/stage_comparison/pipeline_v2_payload_service.py) — discovery + сборка
- [backend/app/api/routers/stage_comparison.py](../backend/app/api/routers/stage_comparison.py) — handler (в конце файла)

## Endpoint

```text
GET /api/stage-comparison/pipeline-v2/{session_id}/ui-payload
GET /api/stage-comparison/pipeline-v2/{session_id}/ui-payload?pair_id={pair_id}
```

Отдаёт UI payload Pipeline V2 (контракт —
[stage_comparison_pipeline_v2_ui_contract.md](stage_comparison_pipeline_v2_ui_contract.md))
по УЖЕ ГОТОВЫМ артефактам. Закрыт портальной аутентификацией наравне с
остальными `/api/*` (middleware), отдельной авторизации не добавлялось.

## Что endpoint НЕ делает

- **НЕ запускает Pipeline V2** (dry-run не вызывается ни при каких условиях);
- **НЕ запускает** Qwen/Opus/LLM/фоновые jobs, не создаёт queue items;
- **НЕ пишет на диск**: без кеша, директории не создаются вовсе — сервис
  резолвит пути «чистыми» `paths.comparison_root_path()` /
  `sessions_root_path()` (без `mkdir`/`.gitkeep`, в отличие от создающих
  `comparison_root()`/`session_dir()`); GET к несуществующей сессии и даже к
  несуществующему `COMPARISON_ROOT` не материализует ни одной папки;
- **НЕ меняет** statuses, comparison_result, findings, runtime comparison data.

Дисковое I/O выполняется через `run_in_threadpool` — sync-тяжёлый handler в
event loop блокировал бы `/api/info` и провоцировал watchdog-restart.

## Path convention

Артефакты ищутся в дереве comparison-сессии (готовность к будущей
runtime-структуре; сейчас прогоны Pipeline V2 живут только в git-excluded
`diagnostics_pipeline_v2/` — endpoint их не видит и видеть не должен):

```text
comparison/sessions/<sid>/pipeline_v2/                      ← session-level
comparison/sessions/<sid>/pairs/<pid>/pipeline_v2/          ← pair-level (?pair_id=)
```

Имена файлов — стандартные имена dry-run:

| Файл | Роль |
|---|---|
| `pipeline_v2_ui_payload.json` | готовый payload — отдаётся как есть (приоритет) |
| `pipeline_v2_summary.json` | обязательный минимум для сборки на лету |
| `entity_diff_report.json` | опционально — полные карточки дельт |
| `delta_explanation_report.json` | опционально — critic-поля карточек |
| `left/right_graphic_descriptor_report.json` | опционально — weak_blocks_preview |

Discovery: запрос без `pair_id` смотрит session-level каталог; если его нет,
ответ `not_found` содержит `available_pairs` — список пар, у которых
артефакты есть (UI делает второй запрос с `?pair_id=`). Никакой авто-магии
«взять единственную пару» нет — выбор за вызывающим.

`session_id`/`pair_id` проходят `_safe_id` (как все пути stage_comparison):
traversal-куски вычищаются, полностью невалидный id → HTTP 400.

## Формат ответа (envelope)

```json
{
  "status": "ok | partial | not_found | error",
  "available": true,
  "session_id": "…", "pair_id": null,
  "source": "ready_payload | built_from_artifacts | null",
  "message": "…",
  "payload": { "kind": "stage_comparison_pipeline_v2_ui_payload", "…": "…" },
  "warnings": [],
  "artifacts_dir": "sessions/<sid>/pipeline_v2",
  "available_pairs": ["p1"]
}
```

| status | HTTP | Когда |
|---|---|---|
| `ok` | 200 | готовый payload с диска, либо собран из полного набора отчётов |
| `partial` | 200 | собран из summary, но diff/explanation отчётов не хватает или часть битая (payload деградирован, counters из summary) |
| `not_found` | 200 | артефактов нет (ни готового payload, ни summary); `available_pairs` подсказывает пары |
| `error` | 200 | артефакты есть, но непригодны (битый/не-объектный summary JSON, исключение builder'а) — fail-soft, не 500 |
| — | 400 | невалидный `session_id`/`pair_id` |

`not_found` — это HTTP 200 с JSON (контракт для портала из задачи), не 404:
отсутствие артефактов — нормальное состояние сессии, а не ошибка запроса.

Fail-soft детали:

- битый `pipeline_v2_ui_payload.json` (чужой `kind`, не-объект, нечитаемый
  JSON) не фатален: warning + пересборка из артефактов dry-run;
- `pipeline_v2_summary.json` с валидным JSON, но не объектом → `error` с
  warning (не маскируется под `not_found`);
- любое исключение чтения/сборки (включая патологический JSON) ловится на
  двух уровнях (`_read_json` + общий guard) → `status=error`, не HTTP 500;
- `NaN`/`Infinity` в числовых полях артефактов (json.loads их принимает, а
  сериализация ответа падает) санитизируются в `null` + warning
  `non-finite numeric values sanitized` — ответ всегда строгий JSON.

## Деплой

Endpoint попадает в live backend только с **backend restart** (uvicorn без
`--reload` держит модули в памяти). Перед рестартом обязательно:

1. проверить active/running jobs (audit queue, md-enrichment, unified
   analysis, pipeline queue, авто-матчинг) — рестарт прерывает `asyncio.Task`
   → `failed_interrupted`;
2. active jobs > 0 → **НЕ рестартить**;
3. рестарт только по явному разрешению пользователя.

## Rollback plan

Endpoint аддитивный: ни одна существующая логика Stage Comparison не
изменена (в роутер добавлены import + один GET-handler, новый сервис-модуль
никем больше не импортируется).

- **Код:** `git revert <commit>` (или checkout предыдущего commit в deploy
  worktree) + backend restart — поведение идентично сборке без фичи.
- **Данных для отката нет:** endpoint ничего не пишет, миграций нет.
- **Деградация без отката:** пока артефактов в `comparison/sessions/.../
  pipeline_v2/` нет, endpoint всем отвечает `not_found` — это безопасное
  «выключенное» состояние.

## Тесты

[tests/test_stage_comparison_pipeline_v2_readonly_endpoint.py](../tests/test_stage_comparison_pipeline_v2_readonly_endpoint.py)
(TestClient + tmp `COMPARISON_ROOT`, без сети/LLM): готовый payload; сборка из
артефактов; partial без отчётов; not_found (+ discovery пар, + ничего не
создаётся для неизвестной сессии); pair-level через `?pair_id=`; 400 на
невалидный id; сеть убита socket-monkeypatch'ем — сервис работает; dry-run
замокан AssertionError'ом — не вызывается; source-scan сервиса на
LLM/job-импорты; снапшот дерева до/после запросов идентичен (read-only);
битый summary/ready/diff → error/partial JSON, не 500; контракт 5 секций
(порядок, default_visible, display_hint, show_in_diagnostics) сохраняется.

## Следующий шаг

Frontend-панель в портале (читает этот endpoint) и/или запись артефактов
Pipeline V2 в `comparison/sessions/<sid>/pairs/<pid>/pipeline_v2/` отдельным
controlled-прогоном. Оба шага — отдельные задачи с отдельным подтверждением.
