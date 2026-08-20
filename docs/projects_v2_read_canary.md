# projects_v2 — opt-in read-only endpoint canary

**Дата:** 2026-06-16
**Статус:** limited canary (по умолчанию **ВЫКЛЮЧЕНО**, один флаг). НЕ full cutover.
**Модуль:** [backend/app/services/storage/read_canary.py](../backend/app/services/storage/read_canary.py)

## Зачем

После успешного read-only canary через shadow API нужно проверить чтение
`projects_v2` **на обычных production-endpoint'ах**, но без переключения обычных
пользователей и UI. Решение — **opt-in** слой на 1–2 безопасных GET-endpoint'ах:
обычный запрос читает legacy как прежде, а явный opt-in (только при включённом
флаге) читает `projects_v2`.

Это НЕ full cutover: `AUDIT_STORAGE_BACKEND` остаётся `legacy`, основной
read-path не подключается к `projects_v2`, UI не меняется.

## Подключённые endpoint'ы (7)

Первый этап (2):

| Endpoint | legacy (без opt-in) | canary (opt-in + флаг) |
|---|---|---|
| `GET /api/projects` | список legacy-проектов | список документов `projects_v2` (`v2_projects_list`) |
| `GET /api/findings/{project_id}` | legacy findings | findings/counts документа (`v2_findings`) |

Расширение (ещё 5 → 4 подключены, 1 not_ready):

| Endpoint | canary-билдер | статус |
|---|---|---|
| `GET /api/projects/{project_id}` | `v2_project_details` (snapshot: status, findings_count, 01/02/03, pipeline_log) | ✅ |
| `GET /api/projects/{project_id}/versions` | `v2_project_versions` (list_versions + metadata) | ✅ |
| `GET /api/findings/{project_id}/finding/{finding_id}` | `v2_finding_by_id` (findings_list filter) | ✅ |
| `GET /api/tiles/{project_id}/blocks/analysis` | `v2_blocks_analysis` (02_blocks_analysis) | ✅ |
| `GET /api/projects/{project_id}/config` | — | **not_ready**: маршрут перекрыт catch-all `/{project_id:path}` (зарегистрирован раньше), недостижим как отдельный route без смены порядка маршрутов |

Выбор обоснован: только GET, не запускают pipeline, ничего не пишут, не меняют
статусы, не используются для destructive-действий, и имеют чистый v2-эквивалент в
`ProjectsV2Adapter`.

### Намеренно НЕ подключены (not_ready, с причинами)

- **`/api/objects`** — разная семантика legacy↔v2 (legacy 3 объекта vs v2 2);
- **`GET /api/tiles/{id}/blocks`** + **`/blocks/image/{block_id}`** — индекс/картинки Gemma-кропов; v2-адаптер их не отдаёт (binary вне scope);
- **`GET /api/findings/{id}/block-map`** — finding→block_ids выводится из evidence; нет adapter-метода, деривация рискует контрактом;
- **`GET /api/document/{id}/pages`**, **`/page/{n}`** — рендер страниц PDF; адаптер не рендерит;
- **`GET /api/audit/{id}/log`** — `audit_log.jsonl` (event-лог); v2 хранит `pipeline_log.json` (другой артефакт);
- **`GET /api/audit/{id}/status`** — смешивает live pipeline-manager job state с данными (не чистый read);
- **`GET /api/audit/{id}/resume-info`** — состояние продолжения pipeline (вне scope canary);
- **`GET /api/projects/{id}/versions/{version_id}/files`** — листинг файлов; раскладка версии v2 отличается;
- **`GET /api/projects/{id}/config`** — см. выше (route shadowing).

Все POST/PUT/DELETE, reset/clear/re-run, upload, expert-review save, queue/pipeline
start, comparison-write endpoints — **не трогались**.

### version_id в canary

Билдеры принимают `?version_id=` и сопоставляют как v2-форму (`v001`), так и
legacy-форму (`v1`→`v001`); неизвестный id → current (мягко, без 500, без
fallback в legacy).

## Opt-in механизм

Любой из двух (равнозначны):

- query-параметр: `?storage=projects_v2`
- header: `X-Audit-Storage: projects_v2`

## Поведение (матрица)

| opt-in | флаг `AUDIT_PROJECTS_V2_READ_CANARY_ENABLED` | результат |
|---|---|---|
| нет | любой | **legacy** (ветка не меняется, байт-в-байт) |
| да | OFF (default) | **HTTP 403** (явный отказ; не тихий legacy) |
| да | ON | **projects_v2** (read-only) |
| да | ON, но документ не найден в v2 | **HTTP 404 canary-error** (НЕ silent fallback в legacy) |

Ключевые инварианты:

- обычный production-запрос (без opt-in) поведение **не меняет**;
- canary НЕ читает и НЕ меняет `AUDIT_STORAGE_BACKEND`;
- адаптер только читает (`ProjectsV2Adapter` не пишет, не создаёт файлы);
- флаг читается на **каждый** запрос (как у shadow API) → включение/выключение
  без переимпорта; default false.

## Флаг

```env
AUDIT_PROJECTS_V2_READ_CANARY_ENABLED=false   # default
```

Включение требует, чтобы значение попало в окружение процесса. main.py грузит
`.env` через `os.environ.setdefault` при старте, поэтому **смена флага требует
рестарта backend** (controlled restart после idle-gate).

## canary-ответы (shape)

`GET /api/projects?storage=projects_v2`:
```json
{"storage_backend": "projects_v2", "canary": true, "count": N, "documents": [ ... ]}
```

`GET /api/findings/{project_id}?storage=projects_v2`:
```json
{"storage_backend": "projects_v2", "canary": true, "document_code": "...",
 "object_id": "...", "object_folder": "...", "discipline": "...",
 "version_id": "v001", "version_count": 1, "analysis_status": "complete",
 "findings_count": 7, "findings_by_severity": {...}, "findings": [ ... ]}
```

`project_id` резолвится в v2 `document_code` по basename (срезается `(main)`),
опционально уточняется `?object_id=` при неоднозначности.

## Тесты

[tests/test_projects_v2_read_canary.py](../tests/test_projects_v2_read_canary.py)
— без opt-in→legacy (production path не меняется); opt-in+флаг OFF→403;
opt-in+флаг ON→v2; нужные поля; findings_count/version_count не теряются;
source_only/legacy_partial не падают; opt-in через header; v2 miss→404 canary;
read-only (дерево не меняется); независимость от `AUDIT_STORAGE_BACKEND`.

## Rollback

1. `AUDIT_PROJECTS_V2_READ_CANARY_ENABLED=false` в deploy `.env` (флаг читается на
   старте → нужен рестарт).
2. Controlled restart после idle-gate (jobs=0, queues empty).
3. Проверить: `?storage=projects_v2` снова даёт 403, обычные endpoint'ы работают.

Код инертен при выключенном флаге, поэтому даже без отката риска нет.

## Связанные файлы

- [backend/app/services/storage/read_canary.py](../backend/app/services/storage/read_canary.py)
- [backend/app/api/routers/projects.py](../backend/app/api/routers/projects.py) — `GET /api/projects`
- [backend/app/api/routers/findings.py](../backend/app/api/routers/findings.py) — `GET /api/findings/{project_id}`
- [backend/app/services/storage/projects_v2_adapter.py](../backend/app/services/storage/projects_v2_adapter.py) — read-only адаптер
- [docs/projects_v2_storage_standard.md](projects_v2_storage_standard.md), [docs/projects_v2_migration_plan.md](projects_v2_migration_plan.md)
