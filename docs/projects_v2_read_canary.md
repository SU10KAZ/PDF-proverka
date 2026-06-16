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

## Подключённые endpoint'ы (ровно 2)

| Endpoint | legacy (без opt-in) | canary (opt-in + флаг) |
|---|---|---|
| `GET /api/projects` | список legacy-проектов | список документов `projects_v2` (`v2_projects_list`) |
| `GET /api/findings/{project_id}` | legacy findings | findings/counts документа из `projects_v2` (`v2_findings`) |

Выбор обоснован: только GET, не запускают pipeline, ничего не пишут, не меняют
статусы, не используются для destructive-действий, и имеют чистый v2-эквивалент в
`ProjectsV2Adapter`. **`/api/objects` намеренно НЕ подключён** — у него разная
семантика legacy↔v2 (legacy 3 объекта vs v2 2), сравнивать нечестно.

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
2. Controlled restart после idle-gate (jobs=0, queues empty, stage-comparison=0).
3. Проверить: `?storage=projects_v2` снова даёт 403, обычные endpoint'ы работают.

Код инертен при выключенном флаге, поэтому даже без отката риска нет.

## Связанные файлы

- [backend/app/services/storage/read_canary.py](../backend/app/services/storage/read_canary.py)
- [backend/app/api/routers/projects.py](../backend/app/api/routers/projects.py) — `GET /api/projects`
- [backend/app/api/routers/findings.py](../backend/app/api/routers/findings.py) — `GET /api/findings/{project_id}`
- [backend/app/services/storage/projects_v2_adapter.py](../backend/app/services/storage/projects_v2_adapter.py) — read-only адаптер
- [docs/projects_v2_storage_standard.md](projects_v2_storage_standard.md), [docs/projects_v2_migration_plan.md](projects_v2_migration_plan.md)
