# Webapp internals

FastAPI + Vue 3 SPA (без сборки, CDN). Слушает **127.0.0.1:8081** (на 8080 — Apache2).

## Запуск

```bash
cd webapp && python main.py    # http://localhost:8081
```

## Структура

`main.py` (uvicorn) → `routers/` (REST API по `/api/*`) → `services/` (бизнес-логика) → `models/` (Pydantic).

**Ключевые сервисы:**
- `pipeline_service.py` — оркестрация аудита (PipelineManager, AuditJob)
- `claude_runner.py` → `task_builder.py` → `cli_utils.py` — запуск Claude CLI, формирование промптов, парсинг
- `usage_service.py` — два трекера токенов (см. ниже)
- `ws/manager.py` — WebSocket live-лог (`/ws/audit/{project_id}`)
- `discipline_service.py` — загружает профиль по `section` из `project_info.json`

**Ключевые параметры:** таймаут пакета 600с, аудита 3600с, до 3 параллельных Claude-сессий. `OBJECT_NAME` в config.py — название объекта на дашборде.

## Гибридные модели per-stage

`config.py` → `_stage_models` задаёт модель для каждого этапа:
- **Sonnet** (по умолчанию) — структурные задачи
- **Opus** — `findings_merge` и `optimization`
- **Sonnet** — все critic/corrector (findings и optimization)

API: `GET/POST /api/audit/model/stages`.

## Batch queue

`pipeline_service.py` — последовательный аудит выбранных проектов. Очередь **динамическая**: можно добавлять проекты в работающую очередь через `POST /api/audit/batch/add`.

Цикл — `while`, не `for`, чтобы подхватывать добавленные элементы.

## Пауза конвейера

`PipelineManager` поддерживает `pause(mode)` / `unpause()` через `asyncio.Event`.

**Два режима:**
- `finish_current` — дождаться текущего CLI
- `interrupt` — убить процесс

Проверка паузы встроена в `_check_before_launch()` — покрывает ВСЕ вызовы Claude CLI.

**API:** `POST /api/audit/pause`, `POST /api/audit/resume`, `GET /api/audit/pause/status`.
Статус паузы также в `GET /api/audit/live-status` (piggyback).

## Два трекера токенов (usage_service.py)

ДВА независимых источника данных. **НЕ сравнимы напрямую.**

### 1. UsageTracker — записи только от webapp

- Файл: `webapp/data/usage_data.json`
- Создаётся при каждом вызове Claude CLI через PipelineManager
- Обогащается точными данными из JSONL сессии (`enrich_from_jsonl`)
- Используется для per-project usage (карточки на дашборде)
- Хранит до 30 дней
- **Покрытие:** all-time (до 30 дней)

### 2. GlobalUsageScanner — парсинг всех JSONL

- Источник: `~/.claude/projects/`
- Сканирует ВСЕ сессии Claude Code (включая ручные, не через webapp)
- Используется для шапки дашборда: 5ч окно, недельный лимит, Sonnet %
- Кэш 30 секунд, фильтрация по mtime
- **Покрытие:** только текущая неделя

## Обработка ошибок LLM

- `_validate_and_repair_json()` — автовалидация JSON после LLM-записи. Чинит unescaped кавычки, делает бэкап `.json.broken`
- **Critic результат** определяется по наличию файла review, а НЕ по exit code Claude CLI (CLI может вернуть −1 при успешной записи)
- **Retry:** `POST /api/audit/{id}/retry/{stage}` — повтор конкретного этапа
- На дашборде красные теги `pipeline_issues` для проектов с ошибками или пропущенными этапами

## Фронтенд (webapp/static/)

Vue 3 Composition API без сборки — CDN-загрузка. Один HTML + один JS + один CSS.

- `index.html` — шаблоны Vue, `?v=N` для cache bust
- `js/app.js` — маршрутизация (dashboard/project/findings/tiles/blocks), API, WebSocket, polling
- `css/styles.css` — тема "Industrial Blueprint" (тёмная, cyan/indigo)

**При изменении CSS/JS:** bump версию `?v=N` в соответствующем теге `index.html`.

## Стартовый хук

При каждом запуске Claude Code выполняется `.claude/hooks/load_context.py`:
сканирует `projects/` и показывает статус каждого (PDF, текст, тайлы, аудит).
Настроен в `.claude/settings.json` → `hooks.SessionStart`.
