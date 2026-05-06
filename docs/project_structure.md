# Project Structure
**Дата:** 2026-05-06

## Корневая структура

```
project-root/
  frontend/              ← Vue 3 SPA (Vite dev-сервер / build)
  backend/               ← FastAPI + pipeline
  projects/              ← runtime-данные: проекты аудита (НЕ трогать)
  prompts/               ← шаблоны задач для LLM-этапов
  knowledge_base/        ← экспертные решения, паттерны
  norms/                 ← нормативная база РФ (код + vault)
  reports/               ← итоговые отчёты (генерируются)
  docs/                  ← документация разработки
  scripts/               ← benchmark/experiment скрипты
  disciplines/           ← профили дисциплин (роль, чеклист, нормы)
  .env                   ← секреты (API ключи, URL туннелей)
  CLAUDE.md              ← инструкции для Claude Code

  # CLI wrappers (остаются в корне для совместимости)
  process_project.py     ← подготовка проекта
  blocks.py              ← crop/batches/merge блоков
  gemma_enrich.py        ← Gemma OCR enrichment
  generate_excel_report.py ← Excel-отчёт
  graph_builder.py       ← builder для document graph
  block_markdown.py      ← парсер MD-блоков
  gemma_enrichment_contract.py ← контракт Gemma
  gemma_findings_only.py ← Stage 02 findings
```

## Frontend

```
frontend/
  index.html             ← главный дашборд
  model-control.html     ← управление LLM-моделями
  css/
    styles.css
    model-control.css
  js/
    app.js               ← Vue 3 логика дашборда
    model-control.js     ← Vue 3 логика model-control
    vue.global.prod.js   ← Vue 3 CDN bundle
    marked.min.js        ← Markdown renderer
  package.json           ← npm: vite dev-сервер
  vite.config.js         ← proxy /api → :8081, /ws → :8081
  README.md
```

**Запуск frontend:**
```bash
cd frontend
npm install
npm run dev   # http://localhost:5173 (proxy → :8081)
```

## Backend

```
backend/
  app/
    main.py              ← FastAPI entrypoint
    core/
      config.py          ← ROOT_DIR, PROJECTS_DIR, ENV, модели, константы
    api/
      routers/           ← REST API /api/... (14 файлов)
        audit.py         ← /api/audit/*
        projects.py      ← /api/projects/*
        findings.py      ← /api/findings/*
        blocks.py        ← /api/blocks/*
        optimization.py  ← /api/optimization/*
        export.py        ← /api/export/*
        usage.py         ← /api/usage/*
        document.py      ← /api/document/*
        discussions.py   ← /api/discussions/*
        knowledge_base.py← /api/knowledge_base/*
        objects.py       ← /api/objects/*
        model_control.py ← /api/model-control/*
        lms.py           ← /api/lms/*
    models/              ← Pydantic-модели (audit, findings, project, usage и др.)
    schemas/             ← JSON Schemas для structured output LLM
    ws/
      manager.py         ← WebSocket manager
    data/                ← Персистентные JSON (stage_models.json и др.)
    services/
      common/            ← Общие сервисы
        process_runner.py   ← subprocess runner
        audit_logger.py     ← pipeline_log.json writer
        usage_service.py    ← usage/cost tracking
        project_service.py  ← resolve_project_dir, iter_project_dirs
        object_service.py   ← объекты (группы проектов)
        group_service.py    ← группировка
        discipline_service.py ← профили дисциплин
        cli_utils.py        ← CLI утилиты
      llm/               ← LLM runners
        llm_runner.py       ← run_llm (OpenRouter/Direct)
        claude_runner.py    ← Claude CLI runner
        gemini_direct_runner.py ← прямой Gemini API
        openrouter_block_batch.py ← Stage 02 batch (OR)
        lms_service.py      ← LM Studio API
        lmstudio_lifecycle_service.py ← lifecycle (unload/reload)
        model_control_service.py ← управление моделями из UI
      findings/          ← Сервисы замечаний
        findings_service.py  ← CRUD + enrichment замечаний
        finding_quality.py   ← оценка качества
        grounding_service.py ← evidence grounding
      knowledge_base/    ← База знаний
        knowledge_base_service.py
        missing_norms_service.py
      discussions/       ← Обсуждения
        discussion_service.py
      export/            ← Excel export
        excel_service.py
    pipeline/            ← Конвейер аудита
      manager.py         ← PipelineManager (оркестратор)
      resume_detector.py ← детектор resume/retry
      stages/
        prepare/         ← Этап 00: подготовка проекта
          process_project.py   ← построение document_graph.json
          graph_builder.py     ← Document Knowledge Graph
          prepare_service.py   ← queue для crop + Gemma enrichment
          task_builder.py      ← builder задач для Claude
          prompt_builder.py    ← builder промптов
        crop_blocks/     ← Этап: кропинг блоков
          blocks.py            ← crop/batches/merge/recrop
          block_markdown.py    ← парсер MD-блоков
        gemma_enrichment/← Этап: Gemma OCR enrichment
          gemma_enrich.py           ← base 100 DPI + high-detail 300 DPI
          gemma_enrichment_contract.py ← контракт (профили, маркеры, summary)
          gemma_gate.py             ← gate readiness validation
        block_analysis/  ← Этап 02: визуальный анализ блоков
          gemma_findings_only.py    ← Stage 02 single-block (findings only)
        text_analysis/   ← Этап 01: анализ текста MD
        findings_merge/  ← Этап 03: свод замечаний
        findings_review/ ← Этап 03b: critic → corrector
        norms/           ← Этап 04: верификация норм
          _core.py             ← verify, update, norm_fix, norm_requote
          _native_verify.py    ← детерминированная проверка из norms_db.json
          external_provider.py ← WebSearch/MCP provider
        optimization/    ← Этап 05: оптимизация
        report/          ← Финальный отчёт
          generate_excel_report.py
  requirements.txt
  README.md
```

**Запуск backend:**
```bash
# Из корня проекта:
uvicorn backend.app.main:app --host 0.0.0.0 --port 8081 --reload

# Или:
python backend/app/main.py
```

## Конфигурация путей (backend/app/core/config.py)

| Переменная | ENV override | Значение по умолчанию |
|-----------|-------------|----------------------|
| `ROOT_DIR` | `AUDIT_ROOT_DIR` | автодетекция от config.py |
| `BACKEND_DIR` | - | ROOT_DIR/backend |
| `FRONTEND_DIR` | - | ROOT_DIR/frontend |
| `PROJECTS_DIR` | `AUDIT_PROJECTS_DIR` | ROOT_DIR/projects |
| `PROMPTS_DIR` | `AUDIT_PROMPTS_DIR` | ROOT_DIR/prompts |
| `REPORTS_DIR` | - | ROOT_DIR/отчет |
| `KNOWLEDGE_BASE_DIR` | - | ROOT_DIR/knowledge_base |

## CLI команды (работают из корня)

```bash
# Подготовка
python process_project.py projects/<name>

# Блоки
python blocks.py crop projects/<name>
python blocks.py crop projects/<name> --output-dir blocks_gemma_100 --dpi 100
python blocks.py batches projects/<name>
python blocks.py merge projects/<name> [--cleanup]

# Gemma enrichment
python gemma_enrich.py projects/<name>
python gemma_enrich.py projects/<name> --force

# Отчёт
python generate_excel_report.py

# Веб-приложение (старый способ — из папки webapp)
cd webapp && python main.py    # http://localhost:8081

# Веб-приложение (новый способ — из корня)
uvicorn backend.app.main:app --port 8081 --reload
```

## Runtime-данные (не трогать, не удалять)

- `projects/` — результаты аудита, _output/, JSON-файлы пайплайна
- `norms/vault/` — PDF-файлы нормативов
- `norms/norms_db.json` — статус 176+ нормативных документов
- `norms/norms_paragraphs.json` — верифицированные цитаты пунктов
- `webapp/data/` → теперь `backend/app/data/` — персистентные конфиги
- `.env` — секреты (никогда не коммитить)

## Обратная совместимость

Старый `webapp/` **не удалён** — продолжает работать:
```bash
cd webapp && python main.py
```

Новый backend работает параллельно через:
```bash
uvicorn backend.app.main:app --port 8081
```

Корневые CLI-скрипты (`process_project.py`, `blocks.py`, и др.) не изменились —
они продолжают работать как раньше. Backend-версии в `backend/app/pipeline/stages/`
содержат те же файлы с обновлёнными импортами для работы внутри пакета.
