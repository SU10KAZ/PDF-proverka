# Audit Manager — Backend

FastAPI + WebSocket. Порт **8081**.

## Запуск

```bash
# Из корня проекта:
uvicorn backend.app.main:app --host 0.0.0.0 --port 8081 --reload

# Или напрямую:
python backend/app/main.py
```

## Структура

```
backend/
  app/
    main.py                  ← FastAPI entrypoint
    core/
      config.py              ← Все пути, константы, env-переменные
    api/
      routers/               ← REST API маршруты (/api/...)
    models/                  ← Pydantic-модели
    schemas/                 ← JSON Schemas для structured output
    ws/                      ← WebSocket manager
    data/                    ← Персистентные JSON-конфиги (stage_models.json и др.)
    services/
      common/                ← Общие сервисы (проекты, audit_logger, usage и др.)
      llm/                   ← LLM runners (Claude CLI, OpenRouter, LM Studio)
      findings/              ← Сервисы замечаний (findings, grounding)
      knowledge_base/        ← База знаний и missing_norms
      discussions/           ← Обсуждения по замечаниям
      export/                ← Excel export
    pipeline/
      manager.py             ← Pipeline orchestrator (бывший pipeline_service.py)
      resume_detector.py     ← Детектор resume/retry
      stages/
        prepare/             ← process_project, graph_builder, prepare_service
        crop_blocks/         ← blocks.py, block_markdown.py
        gemma_enrichment/    ← gemma_enrich, gemma_enrichment_contract, gemma_gate
        block_analysis/      ← gemma_findings_only (Stage 02)
        text_analysis/       ← Stage 01 text analysis
        findings_merge/      ← Свод замечаний
        findings_review/     ← Critic → Corrector
        norms/               ← _core, _native_verify, external_provider
        optimization/        ← Оптимизация
        report/              ← generate_excel_report
  requirements.txt           ← зависимости Python
```

## Переменные окружения

| Переменная | Описание | Дефолт |
|-----------|----------|--------|
| `AUDIT_ROOT_DIR` | Корневая папка проекта | автодетекция |
| `AUDIT_BASE_DIR` | Алиас для AUDIT_ROOT_DIR | - |
| `AUDIT_DATA_DIR` | Папка runtime-данных | = ROOT_DIR |
| `AUDIT_PROJECTS_DIR` | Папка проектов | DATA_DIR/projects |
| `AUDIT_PROMPTS_DIR` | Папка промптов | DATA_DIR/prompts |
| `OPENROUTER_API_KEY` | API ключ OpenRouter | - |
| `CHANDRA_BASE_URL` | URL LM Studio (ngrok) | - |
| `APP_PORT` | Порт backend | 8081 |

## API

- REST: `http://localhost:8081/api/...`
- Swagger: `http://localhost:8081/docs`
- WebSocket проекта: `ws://localhost:8081/ws/audit/{project_id}`
- WebSocket глобальный: `ws://localhost:8081/ws/global`

## CLI команды (работают из корня проекта)

```bash
python process_project.py projects/<name>
python blocks.py crop projects/<name>
python blocks.py batches projects/<name>
python blocks.py merge projects/<name>
python gemma_enrich.py projects/<name>
python generate_excel_report.py
```

Корневые CLI-скрипты — тонкие wrappers, делегирующие к `backend/app/pipeline/stages/`.
