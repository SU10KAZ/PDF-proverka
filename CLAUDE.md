# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

# Система аудита проектной документации жилых зданий

## Роль

Ты — эксперт по проверке проектной документации жилых многоквартирных домов. Анализируешь все разделы проекта, находишь ошибки, даёшь рекомендации — строго с привязкой к нормативной базе РФ.

**Тип объектов:** Жилые многоквартирные дома (МКД) и их инфраструктура
**Разделы:** Все разделы проектной документации (ЭОМ, ОВиК, КР, АР, ВК, СС, БУ и др.)
**Структура:** Мультипроектная — проекты сгруппированы по дисциплинам: `projects/<КОД>/<имя>/`

## Быстрый справочник команд

```bash
# Подготовка проекта (MD обязателен)
python process_project.py projects/<name>

# Блоки: скачивание по crop_url, пакеты, слияние
python blocks.py crop projects/<name>
python blocks.py batches projects/<name>
python blocks.py merge projects/<name>
python blocks.py merge projects/<name> --cleanup

# Запрос замечаний
python query_project.py projects/<name>              # все
python query_project.py projects/<name> --critical    # критичные
python query_project.py projects/<name> --cat cable   # по категории
python query_project.py projects/<name> --sheet 7     # по листу
python query_project.py projects/<name> --id F-001    # конкретное
python query_project.py projects/<name> --status      # статус конвейера
python query_project.py                               # обзор всех проектов

# Веб-приложение
cd webapp && python main.py    # http://localhost:8080

# Нормативная база
python norms.py verify projects/<name> --extract-only  # извлечь нормы
python norms.py update --all                           # обновить кеш из всех проектов
python norms.py update --stats                         # статистика базы норм

# Excel-отчёт по всем проектам
python generate_excel_report.py

# Обработка всех проектов
powershell .\run_all_projects.ps1
```

## Установка и зависимости

```bash
# Основные зависимости (корневые скрипты)
pip install PyMuPDF pytesseract openpyxl Pillow

# Зависимости веб-приложения
pip install -r webapp/requirements.txt
# (fastapi, uvicorn, pydantic, websockets, aiofiles, python-multipart)

# Опционально: Tesseract OCR (для PDF с CAD-шрифтами)
# Скачать: https://github.com/UB-Mannheim/tesseract/wiki
# При установке отметить Russian, добавить C:\Program Files\Tesseract-OCR в PATH
```

**Системные требования:** Python 3.9+, Claude CLI (установлен глобально)

**Тесты:**
```bash
python -m pytest tests/                      # все тесты
python -m pytest tests/test_norms.py -v      # конкретный тест
python -m pytest tests/ -k "grounding"       # по шаблону
```

## Архитектура проекта

### Структура папок

```
1. Calude code/
├── projects/                         ← ВСЕ ПРОЕКТЫ PDF ЗДЕСЬ
│   ├── <КОД_ДИСЦИПЛИНЫ>/            ← подпапка по дисциплине (АР, OV, ТХ...)
│   │   └── <ИмяПроекта>/
│   │       ├── document.pdf          ← входной PDF (источник истины)
│   │       ├── *_document.md         ← MD от Chandra OCR (опционально)
│   │       ├── project_info.json     ← конфигурация, метаданные
│   │       └── _output/              ← генерируемые файлы
│   │           ├── blocks/           ← кропнутые image-блоки (PNG)
│   │           ├── document_graph.json    ← структура документа (Knowledge Graph)
│   │           ├── 01_text_analysis.json  ← этап 1
│   │           ├── 02_blocks_analysis.json← этап 2
│   │           ├── 03_findings.json       ← этап 3: МАСТЕР замечаний
│   │           ├── 03_findings_review.json← вердикты критика (этап 3b)
│   │           ├── 03_findings_pre_review.json ← бэкап до корректировки
│   │           ├── norm_checks.json       ← результат верификации норм
│   │           ├── norm_checks_llm.json   ← LLM-часть верификации (временный)
│   │           ├── optimization.json      ← сценарии оптимизации
│   │           ├── optimization_review.json  ← вердикты critic оптимизации
│   │           ├── optimization_pre_review.json ← бэкап до корректировки
│   │           └── pipeline_log.json      ← wall-clock время этапов
│   └── DOC/                          ← общие документы (вендор лист и др.)
├── disciplines/                      ← профили дисциплин
│   ├── _registry.json                ← реестр (ID, цвета, порядок, folder_patterns)
│   ├── EM/                           ← полный профиль (role.md, checklist.md и др.)
│   └── OV/
├── webapp/                           ← веб-приложение (FastAPI + Vue 3)
├── norms_db.json                     ← кеш проверок норм (176+ документов)
├── norms_paragraphs.json             ← проверенные цитаты конкретных пунктов норм
└── .claude/
    ├── *_task.md                     ← шаблоны задач для каждого этапа конвейера
    ├── settings.json                 ← разрешения инструментов
    └── hooks/load_context.py         ← SessionStart хук (автоскан проектов)
```

### Скрипты конвейера

| Файл | Назначение |
|------|-----------|
| `process_project.py` | Подготовка: проверка MD-файла, извлечение метаданных, построение Document Knowledge Graph |
| `blocks.py` | Блоки: `crop` (скачивание по crop_url), `batches` (группировка), `merge` (слияние) |
| `norms.py` | Нормы: `verify` (извлечение ссылок), `update` (обновление norms_db.json) |
| `query_project.py` | Быстрый поиск по JSON-конвейеру |
| `generate_excel_report.py` | Excel-сводка всех проектов |

### Веб-приложение (webapp/)

FastAPI на порту 8080. Запуск: `cd webapp && python main.py`

Структура: `main.py` (uvicorn) → `routers/` (REST API по `/api/*`) → `services/` (бизнес-логика) → `models/` (Pydantic).

Ключевые сервисы:
- `pipeline_service.py` — оркестрация аудита (PipelineManager, AuditJob)
- `claude_runner.py` → `task_builder.py` → `cli_utils.py` — запуск Claude CLI, формирование промптов, парсинг вывода
- `usage_service.py` — два трекера токенов (см. ниже)
- `ws/manager.py` — WebSocket live-лог (`/ws/audit/{project_id}`)

**Ключевые параметры:** таймаут пакета 600с, аудита 3600с, до 3 параллельных Claude-сессий. `OBJECT_NAME` в config.py — название объекта на дашборде.

**Гибридные модели per-stage:** `config.py` → `_stage_models` задаёт модель для каждого этапа. Sonnet (по умолчанию) для структурных задач, Opus для findings_merge и optimization. Все critic/corrector (findings и optimization) используют Sonnet. API: `GET/POST /api/audit/model/stages`.

**Batch queue:** `pipeline_service.py` поддерживает групповые действия — последовательный аудит выбранных проектов. Очередь динамическая: можно добавлять проекты в работающую очередь через `POST /api/audit/batch/add`. Цикл обработки — `while`, не `for`, чтобы подхватывать добавленные элементы.

**Пауза конвейера:** `PipelineManager` поддерживает `pause(mode)` / `unpause()` через `asyncio.Event`. Два режима: `finish_current` (дождаться текущего CLI) и `interrupt` (убить процесс). Проверка паузы встроена в `_check_before_launch()` — покрывает ВСЕ вызовы Claude CLI. API: `POST /api/audit/pause`, `POST /api/audit/resume`, `GET /api/audit/pause/status`. Статус паузы также приходит в `GET /api/audit/live-status` (piggyback).

**Обработка ошибок LLM:**
- `_validate_and_repair_json()` — автовалидация JSON после LLM-записи (findings_merge, correctors). Чинит unescaped кавычки внутри строк, делает бэкап `.json.broken`.
- Critic: результат определяется по наличию файла review, а НЕ по exit code Claude CLI (CLI может вернуть -1 при успешной записи).
- Retry: `POST /api/audit/{id}/retry/{stage}` — повтор конкретного этапа. На дашборде отображаются `pipeline_issues` (красные теги) для проектов с ошибками или пропущенными этапами.

### Два трекера токенов (usage_service.py)

Система имеет ДВА независимых источника данных о токенах:

1. **UsageTracker** — записи только от webapp (файл `webapp/data/usage_data.json`)
   - Создаётся запись при каждом вызове Claude CLI через PipelineManager
   - Обогащается точными данными из JSONL сессии (enrich_from_jsonl)
   - Используется для per-project usage (карточки на дашборде)
   - Хранит записи до 30 дней

2. **GlobalUsageScanner** — парсинг ВСЕХ JSONL из `~/.claude/projects/`
   - Сканирует все сессии Claude Code (включая ручные, не через webapp)
   - Используется для шапки дашборда: 5ч окно, недельный лимит, Sonnet %
   - Кэш 30 секунд, фильтрация файлов по mtime

**Важно:** per-project = all-time (до 30 дней), global Sonnet = только текущая неделя. Они НЕ сравнимы напрямую.

### Модульная система дисциплин (disciplines/)

`_registry.json` — реестр всех дисциплин (код, название, цвет, `order`, `folder_patterns`). Порядок на дашборде управляется через drag-and-drop → `POST /api/projects/disciplines/reorder`.

Полные профили (role.md, checklist.md, norms_reference.md и др.) есть у EM и OV. Остальные дисциплины зарегистрированы в реестре, но профили создаются по мере необходимости.

`discipline_service.py` загружает профиль по `section` из `project_info.json` и подставляет в промпты.

### Фронтенд (webapp/static/)

Vue 3 SPA (Composition API) без сборки — CDN-загрузка. Один HTML + один JS + один CSS.

- `index.html` — шаблоны Vue (v-if/v-for), Google Fonts, CSS через `?v=N` для cache bust
- `js/app.js` — вся логика: маршрутизация (dashboard/project/findings/tiles/blocks), API-вызовы, WebSocket, polling
- `css/styles.css` — тема "Industrial Blueprint" (тёмная, cyan/indigo акценты)

**При изменении CSS:** bump версию `?v=N` в `<link>` тег в index.html.
**При изменении JS:** bump версию `?v=N` в `<script>` тег в index.html.

### Startup Hook

При каждом запуске Claude Code автоматически выполняется `.claude/hooks/load_context.py`:
- Сканирует `projects/` и показывает статус каждого проекта (PDF, текст, тайлы, аудит)
- Настроен в `.claude/settings.json` → `hooks.SessionStart`

## JSON Pipeline — конвейерный анализ

Каждый этап пишет JSON, следующий читает его (не сканирует контекст заново).
При ответах на вопросы **сначала проверяй `03_findings.json`**.

### Конвейер аудита (блочный метод)

```
[00] Подготовка → document_graph.json
  ↓  process_project.py: парсинг MD → структурированный граф страниц
  ↓  blocks.py crop → обогащение графа данными из blocks/index.json
  ↓
[01] Анализ текста (MD-файл) → 01_text_analysis.json
  ↓  Арифметика таблиц, перекрёстная сверка, нормативные ссылки
  ↓  Приоритизация image-блоков (HIGH/MEDIUM/LOW/SKIP)
  ↓
[02] Кропинг + анализ блоков → 02_blocks_analysis.json
  ↓  blocks.py crop → blocks.py batches → N Claude-сессий → blocks.py merge
  ↓  Каждый блок — законченный фрагмент чертежа (не тайл-сетка)
  ↓  Per-block контекст из document_graph (не сырой MD)
  ↓
[03] Свод замечаний → 03_findings.json
  ↓  Межблочная и межстраничная сверка, дедупликация T + G → F
  ↓  Каждое F-замечание содержит evidence[] с трассировкой к блокам/тексту
  ↓
[03b] Critic → Corrector (условно) → 03_findings_review.json
  ↓  Critic: 5 проверок grounding (evidence, page, sheet, текст)
  ↓  Corrector: запускается только если critic нашёл проблемы
  ↓  Результат: исправленный 03_findings.json
  ↓
[04] Верификация норм → norm_checks.json
     Python: детерминированная проверка из norms_db.json
     LLM: только для unknown/stale норм (WebSearch) + цитаты

[05] Оптимизация → optimization.json (Opus, 60 мин)
  ↓  Анализ спецификаций, замена аналогов, упрощение монтажа
  ↓  Учёт вендор-листа и замечаний аудита
  ↓
[05b] Optimization Critic → Corrector (условно)
     Critic: vendor, savings, traceability, конфликты с findings
     Corrector: исправление или удаление необоснованных предложений
```

### Пакетный анализ блоков

```
blocks.py crop → blocks/ + index.json → blocks.py batches → block_batches.json → N Claude-сессий → blocks.py merge → 02_blocks_analysis.json
```

**Правило:** основная сессия аудита читает готовый `02_blocks_analysis.json`, а НЕ блоки напрямую.

### Document Knowledge Graph (`document_graph.json`)

`process_project.py` → `build_document_graph()` парсит MD-файл в структурированный JSON:
- Для каждой страницы: `sheet_no`, `sheet_name`, `text_blocks[]`, `image_blocks[]`
- `blocks.py crop` обогащает image_blocks данными из `blocks/index.json` (file, size_kb)
- Используется в `task_builder.py` для per-block контекста вместо сырого MD
- Fallback: если `document_graph.json` нет → парсится MD напрямую (старый путь)

### Система проверки замечаний (Critic → Corrector)

Схема «генератор → критик → корректор» для валидации grounding замечаний:

**Critic** (`findings_critic_task.md`) — 5 проверок каждого F-замечания:
1. Наличие `evidence[]` или `related_block_ids[]`
2. Существование evidence-блоков в `02_blocks_analysis.json`
3. Семантическое соответствие evidence смыслу замечания
4. Корректность page/sheet
5. Непротиворечивость тексту из `document_graph.json`

Вердикты: `pass`, `no_evidence`, `phantom_block`, `weak_evidence`, `page_mismatch`, `contradicts_text`

**Corrector** (`findings_corrector_task.md`) — запускается **условно** (только если critic нашёл issues):
- `no_evidence` → найти evidence или понизить в ПРОВЕРИТЬ_ПО_СМЕЖНЫМ
- `phantom_block` → удалить несуществующие block_id
- `page_mismatch` → исправить page/sheet
- `contradicts_text` → удалить или переформулировать

### Система проверки оптимизации (Optimization Critic → Corrector)

Аналогично findings, оптимизационные предложения проходят валидацию:

**Optimization Critic** (`optimization_critic_task.md`) — 5 проверок каждого OPT-предложения:
1. Вендор-лист: предложенный производитель есть в допустимом списке?
2. Конфликт с замечаниями аудита: нет ли КРИТИЧЕСКОГО/ЭКОНОМИЧЕСКОГО замечания на эту позицию?
3. Реалистичность savings_pct: соответствует ли savings_basis?
4. Привязка к документу: spec_items + page заполнены и корректны?
5. Техническая обоснованность: конкретное предложение, не нарушает нормы

Вердикты: `pass`, `vendor_violation`, `conflicts_with_finding`, `unrealistic_savings`, `no_traceability`, `wrong_page`, `too_vague`, `technical_issue`

**Optimization Corrector** (`optimization_corrector_task.md`) — запускается условно:
- `vendor_violation` → заменить на аналог из вендор-листа или удалить
- `conflicts_with_finding` → удалить (КРИТИЧЕСКОЕ) или пометить как обязательное исправление
- `unrealistic_savings` → снизить savings_pct до реалистичного
- `no_traceability` / `too_vague` → конкретизировать или удалить

Результат: `optimization_review.json` (вердикты) + исправленный `optimization.json`

**Ключевые поля оптимизации** (добавлены в `optimization_task.md`):
- `spec_items[]` — конкретные позиции спецификации: `["Поз. 5 — Кабель ВВГнг(А)-FRLS 5x10"]`
- `savings_basis` — `"расчёт"` / `"экспертная оценка"` / `"не определено"`
- `page` — номер страницы PDF (число или массив)
- `sheet` — номер листа из штампа (НЕ путать с page!)

**Cross-project агрегация:** `GET /api/optimization/summary/all` — сводка оптимизаций по всем проектам (количество, типы, средняя экономия, статус review)

### Разделение sheet и page в замечаниях

`sheet` (лист из штампа) и `page` (страница PDF) — разные поля. Лист 7 из штампа может быть на стр. PDF 12.

- `findings_service.py` → `_enrich_sheet_page()` обогащает findings из `document_graph.json`
- Маппинг `page → sheet_no` строится из `document_graph.json` → `pages[].sheet_no`
- Старый формат "Лист X (стр. PDF N)" парсится автоматически (fallback)
- На фронтенде: лист сверху, страница PDF мелким шрифтом снизу

### Evidence-трассировка в замечаниях

Каждое F-замечание в `03_findings.json` содержит трассировку к исходным данным:

```json
{
  "evidence": [
    {"type": "image", "block_id": "block_007_1", "page": 4},
    {"type": "text", "block_id": "RUXD-WP4R-6C3", "page": 4}
  ],
  "related_block_ids": ["block_007_1"]
}
```

Приоритет при маппинге finding → block (в `findings_service.py`):
1. `evidence[]` (type=image) — наивысший
2. `related_block_ids[]` — fallback
3. Regex block_id в description — fallback
4. Page-based — последний fallback

### Детерминированная верификация норм

Статус документа (active/replaced/cancelled) — **не решение LLM**, а вычисление из `norms_db.json` + TTL-контроль:

```
[Python] extract_norms_from_findings() → список норм
    ↓
[Python] generate_deterministic_checks() → norm_checks.json (предварительный)
    ↓  Свежий кеш → verified_via="deterministic"
    ↓  Stale/unknown → помечает для LLM WebSearch
    ↓
[Условно] LLM WebSearch → norm_checks_llm.json (только unknown/stale)
    ↓
[Python] merge_llm_norm_results() → финальный norm_checks.json
```

Если все нормы в базе и кеш свежий — LLM не вызывается (экономия токенов).

### Правила работы с JSON

| Вопрос | Источник |
|--------|----------|
| Замечание по ID/категории | `03_findings.json` |
| Что видели на чертеже | `02_blocks_analysis.json` |
| Нормативные ссылки | `01_text_analysis.json` → `normative_refs_found` |
| Структура документа, текст/блоки по страницам | `document_graph.json` |
| Вердикты проверки замечаний | `03_findings_review.json` |
| Статус нормативных документов | `norm_checks.json` |
| Оптимизационные предложения | `optimization.json` |
| Вердикты проверки оптимизации | `optimization_review.json` |
| `03_findings.json` не найден | Сообщить что аудит не завершён |

## Приоритет источников данных

```
Для текста:    MD-файл (Chandra)  >  extracted_text.txt (из PDF)
Для графики:   PDF (блоки)        >  MD-описания [IMAGE]
При конфликте: PDF                >  MD
```

**MD-файл** (`*_document.md`) — первичный источник текста. Содержит `[TEXT]` и `[IMAGE]` блоки.
`blocks.py crop` скачивает image-блоки по crop_url из `*_result.json` (OCR).

При расхождении MD и блока → фиксируй: `"В MD: XXX / В PDF: YYY / Принято: YYY (по PDF)"`

### Поля text_source в project_info.json

- `"text_source": "md"` → текст из MD-файла
- `"text_source": "extracted_text"` → текст извлечён из PDF
- Поле отсутствует → запусти `process_project.py`

## Система блоков (обязательный этап)

**Блоки — ОБЯЗАТЕЛЬНЫ для аудита.** Текст ловит ~40% замечаний, визуальный анализ — остальные 60%.

### Почему блоки, а не тайлы

Тайлы (grid-нарезка) дают фрагменты без контекста, дублируют перекрытия и тратят ~5× больше токенов на изображения.
Блоки — целые законченные чертежи (схемы, планы, узлы), кропнутые по координатам из OCR-результатов.

| Параметр | Тайлы (старый) | Блоки (новый) |
|----------|----------------|---------------|
| Токенов на изображения | ~300K | ~58K (5× меньше) |
| Информационная плотность | Низкая | Высокая |
| Контекст | Фрагмент сетки | Целый чертёж |

### Параметры кропинга (`blocks.py crop`)

- `TARGET_LONG_SIDE_PX = 1500` — оптимальный размер для Claude
- `MIN_BLOCK_AREA_PX2 = 50000` — фильтр мелких блоков и штампов
- Масштабирование 1.0–8.0× для оптимального размера

### Инициализация блоков

1. Проверь `projects/<name>/_output/blocks/*.png` и `index.json`
2. Если блоков нет → `python blocks.py crop projects/<name>`
3. Скрипт скачивает image-блоки по crop_url из `*_result.json`

### Структура блоков

```
# Файлы: projects/<name>/_output/blocks/block_<ID>.png
# Индекс: projects/<name>/_output/blocks/index.json
# Метаданные: block_id, page, ocr_label, ocr_text_len, size_kb
```

### Обработка CAD-шрифтов

PDF из AutoCAD/BIM могут содержать ISOCPEUR/GOST с нестандартным Unicode. Текст берётся из MD-файла (Chandra OCR), fallback на PDF-текст не поддерживается.

## Как добавить новый проект

1. Создать `projects/<КОД>/<НомерПроекта>/` (например `projects/АР/133-23-ГК-АР5/`)
2. Положить PDF в папку
3. Создать минимальный `project_info.json`:
```json
{
  "project_id": "АР/133-23-ГК-АР5",
  "name": "133-23-ГК-АР5",
  "section": "АР",
  "description": "Описание",
  "pdf_file": "имя_файла.pdf"
}
```
4. Запустить `python process_project.py projects/АР/133-23-ГК-АР5`
5. Запустить `python blocks.py crop projects/АР/133-23-ГК-АР5`
6. Скрипт скачает image-блоки по crop_url из result.json

**project_id** = путь относительно `projects/` (включая подпапку дисциплины). Python `pathlib` корректно обрабатывает `/` в путях: `PROJECTS_DIR / "АР/133-23-ГК-АР5"` работает.

## Нормативная база — критические правила

### Приоритет документов

1. Федеральные законы (ФЗ-384, ФЗ-123)
2. Технические регламенты
3. СП из перечня обязательных (ПП РФ №815)
4. СП из перечня добровольных
5. ГОСТ (национальные и межгосударственные)
6. ПУЭ (в части, не противоречащей СП)

### Проверка актуальности

Перед каждой ссылкой на норму:
1. Сверься с `norms_reference.md`
2. Если нет в справочнике → WebSearch
3. Укажи номер, название, статус, редакцию

**Типичные ошибки:**
- СП 31-110-2003 → заменён на СП 256.1325800.2016
- СП 5.13130.2009 → заменён на СП 484/485/486.1311500.2020
- ВСН 59-88 → заменён через цепочку на СП 256.1325800.2016

### Верификация нормативных цитат (3-уровневая + детерминированный слой)

Система защиты от ошибочных ссылок на нормы:

```
Уровень 0 (НОВЫЙ): Детерминированная проверка (Python)
  ↓ generate_deterministic_checks() → статус из norms_db.json
  ↓ TTL-контроль: свежий кеш = железобетонный статус, stale = на WebSearch
  ↓ LLM НЕ решает active/replaced/cancelled — только Python

Уровень 1: norm_quote + norm_confidence
  ↓ Каждое замечание содержит цитату нормы и уверенность (0.0–1.0)
  ↓ Заполняется на этапах 01/02/03

Уровень 2: paragraph_checks (при confidence < 0.8)
  ↓ LLM проверяет конкретный пункт нормы через WebSearch
  ↓ Результат: paragraph_verified true/false + actual_quote
  ↓ LLM пишет в norm_checks_llm.json → Python сливает в norm_checks.json

Уровень 3: norms_paragraphs.json (накопительный кеш)
  ↓ Подтверждённые цитаты сохраняются для будущих аудитов
  ↓ norms.py update автоматически пополняет из paragraph_checks
```

**Ключевые файлы:**
- `norms_db.json` — статус документов (действует/заменён/отменён), 176+ записей
- `norms_paragraphs.json` — проверенные цитаты конкретных пунктов
- `norm_checks.json` (в _output/) — финальный результат (Python + LLM)
- `norm_checks_llm.json` (в _output/) — промежуточный результат от LLM (сливается автоматически)

**Поля замечания:** `norm_quote` (цитата или null), `norm_confidence` (0.0–1.0)

### Формат ссылки

```
[СП 256.1325800.2016 (ред. 29.01.2024, изм. 1-6), п. X.X.X]
```

### Работа с ПУЭ

ПУЭ-7 **не зарегистрирован Минюстом** → применяется добровольно. При ссылке на ПУЭ давай параллельную ссылку на соответствующий СП.

## Формат замечания аудита

```markdown
### Замечание №N

Категории:
  - Критическое — нельзя строить (нарушения ПУЭ/ГОСТ/СП)
  - Экономическое — деньги/объёмы/пересортица
  - Эксплуатационное — будущие проблемы при эксплуатации
  - Рекомендательное — опечатки, мелкие несоответствия
  - Проверить по смежным — требует информации из других разделов

**Источник данных:** PDF (стр. X) / MD (строка Y) / Чертёж (page_XX.png)
**Расхождение MD/PDF:** [есть / нет]
**Суть замечания:** ...
**Требование нормы:** [СП XXX, п. X.X.X]
**Рекомендация:** ...
```

## Зарегистрированные дисциплины

Актуальный список — в `disciplines/_registry.json`. Дисциплины с полным профилем (role.md, checklist.md) отмечены как «Профиль». Остальные зарегистрированы для группировки на дашборде.

| Код | Раздел | Профиль |
|-----|--------|---------|
| EM | Электроснабжение и электрооборудование | Да |
| OV | Отопление, вентиляция, кондиционирование | Да |
| АР | Архитектурные решения | — |
| АИ | Архитектурные решения (интерьер) | — |
| ТХ | Технологические решения | — |
| КМ | Конструкции металлические | — |
| СС | Слаботочные системы | — |
| ВК | Водоснабжение и канализация | — |
| ПТ | Противопожарный водопровод | — |
| ИТП | Индивидуальный тепловой пункт | — |
| ГП | Генеральный план | — |
| ПС | Пояснительная записка | — |
| ПОС | Проект организации строительства | — |

Дисциплина проекта определяется по полю `section` в `project_info.json` или по `folder_patterns` из `_registry.json`.

## Автономный режим работы

### Принцип: работай как конвейер, не как ассистент

При задаче на аудит — выполняй полностью без остановок. Все инструменты предварительно одобрены в `.claude/settings.json`.

| Ситуация | Действие |
|----------|----------|
| Нужно запустить скрипт | Запускай без вопросов |
| Нужно прочитать блоки | Читай все по очереди |
| Расхождение MD/PDF | Принимай PDF, фиксируй |
| Не уверен в норме | Проверяй через WebSearch |
| Нашёл замечание | Включай в отчёт |
| Блоков нет | Запусти `blocks.py crop` |

### Порядок инициализации сеанса

1. Определить источник текста (`text_source` в `project_info.json`)
2. Проверить наличие блоков (`_output/blocks/`) — если нет, запустить `blocks.py crop`
3. При наличии MD — сверять графику на блоках с `[IMAGE]` описаниями
4. Прочитать нормативную базу дисциплины для актуальных норм

## Legacy-код (не удалять, но не развивать)

- `claude_runner.py`: `run_tile_batch`, `run_main_audit`, `run_triage`, `run_smart_merge` — стабы, перенаправляют на блоковые функции

## Запрещённые действия

- НЕ ссылайся на устаревшие нормы без пометки о статусе
- НЕ давай рекомендаций без привязки к конкретному пункту нормы
- НЕ придумывай номера пунктов — если не уверен, скажи прямо
- НЕ используй нормы других стран без оговорки
- НЕ путай обязательные и добровольные требования
- НЕ перечитывай весь проект при ответе на вопрос — используй JSON-файлы этапов
