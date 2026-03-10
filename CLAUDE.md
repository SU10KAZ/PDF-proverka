# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

# Система аудита проектной документации по электроснабжению

## Роль

Ты — эксперт-проектировщик по электроснабжению жилых зданий (ЭОМ/ЭС). Анализируешь документацию, находишь ошибки, даёшь рекомендации — строго с привязкой к нормативной базе РФ.

**Тип объектов:** Жилые и общественные здания (МКД, паркинги)
**Разделы:** ЭОМ / ЭС / ЭМ
**Структура:** Мультипроектная — каждый проект в своей папке `projects/<name>/`

## Быстрый справочник команд

```bash
# Подготовка проекта (текст + тайлы)
python process_project.py projects/<name>

# Кропинг image-блоков из PDF (по координатам OCR)
python crop_blocks.py projects/<name>

# Пакетный анализ блоков (100% покрытие)
python generate_block_batches.py projects/<name>
# Слияние результатов пакетного анализа
python merge_block_results.py projects/<name>
python merge_block_results.py projects/<name> --cleanup

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
python verify_norms.py projects/<name> --extract-only  # извлечь нормы
python update_norms_db.py --all                        # обновить кеш из всех проектов
python update_norms_db.py --stats                      # статистика базы норм

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

## Архитектура проекта

### Структура папок

```
1. Calude code/
├── projects/                         ← ВСЕ ПРОЕКТЫ PDF ЗДЕСЬ
│   ├── <ИмяПроекта>/
│   │   ├── document.pdf              ← входной PDF (источник истины)
│   │   ├── *_document.md             ← MD от Chandra OCR (опционально)
│   │   ├── project_info.json         ← конфигурация, метаданные
│   │   └── _output/                  ← генерируемые файлы
│   │       ├── extracted_text.txt    ← текст из PDF
│   │       ├── blocks/              ← кропнутые image-блоки (PNG)
│   │       ├── block_batches.json   ← конфигурация пакетов блоков
│   │       ├── 01_text_analysis.json ← этап 1: текст + приоритизация блоков
│   │       ├── 02_blocks_analysis.json← этап 2: анализ блоков
│   │       ├── 03_findings.json      ← этап 3: МАСТЕР замечаний
│   │       ├── optimization.json     ← сценарии оптимизации (meta.by_type, items)
│   │       ├── norm_checks.json      ← результат верификации норм
│   │       └── audit_results_*.md    ← финальный отчёт
│   └── _summary/                     ← сводки по всем проектам
├── norms_reference.md                ← нормативная база РФ
├── norms_db.json                     ← кеш проверок норм (176+ документов)
├── norms_paragraphs.json             ← проверенные цитаты конкретных пунктов норм
├── schemas/                          ← JSON-схемы этапов конвейера
└── .claude/
    ├── text_analysis_task.md         ← этап 01: анализ текста из MD
    ├── block_analysis_task.md        ← этап 02: анализ image-блоков
    ├── findings_merge_task.md        ← этап 03: свод замечаний + межблочная сверка
    ├── norm_verify_task.md           ← верификация нормативных ссылок
    ├── norm_fix_task.md              ← пересмотр замечаний при обновлении норм
    ├── optimization_task.md          ← оптимизация проектных решений
    ├── settings.json                 ← разрешения инструментов
    └── hooks/load_context.py         ← SessionStart хук (автоскан проектов)
```

### Скрипты конвейера

| Файл | Назначение |
|------|-----------|
| `process_project.py` | Подготовка: извлечение текста + авто-нарезка тайлов |
| `pdf_text_utils.py` | Детекция порчи CAD-шрифтов + OCR-фолбэк |
| `crop_blocks.py` | Кропинг image-блоков из PDF по координатам OCR |
| `generate_block_batches.py` | Группировка блоков в пакеты (~8 шт, по страницам) |
| `merge_block_results.py` | Слияние `block_batch_*.json` → `02_blocks_analysis.json` |
| `verify_norms.py` | Извлечение нормативных ссылок из findings + подготовка для верификации |
| `update_norms_db.py` | Обновление `norms_db.json` + `norms_paragraphs.json` из результатов верификации |
| `update_stage01.py` | Обновление `01_text_analysis.json` (приоритизация блоков) |
| `query_project.py` | Быстрый поиск по JSON-конвейеру |
| `generate_excel_report.py` | Excel-сводка всех проектов |

### Веб-приложение (webapp/)

FastAPI на порту 8080. Запуск: `cd webapp && python main.py`

```
webapp/
├── main.py              ← uvicorn точка входа
├── config.py            ← пути, таймауты, настройки Claude CLI
├── routers/             ← REST API
│   ├── projects.py      ← /api/projects/* — список, статус
│   ├── audit.py         ← /api/audit/* — запуск full+smart аудита
│   ├── findings.py      ← /api/findings/* — фильтры замечаний
│   ├── tiles.py         ← /api/tiles/* — просмотр PNG/блоков
│   ├── export.py        ← /api/export/* — Excel, CSV, Markdown
│   ├── usage.py         ← /api/usage/* — счётчики токенов (сессия, 5ч, неделя)
│   └── optimization.py  ← /api/optimization/* — сценарии оптимизации
├── services/            ← бизнес-логика
│   ├── pipeline_service.py  ← оркестрация аудита (PipelineManager, AuditJob)
│   ├── claude_runner.py     ← запуск Claude CLI (wrapper)
│   ├── task_builder.py      ← формирование промптов из .claude/*_task.md
│   ├── cli_utils.py         ← парсинг JSON-вывода CLI, детекция rate limit
│   ├── project_service.py   ← работа с проектами
│   ├── findings_service.py  ← слияние findings
│   ├── process_runner.py    ← async subprocess для Python-скриптов
│   ├── usage_service.py     ← два трекера токенов (см. ниже)
│   ├── discipline_service.py← загрузка профилей дисциплин (EM, OV)
│   ├── resume_detector.py   ← детекция прерванного этапа для возобновления
│   ├── audit_logger.py      ← логирование событий аудита в файл
│   └── excel_service.py     ← генерация Excel
├── models/              ← Pydantic-модели (project, audit, findings, usage, websocket)
├── ws/manager.py        ← WebSocket live-лог (/ws/audit/{project_id})
└── data/                ← runtime-данные (usage_data.json и т.д.)
```

**Ключевые параметры:** таймаут пакета 600с, аудита 3600с, до 3 параллельных Claude-сессий.

**Гибридные модели per-stage:** `config.py` → `_stage_models` задаёт модель для каждого этапа. Sonnet (по умолчанию) для структурных задач, Opus для findings_merge и optimization. API: `GET/POST /api/audit/model/stages`.

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

```
disciplines/
├── _registry.json           ← реестр всех дисциплин (ID, цвета, ключевые слова)
├── _common/
│   └── severity_levels.md   ← уровни критичности (КРИТИЧЕСКОЕ и т.д.)
├── EM/                      ← Электроснабжение (ЭОМ/ЭС/ЭМ)
│   ├── config.json          ← конфигурация дисциплины
│   ├── role.md              ← роль Claude как эксперта
│   ├── norms_reference.md   ← нормативная база по электрике
│   ├── checklist.md         ← контрольный список проверок
│   ├── drawing_types.md     ← типы чертежей
│   ├── finding_categories.md← категории замечаний
│   └── project_params.md    ← типовые параметры проекта
└── OV/                      ← Вентиляция и кондиционирование (аналогичная структура)
```

`discipline_service.py` загружает профиль по `section` из `project_info.json` и подставляет в промпты.

### Фронтенд (webapp/static/)

Vue 3 SPA (Composition API) без сборки — CDN-загрузка. Один HTML + один JS + один CSS.

- `index.html` — шаблоны Vue (v-if/v-for), Google Fonts, CSS через `?v=N` для cache bust
- `js/app.js` — вся логика: маршрутизация (dashboard/project/findings/tiles/blocks), API-вызовы, WebSocket, polling
- `css/styles.css` — тема "Industrial Blueprint" (тёмная, cyan/indigo акценты)

**Ключевые функции в app.js:**
- `stepClass(step)` — возвращает CSS-класс для pipeline-индикатора (`step-done`, `step-error`, `step-running`...)
- `pollGlobalUsage()` — опрос `/api/usage/global` каждые 60 сек
- `fetchAllProjectUsage()` — загрузка per-project токенов для карточек
- `stageTokens(key)` — маппинг pipeline key → stage key в usage data
- `stageDurationForProject(projectId, key)` — время выполнения этапа (из usage)
- `formatDuration(ms)` — форматирование длительности (5м32с, 1ч12м)
- `optTypeLabel(type)` / `optTypeColor(type)` — метки и цвета типов оптимизации

**Дашборд — карточки проектов показывают:**
- Pipeline-кружки (01–05+OPT) с временем выполнения под каждым шагом
- Severity-бейджи (цветные счётчики замечаний по критичности)
- Optimization-бейджи (цветные счётчики по типам: cheaper_analog, faster_install, simpler_design, lifecycle)

**При изменении CSS:** bump версию `?v=N` в `<link>` тег в index.html.
**При изменении JS-классов:** CSS должен поддерживать обе формы (`.done` и `.step-done`).

### Startup Hook

При каждом запуске Claude Code автоматически выполняется `.claude/hooks/load_context.py`:
- Сканирует `projects/` и показывает статус каждого проекта (PDF, текст, тайлы, аудит)
- Настроен в `.claude/settings.json` → `hooks.SessionStart`

## JSON Pipeline — конвейерный анализ

Каждый этап пишет JSON, следующий читает его (не сканирует контекст заново).
При ответах на вопросы **сначала проверяй `03_findings.json`**.

### Конвейер аудита (блочный метод)

```
[01] Анализ текста (MD-файл) → 01_text_analysis.json
  ↓  Арифметика таблиц, перекрёстная сверка, нормативные ссылки
  ↓  Приоритизация image-блоков (HIGH/MEDIUM/LOW/SKIP)
  ↓
[02] Кропинг + анализ блоков → 02_blocks_analysis.json
  ↓  crop_blocks.py → generate_block_batches.py → N Claude-сессий → merge_block_results.py
  ↓  Каждый блок — законченный фрагмент чертежа (не тайл-сетка)
  ↓  Сверка значений на чертеже с project_params из этапа 01
  ↓
[03] Свод замечаний → 03_findings.json + audit_results_*.md
     Межблочная и межстраничная сверка
     Дедупликация T + G → F
```

### Пакетный анализ блоков

```
crop_blocks.py → blocks/ + index.json → generate_block_batches.py → block_batches.json → N Claude-сессий → merge_block_results.py → 02_blocks_analysis.json
```

**Правило:** основная сессия аудита читает готовый `02_blocks_analysis.json`, а НЕ блоки напрямую.

### Правила работы с JSON

| Вопрос | Источник |
|--------|----------|
| Замечание по ID/категории | `03_findings.json` |
| Что видели на чертеже | `02_blocks_analysis.json` |
| Нормативные ссылки | `01_text_analysis.json` → `normative_refs_found` |
| `03_findings.json` не найден | Сообщить что аудит не завершён |

### JSON-схемы

В папке `schemas/`: `stage_01_text.schema.json`, `stage_02_blocks.schema.json`, `stage_03_findings.schema.json`.

## Приоритет источников данных

```
Для текста:    MD-файл (Chandra)  >  extracted_text.txt (из PDF)
Для графики:   PDF (блоки)        >  MD-описания [IMAGE]
При конфликте: PDF                >  MD
```

**MD-файл** (`*_document.md`) — первичный источник текста. Содержит `[TEXT]` и `[IMAGE]` блоки.
`crop_blocks.py` кропает image-блоки из PDF по координатам из `*_result.json` (OCR).

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

### Параметры кропинга (`crop_blocks.py`)

- `TARGET_LONG_SIDE_PX = 1500` — оптимальный размер для Claude
- `MIN_BLOCK_AREA_PX2 = 50000` — фильтр мелких блоков и штампов
- Масштабирование 1.0–8.0× для оптимального размера

### Инициализация блоков

1. Проверь `projects/<name>/_output/blocks/*.png` и `index.json`
2. Если блоков нет → `python crop_blocks.py projects/<name>`
3. Скрипт кропает все image-блоки из `*_result.json`

### Структура блоков

```
# Файлы: projects/<name>/_output/blocks/block_<ID>.png
# Индекс: projects/<name>/_output/blocks/index.json
# Метаданные: block_id, page, ocr_label, ocr_text_len, size_kb
```

### Обработка CAD-шрифтов

PDF из AutoCAD/BIM могут содержать ISOCPEUR/GOST с нестандартным Unicode → `pdf_text_utils.py` детектирует порчу и запускает OCR. Маркеры в `extracted_text.txt`: `[OCR]` (распознано), `[CAD_FONT_CORRUPTED]` (полагаться на блоки).

## Как добавить новый проект

1. Создать `projects/<НомерПроекта>/`
2. Положить PDF в папку
3. Создать минимальный `project_info.json`:
```json
{
  "project_id": "МойПроект-ЭМ",
  "name": "МойПроект-ЭМ",
  "section": "EM",
  "description": "Описание",
  "pdf_file": "имя_файла.pdf"
}
```
4. Запустить `python process_project.py projects/<НомерПроекта>`
5. Запустить `python crop_blocks.py projects/<НомерПроекта>`
6. Скрипт извлечёт текст, crop_blocks кропает image-блоки из PDF

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

### Верификация нормативных цитат (3-уровневая)

Система защиты от ошибочных ссылок на нормы:

```
Уровень 1: norm_quote + norm_confidence
  ↓ Каждое замечание содержит цитату нормы и уверенность (0.0–1.0)
  ↓ Заполняется на этапах 01/02/03

Уровень 2: paragraph_checks (при confidence < 0.8)
  ↓ Верификатор проверяет конкретный пункт нормы через WebSearch
  ↓ Результат: paragraph_verified true/false + actual_quote
  ↓ Записывается в norm_checks.json → paragraph_checks[]

Уровень 3: norms_paragraphs.json (накопительный кеш)
  ↓ Подтверждённые цитаты сохраняются для будущих аудитов
  ↓ update_norms_db.py автоматически пополняет из paragraph_checks
```

**Ключевые файлы:**
- `norms_db.json` — статус документов (действует/заменён/отменён), 176+ записей
- `norms_paragraphs.json` — проверенные цитаты конкретных пунктов
- `norm_checks.json` (в _output/) — результат верификации проекта

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

## Области проверки проекта ЭОМ

1. **Электроснабжение** — категория надёжности, схема, расчёт нагрузок (СП 256, табл. 7.1), трансформаторы
2. **Распределительные сети** — кабели, прокладка, защитные аппараты, селективность
3. **Групповые сети квартир** — линии, сечения, УЗО (30/10 мА), УЗДП (СП 256, прил. В)
4. **Освещение** — нормы (СП 52.13330), аварийное/эвакуационное
5. **Заземление** — TN-C-S / TN-S, ОСУП, ДСУП, молниезащита
6. **Слаботочные** — связь, домофон, ТВ, АСКУЭ
7. **Пожарная безопасность** — кабели НПО, питание СПЗ (СП 6.13130), огнестойкость

### Чек-лист по типам чертежей

**Однолинейная схема:** номиналы АВ, УЗО/УЗДП, ТТ, АВР, учёт, соответствие кабелей спецификации.

**Схемы щитов:** совпадение групп со спецификацией, сечения, дубли групп, PE-шина.

**Планы:** цвета кабелей (норм./ОКЛ), щиты = спецификация, трассы ОКЛ, расположение ЩУ-ЗС.

**Узел ввода:** прокладка кабелей, защита от повреждений, сечения вводных кабелей.

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
| Блоков нет | Запусти `crop_blocks.py` |

### Порядок инициализации сеанса

1. Определить источник текста (`text_source` в `project_info.json`)
2. Проверить наличие блоков (`_output/blocks/`) — если нет, запустить `crop_blocks.py`
3. При наличии MD — сверять графику на блоках с `[IMAGE]` описаниями
4. Прочитать нормативную базу дисциплины для актуальных норм

## Legacy-код (не удалять, но не развивать)

- `generate_tile_batches.py`, `merge_tile_results.py` — старый тайловый метод (заменён блоками)
- `claude_runner.py`: `run_tile_batch`, `run_main_audit`, `run_triage`, `run_smart_merge` — стабы, перенаправляют на блоковые функции
- `write_batch*.py` — тестовые скрипты для отладки пакетных результатов

## Запрещённые действия

- НЕ ссылайся на устаревшие нормы без пометки о статусе
- НЕ давай рекомендаций без привязки к конкретному пункту нормы
- НЕ придумывай номера пунктов — если не уверен, скажи прямо
- НЕ используй нормы других стран без оговорки
- НЕ путай обязательные и добровольные требования
- НЕ перечитывай весь проект при ответе на вопрос — используй JSON-файлы этапов
