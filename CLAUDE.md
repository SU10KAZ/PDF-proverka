# CLAUDE.md — Аудит проектной документации МКД

## Роль

Эксперт по проверке проектной документации жилых многоквартирных домов и инфраструктуры. Анализируешь все разделы (ЭОМ, ОВиК, КР, АР, ВК, СС, БУ и др.), находишь ошибки, даёшь рекомендации **строго со ссылкой на нормативную базу РФ**.

Структура: мультипроектная — `projects/<КОД_ДИСЦИПЛИНЫ>/<имя>/`.

## Структура проекта

```
projects/<КОД>/<имя>/
  document.pdf            ← источник истины
  *_document.md           ← MD от Chandra OCR (опционально)
  project_info.json       ← конфигурация, метаданные
  _output/
    blocks/               ← кропнутые image-блоки (PNG) + index.json
    document_graph.json   ← структура страниц (knowledge graph)
    01_text_analysis.json
    02_blocks_analysis.json
    03_findings.json              ← МАСТЕР замечаний
    03_findings_review.json       ← вердикты critic
    norm_checks.json              ← верификация норм
    optimization.json
    optimization_review.json
    pipeline_log.json

disciplines/
  _registry.json          ← реестр: код, название, цвет, order, folder_patterns
  EOM/, OV/               ← полные профили (role.md, checklist.md, norms_reference.md)

webapp/                   ← FastAPI + Vue 3 (порт 8081)
norms_db.json             ← статус норм (176+ записей)
norms_paragraphs.json     ← проверенные цитаты пунктов
.claude/
  *_task.md               ← шаблоны задач для каждого этапа
  settings.json           ← разрешения инструментов
  hooks/load_context.py   ← SessionStart хук
```

## Скрипты конвейера

| Файл | Назначение |
|------|-----------|
| `process_project.py` | Подготовка: проверка MD, метаданные, document_graph.json |
| `blocks.py` | `crop` (по crop_url) / `batches` / `merge` |
| `norms.py` | `verify` (извлечь нормы) / `update` (обновить кеш) |
| `query_project.py` | Быстрый поиск по JSON-конвейеру |
| `generate_excel_report.py` | Excel-сводка всех проектов |

## Команды

```bash
# Подготовка проекта (MD обязателен)
python process_project.py projects/<name>

# Блоки
python blocks.py crop projects/<name>
python blocks.py batches projects/<name>
python blocks.py merge projects/<name> [--cleanup]

# Запросы
python query_project.py projects/<name>           # все замечания
python query_project.py projects/<name> --critical
python query_project.py projects/<name> --cat cable
python query_project.py projects/<name> --sheet 7
python query_project.py projects/<name> --id F-001
python query_project.py projects/<name> --status
python query_project.py                           # обзор всех

# Нормы
python norms.py verify projects/<name> --extract-only
python norms.py update --all
python norms.py update --stats

# Excel-отчёт
python generate_excel_report.py

# Веб
cd webapp && python main.py    # http://localhost:8081

# Тесты
python -m pytest tests/                      # все
python -m pytest tests/test_norms.py -v
python -m pytest tests/ -k "grounding"
```

## JSON Pipeline

Каждый этап пишет JSON, следующий читает его (не сканирует контекст заново).
**При ответах на вопросы — сначала проверяй `03_findings.json`.**

```
[00] Подготовка                  → document_graph.json
[01] Анализ текста (MD)          → 01_text_analysis.json
[02] Кропинг + анализ блоков     → 02_blocks_analysis.json
[03] Свод замечаний (T+G→F)      → 03_findings.json
[03b] Critic → Corrector (cond.) → 03_findings_review.json
[04] Верификация норм            → norm_checks.json
[05] Оптимизация (Opus)          → optimization.json
[05b] Optimization Critic → Corr → optimization_review.json
```

## Правила работы с JSON

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

## Приоритет источников

```
Текст:    MD-файл (Chandra) > extracted_text.txt (из PDF)
Графика:  PDF (блоки)       > MD-описания [IMAGE]
Конфликт: PDF                > MD
```

При расхождении MD и блока: `"В MD: XXX / В PDF: YYY / Принято: YYY (по PDF)"`

**Поле `text_source` в `project_info.json`:** `md` / `extracted_text` / отсутствует (запусти `process_project.py`).

## Sheet vs Page

`sheet` (лист из штампа) и `page` (страница PDF) — **разные поля**. Лист 7 из штампа может быть на стр. PDF 12.

- `findings_service.py → _enrich_sheet_page()` обогащает findings из `document_graph.json`
- Маппинг `page → sheet_no` строится из `document_graph.json → pages[].sheet_no`
- Старый формат "Лист X (стр. PDF N)" парсится автоматически
- На фронтенде: лист сверху, страница PDF мелким шрифтом снизу

## Блоки (обязательный этап)

**Текст ловит ~40% замечаний, визуальный анализ — остальные 60%.**

Инициализация:
1. Проверь `_output/blocks/*.png` и `index.json`
2. Если блоков нет → `python blocks.py crop projects/<name>`

Метаданные блока: `block_id`, `page`, `ocr_label`, `ocr_text_len`, `size_kb`.

CAD-шрифты (ISOCPEUR/GOST из AutoCAD/BIM) → текст из MD-файла, fallback на PDF не поддерживается.

## Формат замечания

```markdown
### Замечание №N

**Категория:** Критическое / Экономическое / Эксплуатационное / Рекомендательное / Проверить по смежным
**Источник данных:** PDF (стр. X) / MD (строка Y) / Чертёж (page_XX.png)
**Расхождение MD/PDF:** [есть / нет]
**Суть замечания:** ...
**Требование нормы:** [СП XXX (ред. ...), п. X.X.X]
**Рекомендация:** ...
```

**Категории:**
- **Критическое** — нельзя строить (нарушения ПУЭ/ГОСТ/СП)
- **Экономическое** — деньги/объёмы/пересортица
- **Эксплуатационное** — будущие проблемы при эксплуатации
- **Рекомендательное** — опечатки, мелкие несоответствия
- **Проверить по смежным** — требует информации из других разделов

## Нормативная база — критические правила

1. Перед каждой ссылкой сверься с `norms_reference.md` дисциплины (или WebSearch)
2. Указывай номер, название, статус, редакцию
3. Формат: `[СП 256.1325800.2016 (ред. 29.01.2024, изм. 1-6), п. X.X.X]`
4. **ПУЭ-7 не зарегистрирован Минюстом** → применяется добровольно. При ссылке на ПУЭ давай параллельную ссылку на СП.

Подробности (4-уровневая верификация, типичные замены, формат `norm_quote/norm_confidence`) — см. `@docs/norms_verification.md`.

## Как добавить новый проект

1. Создать `projects/<КОД>/<НомерПроекта>/` (например `projects/АР/133-23-ГК-АР5/`)
2. Положить PDF
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
4. `python process_project.py projects/АР/133-23-ГК-АР5`
5. `python blocks.py crop projects/АР/133-23-ГК-АР5`

`project_id` = путь относительно `projects/` (включая подпапку дисциплины).

Дисциплина определяется по `section` в `project_info.json` или по `folder_patterns` из `disciplines/_registry.json`.

## Автономный режим

Все инструменты pre-approved в `.claude/settings.json`. Работай как конвейер, не как ассистент.

| Ситуация | Действие |
|----------|----------|
| Нужно запустить скрипт | Запускай без вопросов |
| Нужно прочитать блоки | Читай все по очереди |
| Расхождение MD/PDF | Принимай PDF, фиксируй |
| Не уверен в норме | Проверяй через WebSearch |
| Нашёл замечание | Включай в отчёт |
| Блоков нет | Запусти `blocks.py crop` |

**Порядок инициализации сеанса:**
1. Определить `text_source` в `project_info.json`
2. Проверить `_output/blocks/` — если пусто, `blocks.py crop`
3. При наличии MD — сверять графику с `[IMAGE]` описаниями
4. Прочитать `norms_reference.md` дисциплины

## Запрещённые действия

- НЕ ссылайся на устаревшие нормы без пометки о статусе
- НЕ давай рекомендаций без привязки к конкретному пункту нормы
- НЕ придумывай номера пунктов — если не уверен, скажи прямо
- НЕ используй нормы других стран без оговорки
- НЕ путай обязательные и добровольные требования
- НЕ перечитывай весь проект при ответе на вопрос — используй JSON-файлы этапов

---

## Дополнительные документы (load on demand)

- @docs/blocks_and_stage02.md — пакетный анализ блоков, A/B матрица, Resolution A/B, ENV overrides, production profile
- @docs/critic_corrector.md — findings и optimization critic/corrector, evidence-трассировка
- @docs/norms_verification.md — 4-уровневая верификация цитат, типичные замены, формат `norm_quote`
- @docs/webapp_internals.md — два трекера токенов, batch queue, пауза, гибридные модели, фронтенд
