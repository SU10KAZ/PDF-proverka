# ТРИАЖ СТРАНИЦ — Определение приоритетов для анализа

## Режим работы
Работай АВТОНОМНО. Не задавай вопросов. Не жди подтверждений.
Прочитай текстовые данные проекта. Определи приоритеты страниц. Запиши результат в JSON.

## Проект
- **ID:** {PROJECT_ID}
- **Папка:** {PROJECT_PATH}
- **Выходная папка:** {OUTPUT_PATH}

## Входные данные

### 1. MD-файл (основной источник текста)
**MD-файл:** `{MD_FILE_PATH}`

Если путь к MD-файлу НЕ "(нет)":
1. **ЧИТАТЬ MD-файл как ОСНОВНОЙ текстовый источник** — содержит:
   - Структурированный текст каждой страницы (`## СТРАНИЦА N`)
   - Таблицы Markdown (спецификации, ведомости)
   - Описания чертежей (`IMAGE:` блоки — тип, оси, сущности)
2. `extracted_text.txt` использовать как **дополнительный источник**

Если MD-файл отсутствует ("(нет)"):
- Читать `{OUTPUT_PATH}/extracted_text.txt` как единственный источник

### 2. Нормативная база
Сверяться с файлом: `{DISCIPLINE_NORMS_FILE}`

## ЭТАП 00 — ИНИЦИАЛИЗАЦИЯ

Прочитать `{PROJECT_PATH}/project_info.json`.

ЗАПИСАТЬ ФАЙЛ `{OUTPUT_PATH}/00_init.json`:
```json
{
  "project_id": "<из project_info.json>",
  "project_name": "<из project_info.json>",
  "section": "<из project_info.json>",
  "audit_started": "<ISO datetime>",
  "audit_mode": "smart_parallel",
  "files": {
    "pdf": { "path": "document.pdf", "exists": true, "size_kb": 0 },
    "extracted_txt": { "path": "_output/extracted_text.txt", "exists": true, "size_kb": 0 },
    "md_file": { "path": "<md_file или null>", "exists": true, "size_kb": 0 }
  },
  "text_source": "md | extracted_text",
  "tiles": {
    "available": false,
    "note": "Тайлы будут нарезаны после триажа"
  },
  "pipeline_status": {
    "00_init": "done",
    "01_text_analysis": "pending",
    "01_5_tile_cutting": "pending",
    "02_tiles_analysis": "pending",
    "03_findings": "pending"
  }
}
```

## ЭТАП 01 — АНАЛИЗ ТЕКСТА + ТРИАЖ СТРАНИЦ

### Шаг 01.1: Текстовый анализ

Что искать:
{DISCIPLINE_TEXT_ANALYSIS}
- Противоречия между разделами ПЗ

### Шаг 01.2: ТРИАЖ СТРАНИЦ (КРИТИЧЕСКИ ВАЖНЫЙ ШАГ)

Для каждой страницы с графикой (IMAGE в MD) определи приоритет проверки.

**Правила триажа:**

{DISCIPLINE_TRIAGE_TABLE}

**Повышение приоритета:**
- Если в тексте найдено подозрительное значение → повысить до HIGH
- Если в тексте ссылка на конкретный чертёж с проблемой → HIGH
- Если preliminary_finding с `needs_tile_verification: true` → HIGH

### Шаг 01.3: Запись результата

ЗАПИСАТЬ ФАЙЛ `{OUTPUT_PATH}/01_text_analysis.json`:
```json
{
  "meta": { "pages_read": 0, "timestamp": "<ISO>" },
  "project_params": {DISCIPLINE_PROJECT_PARAMS_JSON},
  "normative_refs_found": [
    {
      "doc": "СП/ГОСТ/ПУЭ...",
      "stated_edition": "...",
      "actual_edition": "...",
      "status": "actual|outdated_edition|cancelled|replaced",
      "severity": "ok|minor|major|critical",
      "page_pz": 0
    }
  ],
  "cable_marks_mentioned": [
    { "mark": "...", "pages": [0], "status": "valid|invalid|check" }
  ],
  "preliminary_findings": [
    {
      "id": "T-NNN",
      "severity": "КРИТИЧЕСКОЕ|ЭКОНОМИЧЕСКОЕ|ЭКСПЛУАТАЦИОННОЕ|РЕКОМЕНДАТЕЛЬНОЕ|ПРОВЕРИТЬ ПО СМЕЖНЫМ",
      "category": "normative_refs|cable|protection|documentation|calculation",
      "source": { "file": "md_file или extracted_text.txt", "page_pdf": 0 },
      "finding": "Конкретное описание проблемы",
      "norm": "Документ, пункт",
      "needs_tile_verification": true,
      "verify_tile": "page_XX или null"
    }
  ],
  "page_triage": [
    {
      "page": 7,
      "priority": "HIGH",
      "type": "Однолинейная схема",
      "reason": "Основная однолинейная — проверка номиналов АВ, кабелей, селективности",
      "expected_checks": ["номиналы АВ", "марки кабелей", "УЗО/УЗДП", "ТТ"]
    },
    {
      "page": 15,
      "priority": "SKIP",
      "type": "Условные обозначения",
      "reason": "Легенда — маловероятны замечания"
    }
  ]
}
```

## Правила

1. Прочитай ВЕСЬ текстовый источник — не пропускай страницы
2. page_triage ОБЯЗАН содержать ВСЕ страницы с графикой (IMAGE)
3. Для каждой страницы ОБЯЗАТЕЛЕН priority, type, reason
4. severity в findings — ТОЛЬКО одно из 5 значений: КРИТИЧЕСКОЕ, ЭКОНОМИЧЕСКОЕ, ЭКСПЛУАТАЦИОННОЕ, РЕКОМЕНДАТЕЛЬНОЕ, ПРОВЕРИТЬ ПО СМЕЖНЫМ
5. Пиши JSON через инструмент Write — НЕ выводи в чат
6. После записи JSON выведи краткий итог: сколько HIGH, MEDIUM, LOW, SKIP
