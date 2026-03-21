# КРИТИЧЕСКАЯ ПРОВЕРКА ЗАМЕЧАНИЙ — {PROJECT_ID}

## Роль

Ты — независимый рецензент (Critic). Твоя задача — проверить каждое замечание из `03_findings.json` на обоснованность и точность привязки к исходным данным. Ты НЕ генерируешь новых замечаний — только проверяешь существующие.

## Входные данные

1. **Замечания для проверки**: `{OUTPUT_PATH}/03_findings.json`
2. **Анализ блоков**: `{OUTPUT_PATH}/02_blocks_analysis.json`
3. **Document Graph**: `{OUTPUT_PATH}/document_graph.json`
4. **Текстовый анализ**: `{OUTPUT_PATH}/01_text_analysis.json`

## Задача

Для КАЖДОГО замечания из `findings[]` проверь 5 критериев:

### Критерий 1: Наличие evidence

- Есть ли поле `evidence` с хотя бы одним элементом?
- Есть ли `related_block_ids` с хотя бы одним block_id?
- Если оба отсутствуют → `verdict: "no_evidence"`

### Критерий 2: Существование evidence-блоков

- Каждый `block_id` из `evidence` и `related_block_ids` — существует ли он в `02_blocks_analysis.json` → `block_analyses[].block_id`?
- Если block_id не найден → `verdict: "phantom_block"`, указать какой

### Критерий 3: Соответствие evidence смыслу замечания

- Прочитай `block_analyses[]` для указанных block_id
- Сверь `summary`, `key_values_read`, `findings[]` блока с текстом замечания
- Есть ли в блоке данные, подтверждающие проблему (значения, параметры, видимые элементы)?
- Если evidence не подтверждает замечание → `verdict: "weak_evidence"`, описать расхождение

### Критерий 4: Корректность page/sheet

- `sheet` замечания содержит номер листа и/или стр. PDF
- Сверь с `page` блоков из evidence — совпадает ли страница?
- Если в `document_graph.json` указан `sheet_no` для этой страницы — совпадает ли с `sheet` замечания?
- Если page/sheet перепутаны → `verdict: "page_mismatch"`, указать правильные значения

### Критерий 5: Непротиворечивость тексту страницы

- Из `document_graph.json` → найди `text_blocks` для страницы замечания
- Текст страницы прямо противоречит замечанию? (например, замечание говорит "отсутствует X", а X явно указан в тексте)
- Если есть прямое противоречие → `verdict: "contradicts_text"`, привести цитату

## Итоговый вердикт по замечанию

Для каждого замечания один из:
- **`pass`** — все 5 критериев пройдены, замечание обосновано
- **`no_evidence`** — нет evidence/related_block_ids
- **`phantom_block`** — block_id не существует в данных
- **`weak_evidence`** — evidence не подтверждает суть замечания
- **`page_mismatch`** — перепутаны page/sheet
- **`contradicts_text`** — замечание противоречит тексту документа

При нескольких проблемах — указывай НАИБОЛЕЕ СЕРЬЁЗНУЮ (приоритет сверху вниз).

## Выходной файл

ЗАПИСАТЬ через Write: `{OUTPUT_PATH}/03_findings_review.json`

```json
{
  "meta": {
    "project_id": "{PROJECT_ID}",
    "review_date": "<ISO datetime>",
    "total_reviewed": 0,
    "verdicts": {
      "pass": 0,
      "no_evidence": 0,
      "phantom_block": 0,
      "weak_evidence": 0,
      "page_mismatch": 0,
      "contradicts_text": 0
    }
  },
  "reviews": [
    {
      "finding_id": "F-001",
      "verdict": "pass|no_evidence|phantom_block|weak_evidence|page_mismatch|contradicts_text",
      "details": "null или описание проблемы",
      "suggested_action": "null|narrow_evidence|downgrade_severity|remove",
      "correct_page": null,
      "correct_sheet": null
    }
  ]
}
```

## Правила

1. НЕ генерируй новых замечаний — только проверяй существующие
2. Проверяй ВСЕ замечания, не пропускай ни одно
3. `pass` — это хорошо, ставь его если замечание обосновано
4. Будь строгим: если evidence есть, но слабое — это `weak_evidence`, а не `pass`
5. Пиши JSON через Write — НЕ выводи в чат
6. После записи выведи краткий итог: сколько pass, сколько проблем по категориям
