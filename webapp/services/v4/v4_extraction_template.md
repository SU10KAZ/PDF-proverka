> **OUTPUT LANGUAGE:** Все текстовые значения на русском (как в проекте).
> **RESPONSE FORMAT:** Valid JSON only.

# TYPED EXTRACTION — Пакет {BATCH_ID} из {TOTAL_BATCHES}

## ГЛАВНОЕ ПРАВИЛО

Ты — extractor. Читаешь блоки → возвращаешь **структурированные факты** в JSON.

**НЕ ищешь ошибки. НЕ сравниваешь блоки. НЕ пишешь findings.**

Только: увидел сущность → записал её атрибуты в JSON.

---

## ПРИОРИТЕТНЫЕ ЗАДАЧИ

{V4_PRIORITY_TASKS}

---

## Вторичные задачи (если остались ресурсы)

После выполнения главных задач, можешь извлечь дополнительные атрибуты любых сущностей, которые видишь на чертеже.

---

## Входные данные

**Блоки для анализа:**
{BLOCK_LIST}

**Контекст страниц:**
{BLOCK_MD_CONTEXT}

`page` = номер страницы, `sheet` = лист из штампа. Оба в метаданных блока выше.

---

## Scope

{V4_SCOPE}

---

## Формат выхода

```json
{
  "schema_version": "1.0",
  "document_id": "{PROJECT_ID}",
  "discipline": "{SECTION}",
  "scope": {
    "included": {V4_SCOPE_INCLUDED},
    "excluded": {V4_SCOPE_EXCLUDED}
  },
  "entity_mentions": [ ... ],
  "relation_mentions": [ ... ],
  "uncertainty_events": [ ... ]
}
```

---

## entity_mentions — структура

```json
{
  "mention_id": "M-001",
  "entity_type": "{V4_ENTITY_TYPES_ENUM}",
  "normalized_label": "...",
  "raw_label": "...",
  "exact_keys": {V4_EXACT_KEYS_EXAMPLE},
  "source_context": {
    "page": 7,
    "sheet": "Лист 3",
    "block_id": "A99X-NV3N-KRM",
    "view_type": "plan | specification | scheme | general_notes | detail | section | other",
    "bbox": { "x1": 0.3, "y1": 0.2, "x2": 0.5, "y2": 0.4 }
  },
  "attributes": [
    {
      "name": "...",
      "value_type": "string | integer | float | boolean",
      "value_raw": "...",
      "value_norm": "...",
      "unit": null,
      "value_source": "vision",
      "parse_confidence": 0.9,
      "ambiguous_values": [],
      "likely_ocr_artifact": false
    }
  ],
  "raw_text_excerpt": "...",
  "flags": {
    "likely_ocr_artifact": false,
    "needs_adjacent_sections": false,
    "out_of_scope_ref": false
  },
  "confidence": 0.9
}
```

### Допустимые entity_type

{V4_ENTITY_TYPES_LIST}

### Допустимые атрибуты (name) по entity_type

{V4_ATTRIBUTES_LIST}

---

## relation_mentions — связи

```json
{
  "relation_id": "R-001",
  "relation_type": "feeds | protects | described_in | applies_to | located_in | mirrors | duplicate_identifier_with",
  "from_mention_id": "M-001",
  "to_mention_id": "M-005",
  "confidence": 0.9,
  "evidence_refs": [{ "page": 7, "sheet": "Лист 3", "view_type": "plan" }]
}
```

**`duplicate_identifier_with`** — если одно обозначение на ОДНОМ чертеже используется для разных элементов, создай эту связь между mentions.

---

## uncertainty_events

```json
{
  "uncertainty_id": "U-001",
  "mention_id": "M-015",
  "kind": "ocr_artifact | ambiguous_reading | missing_attribute | low_resolution",
  "reason": "Мелкий текст, возможно неверное прочтение",
  "severity": "low | medium | high"
}
```

---

## ЧЕК-ЛИСТ перед выдачей JSON

1. ✅ Каждый видимый элемент на чертеже имеет mention с максимумом атрибутов?
2. ✅ Каждая строка спецификации (если есть) имеет designation + description полностью?
3. ✅ Каждое примечание имеет полный note_text дословно?
4. ✅ Дубль обозначения на одной схеме → два отдельных mention + relation duplicate_identifier_with?
5. ✅ Все числовые значения (размеры, площади, расходы) записаны как числа, не строки?

Если хотя бы одна галочка не стоит — **перечитай блок ещё раз**.

---

## Output

WRITE via Write tool: `{OUTPUT_PATH}/typed_facts_batch_{BATCH_ID_PADDED}.json`

## Правила

1. Читай КАЖДЫЙ блок через Read tool
2. НЕ ищи ошибки — только извлекай факты
3. Для сомнительных значений — `ambiguous_values` и `uncertainty_events`
4. Сохраняй точные цитаты в `raw_text_excerpt`
5. Пиши JSON через Write — НЕ выводи в чат
