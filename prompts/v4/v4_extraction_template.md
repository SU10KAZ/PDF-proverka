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

### ⚡ ПРАВИЛА СЖАТИЯ OUTPUT (обязательны)

Пропускай опциональные поля, когда они пусты или совпадают со значением по умолчанию. Это режет размер ответа ~20% без потери информации:

- **`raw_label`** — указывай ТОЛЬКО если отличается от `normalized_label`. Одинаково → не пиши это поле совсем.
- **`exact_keys`** — указывай ТОЛЬКО релевантный ключ для данного `entity_type` (см. mapping ниже). Прочие поля не добавляй — их там быть не должно.

  {V4_EXACT_KEYS_EXAMPLE}

- **`source_context.bbox`** — пиши ТОЛЬКО если блок занимает меньше всего чертежа (иначе не пиши bbox совсем). Не пиши `[0,0,1,1]`.
- **`attributes[].ambiguous_values`** — опускай, если массив пустой.
- **`attributes[].likely_ocr_artifact`** — опускай, если `false`.
- **`attributes[].unit`** — опускай для нечисловых значений и когда единица не нужна.
- **`flags`** — указывай ТОЛЬКО те флаги, которые `true`. Если все три false → не пиши объект `flags` совсем.
- **`confidence`** на уровне mention — опускай (достаточно per-attribute `parse_confidence`).
- **`raw_text_excerpt`** — опускай, если совпадает с `attributes[0].value_raw`.

### 🗒 СПЕЦИАЛЬНО ДЛЯ ДЛИННЫХ ПРИМЕЧАНИЙ (`entity_type: note`)

Если текст примечания уже приведён в `BLOCK_MD_CONTEXT` с маркером `[text_block_id: XXXX-XXXX-XXX]` и длиннее 300 символов — **НЕ копируй текст в output**. Вместо этого:
- `attributes[0].value_raw` = `"TEXT_REF:XXXX-XXXX-XXX"` (ровно префикс + id из маркера)
- `raw_text_excerpt` — не пиши совсем

Короткие примечания (<300 симв.) пиши дословно как раньше.

### Минимальный пример mention

```json
{
  "mention_id": "M-001",
  "entity_type": "room",
  "normalized_label": "Кухня 7.2",
  "exact_keys": { "room_no": "12" },
  "source_context": {
    "page": 7,
    "sheet": "Лист 3",
    "block_id": "A99X-NV3N-KRM",
    "view_type": "plan"
  },
  "attributes": [
    {
      "name": "area_m2",
      "value_type": "float",
      "value_raw": "7.2",
      "value_norm": 7.2,
      "unit": "м²",
      "value_source": "vision",
      "parse_confidence": 0.95
    }
  ]
}
```

Поля `raw_label`, `bbox`, `flags`, `confidence`, `ambiguous_values`, `likely_ocr_artifact`, `raw_text_excerpt` — здесь опущены, **так и должно быть**, если они имеют дефолтные значения.

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

## Привязка размеров к элементам (ОБЯЗАТЕЛЬНО)

Каждый размер/выноска/число с чертежа должен стать атрибутом конкретной сущности (entity_mention), а не висеть отдельно.

- Толщина слоя (минвата, раствор, утеплитель) → атрибут `thickness_mm` соответствующей сущности `layer`/`material`.
- Зазор, шаг, отступ → атрибут (`gap_mm`, `pitch_mm`, `offset_mm`) привязанного узла.
- Длина/сечение/диаметр → атрибут конкретного элемента (кабель, уголок, анкер).

**ЗАПРЕЩЕНО** создавать mention только с числом без привязки к элементу. Если видишь размер, но не можешь определить, к чему он относится — оформи `uncertainty_event` с `kind: "ambiguous_reading"` и reason `"размер X мм без явной привязки"`, а не бросай число в output.

---

## ЧЕК-ЛИСТ перед выдачей JSON

1. ✅ Каждый видимый элемент на чертеже имеет mention с максимумом атрибутов?
2. ✅ Каждая строка спецификации (если есть) имеет designation + description полностью?
3. ✅ Короткие примечания (<300 симв) — дословный текст. Длинные — `TEXT_REF:<block_id>` вместо копии?
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
4. Сохраняй точные цитаты в `raw_text_excerpt` (только если их нет в `attributes[].value_raw`)
5. Пиши JSON через Write — НЕ выводи в чат
