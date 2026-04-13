> **OUTPUT LANGUAGE:** Все текстовые значения на русском (как в проекте).
> **RESPONSE FORMAT:** Valid JSON only.

# TYPED EXTRACTION — Пакет {BATCH_ID} из {TOTAL_BATCHES}

## ГЛАВНОЕ ПРАВИЛО

Ты — extractor. Читаешь блоки → возвращаешь **структурированные факты** в JSON.

**НЕ ищешь ошибки. НЕ сравниваешь блоки. НЕ пишешь findings.**

Только: увидел сущность → записал её атрибуты в JSON.

---

## ПРИОРИТЕТ — 5 критичных задач

Эти 5 задач важнее всего остального. Если видишь эти данные — **обязательно** их извлеки.

### Задача 1: Парсинг кабельных марок (КРИТИЧНО)

**Везде где видишь марку кабеля — разобрать её на жилы.**

Формат: `МАРКА ФАЗЫ×(1×СЕЧЕНИЕ)+NPE×(1×СЕЧЕНИЕ_NPE)`

**5 примеров парсинга:**

```
"ППГнг(А)-HF 4×(1×185)+1×(1×185)"
→ cable_mark="ППГнг(А)-HF", phase_count=4, phase_section_mm2=185,
  pe_or_n_count=1, pe_or_n_section_mm2=185
```

```
"ППГнг(А)-HF 2×(4×(1×185))+1×(1×185)"
→ cable_mark="ППГнг(А)-HF", phase_count=8 (две параллельные по 4),
  phase_section_mm2=185, pe_or_n_count=1, pe_or_n_section_mm2=185
```

```
"ППГнг(А)-HF 2×4×(1×185)+2×(1×185)"
→ cable_mark="ППГнг(А)-HF", phase_count=8, phase_section_mm2=185,
  pe_or_n_count=2, pe_or_n_section_mm2=185
```

```
"ППГнг(А)-HF 4×(1×240)+(1×120)"
→ cable_mark="ППГнг(А)-HF", phase_count=4, phase_section_mm2=240,
  pe_or_n_count=1, pe_or_n_section_mm2=120
```

```
"ППГнг(А)-HF 4×(1×70)+(1×50)"
→ cable_mark="ППГнг(А)-HF", phase_count=4, phase_section_mm2=70,
  pe_or_n_count=1, pe_or_n_section_mm2=50
```

**Каждое число в марке кабеля МОЖЕТ быть источником ошибки.** Не пропускай их.

Если на блоке есть таблица/узел с кабелями — для **каждой** линии (М-1.1, М-2.3 и т.д.) прочитай её марку и разбери её **по всем 5 полям**.

### Задача 2: Описания щитов учёта (КРИТИЧНО для #2)

Если видишь таблицу спецификации оборудования — для **каждой строки** с ЩУ/ВРУ/ЩСН прочитай **полное описание дословно**.

**Пример строки спецификации:**

```
Поз.5  ЩУ-2/Т  Шкаф учёта на 12 счётчиков трансф.вкл.  1шт
```

→ извлечь как `entity_type: spec_row` с полями:
- `designation = "ЩУ-2/Т"`
- `description = "Шкаф учёта на 12 счётчиков трансф.вкл."`

**ОСОБЕННО** важно: не обрезать описание. Полностью. Даже если оно длинное. Мелкий текст после обозначения — это и есть description.

### Задача 3: Примечания с требованиями (КРИТИЧНО для #55)

Если на блоке есть текст-примечание — прочитай его **дословно**.

**Ключевые фразы которые нужно ловить:**
- "применить [что-то] с электронными расцепителями"
- "применить кабели с индексом FR"
- "не менее N% резервных АВ"
- "в огнестойком исполнении EI[число]"
- "ТТ класс точности не хуже 0.5S"
- "предусмотреть дополнительные контакты SA/SF"
- "прокладка взаиморезервируемых кабелей в разных лотках"

Для **каждого** такого примечания — создай `entity_type: note`, `note_text = полный дословный текст`.

### Задача 4: Дубли маркеров (КРИТИЧНО для #5)

Если на **ОДНОЙ схеме** видишь один и тот же маркер линии (например `М-1.5`) использованный **дважды** для разных направлений:

1. Создай **два отдельных** entity_mention с `line_id = "М-1.5"`
2. У каждого — свой `destination_panel` (куда ведёт)
3. Добавь `relation_type: "duplicate_identifier_with"` между их mention_id

**НЕ объединяй** два упоминания в одно. Это **самое важное**.

### Задача 5: Автоматы QF с марками (для #55)

Каждый автомат QF — обязательно извлеки:
- `designation = "QF1.1"` (или как написано)
- `breaker_model = "ВА-335А"` (или как написано)
- `breaker_nominal_a = 630` (число ампер)
- `location` — где установлен (РП1, РП2, ГРЩ, ЩСН)

---

## Вторичные задачи (если остались ресурсы)

После выполнения 5 главных задач, можешь извлечь:
- length_m, voltage_drop_pct, ikz_1a (это НЕ заменяет phase_section_mm2!)
- power_kw, apparent_power_kva, demand_factor
- ТТ (current_transformer) с ratio_primary_a и accuracy_class
- Номера помещений (room_no) если явно видны

**Эти поля — бонус. Главные — Задачи 1-5.**

---

## Входные данные

**Блоки для анализа:**
{BLOCK_LIST}

**Контекст страниц:**
{BLOCK_MD_CONTEXT}

`page` = страница PDF, `sheet` = лист из штампа. Оба в метаданных блока выше.

---

## Scope ГРЩ

**В scope:** кабели и щиты ГРЩ (ВРУ, ЩСН, ЩУ), автоматы, ТТ, примечания к ГРЩ, спецификации.

**Вне scope (помечай `out_of_scope_ref: true`):** заземление (ЭГ), УЗДП квартирных щитов, молниезащита, рабочая документация квартир.

---

## Формат выхода

```json
{
  "schema_version": "1.0",
  "document_id": "{PROJECT_ID}",
  "discipline": "GRSh",
  "scope": {
    "included": ["GRSh"],
    "excluded": ["EG", "UZDP", "ApartmentPanels", "LightningProtection"]
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
  "entity_type": "line | panel | breaker | cable | current_transformer | note | spec_row | room | other",
  "normalized_label": "М-1.1",
  "raw_label": "М-1.1",
  "exact_keys": {
    "line_id": "М-1.1",
    "panel_id": null,
    "breaker_id": null,
    "ct_id": null,
    "room_no": null,
    "spec_position": null
  },
  "source_context": {
    "page": 7,
    "sheet": "Лист 3",
    "block_id": "A99X-NV3N-KRM",
    "view_type": "single_line_scheme | cable_layout_node | specification | general_notes | load_table | plan | other",
    "bbox": { "x1": 0.3, "y1": 0.2, "x2": 0.5, "y2": 0.4 }
  },
  "attributes": [
    {
      "name": "cable_mark",
      "value_type": "string",
      "value_raw": "ППГнг(А)-HF",
      "value_norm": "ППГнг(А)-HF",
      "unit": null,
      "value_source": "vision",
      "parse_confidence": 0.9,
      "ambiguous_values": [],
      "likely_ocr_artifact": false
    },
    {
      "name": "phase_section_mm2",
      "value_type": "integer",
      "value_raw": "185",
      "value_norm": 185,
      "unit": "mm²",
      "value_source": "vision",
      "parse_confidence": 0.9
    }
  ],
  "raw_text_excerpt": "ППГнг(А)-HF 2×(4×(1×185))+1×(1×185)",
  "flags": {
    "likely_ocr_artifact": false,
    "needs_adjacent_sections": false,
    "out_of_scope_ref": false
  },
  "confidence": 0.9
}
```

### Поля атрибутов (name) — enum

Используй ТОЛЬКО эти имена (не выдумывай):

**Для кабелей:**
- `cable_mark` — марка (ППГнг(А)-HF)
- `phase_count` — количество фазных жил (integer)
- `phase_section_mm2` — сечение фазной жилы (integer)
- `pe_or_n_count` — количество PE/N жил (integer)
- `pe_or_n_section_mm2` — сечение PE/N жилы (integer)
- `neutral_count` — количество нейтральных (если явно отделено от PE)
- `neutral_section_mm2` — сечение нейтрали (если явно)
- `pe_count` — количество PE (если явно отделено от N)
- `pe_section_mm2` — сечение PE (если явно)
- `source_panel` — откуда питание
- `destination_panel` — куда идёт
- `length_m` — длина
- `power_kw` — мощность
- `voltage_drop_pct` — потери
- `ikz_1a` — ток КЗ

**Для автоматов:**
- `breaker_model` — марка (ВА-335А)
- `breaker_nominal_a` — номинал A
- `trip_unit_type` — "термомагнитный" / "электронный" / null
- `position` — где установлен

**Для щитов/спецификации:**
- `designation` — обозначение (ЩУ-2/Т)
- `description` — полное описание (ОБЯЗАТЕЛЬНО для строк спецификации)

**Для ТТ:**
- `ct_ratio_primary_a` — первичный ток
- `ct_ratio_secondary_a` — вторичный ток (обычно 5)
- `ct_accuracy_class` — класс точности ("0.5S")

**Для примечаний:**
- `note_text` — ПОЛНЫЙ дословный текст (не обрезать!)
- `requirement_type` — тип требования

**Для помещений:**
- `room_no` — номер

---

## relation_mentions — связи

```json
{
  "relation_id": "R-001",
  "relation_type": "feeds | protects | described_in | applies_to | located_in | mirrors | duplicate_identifier_with",
  "from_mention_id": "M-001",
  "to_mention_id": "M-005",
  "confidence": 0.9,
  "evidence_refs": [{ "page": 7, "sheet": "Лист 3", "view_type": "single_line_scheme" }]
}
```

**`duplicate_identifier_with`** — если один и тот же `line_id` на ОДНОЙ схеме использован для разных направлений (см. Задачу 4), создай эту связь между mentions.

---

## uncertainty_events

```json
{
  "uncertainty_id": "U-001",
  "mention_id": "M-015",
  "kind": "ocr_artifact | ambiguous_reading | missing_attribute | low_resolution",
  "reason": "Мелкий текст сечения кабеля, возможно 120 или 150",
  "severity": "low | medium | high"
}
```

---

## ЧЕК-ЛИСТ перед выдачей JSON

Перед тем как выдать результат, пройдись по этому списку:

1. ✅ **Каждый видимый line_id** имеет mention с максимумом атрибутов?
2. ✅ **Каждая марка кабеля** разобрана на cable_mark + phase_count + phase_section_mm2 + pe_or_n_count + pe_or_n_section_mm2?
3. ✅ **Каждая строка спецификации** (если есть) имеет designation + description полностью?
4. ✅ **Каждое примечание** имеет полный note_text дословно?
5. ✅ **Каждый QF** имеет breaker_model + breaker_nominal_a + location?
6. ✅ **Дубль маркера на одной схеме** → два отдельных mention + relation duplicate_identifier_with?

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
