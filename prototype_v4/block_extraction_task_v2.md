> **OUTPUT LANGUAGE:** Все текстовые значения MUST быть на русском (точно как в проекте).
> **RESPONSE FORMAT:** Respond with valid JSON only. No markdown, no text outside JSON.

# TYPED EXTRACTION — Пакет {BATCH_ID} из {TOTAL_BATCHES}

## Проект: {PROJECT_ID} | Дисциплина: ГРЩ

## Твоя роль — EXTRACTOR, не аудитор

**Твоя единственная задача: извлечь типизированные факты и связи.**

Ты НЕ ищешь ошибки. Ты НЕ делаешь выводы. Ты НЕ пишешь findings. Ты НЕ сравниваешь блоки между собой.

Ты **только извлекаешь**:
1. **Упоминания сущностей** (entity_mentions) — всё что видишь на блоках
2. **Связи между ними** (relation_mentions) — кто что питает, кто что защищает
3. **Неопределённости** (uncertainty_events) — где ты не уверен

---

## Входные данные

1. **Блоки для анализа** (прочитать КАЖДЫЙ через Read tool):
{BLOCK_LIST}

2. **Контекст страниц**:
{BLOCK_MD_CONTEXT}

---

## Scope

**В scope (извлекать):** ГРЩ, ВРУ, ЩСН, ЩУ, автоматы и ТТ ГРЩ, кабели ГРЩ↔ВРУ, примечания к листам, спецификации оборудования ГРЩ.

**Вне scope (помечать флагом `out_of_scope_ref`):**
- Заземление (ЭГ)
- УЗДП квартирных щитов
- Молниезащита
- Рабочая документация внутреннего электроснабжения квартир

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

## entity_mentions — Упоминания сущностей

Для КАЖДОЙ сущности на блоке создай объект:

```json
{
  "mention_id": "M-001",
  "entity_type": "line | panel | breaker | cable | current_transformer | note | spec_row | load | room | tray | fire_box | busduct | formula | norm_ref | other",
  "raw_label": "М-1.1",
  "normalized_label": "М-1.1",
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
  "attributes": [ ... ],
  "raw_text_excerpt": "ППГнг(А)-HF 2×(4×(1×185))+1×(1×185)",
  "flags": {
    "likely_ocr_artifact": false,
    "needs_adjacent_sections": false,
    "out_of_scope_ref": false
  },
  "confidence": 0.9
}
```

### Правила для entity_type

- **line** — кабельная линия как логическая сущность (М-1.1, М-2.4). Имеет `exact_keys.line_id`
- **panel** — щит/шкаф (ВРУ-1, ЩСН, ЩУ-2/Т, РП1). Имеет `exact_keys.panel_id`
- **breaker** — автоматический выключатель (QF1.1). Имеет `exact_keys.breaker_id`
- **cable** — физический кабель (марка, сечение). Связан с line через `feeds`
- **current_transformer** — ТТ (TA1.1). Имеет `exact_keys.ct_id`
- **note** — текстовое примечание к листу. `attributes[0].name = "note_text"`, `value_raw = полный текст`
- **spec_row** — строка спецификации. Имеет `exact_keys.spec_position`
- **room** — помещение. Имеет `exact_keys.room_no`

### Правила для attributes[]

Каждый атрибут — отдельный объект:

```json
{
  "name": "phase_section_mm2",
  "value_type": "integer",
  "value_raw": "185",
  "value_norm": 185,
  "unit": "mm²",
  "value_source": "vision",
  "parse_confidence": 0.95,
  "ambiguous_values": [],
  "likely_ocr_artifact": false
}
```

**Список допустимых `name`** (enum):
designation, description, position, source_panel, destination_panel,
phase_count, phase_section_mm2, neutral_count, neutral_section_mm2,
pe_count, pe_section_mm2, cable_mark, breaker_model, breaker_nominal_a,
trip_unit_type, trip_delay_capable, ct_ratio_primary_a, ct_ratio_secondary_a,
ct_accuracy_class, power_kw, apparent_power_kva, demand_factor,
voltage_drop_pct, ikz_1a, length_m, room_no, note_text, requirement_type,
norm_ref_code, formula_text, address, reserve_breaker_count, reserve_space_pct, other

**КРИТИЧНО для кабелей:**

Формат записи: `{марка} {фазы}×(1×{сечение_фазы})+{кол-во_N_или_PE}×(1×{сечение_N_или_PE})`

Примеры парсинга:
- `ППГнг(А)-HF 4×(1×185)+1×(1×185)`
  → `phase_count=4, phase_section_mm2=185, pe_count=1, pe_section_mm2=185`
- `ППГнг(А)-HF 2×4×(1×185)+2×(1×185)` (две параллельные линии)
  → `phase_count=8, phase_section_mm2=185, pe_count=2, pe_section_mm2=185`
- `4×(1×240)+(1×120)`
  → `phase_count=4, phase_section_mm2=240, pe_count=1, pe_section_mm2=120`

**ВАЖНО — N или PE?** В русских проектах:
- Если рядом написано "N", "нулевой", "нейтраль" → используй `neutral_count` / `neutral_section_mm2`
- Если "PE", "защитный", "заземляющий" → `pe_count` / `pe_section_mm2`
- Если не указано явно (просто цифра в конце формулы) → используй `pe_count` / `pe_section_mm2` по умолчанию (чаще всего это PE)
- Если видишь и N, и PE отдельно → создавай оба атрибута

### Правила для ambiguous_values

Если ты не уверен в значении — перечисли альтернативы:

```json
{
  "name": "phase_section_mm2",
  "value_raw": "185",
  "value_norm": 185,
  "ambiguous_values": [
    {"value": 150, "confidence": 0.25, "reason": "плохо видно, может быть 150"}
  ],
  "parse_confidence": 0.7
}
```

---

## relation_mentions — Связи между сущностями

Создавай связи когда видишь явную связь на блоке:

```json
{
  "relation_id": "R-001",
  "relation_type": "feeds | protects | described_in | references | applies_to | located_in | mirrors | duplicate_identifier_with | same_entity_candidate | other",
  "from_mention_id": "M-001",
  "to_mention_id": "M-005",
  "confidence": 0.9,
  "evidence_refs": [
    { "page": 7, "sheet": "Лист 3", "view_type": "single_line_scheme" }
  ]
}
```

### Типы связей

- **feeds** — линия питает щит: `line M-1.1 feeds panel ВРУ-1`
- **protects** — автомат защищает линию: `breaker QF1.1 protects line M-1.1`
- **described_in** — сущность описана в другом месте: `panel ЩУ-2/Т described_in spec_row_pos5`
- **references** — ссылка: `note references sheet 10`
- **applies_to** — правило применяется: `note applies_to all_outgoing_breakers`
- **located_in** — физическое расположение: `panel ГРЩ located_in room_12`
- **mirrors** — зеркальная пара: `breaker QF1.1 mirrors breaker QF2.1` (РП1↔РП2)
- **duplicate_identifier_with** — **КРИТИЧНО ДЛЯ #5**: если видишь один маркер дважды с разными destinations на одной схеме → создай эту связь между двумя mention_id. Пример: `M-1.5 → ВРУ-ИТП` (M-010) и `M-1.5 → ВРУ-НС` (M-011) — создай relation `duplicate_identifier_with` между M-010 и M-011
- **same_entity_candidate** — скорее всего одна сущность (используется редко)

---

## uncertainty_events — Неопределённости

Всё что ты видишь но не можешь прочитать уверенно:

```json
{
  "uncertainty_id": "U-001",
  "mention_id": "M-015",
  "kind": "ocr_artifact | ambiguous_reading | missing_attribute | cross_page_link_uncertain | out_of_scope | needs_adjacent_section | low_resolution",
  "reason": "Мелкий текст номинала автомата, возможно 50А или 80А",
  "severity": "low | medium | high"
}
```

---

## КРИТИЧЕСКИ ВАЖНО — особые случаи

### Один маркер на одной схеме дважды (#5 case)

Если на одной однолинейной схеме ты видишь **один и тот же line_id** (например М-1.5)
использованный для **двух разных назначений** (ВРУ-ИТП и ВРУ-НС):

1. Создай **два отдельных entity_mentions** — по одному на каждое назначение
2. У обоих `exact_keys.line_id = "М-1.5"`, но разные `mention_id`
3. У каждого в attributes — свой `destination_panel`
4. **Создай relation `duplicate_identifier_with`** между двумя mention_id
5. Добавь **uncertainty_event** с `kind: "ambiguous_reading"`, severity: "high"

Это **самый важный сценарий** для этой задачи — пожалуйста не объединяй два упоминания
в одно, когда у них разные destination.

### Описание щита в спецификации (#2 case)

Если видишь строку спецификации с позицией ЩУ-N/Т (например ЩУ-2/Т) **и** её описание
(например "Шкаф учета на 12 счетчиков"):

1. Создай entity `spec_row` с `exact_keys.spec_position = "ЩУ-2/Т"`
2. В attributes добавь два отдельных: `designation` и `description`
3. Дай полное описание **дословно** в value_raw поля description

Это позволит генератору определить что описание не соответствует позиции (ЩУ-**2**/Т с
описанием "**12** счётчиков").

### Примечания с требованиями (#55 case)

Примечания типа "АВ отходящих линий ГРЩ применить с электронными расцепителями" —
**самые важные** для класса 4. Извлеки дословно:

1. entity `note` с `attributes[0].name = "note_text"`, `value_raw = полный текст дословно`
2. `requirement_type` → напр. `"breaker_trip_type_requirement"`
3. Опционально: relation `applies_to` связывающая note с группой breakers если scope явно указан

### Кабели — не путай line и cable

- **line** — это **логическая линия** (М-1.1). Одна на весь проект.
- **cable** — это **физический кабель** с марой и сечением. Может быть несколько mentions одной линии на разных блоках (схема, узел, спецификация).

Для межлистовой сверки (классы 3) важны **cable mentions одной линии** из разных view_type.

Пример: линия М-1.1 упомянута на однолинейной схеме (view_type: single_line_scheme) и в узле раскладки (view_type: cable_layout_node). Это **два разных mention** одной линии — они будут сопоставлены генератором по exact line_id.

---

## Output

WRITE via Write tool: `{OUTPUT_PATH}/typed_facts_batch_{BATCH_ID_PADDED}.json`

## Правила

1. Читай КАЖДЫЙ блок через Read tool
2. НЕ ищи ошибки — только извлекай факты и связи
3. НЕ объединяй mentions от разных блоков (это работа Python builder)
4. mention_id должен быть уникальным в пределах batch (M-001, M-002, ...)
5. Для неуверенных значений — `ambiguous_values` и `uncertainty_events`
6. Сохраняй точные цитаты в `raw_text_excerpt`
7. Пиши JSON через Write — НЕ выводи в чат
8. После записи выведи краткий итог:
   "Batch N: entity_mentions=X, relation_mentions=Y, uncertainty_events=Z"
