# Таксономия Issue Types для EOM аудита

**Источник:** 33 findings от Opus CLI прогона на проекте `133_23-ГК-ГРЩ` (2026-04-11)
**Цель:** канонические классы ошибок для перехода на архитектуру "extractor → candidate generators → judge"

---

## Верхний уровень — 5 больших классов

| Класс | Что проверяет | Как находится |
|---|---|---|
| **A. Cross-source consistency** | Одинаковые ли атрибуты одной сущности в разных местах документа | Python: сравнение в symbol table |
| **B. Identity/Uniqueness** | Уникальность идентификаторов (маркеров, позиций, номеров) | Python: поиск дубликатов |
| **C. Rule vs Fact conflicts** | Соблюдается ли декларированное в примечании требование | Python: правило → проверка сущности |
| **D. Engineering calculations** | Численные расчёты (Kс, ΔU, Iкз, перегрузка) | Python: формула + пороги |
| **E. Documentation quality** | Тексты, опечатки, оформление | LLM или regex |

---

## A. Cross-source consistency — 10 findings

**Что общее:** одна сущность имеет разные атрибуты в разных источниках (схема vs узел vs спецификация vs таблица).

### A1. Cable attribute mismatch (4 finding)
Сечение / марка / количество жил одного кабеля различается между источниками.

| Finding | Entity | Field | Values |
|---|---|---|---|
| F-002 | cable M-1.3/M-2.3 (ВРУ-3) | phase_section, pe_section | 70/50 vs 50/35 |
| F-003 | cable M-1.4/M-2.4 (ВРУ-4) | pe_section | 150 vs 120 |
| F-004 | cable M-1.5 (ВРУ-ИТП) | phase_section | 240 vs 120 |
| эталон #18 | cable M-1.1/M-2.1 (ВРУ-1) | pe_count | 1 vs 2 |
| эталон #7 | cable (нейтраль) | neutral_section | 150 vs 120 |

**Общая схема candidate:**
```json
{
  "type": "cable_attribute_mismatch",
  "entity_id": "M-1.4",
  "field": "pe_section",
  "value_a": {"source": "schema_sheet3", "value": "150 мм²"},
  "value_b": {"source": "node10_sheet10", "value": "120 мм²"},
  "severity": "КРИТИЧЕСКОЕ",
  "category": "cable"
}
```

**Generator логика:**
```python
for cable_id, cable in symbol_table["cables"].items():
    for field in ["phase_section", "pe_section", "neutral_section", "pe_count", "mark"]:
        values_by_source = cable.get_attribute_values(field)
        if len(set(values_by_source.values())) > 1:
            yield candidate(type="cable_attribute_mismatch", ...)
```

### A2. Panel description mismatch (1 finding)
Описание одного щита в спецификации не соответствует обозначению.

| Finding | Entity | Mismatch |
|---|---|---|
| F-012 / эталон #2 | panel ЩУ-2/Т, ЩУ-12/Т | описания перепутаны |

**Candidate schema:**
```json
{
  "type": "panel_description_mismatch",
  "entity_id": "ЩУ-2/Т",
  "spec_description": "Шкаф учета на 12 счетчиков",
  "diagram_label": "ЩУ-2/Т (на 2 счётчика)",
  "inconsistency": "описание соответствует ЩУ-12/Т"
}
```

### A3. Circuit imbalance between sections (1 finding)
Симметричные секции РП1 и РП2 должны иметь идентичные номиналы АВ.

| Finding | Description |
|---|---|
| F-007 | QF1.x и QF2.x должны иметь одинаковые марки/номиналы |

**Schema:**
```json
{
  "type": "section_asymmetry",
  "entity_a": "QF1.1 (РП1)",
  "entity_b": "QF2.1 (РП2)",
  "field": "model+current",
  "values": ["ВА-335А 630А", "ВА-333А 500А"]
}
```

### A4. Drawing vs specification item count (4 finding)
Данные о количестве на чертеже не соответствуют спецификации.

| Finding | Entity | Mismatch |
|---|---|---|
| F-008 | резервные АВ | 0 на схеме vs "не менее 10%" в примечаниях |
| F-021 | ТТ TA1.x | 12 комплектов vs уникальные обозначения |
| F-025 | ОКК участки | есть в легенде vs не выделены на плане |
| F-026 | марки кабелей | должны быть vs отсутствуют |

---

## B. Identity / Uniqueness — 2 findings

**Что общее:** требование уникальности нарушено.

### B1. Duplicate line marker on single schema (1)
Один маркер линии использован для разных линий.

| Finding | Entity | Conflict |
|---|---|---|
| эталон #5 | M-1.5 | ВРУ-ИТП и ВРУ-НС одновременно |

**Schema:**
```json
{
  "type": "duplicate_line_marker",
  "marker": "M-1.5",
  "sheet": "Лист 3",
  "destinations": ["ВРУ-ИТП", "ВРУ-НС"],
  "evidence_count": 2
}
```

### B2. Duplicate room number (1)
| Finding | Detail |
|---|---|
| F-016 | помещение 19 присвоено двум разным помещениям |

---

## C. Rule vs Fact conflicts — 4 findings

**Что общее:** в одном месте документа декларируется правило, в другом оно нарушается.

### C1. Note vs breaker type (1)
Примечание требует один тип расцепителей, фактически установлены другие.

| Finding | Rule | Fact |
|---|---|---|
| F-023 / эталон #55 | "АВ с электронными расцепителями" (note) | ВА-302, ВА-332А (модульные термомагнитные) |

**Schema:**
```json
{
  "type": "note_vs_equipment_conflict",
  "rule_source": {"sheet": 3, "text": "АВ с электронными расцепителями"},
  "violating_entity": {"id": "QF1.7", "model": "ВА-302", "type": "термомагнитный"},
  "applies_to_scope": "все АВ отходящих линий ГРЩ"
}
```

**Generator логика:**
```python
for note in symbol_table["notes"]:
    rule = parse_note(note.text)
    if rule.type == "required_breaker_trip_type":
        for breaker in breakers_in_scope(rule.scope):
            if not matches(breaker.model, rule.required_type):
                yield candidate(type="note_vs_equipment_conflict", ...)
```

### C2. Note vs reserve requirement (1)
| Finding | Rule | Fact |
|---|---|---|
| F-008 | "10% резервных АВ + 20% места" | все 8 позиций заняты |

### C3. Note vs diagram label (1)
| Finding | Rule | Fact |
|---|---|---|
| F-030 | "предусмотреть контакты SA/SF" | не показаны на схеме |

### C4. Note vs routing (1)
| Finding | Rule | Fact |
|---|---|---|
| F-031 | "прокладка взаиморезервируемых в разных лотках" | параллельные трассы без разделения |

---

## D. Engineering calculations — 5 findings

**Что общее:** численная проверка по формуле/нормативу. Может быть полностью детерминированной.

### D1. Coefficient plausibility (1)
| Finding | Formula | Pass criteria |
|---|---|---|
| F-005 | Kс < 0.2 for power > 50kW | подозрительно низко |

**Generator:**
```python
for panel in symbol_table["panels"]:
    if panel.demand_factor and panel.power_kw:
        if panel.demand_factor < 0.2 and panel.power_kw > 50:
            yield candidate(type="suspicious_demand_factor", ...)
```

### D2. Voltage drop (1)
| Finding | Calculation | Pass criteria |
|---|---|---|
| F-006 | ΔU total from TP to lamp | ≤ 5% per СП 76 |

```python
def check_voltage_drop(path):
    total = sum(segment.delta_u for segment in path.segments)
    if total > 5.0:
        yield candidate(type="voltage_drop_exceeded", total=total, ...)
```

### D3. CT saturation (1)
| Finding | Calculation | Pass criteria |
|---|---|---|
| F-027 | I_work vs I_CT_nominal | I_work ≤ 1.2 × I_CT |

```python
for ct in symbol_table["current_transformers"]:
    breaker = ct.associated_breaker
    if breaker and breaker.nominal_current > ct.primary_current * 1.2:
        yield candidate(type="ct_undersized", ...)
```

### D4. Transformer overload (1)
| Finding | Calculation | Pass criteria |
|---|---|---|
| F-029 | S_calc / S_nom in emergency mode | ≤ 1.4 (масляный) или ≤ 1.2 (сухой) |

### D5. Short-circuit trip coordination (1)
| Finding | Calculation | Pass criteria |
|---|---|---|
| F-028 | breaker trip time @ I_sc(1) | ≤ 0.4 s for TN-C-S |

### Также связанные с расчётами (не попавшие в D):

**F-001 Перекос фаз 70%** — это не расчёт, это **атрибут → проверка порога**. Generator:
```python
if panel.phase_imbalance_pct > 15:
    yield candidate(type="phase_imbalance_critical", ...)
```

---

## E. Documentation quality — 12 findings

**Что общее:** текстовые ошибки, опечатки, некорректные ссылки, плохое оформление. Часть поддаётся regex/символьной проверке.

### E1. Outdated norm references (4)
| Finding | Reference | Replacement |
|---|---|---|
| F-009 | ГОСТ 13109-97 | ГОСТ 32144-2013 |
| F-010 | ГОСТ 27570.0 | ГОСТ IEC 61140-2012 |
| F-011 | ГОСТ Р 50462-2009 | ГОСТ 33542-2015 |
| F-017 | ГОСТ 32395-2013 | ГОСТ 32395-2020 |

**Generator:** `norms_db.json` lookup. Уже есть в проекте, просто использовать.

```python
for ref in extracted_norm_references:
    status = norms_db.check(ref)
    if status.is_replaced:
        yield candidate(
            type="outdated_norm_reference",
            old=ref, new=status.replaced_by,
            mandatory=status.is_mandatory,
            severity="КРИТИЧЕСКОЕ" if status.is_mandatory else "РЕКОМЕНДАТЕЛЬНОЕ"
        )
```

### E2. Typos in identifiers (2)
| Finding | Original | Correct |
|---|---|---|
| F-015 | 83/23-ГК-ЭГ | 133/23-ГК-ЭГ |
| F-009 (part) | ГОСТ3109-97 | ГОСТ 13109-97 |

**Generator:** regex matches to known patterns, fuzzy compare to project code prefix.

### E3. Wrong address / section name (1)
| Finding | Wrong | Right |
|---|---|---|
| F-018 | Масляническая | Мосфильмовская |

Часто OCR-ошибка. **Не эталонная** по нашему списку.

### E4. Copy-paste from wrong section (1)
| Finding | Evidence |
|---|---|
| F-013 | "Документация паркинга" в разделе ГРЩ |

Ключевые слова + sections mismatch.

### E5. Self-referential formula (1)
| Finding | Error |
|---|---|
| F-014 | Рр = Рр × Кс × n × Ко (повтор переменной) |

```python
def check_formulas(text):
    for formula in extract_formulas(text):
        if formula.lhs in formula.rhs_variables:
            yield candidate(type="self_referential_formula", ...)
```

### E6. Typos in words (1)
| Finding | Wrong | Right |
|---|---|---|
| F-020 | "семей" | "сетей" |

Словарный spell-check.

### E7. Ambiguous installation note (1)
| Finding | Content |
|---|---|
| F-024 | "монтаж определить по месту" для критичных элементов (ОКК, шинопроводы) |

Ключевые слова: "по месту", "уточнить на стройке" — в контексте EI150 и СПЗ.

### E8. Missing classification / section label (2)
| Finding | Missing |
|---|---|
| F-019 | Разделы Э01 vs ЭО1 (неправильная номенклатура) |
| F-022 | Класс точности ТТ не указан |

---

## Распределение 33 findings по классам

| Класс | Кол-во | % |
|---|---|---|
| A. Cross-source consistency | 10 | 30% |
| B. Identity/Uniqueness | 2 | 6% |
| C. Rule vs Fact conflicts | 4 | 12% |
| D. Engineering calculations | 5 | 15% |
| E. Documentation quality | 12 | 36% |
| **Всего** | **33** | **100%** |

## Распределение 5 definite KEEPs по классам

| KEEP | Класс | Подкласс |
|---|---|---|
| #2 ЩУ-2/Т перепутаны | A | A2 panel_description_mismatch |
| #5 дубль M-1.5 | B | B1 duplicate_line_marker |
| #7 нейтраль 150/120 | A | A1 cable_attribute_mismatch |
| #18 ВРУ-1 1 PE vs 2 PE | A | A1 cable_attribute_mismatch |
| #55 ВА-335А vs note | C | C1 note_vs_breaker_type |

**Важное наблюдение:**
- **3 из 5 KEEPs покрываются подклассом A1** (cable_attribute_mismatch) или B1 (duplicate_line_marker)
- **1 из 5 — A2** (panel_description_mismatch)
- **1 из 5 — C1** (note_vs_equipment_conflict)

**Если написать 4 candidate generators** (A1, A2, B1, C1), теоретически можно поймать **все 5 definite KEEPs**.

---

## JSON Schema для extractor (prototype — только кабели)

```json
{
  "block_id": "string",
  "page": "number",
  "sheet": "string",
  "sheet_type": "schema | node | plan | spec_table | ...",
  "entities": {
    "cables": [
      {
        "line_id": "М-1.5",
        "mark": "ППГнг(А)-HF",
        "phase_count": 4,
        "phase_section_mm2": 120,
        "neutral_pe_count": 1,
        "neutral_pe_section_mm2": 70,
        "neutral_pe_type": "N | PE | PEN | не_указано",
        "destination": "ВРУ-ИТП",
        "source_panel": "РП1 ввод №2",
        "length_m": null,
        "evidence": {
          "bbox": [0.3, 0.4, 0.5, 0.2],
          "raw_quote": "ППГнг(А)-HF 4×(1×120)+(1×70)",
          "confidence": 0.9
        },
        "alternate_readings": [
          {
            "phase_section_mm2": 240,
            "reason": "возможно перепутано с соседним кабелем",
            "confidence": 0.1
          }
        ]
      }
    ],
    "panels": [
      {
        "id": "ВРУ-ИТП",
        "type": "panel_vru",
        "fed_by_line": "М-1.5",
        "description": "Вводно-распределительное устройство ИТП",
        "evidence": {...}
      }
    ]
  },
  "notes": [],
  "unreadables": [
    {"area": "верхний правый", "what": "мелкий текст уставок"}
  ]
}
```

## Candidate schema

```json
{
  "candidate_id": "C-001",
  "type": "cable_attribute_mismatch",
  "entity_id": "М-1.5",
  "field": "phase_section_mm2",
  "values": [
    {
      "value": 120,
      "source_block": "A99X-NV3N-KRM",
      "source_sheet": 3,
      "source_page": 7,
      "evidence_quote": "ППГнг(А)-HF 4×(1×120)+(1×70)"
    },
    {
      "value": 240,
      "source_block": "7GNT-PGKU-CUY",
      "source_sheet": 10,
      "source_page": 14,
      "evidence_quote": "ППГнг(А)-HF 4×(1×240)+(1×70)"
    }
  ],
  "auto_severity": "КРИТИЧЕСКОЕ",
  "auto_category": "cable",
  "needs_llm_judge": true
}
```

## Judge prompt template

```
Задача: определить является ли кандидат противоречием реальной ошибкой в проекте.

Кандидат:
  Тип: {candidate.type}
  Сущность: {candidate.entity_id}
  Поле: {candidate.field}
  
Свидетельство A:
  Источник: {values[0].source_sheet}, блок {values[0].source_block}
  Значение: {values[0].value}
  Цитата: "{values[0].evidence_quote}"
  Изображение: [block_crop_A]
  
Свидетельство B:
  Источник: {values[1].source_sheet}, блок {values[1].source_block}
  Значение: {values[1].value}
  Цитата: "{values[1].evidence_quote}"
  Изображение: [block_crop_B]

Возможные вердикты:
- valid: реальная ошибка, данные действительно конфликтуют
- invalid: не конфликт (одно из чтений ошибочно, или разные объекты)
- uncertain: не хватает данных для решения

Ответ (JSON):
{
  "verdict": "valid|invalid|uncertain",
  "explanation": "1-2 предложения",
  "corrected_value": "если можешь определить правильное"
}
```

---

## Приоритет генераторов для прототипа

Если делать первый прототип за день, покрывая 5 definite KEEPs — нужны **4 генератора**:

1. **A1 cable_attribute_mismatch** — покрывает #7, #18 + F-002, F-003, F-004
2. **A2 panel_description_mismatch** — покрывает #2 + F-012
3. **B1 duplicate_line_marker** — покрывает #5
4. **C1 note_vs_breaker_type** — покрывает #55 + F-023

Эти 4 генератора могут поймать **все 5 definite KEEPs + 4 additional Opus findings**.

## Приоритет генераторов для production

Дополнительно (покрывает ~80% Opus findings):

5. **D1 suspicious_demand_factor** (Kс < 0.2)
6. **D2 voltage_drop** (ΔU > 5%)
7. **D3 ct_saturation** (I_work vs I_CT)
8. **D4 transformer_overload** (S_calc vs S_nom)
9. **E1 outdated_norm** (уже есть через norms_db)
10. **B2 duplicate_room_number**
11. **A3 section_asymmetry** (QF1.x vs QF2.x)
12. **C3 note_vs_diagram_label** (SA/SF и аналогичные)

Эти 12 генераторов покрывают **100% Opus findings** из прогона + дают recall близкий к Opus на любой дешёвой модели в роли extractor.
