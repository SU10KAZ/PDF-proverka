# Document Type — Worked Examples

Each example below is a short, realistic MD snippet labelled with the
expected `document_type` and a rationale that cites which step of the
detection chain (`document_type_design.md` §"Detection priority chain")
should fire.

The Python detection rules in `document_type_detection_rules.py` are
asserted against the same vocabulary used here.

## Example 1 — full_rd (explicit)

`project_info`:
```json
{
  "project_id": "ЭОМ/13АВ-РД-МЗ",
  "section": "ЭОМ",
  "document_type": "full_rd"
}
```

MD snippet:
```markdown
# Пояснительная записка
## 1. Общие данные
## 2. Кабельный журнал
## 3. Однолинейная схема главного распределительного щита
```

**Expected:** `full_rd`, confidence 1.0.
**Rationale:** Step 1 (explicit `document_type`) fires.

## Example 2 — full_rd (content fallback)

`project_info`:
```json
{
  "project_id": "ЭОМ/MZ-K3",
  "section": "ЭОМ",
  "pdf_file": "13АВ_РД_ЭО_К3.pdf"
}
```

MD snippet:
```markdown
# Пояснительная записка к проекту электроснабжения

## Таблица нагрузок ВРУ-1
| Потребитель | Pmax, кВт |
|---|---|
| Освещение | 12.4 |

## Однолинейная схема
[IMAGE: однолинейная.png]

## Кабельный журнал
| № | Тип | Длина |
```

**Expected:** `full_rd`, confidence 0.75.
**Rationale:** Steps 1–3 don't fire (no explicit type, section is just
"ЭОМ", filename has no spec/cross/ТЗ tokens). Step 4 finds 4 strong
full_rd content patterns (`пояснительная записка`, `таблица нагрузок`,
`однолинейная схема`, `кабельный журнал`) → wins on content.

## Example 3 — audit_comparison (section hint)

`project_info`:
```json
{
  "project_id": "MULTI/cross_01",
  "section": "Сравнение разделов ЭОМ vs ОВ",
  "pdf_file": "cross_eom_ov.pdf"
}
```

MD snippet:
```markdown
В разделе ЭОМ указана суммарная мощность 380 кВт.
В разделе ОВ заявлена тепловая нагрузка 290 кВт.
Расхождение между разделами: ЭОМ vs ОВ → разница 90 кВт без обоснования.
```

**Expected:** `audit_comparison`, confidence 0.85.
**Rationale:** Step 2 — `section` matches `сравнение / cross` regex.

## Example 4 — audit_comparison (content hint)

`project_info`:
```json
{
  "project_id": "MULTI/cross_02",
  "section": "MULTI",
  "pdf_file": "fragment.pdf"
}
```

MD snippet:
```markdown
В разделе ВК расход воды на пожаротушение 18 л/с.
В разделе АПС учтён резерв насосной 22 л/с.
Несоответствие между разделами: ВК vs АПС.
```

**Expected:** `audit_comparison`, confidence 0.75.
**Rationale:** Steps 1–3 don't fire. Step 4 hits two audit_comparison
patterns (`в разделе ... в разделе ...`, `несоответствие между`).

## Example 5 — tz_vs_rd (section hint)

`project_info`:
```json
{
  "project_id": "TZRD/cross_05",
  "section": "ТЗ vs РД",
  "pdf_file": "tz_compliance.pdf"
}
```

MD snippet:
```markdown
## Требования ТЗ
Заказчик требует резервирование питания 2-й категории по ПУЭ-7.

## Реализация в РД
Применена одна линия от ТП — резервирование не предусмотрено.
```

**Expected:** `tz_vs_rd`, confidence 0.85.
**Rationale:** Step 2 — `section` regex `\bТЗ\s*vs\s*РД\b` matches.

## Example 6 — tz_vs_rd (content hint)

`project_info`:
```json
{
  "project_id": "MULTI/tz_compliance_03",
  "section": "MULTI",
  "name": "tz_compliance_03"
}
```

MD snippet:
```markdown
По техническому заданию заказчик требует ДГУ на 250 кВА.
Требования ТЗ: тип ДГУ — стационарный, ёмкость бака не менее 500 л.
По ТЗ предусмотрено АВР между основным и резервным вводом.
```

**Expected:** `tz_vs_rd`, confidence 0.75.
**Rationale:** Step 4 — content matches `техническ.. задани..`,
`по ТЗ`, `требования ТЗ`.

## Example 7 — specification_only (filename)

`project_info`:
```json
{
  "project_id": "ЭОМ/13АВ-К3-spec",
  "section": "ЭОМ",
  "pdf_file": "13АВ_РД_ЭО_К3_spec_cables.pdf"
}
```

MD snippet:
```markdown
| Поз. | Кабель | Сечение | Длина, м |
|---|---|---|---|
| 1 | ВВГнг(А)-FRLS 5x10 | 5x10 | 124 |
| 2 | ВВГнг(А)-FRLS 5x6  | 5x6  | 87  |
```

**Expected:** `specification_only`, confidence 0.80.
**Rationale:** Step 3 — filename contains `_spec_` token.

## Example 8 — specification_only (content)

`project_info`:
```json
{
  "project_id": "ВК/cable_journal_alpha",
  "section": "ВК",
  "pdf_file": "cable_journal.pdf"
}
```

MD snippet:
```markdown
# Ведомость потребителей воды
| Поз. 1 | Санузел 1.1 | 0.4 л/с |
| Поз. 2 | Санузел 1.2 | 0.4 л/с |
| Поз. 3 | Душевая    | 1.2 л/с |
```

**Expected:** `specification_only`, confidence 0.75.
**Rationale:** Step 4 hits `ведомость` header pattern and `Поз. \d+` rows.

## Ambiguous Example A — fallback to full_rd

`project_info`:
```json
{
  "project_id": "X/untitled",
  "section": "X",
  "pdf_file": "scan_001.pdf"
}
```

MD snippet:
```markdown
Краткие заметки по проекту.
Объект: ТП-413.
Параметры см. отдельный лист.
```

**Expected:** `full_rd`, confidence 0.5.
**Rationale:** No explicit type, no section hint (`X` matches nothing),
no filename hint, content has zero pattern hits → fallback.

## Ambiguous Example B — explicit beats everything else

`project_info`:
```json
{
  "project_id": "ЭОМ/override_case",
  "section": "ЭОМ",
  "pdf_file": "13АВ_spec_cables.pdf",
  "document_type": "audit_comparison"
}
```

MD snippet:
```markdown
| Поз. | Кабель | Сечение |
|---|---|---|
| 1 | ВВГнг 5x10 | 5x10 |
```

**Expected:** `audit_comparison`, confidence 1.0.
**Rationale:** Step 1 fires — the explicit override wins even though
filename and content both vote `specification_only`. This is by design:
the auditor (or the importing tool) may know context not visible in the
files.

---

**Why these examples were chosen.** The 8 main examples cover all 4 types
twice (once via the high-priority chain, once via the content fallback).
The two ambiguous cases exercise (A) the safe-default behaviour and (B)
the explicit-override path. The same vocabulary is asserted in
`document_type_detection_rules.py`'s `if __name__ == "__main__"` block.
