# `problem_class` — Canonical Vocabulary

`problem_class` is the primary classification slug that drives
`class_dedup`. A well-typed `problem_class` makes the class-key dedup
exact-match and very precise. A missing or free-text `problem_class`
forces fallback to a category-based composite key — still safe, less
discriminating.

This document lists every slug, what it means, when to use it, and what
to use INSTEAD if a slug is tempting but wrong.

The slugs are grouped by lens (the audit aspect that surfaces the
finding). LLM prompts SHOULD pick the most specific slug; if none fits,
use `unknown`.

---

## Normative lens

### `outdated_norm_reference`
**Use when:** the document cites a норматив that has been **replaced or
cancelled** (status from `norms_db.json`).
**Do NOT use for:** a norm whose status is unknown — use
`missing_norm_reference` (no citation at all) or set `confidence < 0.5`
on a generic outdated suspicion.

### `invalid_clause`
**Use when:** the document cites a норматив and an explicit clause
number that **does not exist** in that норматив (e.g. `СП 256.1325800.2016,
п. 99.99`).
**Do NOT use for:** a clause that exists but is misinterpreted — that is
`wrong_norm_for_discipline` or a discipline-specific lens.

### `missing_norm_reference`
**Use when:** a claim that REQUIRES a norm citation has none (e.g. "по
действующим нормам приняты сечения" with no SP/PUE referenced).
**Do NOT use for:** documents that genuinely don't need citations (notes,
appendices).

### `wrong_norm_for_discipline`
**Use when:** the cited норматив is real and exists, but is **inapplicable
to the discipline** (e.g. citing АПС-only norms for a ВК issue).
**Do NOT use for:** an outdated norm — use `outdated_norm_reference`.

---

## Calculations lens

### `arithmetic_error`
**Use when:** sum / product / ratio in a table or formula is wrong by a
computable margin.
**Do NOT use for:** rounding differences within 1% — those are usually
not findings.

### `wrong_formula`
**Use when:** the formula itself is incorrect (wrong source, wrong shape),
even if arithmetic with that formula is consistent.
**Do NOT use for:** a correct formula applied to wrong inputs — that is
`arithmetic_error` or `unit_mismatch`.

### `unit_mismatch`
**Use when:** units don't line up (kW vs kVA confusion, L/s vs m³/h,
N vs kgf), or a value is reported without units where units are required.
**Do NOT use for:** missing units in column headers — that is
`missing_mandatory_parameter`.

### `unrealistic_value`
**Use when:** a value is technically possible to write but **engineering-
unrealistic** for the object (e.g. ВРУ 5 MВт on a single-stairwell МКД,
cable cross-section 0.5 mm² for a 100 A circuit).
**Do NOT use for:** a value contradicting another part of the same
document — use `internal_contradiction`.

---

## Completeness lens

### `missing_mandatory_section`
**Use when:** a section that is mandatory by the discipline checklist is
absent and the document_type permits flagging this (see
`schemas/document_type_design.md`).
**Do NOT use for:** specifications missing a section header — use
`incomplete_specification`.

### `missing_mandatory_schedule`
**Use when:** a mandatory **schedule** is absent (кабельный журнал,
ведомость заземления, спецификация оборудования).
**Do NOT use for:** a schedule that is present but missing rows — use
`incomplete_specification`.

### `missing_mandatory_parameter`
**Use when:** a row / item is present but a mandatory parameter is missing
(e.g. кабель без сечения; автомат без характеристики срабатывания).
**Do NOT use for:** a missing whole row — use `incomplete_specification`.

### `missing_diagram`
**Use when:** a mandatory diagram is missing — однолинейная, структурная,
схема заземления.
**Do NOT use for:** poor-quality diagrams — use a different lens
(typically `missing_mandatory_parameter` if specific labels missing).

### `missing_calculation_basis`
**Use when:** a value or selection is stated without the underlying
calculation (e.g. "выбран кабель ВВГ 5x10" with no nagrузка / ток / падение
напряжения worked out).
**Do NOT use for:** missing input parameters to a calculation — those are
`missing_mandatory_parameter`.

### `incomplete_specification`
**Use when:** a spec / ведомость is **present but partial** (missing
positions, blank cells in mandatory columns).
**Do NOT use for:** entirely missing schedule — use
`missing_mandatory_schedule`.

### `stub_section`
**Use when:** a section header is present but the content is a
placeholder / TBD / "уточняется" / empty.
**Do NOT use for:** a properly-written section with weak content —
that is usually a different lens entirely.

---

## Contradictions lens

### `internal_contradiction`
**Use when:** two places in the **same** document state contradictory
facts (text says 100 кВт, table says 120 кВт).
**Do NOT use for:** contradictions between sections of two different
disciplines — use `cross_section_mismatch` or `cross_discipline_mismatch`.

### `cross_section_mismatch`
**Use when:** different sections of the same discipline (ЭОМ записка vs
ЭОМ кабельный журнал) state contradictory facts.
**Do NOT use for:** different disciplines — use
`cross_discipline_mismatch`.

---

## Cross-discipline lens

### `cross_discipline_mismatch`
**Use when:** two disciplines disagree on a shared fact (ЭОМ дает 380 кВт,
ОВ требует 420 кВт). Must set `discipline_pair`.
**Do NOT use for:** missing interface info — use `interface_undefined`.

### `interface_undefined`
**Use when:** an interface between two disciplines is not specified at
all (no задание на отверстия, no тепловая нагрузка передана от ТХ).
**Do NOT use for:** an interface that IS defined but contradicts — use
`cross_discipline_mismatch`.

---

## Safety lens

### `safety_violation`
**Use when:** a direct violation of mandatory safety norms (СП 1.13130,
ПУЭ Гл. 1.7, СП 12.13330). Almost always КРИТИЧЕСКОЕ.
**Do NOT use for:** safety-adjacent recommendations — use the appropriate
discipline lens with severity ЭКСПЛУАТАЦИОННОЕ.

---

## Other

### `unknown`
**Use when:** none of the above slugs fit and the LLM cannot classify.
Triggers fallback to category-based composite class_key, which still
preserves dedup safety.

**Last-resort guard.** Production should monitor the rate of `unknown` —
if it exceeds ~10% of findings, the LLM prompt likely needs to be updated
with more vocabulary guidance.

---

## Canonicalisation

Old free-text categories should map to the slugs above. Recommended
mapping for the common production phrasings:

| Production v1 category | v2 problem_class |
|---|---|
| "Норматив устарел" | `outdated_norm_reference` |
| "Не указан норматив" | `missing_norm_reference` |
| "Арифметическая ошибка" | `arithmetic_error` |
| "Неверная формула" | `wrong_formula` |
| "Расхождение единиц" | `unit_mismatch` |
| "Нереалистичное значение" | `unrealistic_value` |
| "Отсутствует раздел" | `missing_mandatory_section` |
| "Отсутствует журнал/спецификация" | `missing_mandatory_schedule` |
| "Не указан параметр" | `missing_mandatory_parameter` |
| "Отсутствует схема" | `missing_diagram` |
| "Не приведён расчёт" | `missing_calculation_basis` |
| "Неполная спецификация" | `incomplete_specification` |
| "Заглушка раздела" | `stub_section` |
| "Противоречие внутри документа" | `internal_contradiction` |
| "Расхождение между разделами одной дисциплины" | `cross_section_mismatch` |
| "Расхождение между ЭОМ и ОВ" (etc.) | `cross_discipline_mismatch` |
| "Не передано задание / неопределённый интерфейс" | `interface_undefined` |
| "Нарушение безопасности / ПУЭ / ПБ" | `safety_violation` |
| anything else | `unknown` |

The canonicalisation is **advisory** — `class_dedup` already handles
unknown slugs via the category-fallback path. The mapping above just
lets dedup do its best work.

---

## Slug counts (reference)

- normative: 4
- calculations: 4
- completeness: 7
- contradictions: 2
- cross_discipline: 2
- safety: 1
- other: 1

**Total: 21 slugs.** This is intentionally a closed vocabulary. New
slugs should be proposed by amendment to this file, with a justification
referencing actual findings that didn't fit existing slugs.
