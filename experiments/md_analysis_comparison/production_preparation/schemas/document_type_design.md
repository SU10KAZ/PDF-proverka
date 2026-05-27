# Document Type — Design

**Status:** production-preparation (NOT in production yet)
**Anchor research:** `algorithm_research/reports/phase0_phase1_validation_report.md` §1.1
**Reference prompt:** `algorithm_research/prompt_optimization/optimized_prompts_v2/completeness.md` (lines 24–58)
**Schema version:** 2

Stage 01 / completeness lens need to know **what kind of document** the MD
represents before applying the checklist. Without this signal, the
completeness agent invented "missing single-line diagram" / "missing pояснительная
записка" findings on fragments and audit-comparison documents — 4 FP on a
single case (`cross_01` v1 → fixed in v2).

## The 4 document types

### `full_rd`

**Definition.** A complete Working Documentation (РД) package or a complete
discipline section. The document should logically contain every mandatory
deliverable for that discipline (записка, схемы, журналы, таблицы, расчёты).

**Archetypal example.** `13АВ-РД-МЗ` — full ЭОМ section with pояснительная
записка, кабельный журнал, однолинейная схема.

**MAY flag.**
- Missing mandatory section / schedule / diagram.
- Missing parameter on a spec line.
- Outdated norm citation, arithmetic error, internal contradiction, cross-
  discipline mismatch.
- All categories КРИТ / ЭКОН / ЭКСПЛ / ПРОВ_ПО_СМЕЖНЫМ / РЕКОМ are in scope.

**MAY NOT flag.**
- Nothing is out of scope by virtue of document_type. The full checklist applies.

### `audit_comparison`

**Definition.** A fragment-by-fragment comparison between two or more
sections. The auditor is comparing them, NOT auditing each as a complete РД.

**Archetypal example.** `cross_01` — ЭОМ ВРУ load vs ОВ heat-load comparison
fragment. The MD shows two specific tables side-by-side; it never claimed to
cover пояснительная записка / однолинейная.

**MAY flag.**
- Inconsistency, contradiction, or value mismatch between the compared
  fragments.
- Wrong norm citation on either side.
- Arithmetic error in either fragment.

**MAY NOT flag.**
- "Отсутствует однолинейная схема" — single-line diagram is not the subject.
- "Не представлена пояснительная записка" — pояснительная not the subject.
- Any "missing mandatory section" finding for sections the fragment never
  claimed to include.

### `tz_vs_rd`

**Definition.** A document that juxtaposes ТЗ (terms of reference / задание
заказчика) requirements against РД solutions. The auditor compares the two.

**Archetypal example.** Multi-discipline РД with the customer's ТЗ as a
reference appendix, where each РД item is matched against a ТЗ requirement.

**MAY flag.**
- РД contradicts a ТЗ requirement → КРИТИЧЕСКОЕ.
- РД is silent on a ТЗ item → ПРОВЕРИТЬ_ПО_СМЕЖНЫМ.
- Wrong norm citation in either ТЗ or РД.

**MAY NOT flag.**
- Items that the ТЗ does not mention (no gap can exist).
- "Missing full РД section" if the document is a coverage report, not a
  full РД package.

### `specification_only`

**Definition.** A standalone spec / ведомость / single isolated calculation.
A cable schedule, breaker schedule, lighting calculation, etc. NOT a full РД
section.

**Archetypal example.** ВЛ-Кабельная-ведомость, Кабельный журнал as a
separate document.

**MAY flag.**
- Missing parameter on a row (отсутствует сечение, длина, тип кабеля).
- Wrong unit, arithmetic error, unrealistic value.
- Outdated norm citation.

**MAY NOT flag.**
- Missing other РД sections (пояснительная записка, схема, расчёт нагрузок).
- Any "completeness of full РД" gap.

## Detection priority chain

Implemented in `document_type_detection_rules.py`. First rule that fires wins.

| Step | Source | Confidence |
|------|--------|------------|
| 1 | `project_info["document_type"]` if in ALLOWED set | 1.0 |
| 2 | `project_info["section"]` regex match | 0.85 |
| 3 | `pdf_file` / `name` / `project_id` regex match | 0.80 |
| 4 | `md_text` content pattern scoring | 0.75 |
| 5 | Fallback | 0.5 |

**Allowed set:** `{full_rd, audit_comparison, tz_vs_rd, specification_only}`.
Any value outside this set in step 1 is ignored (falls through).

**Section heuristic** maps strings containing `ТЗ` → `tz_vs_rd`,
`сравнение / cross` → `audit_comparison`, `спецификация / ведомость` →
`specification_only`. See `SECTION_HINTS` in the Python module.

**Filename heuristic** uses the same vocabulary on basename / project_id.

**Content heuristic** counts pattern hits per type and requires the winner
to beat the runner-up by at least 1 hit. `full_rd` is intentionally
gated more strictly here (needs ≥ 2 hits) — otherwise the fallback already
returns `full_rd` at the lower 0.5 confidence rung.

## Fallback logic

If no rule produces confidence ≥ 0.7, the detector returns
`("full_rd", 0.5)`. Rationale:

- `full_rd` is the most conservative — the full checklist applies.
- Phase 1 completeness lens is currently **off for `full_rd`** (per
  `phase0_phase1_validation_report.md` §1.4); only the opt-in lens runs on
  non-full_rd documents. So an incorrect default to `full_rd` only loses
  potential improvement; it does not introduce false-positive completeness
  findings.
- A wrong default to any of the three non-full_rd types could SKIP real
  completeness checks on a true full RD document — strictly worse.

The detector exposes the confidence so the caller can:
- Persist `meta.document_type_confidence` for observability.
- Optionally short-circuit Phase 1 if confidence is below
  `ACCEPT_THRESHOLD = 0.7` (default behaviour for now: always apply the
  detected type but log the confidence).

## Routing rules (Phase 1)

Per `phase0_phase1_validation_report.md` §7 (recommendation):

| document_type | current_method (Stage 01 Opus) | completeness lens (Sonnet) |
|---|---|---|
| `full_rd` | ON | OFF (A0 already strong on full RD) |
| `audit_comparison` | ON | ON — but checklist constrained to compared subjects only |
| `tz_vs_rd` | ON | ON — only items explicitly in ТЗ are scorable |
| `specification_only` | ON | ON — parameter-level checks only, no missing-section findings |

When the completeness lens is ON, the document type is passed to the prompt
via `{DOCUMENT_TYPE}` substitution. The prompt's "Document-type routing
(HARD RULE)" block then constrains what the agent may flag.

## Prompt injection rules

The completeness prompt template contains a literal `{DOCUMENT_TYPE}` token.
Substitution happens in the lens runner just before the LLM call:

```python
prompt = template.replace("{DOCUMENT_TYPE}", detected_type)
```

The prompt's "STRICT BAN" block (`optimized_prompts_v2/completeness.md`
lines 53–58) lists the literal phrases that must never appear in findings
when `document_type != "full_rd"`:

- `"отсутствует полный комплект РД"`
- `"не представлена пояснительная записка"`
- `"нет однолинейной схемы"`

The post-LLM critic verifies this ban — any finding containing these
phrases against a non-full_rd document is marked `out_of_scope` and dropped
by the corrector.

## Observability

When a project goes through the pipeline, the `meta` block on
`01_text_analysis.json` and `03_findings.json` records:

```json
{
  "meta": {
    "schema_version": 2,
    "document_type": "audit_comparison",
    "document_type_confidence": 0.85,
    "completeness_applied": true,
    "completeness_lens_version": "v2"
  }
}
```

See `meta_schema.md` for the full meta block.

## Backwards compatibility

- Old `01_text_analysis.json` files without `meta.document_type` are treated
  as `full_rd` by the pipeline (matches the fallback).
- Existing `text_findings[]` schema is unchanged. New v2 fields are
  optional and live alongside the production fields. See `finding_schema_v2.md`.
- Phase 0 dedup (`class_dedup` + `fuzzy_dedup`) does not require document_type
  to function — both modules work on any finding shape.

## Known limitations

- The content heuristic (step 4) is regex-based and **language-specific**
  (Russian). English-language documents will fall through to the default.
- The current rule set has been validated on the 24-case research dataset
  (8 original + 16 expanded). A single ambiguous case from the dataset
  (`multi_01`) currently routes to `full_rd` by fallback; manual override
  via explicit `project_info["document_type"]` is the supported workaround.
- The detector does NOT look at file size or page count. A single-page
  "full_rd" PDF will still be classified as `full_rd` if explicit / section
  hints say so, even though Phase 1 routing might be more useful with
  `specification_only`.
