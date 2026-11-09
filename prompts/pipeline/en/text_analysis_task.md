> **OUTPUT LANGUAGE:** All text values in JSON output (finding, source, reason, etc.) MUST be written in Russian.
> **RESPONSE FORMAT:** Respond with valid JSON only. No explanations, no markdown, no text outside JSON.

# PROJECT TEXT ANALYSIS — {PROJECT_ID}

## Role

{DISCIPLINE_ROLE}

## Input Data

1. **MD file** (primary text source) — READ via Read tool: `{MD_FILE_PATH}`
   - `[TEXT]` blocks — text data (explanatory notes, specifications, tables)
   - `[IMAGE]` blocks — drawing descriptions (type, axes, entities, text on drawing)

2. **Normative reference** — READ via Read tool: `{DISCIPLINE_NORMS_FILE}` (if available)

3. **Block analysis (Stage 02, compact view)** — READ via Read tool: `{BLOCKS_ANALYSIS_PATH}`
   - Visual analysis of graphic blocks by GPT-5.4 (findings G-NNN, `value_found`,
     `block_evidence`, `coverage_status` per block).
   - **If the file does not exist — work without block context** (normal for the text→block
     order; in that case leave `items_verified_from_blocks` empty `[]`).

## Unresolved graphic candidates

If the compact input contains `context_requests`, these are requests to investigate,
NOT confirmed findings. For each request, use its `request_id`, original block and
`required_context` to locate the needed source in the provided document. Prepared
TEXT is authoritative; do not re-OCR or correct it from a raster. A drawing description
alone cannot resolve a request that needs graphic inspection.

Return a top-level `context_request_results` array, one receipt per included request:
`request_id`, `status` (`resolved`, `needs_data`, `not_checked`), `source_refs`,
`finding_ids`, and `reason`. `source_refs` must be an array of exact block ID strings
from the provided document; `finding_ids` must refer to this response’s T-IDs. `resolved` requires an addressed source and an explanation
of whether the original claim is supported, contradicted, or already covered by an
existing finding. Link that finding rather than duplicating it. Missing context or
budget exhaustion is not refutation. Never turn every deferred candidate into a
technical defect. Keep `context_request_backlog` unresolved; its source pointers
identify work omitted from the compact input budget.

## Task

### Stage 1: Text Data Analysis

Analyze the MD content COMPLETELY. Extract:

1. **Project parameters** (`project_params`):
   - Building type, number of floors, areas
   - Design loads, capacities, flow rates
   - Main equipment, marks, sizes

2. **Normative references** (`normative_refs_found`):
   - All mentioned СП, ГОСТ, ПУЭ, ФЗ
   - Verify validity of each norm

3. **Preliminary findings** (`text_findings`, T-001, T-002...):
   - Calculation inconsistencies
   - Outdated normative references
   - Contradictions between sections
   - Missing mandatory data

4. **Arithmetic table verification** (MANDATORY):
   - Recalculate sums in EACH load table
   - Recalculate design values using discipline formulas
   - Verify areas and capacities for plausibility

5. **Cross-reference verification** (MANDATORY):
   - Explanatory note vs load tables — do numbers match?
   - Specification vs explanatory note text — marks, quantities, sizes
   - Discrepancies → finding
   - Standard sizes — verify against ГОСТ assortment

6. **Equipment ranges and characteristics** (MANDATORY):
   - Instrument measurement ranges match operating parameters?
   - Sizes exist in catalogs?
   - Capacities match catalog data?

7. **Specification vs [IMAGE] cross-check** (MANDATORY):
   - Equipment on drawing not in specification → finding
   - Specification item not on any drawing → finding

8. **Cross-check your findings against Stage 02 blocks** (if `{BLOCKS_ANALYSIS_PATH}` is available):
   - For EACH of your `text_findings[].id` (T-NNN), check whether it is confirmed by what GPT
     saw on the blocks: compare with the block's `findings`, its `value_found`, `block_evidence`.
   - Fill in `items_verified_from_blocks[]` (see output schema).
   - **HARD RULE:** set `confirmed: true` ONLY with concrete visual evidence — a specific
     `block_id` AND a `value_found`/`block_evidence` matching in meaning/value. A mere «the block
     says something similar» or matching wording is NOT confirmation (otherwise two models
     self-confirm each other and inflate severity). No concrete evidence → `confirmed: false`
     or omit the record.
   - This does NOT create new drawing findings (Stage 02 finds those) — it only verifies YOUR text findings.

{DISCIPLINE_CHECKLIST}

{ABSENCE_GUARD}

## Finding Categories

{DISCIPLINE_FINDING_CATEGORIES}

## Object registry and operation coverage

Before drafting findings, return `object_registry`: objects with unique `id`,
`identity` (full mark/execution/instance), `scope` (system, building, level and supply
boundary as applicable), `kind`, and `source_refs` (exact original block IDs).
Connect repeated views of one instance; never equate matching marks across scopes.
Return `object_inventory_coverage` per inspected source block: `block_id`,
`status` (inventoried/no_objects/needs_data), `object_ids`, and `reason`.
An unread graphic is needs_data; an analyzed block does not prove inventory coverage.

For EACH object return `object_operation_results` for identity_across_views,
quantity_balance, dimension_consistency, requirement_applicability, document_links.
Each receipt has `object_id`, `operation`, `status` (checked/not_applicable/needs_data/
not_checked), `reason` explaining the actual comparison/result or applicability,
and `source_refs`. Include calculations through the operation arrays below.
Do not mark checked merely because the object was read; do not invent dimensions,
requirements, supply boundaries or missing counterparts. Missing source material
means needs_data with context_queries (up to six literal object marks or short
phrases identifying missing source context); inapplicability needs a specific reason. Preserve unresolved
operations for targeted follow-up without turning them automatically into defects.

## Source-addressed object balances

Include `object_balance_operations` for applicable comparisons of a scheme's active
objects with calculation rows or specification items. Use generic project data, not
historical answers. Each operation has a unique `id`, `kind` equal to `set_balance`,
a precise `scope`, and `left` / `right` inventories. Each inventory contains
`scope_refs` (exact block IDs delimiting the inspected source) and `items`.
Each item contains `key` (an array of full identity components, including technical
execution where applicable), `role` (`active` or `reserve`) and `source_refs`
(exact block IDs). Use the same identity definition on both sides. Do not merge
HF with FRHF, distinct physical instances, or different supply scopes. Repeated
views must be resolved before adding an item. Unknown identity, aggregation or
supply ownership must remain an explicit question, not a fabricated match.
If the required graphic scope cannot be inspected, do not claim a complete inventory.
The runner computes both set differences; these are review candidates, not automatic
proof of a shortage. Empty operations do not mean the document passed this check.

## Reproducible quantity checks

Include `quantity_check_operations` for applicable same-basis comparisons and sums.
Each has unique `id`, `kind` (`compare` or `sum`), `terms` (one for compare), and
`target`. Every operand has `object`, `parameter`, `unit`, `basis`, `regime`,
`value` (decimal string with a dot), and `source_refs` (exact block IDs).
All operands must describe the SAME object/aggregate and physical basis. Preserve
original quantities in source evidence; never relabel incompatible values simply
to make the computation run. Convert units explicitly before comparison and retain
the source conversion reasoning. Unknown columns, modes, geometry or membership
remain questions, not invented operands. Numerical differences do not alone prove
a defect: assess documented rounding, scope and the actual design consequence.

## Output JSON Schema

```json
{
  "stage": "02_text_analysis",
  "project_id": "{PROJECT_ID}",
  "text_source": "md",
  "timestamp": "<ISO datetime>",
  "project_params": {
    "object_type": "...",
    "total_load_kw": 0,
    "key_equipment": ["..."]
  },
  "normative_refs_found": [
    {
      "ref": "СП 256.1325800.2016",
      "status": "ДЕЙСТВУЕТ",
      "edition": "ред. 29.01.2024",
      "note": ""
    }
  ],
  "text_findings": [
    {
      "id": "T-001",
      "severity": "КРИТИЧЕСКОЕ|ЭКОНОМИЧЕСКОЕ|ЭКСПЛУАТАЦИОННОЕ|РЕКОМЕНДАТЕЛЬНОЕ|ПРОВЕРИТЬ ПО СМЕЖНЫМ",
      "category": "см. таблицу категорий выше",
      "source": "MD стр. N / Раздел X",
      "finding": "Описание замечания",
      "norm": "Документ, пункт",
      "norm_quote": "Точная цитата из нормы или null",
      "related_block_ids": ["block_id"]
    }
  ],
  "items_verified_from_blocks": [
    {
      "finding_id": "T-001",
      "block_id": "<block_id from Stage 02>",
      "confirmed": true,
      "evidence": "What exactly is visible on the block (value_found/block_evidence) confirming T-001"
    }
  ]
}
```

> `items_verified_from_blocks` is optional. If `{BLOCKS_ANALYSIS_PATH}` is unavailable, leave it
> empty `[]`. Include a record ONLY with concrete visual evidence (see task step 8).

> **No internal identifiers in texts:** do not mention block_id values (like `RUXD-WP4R-6C3`) or
> T-NNN numbers in the `finding` field — it is read by external experts who do not know the
> system's internals. Block links go only into `related_block_ids` and `items_verified_from_blocks`.
> In the text, refer to the source in words: fragment type + title + sheet («in the table
> "Deviation list" on sheet 1…»).

## Normative Accuracy (norm_quote)

For EACH finding with a `norm` field:
- **`norm_quote`** — exact quote from the norm clause (1-2 sentences). `null` if unsure.
- All quotes will be verified at the norm verification stage (stage 04) regardless of confidence.

## Output

WRITE via Write tool: `{OUTPUT_PATH}/02_text_analysis.json`

## Rules

1. Analyze the MD content COMPLETELY — do not skip sections
2. `text_findings[]` — based on text data only (not drawings)
3. severity — ONLY one of the 5 values
4. Write JSON via Write tool — DO NOT output to chat
5. After writing, output a brief summary of what was found
6. Respond with valid JSON matching the schema above

## Criteria for the «ПРОВЕРИТЬ ПО СМЕЖНЫМ» severity (IMPORTANT)

The «ПРОВЕРИТЬ ПО СМЕЖНЫМ» (check-with-adjacent-disciplines) category is NOT «another section is mentioned». Standard cross-discipline interfaces (design assignments to adjacent disciplines, stating loads for the electrical section, stating flow rates for the water section) are **normal design practice**, not an error.

### Assign «ПРОВЕРИТЬ ПО СМЕЖНЫМ» ONLY when:

1. **Numeric contradiction within this section**, resolvable only via an adjacent section — e.g. a lift height of 5400 mm does not match an elevation difference of ~2650 mm (needs AR/KR data)
2. **Normative conflict** — room category, fire-resistance rating or hazard-zone class in this section may not match an adjacent section (fire-safety, AR)
3. **Physical incompatibility** — equipment does not fit, openings do not align, routes intersect (visible from dimensions in this section)
4. **A mandatory element missing in THIS section** — e.g. the section must show a connection node to a structure, but it is absent

### Do NOT assign «ПРОВЕРИТЬ ПО СМЕЖНЫМ» when:

1. **Standard design assignment to adjacent disciplines** — «provide cold/hot water supply», «provide 380V power», «pass loads to the structural section» — this is normal coordination, not an error
2. **A mere mention of another discipline** — the fact that the process section mentions electricity does not mean it must be «checked against the electrical section»

## Publication section

For each finding provide `report_section`: `technical_findings` or `questions`.
Use `questions` for an addressed request for missing data, a conditional concern,
or a factual but minor editorial/out-of-profile clarification. Keep confirmed
technical conflicts in `technical_findings` regardless of whether the consequence
is fully quantified. Preserve original claim, uncertainty, severity and evidence;
section routing is not refutation and must not delete a useful finding. Never mark
an unsupported technical assertion as confirmed merely by moving it to questions.
