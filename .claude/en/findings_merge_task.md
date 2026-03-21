> **OUTPUT LANGUAGE:** All text values in JSON output (problem, description, solution, risk, etc.) MUST be written in Russian.

# FINDINGS CONSOLIDATION — {PROJECT_ID}

## Input Data

1. **Text analysis**: `{OUTPUT_PATH}/01_text_analysis.json`
   - `text_findings` (T-001...), `normative_refs_found`, `project_params`

2. **Block analysis**: `{OUTPUT_PATH}/02_blocks_analysis.json`
   - `block_analyses` (findings G-001... within each block), `items_verified_from_stage_01`

3. **MD file** (for context): `{MD_FILE_PATH}`

4. **Normative reference**: `{DISCIPLINE_NORMS_FILE}`

## Task

### Step 1: Cross-Page and Cross-Block Verification (MANDATORY)

Before merging findings — group `block_analyses[]` by page and verify:

1. **Within a single page:**
   - Blocks on the same page describe one concept (mounting details, catalog sheets, etc.)
   - Any contradictions between blocks on the same page (different dimensions, marks, parameters)?
   - Similar blocks (e.g., 10 mounting details) — are the principles consistent?

2. **Between pages:**
   - Specification (text) vs drawings: are all specification items visible on drawings? Is there equipment on drawings missing from the specification?
   - Catalog sheets (graphs, characteristics) vs actually used equipment: any extra catalog sheets for unused sizes?
   - Parameters on one drawing (flow rate, diameter) vs parameters on another (load table, axonometric) — do they match?
   - key_values_read from different pages — any conflicts?

Any discrepancy found → add as a new finding (F-NNN).

### Step 2: Merge Findings

Merge findings from both stages (01 text + 02 blocks).

### Processing items_verified_from_stage_01 (MANDATORY)

Before merging — process `items_verified_from_stage_01`:

- **`confirmed: true`** → text finding confirmed by drawing. Elevate severity by one level (РЕКОМЕНДАТЕЛЬНОЕ → ЭКСПЛУАТАЦИОННОЕ, ЭКСПЛУАТАЦИОННОЕ → ЭКОНОМИЧЕСКОЕ). Keep КРИТИЧЕСКОЕ as-is.
- **`confirmed: false`** → drawing shows something different from text. Two options:
  - If the error is in text (typo, outdated data) but drawing is correct → **remove finding** or downgrade to РЕКОМЕНДАТЕЛЬНОЕ with note "расхождение текста и чертежа"
  - If the drawing also has an error, but a different one → **keep and clarify** description
- **Finding without verification** (T-NNN not in items_verified) → keep as-is, do not elevate severity

### Merge Rules

1. **Deduplication**: same finding in both text and drawing → single entry with more complete description
2. **Severity elevation**: text finding confirmed by drawing → severity increases (see items_verified above)
3. **Severity reduction**: text suspicion NOT confirmed by drawing → downgrade or remove
4. **Renumbering**: final IDs: F-001, F-002...
5. **Block linkage**: for each F-NNN fill `related_block_ids` — list of block_id from `02_blocks_analysis.json` that are the source. For G-NNN → block's block_id. For T-NNN → block_ids that confirmed the text finding (from `items_verified`). For cross-block → all participating block_ids.

### Finding Fields

- `severity`: КРИТИЧЕСКОЕ / ЭКОНОМИЧЕСКОЕ / ЭКСПЛУАТАЦИОННОЕ / РЕКОМЕНДАТЕЛЬНОЕ / ПРОВЕРИТЬ ПО СМЕЖНЫМ
- `problem`: brief summary (1-2 lines)
- `description`: detailed description with numerical data
- `norm`: document + clause (with validity status)
- `solution`: specific corrective action
- `risk`: consequences if not fixed
- `source_block_ids`: block_ids WHERE the finding was actually DETECTED (source-of-truth). Differs from `related_block_ids`: source = "where found", related = "what it relates to".
- `related_block_ids`: block_ids the finding RELATES TO. May include blocks where the problem is not directly visible but are connected.
- `evidence_text_refs`: detailed text↔finding traceability. Transfer from `02_blocks_analysis.json` and deduplicate.
- `evidence`: array of data sources. `{type: "image"|"text", block_id: "...", page: N}`.
- `highlight_regions`: visual regions on the block. Transfer from G-findings. Format: `[{block_id: "...", x: 0.35, y: 0.40, w: 0.20, h: 0.15, label: "..."}]`. Add `block_id` to each region.

## Output File

WRITE via Write tool: `{OUTPUT_PATH}/03_findings.json`

```json
{
  "meta": {
    "project_id": "{PROJECT_ID}",
    "audit_completed": "<ISO>",
    "total_findings": 0,
    "blocks_analyzed": 0,
    "by_severity": {
      "КРИТИЧЕСКОЕ": 0,
      "ЭКОНОМИЧЕСКОЕ": 0,
      "ЭКСПЛУАТАЦИОННОЕ": 0,
      "РЕКОМЕНДАТЕЛЬНОЕ": 0,
      "ПРОВЕРИТЬ ПО СМЕЖНЫМ": 0
    }
  },
  "findings": [
    {
      "id": "F-NNN",
      "severity": "...",
      "category": "...",
      "sheet": "Лист X",
      "page": 12,
      "problem": "Краткая суть",
      "description": "Развёрнутое описание с числами",
      "norm": "Документ (статус), пункт",
      "norm_quote": "Точная цитата из пункта нормы (1-2 предложения) или null",
      "solution": "Действие по исправлению",
      "risk": "Чем грозит",
      "source_block_ids": ["IMG-001"],
      "related_block_ids": ["IMG-001", "IMG-008"],
      "evidence_text_refs": [
        {"text_block_id": "TB-SPEC-001", "role": "table", "used_for": "value_extraction"}
      ],
      "evidence": [
        {"type": "image", "block_id": "IMG-001", "page": 4},
        {"type": "text", "block_id": "RUXD-WP4R-6C3", "page": 4}
      ],
      "highlight_regions": [
        {"block_id": "IMG-001", "x": 0.35, "y": 0.40, "w": 0.20, "h": 0.15, "label": "Марш Л-1, размер 1000"}
      ]
    }
  ]
}
```

### Sheet and Page Rules (MANDATORY)

- `sheet` — sheet number **from the title block** (`sheet_no` from page context / `02_blocks_analysis`). Format: "Лист 7" or "Листы 3, 5". DO NOT confuse with PDF page number!
- `page` — PDF page number (integer). If finding spans multiple pages — array `[12, 13]`.

**STRICT RULE:** Use sheet numbers from `02_blocks_analysis.json` block entries (field `sheet`). If a block has `sheet: "Лист 7"`, use that value. If sheet is not available — set `"sheet": null` and DO NOT guess.

## Normative Accuracy (norm_quote)

When merging T-NNN and G-NNN into final F-NNN — **preserve** `norm_quote` from source stages.

If two findings merge into one:
- `norm_quote` — take from the more detailed source

For new findings (cross-block verification):
- Fill `norm_quote` using the same rules

## Rules

1. Write JSON via Write tool — DO NOT output to chat
2. Finding IDs: F-001, F-002... (sequential numbering)
3. When referencing a norm — indicate status (действует/заменён/отменён)
4. After writing, output a brief summary: finding count by severity
