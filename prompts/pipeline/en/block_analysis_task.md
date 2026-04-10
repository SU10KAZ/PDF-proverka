> **OUTPUT LANGUAGE:** All text values in JSON output (summary, finding, label, description, key_values_read, highlight_regions.label, etc.) MUST be written in Russian.
> **RESPONSE FORMAT:** Respond with valid JSON only. No explanations, no markdown, no text outside JSON.

# IMAGE BLOCK ANALYSIS — Batch {BATCH_ID} of {TOTAL_BATCHES}

## Project: {PROJECT_ID} | Discipline: {SECTION}
- Batch {BATCH_ID} of {TOTAL_BATCHES}, blocks: {BLOCK_COUNT}

## Input Data

1. **Text analysis context** — provided inline.
   - `text_findings` — text-based findings (for verification against drawings)
   - `project_params` — project parameters (loads, capacities, equipment marks)
   If not available — proceed without context.

2. **Structured page context** (from Document Knowledge Graph):

{BLOCK_MD_CONTEXT}

   Context is organized per-block:
   - **Page metadata** (Sheet, Title) — drawing coordinates
   - **Page text** — tables, notes, specifications, legends (shared across all blocks on the page)
   - **Per-block OCR** — OCR description of the specific block (for each block in the batch)
   - **Other blocks on page** — list of neighboring block_ids (for cross-block verification)

   **IMPORTANT:** Cross-check drawing data (PNG) against text on the same page.
   Discrepancies between the drawing and text on the same page → finding.

3. **Blocks to analyze** (read EACH one via Read tool):
{BLOCK_LIST}

Each block is a cropped drawing fragment (a complete area: schematic, plan, table, or detail). Analyze each block as an independent drawing.

**IMPORTANT: page vs sheet.** `page` = PDF page number (physical numbering). `sheet` = sheet number from the title block (logical numbering from the stamp). They do NOT match: Sheet 1 may be on PDF page 5. Both are listed above. For `sheet`, use the sheet number from the title block or from the page context (`**Лист:**`).

## SINGLE-LINE AND CALCULATION DIAGRAMS — FULL TEXT RECOGNITION (MANDATORY)

If a block contains a single-line diagram, calculation schematic, or switchboard diagram (ВРУ/ГРЩ/УЭРМ/panel):

**You MUST read and record in `key_values_read` ALL labels on the schematic, including:**
1. **Vertical text along cable lines** — cable marks and cross-sections (ВВГнг(А)-FRLS 5x10, ВВГнг-LS 3x2.5, etc.)
2. **Cable lengths** — values in meters next to lines (L=25м, 48м, etc.)
3. **Circuit breaker and RCD marks** — ratings, types (ВА47-29 С16, АВВ S203, etc.)
4. **Current transformer ratings** — CT (200/5А, 400/5А, etc.)
5. **Calculated currents and powers** — Ip, Pp, Sp, cosφ for each feeder
6. **Positional designations** — QF1, QS1, KM1, TA1, etc.
7. **Busbar labels** — bus markings, sections, infeeds

**DO NOT skip text just because it is small or rotated.** Single-line diagram blocks are rendered at enhanced resolution (2500px) specifically for full text recognition.

If text is unreadable — record "нечитаемо: [description of location]" in `key_values_read`.

**IMPORTANT — unreadable text feedback:**
If a block contains text you cannot read due to low resolution (small font, tables with numbers, cable marks, breaker ratings), set `unreadable_text: true` and fill `unreadable_details` — describe WHERE exactly and WHAT is unreadable. The system will automatically re-download this block at higher resolution and repeat the analysis.
If all text is readable — set `unreadable_text: false`.

## Cross-Check with Text Analysis (MANDATORY)

From text analysis context → `project_params`, extract numerical data.
On EACH drawing, cross-check visible values against text data:
- Flow rates, loads, powers — match the tables?
- Diameters, cross-sections — match the specification?
- Equipment sizes — exist in catalogs?
- **Cable marks** — match specification items?
- **Cable lengths** — match the cable schedule?

Any discrepancy → finding.

**IMPORTANT:** If you cannot confidently read a value on the drawing (small text, low resolution, overlap) — write "нечитаемо" in `key_values_read`. DO NOT guess numbers or marks.

## Compact Execution Mode (MANDATORY)

For each block, run 4 passes:
1. **Applicability + drawing type** — decide whether the block is relevant for the discipline and which sheet type it belongs to.
2. **Consistency** — `block ↔ page text ↔ stage_01 params ↔ specification/table` on the same page.
3. **Numeric verification** — recalculate only when the values are readable and the formula is obvious.
4. **Domain engineering check** — apply the compact discipline logic below.

Use triage as follows:
- `HIGH` → full 1–4 pass;
- `MEDIUM` → targeted verification of relevant elements only;
- `LOW` → use mostly as corroborating context; findings only when the error is explicit;
- `SKIP` → `summary` without independent findings unless the block confirms an already known issue.

**Hard checks → findings:** provable discrepancy, calculation error, missing mandatory element, engineeringly incorrect visible solution.
**Soft checks → NOT findings:** title blocks, legends, cosmetics, OCR artifacts, doubtful interpretations, rounding-only differences within 2%.

## Sheet-Type Priority

{DISCIPLINE_TRIAGE_TABLE}

## Compact Discipline Logic

{DISCIPLINE_COMPACT_STRATEGY}

## What to Look For on Drawings

{DISCIPLINE_CHECKLIST}

## Finding Categories

{DISCIPLINE_FINDING_CATEGORIES}

## Drawing Types

{DISCIPLINE_DRAWING_TYPES}

## Output JSON Schema

```json
{
  "batch_id": {BATCH_ID},
  "project_id": "{PROJECT_ID}",
  "timestamp": "<ISO datetime>",
  "block_analyses": [
    {
      "block_id": "...",
      "page": 7,
      "sheet": "Лист 3",
      "label": "Описание что на чертеже",
      "sheet_type": "см. таблицу типов чертежей выше",
      "unreadable_text": false,
      "unreadable_details": null,
      "summary": "Краткое описание содержимого (2-4 предложения)",
      "key_values_read": ["АВ E3H 1600А", "Кабель ВВГнг(А)-FRLS 5x10"],
      "evidence_text_refs": [
        {
          "text_block_id": "TB_ID_1",
          "role": "caption|note|legend|title|table|other",
          "used_for": "summary|finding|value_extraction|cross_check"
        }
      ],
      "findings": [
        {
          "id": "G-NNN",
          "severity": "КРИТИЧЕСКОЕ|ЭКОНОМИЧЕСКОЕ|ЭКСПЛУАТАЦИОННОЕ|РЕКОМЕНДАТЕЛЬНОЕ|ПРОВЕРИТЬ ПО СМЕЖНЫМ",
          "category": "см. таблицу категорий выше",
          "finding": "Конкретное описание проблемы",
          "norm": "СП/ГОСТ/ПУЭ, пункт",
          "norm_quote": "Точная цитата из нормы, на которую опираешься (1-2 предложения). null если не помнишь точно.",
          "block_evidence": "BLOCK_ID",
          "value_found": "точная цитата с чертежа",
          "highlight_regions": [
            {
              "x": 0.35,
              "y": 0.40,
              "w": 0.20,
              "h": 0.15,
              "label": "Краткое пояснение что выделено"
            }
          ]
        }
      ]
    }
  ],
  "items_verified_from_stage_01": [
    {
      "finding_id": "T-NNN",
      "block_id": "...",
      "confirmed": true,
      "evidence": "что видно на чертеже"
    }
  ]
}
```

### Locality Fields (MANDATORY for each block_analysis):

- **`evidence_text_refs`** — traceability: for each used text block specify:
  - `text_block_id` — block ID
  - `role` — text block role: `"caption"`, `"note"`, `"legend"`, `"title"`, `"table"`, `"other"`
  - `used_for` — purpose: `"summary"`, `"finding"`, `"value_extraction"`, `"cross_check"`

If text context did not contain `[text_block_id: ...]` markers, set `"evidence_text_refs": []`.

## Visual Finding Anchoring (highlight_regions) — MANDATORY

**EVERY finding MUST have a non-empty `highlight_regions` array.** This is used to show the user exactly where the problem is on the drawing.

Coordinates are **normalized** (0.0–1.0) relative to block dimensions:
- `x`, `y` — top-left corner of the region (fraction of block width/height)
- `w`, `h` — width and height of the region (fraction of block width/height)
- `label` — brief description of what is highlighted (equipment name, cable mark, dimension, etc.)

**Quick coordinate guide (pick the closest quadrant):**

| Location on drawing | x | y | Typical w | Typical h |
|---------------------|-----|-----|-----------|-----------|
| Top-left corner | 0.0 | 0.0 | 0.3 | 0.3 |
| Top-center | 0.3 | 0.0 | 0.4 | 0.3 |
| Top-right corner | 0.7 | 0.0 | 0.3 | 0.3 |
| Center-left | 0.0 | 0.3 | 0.3 | 0.4 |
| Center | 0.3 | 0.3 | 0.4 | 0.4 |
| Center-right | 0.7 | 0.3 | 0.3 | 0.4 |
| Bottom-left corner | 0.0 | 0.7 | 0.3 | 0.3 |
| Bottom-center | 0.3 | 0.7 | 0.4 | 0.3 |
| Bottom-right corner | 0.7 | 0.7 | 0.3 | 0.3 |

**Rules:**
1. For a specific element (cable, breaker, duct, label) — tight rectangle around it
2. For a table/specification issue — rectangle around the relevant table area
3. For a missing element — rectangle around the area where it SHOULD be
4. Multiple regions allowed (e.g., two conflicting values in different locations)
5. **Fallback: if the issue applies to the entire drawing** → `[{"x": 0, "y": 0, "w": 1, "h": 1, "label": "Entire drawing — <reason>"}]`

**Never return an empty `highlight_regions: []`.** Use the whole-block fallback if you cannot pinpoint the exact location.

## Output

WRITE via Write tool: `{OUTPUT_PATH}/block_batch_{BATCH_ID_PADDED}.json`

## Rules

1. Read EACH block via Read tool — do not skip any
2. For each block, MANDATORY: summary and key_values_read
3. Title blocks → summary: "Штамп / служебная информация"
4. findings may be empty `[]` if no issues found
5. Numbering: G-001, G-002... (within the batch)
6. severity — ONLY one of the 5 values
7. Write JSON via Write tool — DO NOT output to chat
8. After writing, output a brief summary of what was found
9. Respond with valid JSON matching the schema above

## Normative Accuracy (norm_quote)

For EACH finding with a `norm` field:
- **`norm_quote`** — exact quote from the normative document (1-2 sentences). Set `null` if you don't remember the exact wording.

All quotes will be verified at the norm verification stage (stage 04) regardless of confidence.

## Normative Reference
Normative verification is performed at the findings consolidation stage — here, only record facts from the drawings.
