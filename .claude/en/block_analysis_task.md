> **OUTPUT LANGUAGE:** All text values in JSON output (summary, finding, label, description, key_values_read, highlight_regions.label, etc.) MUST be written in Russian.

# IMAGE BLOCK ANALYSIS — Batch {BATCH_ID} of {TOTAL_BATCHES}

## Project: {PROJECT_ID} | Discipline: {SECTION}
- Batch {BATCH_ID} of {TOTAL_BATCHES}, blocks: {BLOCK_COUNT}

## Input Data

1. **Text analysis context**: `{OUTPUT_PATH}/01_text_analysis.json`
   - `text_findings` — text-based findings (for verification against drawings)
   - `project_params` — project parameters (loads, capacities, equipment marks)
   If the file does not exist — proceed without context.

2. **Structured page context** (from Document Knowledge Graph):

{BLOCK_MD_CONTEXT}

   Context is organized per-block:
   - **Page metadata** (Sheet, Title) — drawing coordinates
   - **Page text** — tables, notes, specifications, legends (shared across all blocks on the page)
   - **Per-block OCR** — OCR description of the specific block (for each block in the batch)
   - **Other blocks on page** — list of neighboring block_ids (for cross-block verification)

   **IMPORTANT:** Cross-check drawing data (PNG) against text on the same page.
   Discrepancies between the drawing and text on the same page → finding.

3. **Blocks to analyze** (read EACH one via Read):
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

From `01_text_analysis.json` → `project_params`, extract numerical data.
On EACH drawing, cross-check visible values against text data:
- Flow rates, loads, powers — match the tables?
- Diameters, cross-sections — match the specification?
- Equipment sizes — exist in catalogs?
- **Cable marks** — match specification items?
- **Cable lengths** — match the cable schedule?

Any discrepancy → finding.

**IMPORTANT:** If you cannot confidently read a value on the drawing (small text, low resolution, overlap) — write "нечитаемо" in `key_values_read`. DO NOT guess numbers or marks.

## Discipline Gate (MANDATORY for each block)

Expected project discipline: **{SECTION}**

For each block, determine `discipline_detected` by priority:

1. **Title block** (highest priority): discipline code from document_code in the stamp or page text (АР, ОВ, ТХ, ЭМ, КЖ, ВК, etc.)
2. **Graphical symbols** (if no title block): types of conventional graphical designations on the drawing (electrical symbols → EM, ducts → OV, reinforcement/concrete sections → КЖ, plumbing fixtures → ВК)
3. **Terminology** (if neither stamp nor characteristic symbols): concrete grade, rebar class → КЖ; cable cross-section, breaker → EM; air flow rate, duct → OV

Then:
- If `discipline_detected` == `{SECTION}` or it is a general sheet (stamp, table of contents, general notes, register) → **analyze normally** per checklist below
- If `discipline_detected` != `{SECTION}`:
  - DO NOT generate normative findings for this block
  - severity of all findings for the block = "ПРОВЕРИТЬ ПО СМЕЖНЫМ"
  - In `discipline_note`, list 1-2 facts from the sheet that identified a different discipline

## What to Look For on Drawings

{DISCIPLINE_CHECKLIST}

## Finding Categories

{DISCIPLINE_FINDING_CATEGORIES}

## Drawing Types

{DISCIPLINE_DRAWING_TYPES}

## Output File

WRITE via Write tool: `{OUTPUT_PATH}/block_batch_{BATCH_ID_PADDED}.json`

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
      "discipline_detected": "АР",
      "discipline_mismatch": false,
      "discipline_note": null,
      "unreadable_text": false,
      "unreadable_details": null,
      "summary": "Краткое описание содержимого (2-4 предложения)",
      "key_values_read": ["АВ E3H 1600А", "Кабель ВВГнг(А)-FRLS 5x10"],
      "selected_text_block_ids": ["TB_ID_1", "TB_ID_2"],
      "evidence_text_refs": [
        {
          "text_block_id": "TB_ID_1",
          "role": "caption|note|legend|title|table|other",
          "used_for": "summary|finding|value_extraction|cross_check",
          "confidence": 0.9
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
          "norm_confidence": 0.9,
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

- **`selected_text_block_ids`** — list of text_block_id (from context `[text_block_id: ...]`) that you actually used when analyzing this block. If no text blocks were used — empty list `[]`.
- **`evidence_text_refs`** — detailed traceability: for each used text block specify:
  - `text_block_id` — block ID
  - `role` — text block role: `"caption"`, `"note"`, `"legend"`, `"title"`, `"table"`, `"other"`
  - `used_for` — purpose: `"summary"`, `"finding"`, `"value_extraction"`, `"cross_check"`
  - `confidence` — confidence (0.0–1.0) that the text block actually relates to this image block

If text context did not contain `[text_block_id: ...]` markers, set `"selected_text_block_ids": []` and `"evidence_text_refs": []`.

## Visual Finding Anchoring (highlight_regions)

For EACH finding with `block_evidence` — provide `highlight_regions`: an array of rectangles showing WHERE on the block the issue is located.

Coordinates are **normalized** (0.0–1.0) relative to block dimensions:
- `x`, `y` — top-left corner of the region (fraction of block width/height)
- `w`, `h` — width and height of the region (fraction of block width/height)
- `label` — brief description (what is highlighted)

**How to determine coordinates:**
- Mentally divide the block into a 10×10 grid. If the issue is in the bottom-right corner → x≈0.7, y≈0.7
- For text elements (marks, dimensions) — narrow rectangle around the label
- For graphical elements (equipment, details) — rectangle around the element
- If the issue concerns the entire block → `[{"x": 0, "y": 0, "w": 1, "h": 1, "label": "Весь чертёж"}]`
- Multiple regions per finding are allowed (e.g., two conflicting locations)

If you cannot determine a specific region — use empty array `[]`.

## Rules

1. Read EACH block — do not skip any
2. For each block, MANDATORY: summary and key_values_read
3. Title blocks → summary: "Штамп / служебная информация"
4. findings may be empty `[]` if no issues found
5. Numbering: G-001, G-002... (within the batch)
6. severity — ONLY one of the 5 values
7. Write JSON via Write tool — DO NOT output to chat
8. After writing, output a brief summary

## Normative Accuracy (norm_quote + norm_confidence)

For EACH finding with a `norm` field:
- **`norm_quote`** — exact quote from the normative document (1-2 sentences). Set `null` if you don't remember the exact wording.
- **`norm_confidence`** — confidence (0.0–1.0) that the specified clause actually contains this requirement:
  - **1.0** — you know the clause text exactly (e.g., well-known ПУЭ/СП requirements)
  - **0.7–0.9** — you remember the essence but are unsure of the exact wording or clause number
  - **0.5–0.7** — you know the requirement exists but are unsure of the specific clause
  - **< 0.5** — you're guessing → better to set `null` for norm_quote

These fields are used during norm verification: at `confidence < 0.8` the verifier will check the quote via WebSearch.

## Normative Reference
Normative verification is performed at the findings consolidation stage — here, only record facts from the drawings.
