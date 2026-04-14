> **OUTPUT LANGUAGE:** All text values in JSON output (summary, finding, label, description, key_values_read, highlight_regions.label, etc.) MUST be written in Russian.
> **RESPONSE FORMAT:** Respond with valid JSON only. No explanations, no markdown, no text outside JSON.

# IMAGE BLOCK ANALYSIS — Batch {BATCH_ID} of {TOTAL_BATCHES}

## Project: {PROJECT_ID} | Discipline: {SECTION}
- Batch {BATCH_ID} of {TOTAL_BATCHES}, blocks: {BLOCK_COUNT}

## Role

{DISCIPLINE_ROLE}

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

**IMPORTANT: page vs sheet.** `page` = page number (physical numbering). `sheet` = sheet number from the title block (logical numbering from the stamp). They do NOT match: Sheet 1 may be on page 5. Both are listed above. For `sheet`, use the sheet number from the title block or from the page context (`**Лист:**`).

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

## MOUNTING DETAILS, NODES, AND SECTIONS — CONSTRUCTION METHOD DESCRIPTION (MANDATORY)

If a block contains a mounting detail, node (узел), section (разрез), or installation drawing:

**You MUST describe in `summary` and `key_values_read` the CONSTRUCTION METHOD, not just what is depicted:**

1. **Mounting method** — how is the element attached? (brackets, anchors, mounting panel, DIN rail, embedded parts, welding, etc.)
2. **Materials** — what are the structural elements made of? (steel angle, channel, perforated profile, concrete, etc.)
3. **Connections** — how are elements joined? (bolts, welding, clamps, quick-release connectors, etc.)
4. **Dimensions and clearances** — key dimensions, distances from walls/ceiling/floor, clearance zones
5. **Cable/pipe entry method** — how cables or pipes enter: through sleeves, openings, cable glands, fire-rated penetrations
6. **Fire protection** — fire-rated enclosures, coatings, seals (type, rating EI30/EI60/EI150)
7. **Quantity and repetition** — how many times this node is repeated in the project (if visible from context)

**Purpose:** This information is used at the optimization stage to evaluate whether the mounting/construction approach can be simplified, standardized, or made more cost-effective.

**Example key_values_read for a detail:**
- `"Щит ЩУАХП: крепление на кронштейнах к кирпичной стене, 4 анкера М10"`
- `"Кабельный ввод снизу через стальную гильзу Ø50, заделка огнестойкой пеной"`
- `"Лоток 200×50 на шпильках М8 к перекрытию, шаг 1000 мм"`

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
      "key_values_read": ["АВ E3H 1600А", "Кабель ВВГнг(А)-FRLS 5x10, L=48м"],
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

### key_values_read Format — HUMAN-READABLE LANGUAGE

`key_values_read` is shown directly to the auditor engineer on screen. Write in **human-readable Russian**, NOT as technical field dumps.

**BAD** (technical dump):
- `"opening Р-1: opening_type: решетка, width_mm: 752, height_mm: 550"`
- `"room_name: Жилая комната, purpose: жилая, area_m2: 15.6"`

**GOOD** (human-readable):
- `"Решетка Р-1: 752×550 мм"`
- `"Жилая комната — 15,6 м²"`
- `"Проём Д-3: ширина 900 мм, EI 60"`
- `"Помещение 101 «Лобби»: 42,3 м², класс Ф3.1"`
- `"АВ E3H 1600А, Iку=50кА"`
- `"Кабель ВВГнг(А)-FRLS 5×10, L=48 м"`

**Rule:** include units of measurement. Start with the label/mark (Р-1, Д-3, поз. 5) if one exists.

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
