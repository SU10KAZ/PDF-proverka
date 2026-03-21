> **OUTPUT LANGUAGE:** All text values in JSON output (finding, source, reason, etc.) MUST be written in Russian.

# PROJECT TEXT ANALYSIS — {PROJECT_ID}

## Input Data

1. **MD file** (primary text source): `{MD_FILE_PATH}`
   - `[TEXT]` blocks — text data (explanatory notes, specifications, tables)
   - `[IMAGE]` blocks — drawing descriptions (type, axes, entities, text on drawing)

2. **Image block index**: `{OUTPUT_PATH}/blocks/index.json`
   - `block_id`, `page`, `ocr_label`, `ocr_text_len`

3. **Normative reference**: `{DISCIPLINE_NORMS_FILE}`

## Task

### Stage 1: Text Data Analysis

Read the MD file COMPLETELY. Extract:

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

{DISCIPLINE_CHECKLIST}

### Stage 2: Image Block Prioritization

For EACH block from `index.json`, determine priority:

| Priority | Criteria |
|----------|----------|
| **HIGH** | Schematics, plans with routing, key drawings |
| **MEDIUM** | Specifications, tables, details |
| **LOW** | General views, facades |
| **SKIP** | Title blocks, title pages, tables of contents |

## Finding Categories

{DISCIPLINE_FINDING_CATEGORIES}

## Output File

WRITE via Write tool: `{OUTPUT_PATH}/01_text_analysis.json`

```json
{
  "stage": "01_text_analysis",
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
      "norm_confidence": 0.9,
      "needs_visual_check": true,
      "related_block_ids": ["block_id"]
    }
  ],
  "blocks_for_review": [
    {"block_id": "...", "page": 7, "priority": "HIGH", "reason": "Описание"}
  ],
  "blocks_skipped": [
    {"block_id": "...", "page": 3, "priority": "SKIP", "reason": "Штамп"}
  ]
}
```

## Normative Accuracy (norm_quote + norm_confidence)

For EACH finding with a `norm` field:
- **`norm_quote`** — exact quote from the norm clause (1-2 sentences). `null` if unsure.
- **`norm_confidence`** — confidence 0.0–1.0. At < 0.8, the verifier will check via WebSearch.

## Rules

1. Read the MD file COMPLETELY — do not skip sections
2. `text_findings[]` — based on text data only (not drawings)
3. `blocks_for_review[]` — fill for each block from index.json
4. severity — ONLY one of the 5 values
5. Write JSON via Write tool — DO NOT output to chat
6. After writing, output a brief summary
