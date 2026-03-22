> **OUTPUT LANGUAGE:** All text values in JSON output (finding, source, reason, etc.) MUST be written in Russian.
> **RESPONSE FORMAT:** Respond with valid JSON only. No explanations, no markdown, no text outside JSON.

# PROJECT TEXT ANALYSIS — {PROJECT_ID}

## Input Data

1. **MD file** (primary text source) — READ via Read tool: `{MD_FILE_PATH}`
   - `[TEXT]` blocks — text data (explanatory notes, specifications, tables)
   - `[IMAGE]` blocks — drawing descriptions (type, axes, entities, text on drawing)

2. **Normative reference** — READ via Read tool: `{DISCIPLINE_NORMS_FILE}` (if available)

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

{DISCIPLINE_CHECKLIST}

## Finding Categories

{DISCIPLINE_FINDING_CATEGORIES}

## Output JSON Schema

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
      "related_block_ids": ["block_id"]
    }
  ]
}
```

## Normative Accuracy (norm_quote)

For EACH finding with a `norm` field:
- **`norm_quote`** — exact quote from the norm clause (1-2 sentences). `null` if unsure.
- All quotes will be verified at the norm verification stage (stage 04) regardless of confidence.

## Output

WRITE via Write tool: `{OUTPUT_PATH}/01_text_analysis.json`

## Rules

1. Analyze the MD content COMPLETELY — do not skip sections
2. `text_findings[]` — based on text data only (not drawings)
3. severity — ONLY one of the 5 values
4. Write JSON via Write tool — DO NOT output to chat
5. After writing, output a brief summary of what was found
6. Respond with valid JSON matching the schema above
