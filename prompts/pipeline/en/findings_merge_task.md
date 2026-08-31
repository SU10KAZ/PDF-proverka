> **OUTPUT LANGUAGE:** All text values in JSON output (problem, description, solution, risk, etc.) MUST be written in Russian.
> **RESPONSE FORMAT:** Respond with valid JSON only. No explanations, no markdown, no text outside JSON.

# FINDINGS CONSOLIDATION — {PROJECT_ID}

## Role

{DISCIPLINE_ROLE}

## Input Data

1. **Text analysis** — READ via Read tool: `{OUTPUT_PATH}/02_text_analysis.json`
   - `text_findings` (T-001...), `normative_refs_found`, `project_params`
   - `items_verified_from_blocks` (optional) — present if text ran AFTER blocks: its cross-check
     of its own T-findings against Stage 01 blocks.

2. **Block analysis** — READ via Read tool: `{OUTPUT_PATH}/01_blocks_analysis.json`
   - `block_analyses` (findings G-001... within each block)
   - `items_verified_from_stage_01` (optional, legacy) — present if blocks ran AFTER text.
   - If `stage01_meta.uncovered_blocks`, `stage01_meta.failed_blocks`, or block-level
     `coverage_status: "missing_block_context" | "single_block_analysis_failed"` is present,
     DO NOT treat those blocks as “no findings”. They were not fully analyzed.

3. **MD file** (for context) — READ via Read tool: `{MD_FILE_PATH}`

4. **Normative reference** — provided in system context.

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

Merge findings from both stages (01 blocks + 02 text).

### GPT+Codex comparison handling (MANDATORY when present)

A Stage 01 G-item may contain `detector_comparison`. This is the completed semantic
comparison of the independent detectors. Do not recalculate or ignore it:

- `match`: produce one F-finding and include both G-ids in `source_finding_ids`;
- `extension`: produce one richer F-finding, preserving details and both G-ids;
- `new`: retain it as an independent finding; `origin: "gap_search"` means the
  post-comparison pass found it;
- `disputed`: do not choose one detector and do not collapse the pair as an ordinary
  duplicate. Produce an explicit verification finding containing both conflicting
  claims and both G-ids. If the image cannot resolve it confidently, use severity
  `ПРОВЕРИТЬ ПО СМЕЖНЫМ`.

The backend restores exact detector attribution from `source_finding_ids`; never invent G-ids.

### Coverage Warning Sections (MANDATORY)

If `01_blocks_analysis.json` contains uncovered/failed blocks, add three sections under
`meta.analysis_coverage.sections`:
- `Блоки без подготовленного контекста`
- `Ошибки single-block анализа`
- `Блоки, исключённые из полноценного анализа`

These blocks are not clean “no findings” blocks; they were not fully analyzed. Do not invent
findings for them, but preserve their block ids and reasons in meta.

### Processing text↔block verification (MANDATORY)

Take the verification array from whichever file has it (stage order may be either):
- `items_verified_from_blocks` from `02_text_analysis.json` (block→text order — primary), OR
- `items_verified_from_stage_01` from `01_blocks_analysis.json` (legacy text→block order).

Both describe the same thing: a text finding T-NNN cross-checked against a drawing. Process each record:

- **`confirmed: true` WITH CONCRETE evidence** (a `block_id` and an `evidence` citing a specific
  value/detail from the drawing) → text finding confirmed. Elevate severity by one level
  (РЕКОМЕНДАТЕЛЬНОЕ → ЭКСПЛУАТАЦИОННОЕ, ЭКСПЛУАТАЦИОННОЕ → ЭКОНОМИЧЕСКОЕ). Keep КРИТИЧЕСКОЕ as-is.
  **Do NOT elevate** on a bare `confirmed: true` without concrete evidence (otherwise two models
  self-confirm each other and inflate severity).
- **`confirmed: false`** → drawing shows something different from text. Two options:
  - If the error is in text (typo, outdated data) but drawing is correct → **remove finding** or downgrade to РЕКОМЕНДАТЕЛЬНОЕ with note "расхождение текста и чертежа"
  - If the drawing also has an error, but a different one → **keep and clarify** description
- **Finding without verification** (T-NNN not in the array) → keep as-is, do not elevate severity

### Merge Rules

1. **Deduplication**: same finding in both text and drawing → single entry with more complete description. Never collapse Stage 01 `disputed` pairs as ordinary duplicates
2. **Severity elevation**: text finding confirmed by drawing with concrete evidence → severity increases (see verification section above)
3. **Severity reduction**: text suspicion NOT confirmed by drawing → downgrade or remove
4. **Renumbering**: final IDs: F-001, F-002...
5. **Block linkage**: for each F-NNN fill `related_block_ids` — list of block_id from block analysis that are the source. For G-NNN → block's block_id. For T-NNN → block_ids that confirmed the text finding (from the verification array). For cross-block → all participating block_ids.
6. **Source tracing**: fill `source_finding_ids` with the exact T-NNN/G-NNN ids used by each F-finding. Do not rewrite `provenance` or `detector_comparison`; the backend restores them deterministically from these ids.

### No internal identifiers in human-readable text (MANDATORY)

The fields `problem`, `description`, `solution`, `risk` are read by external
experts who do NOT know what a block_id or T-/G-number is. NEVER mention
internal identifiers there: block_id (like `RUXD-WP4R-6C3`), hypothesis ids
T-NNN/G-NNN, or other findings' F-NNN numbers.

- Block linkage belongs ONLY in structured fields: `source_block_ids`,
  `related_block_ids`, `evidence`, `highlight_regions`.
- In text, refer to the source in words: fragment type + short name + sheet.
  - BAD: «Текстовое замечание подтверждено блоками RUXD-WP4R-6C3 и 3C6E-3QEP-D39.»
  - GOOD: «Текстовое замечание подтверждено схемой систем К2 (лист 18) и
    таблицей «Перечень отклонений» (лист 1).»
- Take the name and sheet from `01_blocks_analysis.json` (`label`, `sheet`)
  or from the page context in `document_graph.json`.

### Finding Fields

- `severity`: КРИТИЧЕСКОЕ / ЭКОНОМИЧЕСКОЕ / ЭКСПЛУАТАЦИОННОЕ / РЕКОМЕНДАТЕЛЬНОЕ / ПРОВЕРИТЬ ПО СМЕЖНЫМ
- `problem`: brief summary (1-2 lines)
- `description`: detailed description with numerical data
- `norm`: compatibility text for candidates; the backend rebuilds it from `candidate_norm_references`
- `candidate_norm_references`: one candidate for EACH normative document. Required fields: `designation`, `candidate_relevance`, `reason`, `provenance`. Optional, unproved hints: `clause_candidate`, `quote_candidate`
- `solution`: specific corrective action
- `risk`: consequences if not fixed
- `source_block_ids`: block_ids WHERE the finding was actually DETECTED (source-of-truth). Differs from `related_block_ids`: source = "where found", related = "what it relates to".
- `source_finding_ids`: exact source T-NNN/G-NNN ids used for this finding.
- `related_block_ids`: block_ids the finding RELATES TO. May include blocks where the problem is not directly visible but are connected.
- `evidence_text_refs`: detailed text↔finding traceability. Transfer from block analysis and deduplicate.
- `evidence`: array of data sources. `{type: "image"|"text", block_id: "...", page: N}`.
- `highlight_regions`: visual regions on the block. Transfer from G-findings. Format: `[{block_id: "...", x: 0.35, y: 0.40, w: 0.20, h: 0.15, label: "..."}]`. Add `block_id` to each region.

## Output JSON Schema

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
      "norm": "Документ — норматив-кандидат",
      "norm_quote": null,
      "candidate_norm_references": [
        {
          "designation": "ГОСТ Р 21.101-2020",
          "cited_designation": "ГОСТ Р 21.101-2020",
          "candidate_relevance": 0.8,
          "reason": "Why this document may govern the problem",
          "clause_candidate": "5.1.6",
          "quote_candidate": "Input quote candidate or null",
          "provenance": {
            "source_finding_ids": ["G-001"],
            "designation_source": "source finding"
          }
        }
      ],
      "solution": "Действие по исправлению",
      "risk": "Чем грозит",
      "source_finding_ids": ["G-001", "T-003"],
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

- `sheet` — sheet number **from the title block** (`sheet_no` from page context / block analysis). Format: "Лист 7" or "Листы 3, 5". DO NOT confuse with page number!
- `page` — page number (integer). If finding spans multiple pages — array `[12, 13]`.

**STRICT RULE:** Use sheet numbers from block analysis block entries (field `sheet`). If a block has `sheet: "Лист 7"`, use that value. If sheet is not available — set `"sheet": null` and DO NOT guess.

## Normative Candidates

Create a separate object in `candidate_norm_references` for every norm. One object must
never contain several designations.

- `designation` identifies a potentially relevant document.
- `candidate_relevance` is a 0..1 estimate of document relevance.
- `reason` says what requirement should be searched for in that document.
- `clause_candidate` and `quote_candidate` are hints, never proof. Use `null`
  when uncertain and never copy one quote candidate to different documents.
- `provenance` belongs to this candidate only.
- Do not determine edition status or rewrite an old edition to a new one.

Stage 03 is not the source of truth for clauses or quotes. Always set
`norm_quote` to `null` and do not produce `norm_references`. The backend
normalizes designations; Norm Resolver searches real clauses only within that
vault document; the deterministic verifier publishes proven references.

## Output

WRITE via Write tool: `{OUTPUT_PATH}/03_findings.json`

## Rules

1. Write JSON via Write tool — DO NOT output to chat
2. After writing, output a brief summary of findings
3. Finding IDs: F-001, F-002... (sequential numbering)
4. When referencing a norm — indicate status (действует/заменён/отменён)
5. Respond with valid JSON matching the schema above
