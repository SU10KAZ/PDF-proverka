> **OUTPUT LANGUAGE:** All text values in JSON output MUST be written in Russian.

# NORMATIVE REFERENCE VERIFICATION — Deterministic Mode

## Operating Mode
Work AUTONOMOUSLY. Do not ask questions.
Document statuses (active/replaced/cancelled) are already determined by Python from norms_db.json.
**Your task is only WebSearch for unknown norms and quote verification.**

## Project
- **ID:** {PROJECT_ID}
- **Folder:** {PROJECT_PATH}

## Input Data

### Preliminary norm_checks.json (already created by Python)
READ: `{PROJECT_PATH}/_output/norm_checks.json`
This file already contains deterministic statuses from norms_db.json.
DO NOT overwrite it entirely — only update the entries marked below.

### LLM Work Items
{LLM_WORK}

### Local Reference (informational)
READ: `{DISCIPLINE_NORMS_FILE}`

### Paragraph Reference (verified quotes cache)
READ: `{BASE_DIR}/norms_paragraphs.json`
If the needed clause is already verified — use it instead of WebSearch.

## Task

### Part 1: WebSearch for Unknown/Outdated Norms

For each norm from the "Part 1" section of the input data:

1. Execute WebSearch:
```
WebSearch: "[document number] статус действующий актуальная редакция site:docs.cntd.ru"
```

If docs.cntd.ru yielded no results:
```
WebSearch: "[document number] действующая редакция 2025 2026"
```

2. Determine status:
- **active** — in force, cited edition is current
- **outdated_edition** — document is in force but cited edition is outdated
- **replaced** — document replaced by another
- **cancelled** — document cancelled without replacement
- **not_found** — could not verify

3. For ПУЭ: check which chapters are in force in the 7th edition, which remain from the 6th
4. For ГОСТ: check if superseded by a newer one

### Part 2: Clause Quote Verification

For each finding from the "Part 2" section of the input data:

1. Read `{PROJECT_PATH}/_output/03_findings.json`
2. Find the finding by ID, extract `norm` and `norm_quote`
3. Execute WebSearch:
   ```
   WebSearch: "[document number] пункт [X.X.X] текст требования"
   ```
4. Compare:
   - **Match** → `paragraph_verified: true`
   - **Mismatch** → `paragraph_verified: false`, record actual text in `actual_quote`
   - **Clause not found** → `paragraph_verified: false`, `actual_quote: null`

**Limit:** no more than 10 quotes per session. Priority: КРИТИЧЕСКОЕ/ЭКОНОМИЧЕСКОЕ.

## Output File Format

WRITE: `{PROJECT_PATH}/_output/norm_checks_llm.json`

**IMPORTANT:** Write results to a separate file `norm_checks_llm.json`, NOT to `norm_checks.json`.
Python will automatically merge the results.

```json
{
  "meta": {
    "project_id": "{PROJECT_ID}",
    "check_date": "<ISO datetime>",
    "total_checked_by_llm": 0,
    "norms_searched": 0,
    "paragraphs_verified": 0
  },
  "checks": [
    {
      "norm_as_cited": "СП 256.1325800.2016 (ред. изм. 1-5)",
      "doc_number": "СП 256.1325800.2016",
      "status": "active|outdated_edition|replaced|cancelled|not_found",
      "current_version": "СП 256.1325800.2016 (ред. 29.01.2024, изм. 1-7)",
      "replacement_doc": null,
      "source_url": "https://docs.cntd.ru/document/...",
      "details": "Краткое пояснение — что изменилось",
      "affected_findings": ["F-003"],
      "needs_revision": true,
      "verified_via": "websearch"
    }
  ],
  "paragraph_checks": [
    {
      "finding_id": "F-001",
      "norm": "СП 256.1325800.2016, п.14.9",
      "claimed_quote": "Цитата из norm_quote замечания",
      "actual_quote": "Реальный текст пункта (из WebSearch) или null",
      "paragraph_verified": true,
      "mismatch_details": "null или описание расхождения",
      "norm_confidence_original": 0.7,
      "verified_via": "websearch|norms_paragraphs"
    }
  ]
}
```

## Rules

1. **DO NOT check norms not in the assignment** — Python already checked the rest deterministically
2. **DO NOT overwrite `norm_checks.json`** — write only to `norm_checks_llm.json`
3. Write JSON via Write tool — DO NOT output to chat
4. After writing, output a brief summary:
   - How many norms checked via WebSearch
   - How many quotes verified (paragraph_checks)
   - How many mismatches found
