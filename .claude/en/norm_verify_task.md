> **OUTPUT LANGUAGE:** All text values in JSON output MUST be written in Russian.
> **RESPONSE FORMAT:** Respond with valid JSON only. No explanations, no markdown, no text outside JSON.

# NORMATIVE REFERENCE VERIFICATION — Deterministic Mode

## Operating Mode
Work AUTONOMOUSLY. Do not ask questions.
Document statuses (active/replaced/cancelled) are already determined by Python from norms_db.json.
**Your task is only to verify unknown norms and quotes from your knowledge base.**

## Project
- **ID:** {PROJECT_ID}

## Input Data

READ via Read tool:

1. **Preliminary norm_checks.json** — READ: `{PROJECT_PATH}/_output/norm_checks.json`
   Already contains deterministic statuses from norms_db.json.
   DO NOT overwrite it entirely — only provide updates for the entries marked below.

### LLM Work Items
{LLM_WORK}

2. **Normative reference** — READ: `{DISCIPLINE_NORMS_FILE}` (if available)

3. **Paragraph reference (verified quotes cache)** — READ: `{BASE_DIR}/norms_paragraphs.json`
   If the needed clause is already verified — use it instead of searching.

## Task

### Part 1: Verify Unknown/Outdated Norms

For each norm from the "Part 1" section of the input data:

1. Verify norm status from your knowledge:
   - Check if the document is currently in force
   - Check if the cited edition is current

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

1. Find the finding by ID, extract `norm` and `norm_quote`
2. Verify the quote from your knowledge:
   - Compare against known norm text
3. Result:
   - **Match** → `paragraph_verified: true`
   - **Mismatch** → `paragraph_verified: false`, record actual text in `actual_quote`
   - **Clause not found / unsure** → `paragraph_verified: false`, `actual_quote: null`

Verify ALL quotes listed in Part 2 input. Priority order: КРИТИЧЕСКОЕ → ЭКОНОМИЧЕСКОЕ → others.

## Output JSON Schema

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
      "source_url": null,
      "details": "Краткое пояснение — что изменилось",
      "affected_findings": ["F-003"],
      "needs_revision": true,
      "verified_via": "llm_knowledge"
    }
  ],
  "paragraph_checks": [
    {
      "finding_id": "F-001",
      "norm": "СП 256.1325800.2016, п.14.9",
      "claimed_quote": "Цитата из norm_quote замечания",
      "actual_quote": "Реальный текст пункта или null",
      "paragraph_verified": true,
      "mismatch_details": "null или описание расхождения",
      "verified_via": "llm_knowledge|norms_paragraphs"
    }
  ]
}
```

## Output

WRITE: `{PROJECT_PATH}/_output/norm_checks_llm.json`

## Rules

1. **DO NOT check norms not in the assignment** — Python already checked the rest deterministically
2. Write JSON via Write tool — DO NOT output to chat
3. Respond with valid JSON matching the schema above
4. If unsure about a norm status, set `status: "not_found"` — do not guess
