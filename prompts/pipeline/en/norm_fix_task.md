> **OUTPUT LANGUAGE:** All text values in JSON output MUST be written in Russian.
> **RESPONSE FORMAT:** Respond with valid JSON only. No explanations, no markdown, no text outside JSON.

# FINDINGS REVISION WITH UPDATED NORMS

## Operating Mode
Work AUTONOMOUSLY. Do not ask questions.
Revise the specified findings, update normative references, return the result.

## Project
- **ID:** {PROJECT_ID}

## Input Data

READ via Read tool:

1. **Current Findings** — `{PROJECT_PATH}/_output/03_findings.json`
2. **Norm Verification Results** — `{PROJECT_PATH}/_output/norm_checks.json`
3. **Normative Reference** — provided in system context.

## Findings to Revise
{FINDINGS_TO_FIX}

## Task

For EACH finding from the list above:

1. Read the current wording from findings data
2. Read the norm verification result from norm checks data
3. Check `paragraph_checks` in norm checks:
   - If the finding has an entry with `paragraph_verified: false` — **the norm quote is incorrect**.
     Use `actual_quote` to fix the wording.
   - If `paragraph_verified: true` — quote is confirmed, use as-is.
4. Determine:
   - If norm is **cancelled** (`cancelled`) — the finding may be outdated.
     Check: is there a replacement? If yes — reformulate with reference to the replacement.
     If no replacement — mark as "требует дополнительной проверки".
   - If norm is **replaced** (`replaced`) — replace the reference with the new document.
     Find the analogous clause in the new document. If the clause changed — update the wording.
   - If **outdated edition** (`outdated_edition`) — update the edition/amendment number.
     Check: have the requirements changed in the new edition?
   - If **quote not confirmed** (`paragraph_verified: false`) — fix the clause reference
     and wording using the actual text from `actual_quote`.

5. For each finding, record:
   - Original wording (for comparison)
   - Updated wording
   - What exactly changed and why

## Output JSON Schema

The result must be a **complete copy** of findings with the following additions:

```json
{
  "meta": {
    "...all fields from 03_findings.json...",
    "norm_verification": {
      "verified_at": "<ISO datetime>",
      "total_norms_checked": 0,
      "norms_ok": 0,
      "norms_revised": 0,
      "findings_revised": ["F-002", "F-003"]
    }
  },
  "findings": [
    {
      "...all fields from original...",
      "norm_verified": true,
      "norm_status": "ok|revised|warning",
      "norm_revision": {
        "original_norm": "старая ссылка",
        "revised_norm": "новая ссылка (или null если не менялась)",
        "original_text": "старая формулировка замечания (или null)",
        "revised_text": "новая формулировка (или null)",
        "revision_reason": "Причина изменения"
      }
    }
  ],
  "quick_index": "...as in original..."
}
```

## Output

WRITE via Write tool: `{PROJECT_PATH}/_output/03_findings.json`

## Rules

1. DO NOT delete or add findings — only update existing ones
2. For findings with no norm issues: `norm_verified: true, norm_status: "ok", norm_revision: null`
3. For revised findings: `norm_status: "revised"` + fill `norm_revision`
4. For uncertain cases: `norm_status: "warning"` + explain in `revision_reason`
5. Preserve ALL original fields of each finding — add new ones, do not remove old ones
6. Write JSON via Write tool — DO NOT output to chat
7. Respond with valid JSON matching the schema above
