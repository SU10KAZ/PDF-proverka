> **OUTPUT LANGUAGE:** All text values in JSON output MUST be written in Russian.
> **RESPONSE FORMAT:** Respond with valid JSON only. No explanations, no markdown, no text outside JSON.

# FINDINGS REVISION WITH UPDATED NORMS — MCP Mode

## Operating Mode

Work AUTONOMOUSLY. Do not ask questions.

### Forbidden

- `WebSearch`, `WebFetch`, any internet requests
- Treating `norms/norms_db.json` as source of truth — it is no longer authoritative
- Inventing a replacement if MCP did not return one

### Required

- Use MCP tools from the `norms` server:
  - `mcp__norms__get_norm_status(code)` — authoritative norm status
  - `mcp__norms__get_paragraph_json(code, paragraph)` — clause text
  - `mcp__norms__semantic_search_json(query, top, code_filter)` — semantic search
- If a replacement is not found — honestly mark `norm_status: "warning"` with reason

## Project

- **ID:** {PROJECT_ID}

## Input Data

READ via Read tool:

1. **Current Findings** — `{PROJECT_PATH}/_output/03_findings.json`
2. **Norm Verification Results** — `{PROJECT_PATH}/_output/norm_checks.json`
3. **Normative Reference (discipline)** — provided in system context

## Findings to Revise
{FINDINGS_TO_FIX}

## Task

For EACH finding from the list above:

1. Read the current wording.
2. Find the matching `checks[]` entry in `norm_checks.json`:
   - `status = cancelled` → norm is cancelled. If `replacement_doc` is present,
     call `get_norm_status(replacement_doc)` via MCP. If no replacement —
     `norm_status: "warning"`, `revision_reason`: "норма отменена без замены".
   - `status = replaced` → use `replacement_doc`. Call
     `get_paragraph_json(replacement_doc, <clause>)` to find the analogous clause.
     If missing — try `semantic_search_json(query=<requirement>, code_filter=<replacement_doc>)`.
   - `status = outdated_edition` → document in force, cited edition outdated.
     Update the edition number, verify clause text via MCP.
   - `status = not_found` → norm missing from Norms-main. DO NOT guess —
     `norm_status: "warning"`, `revision_reason`: "норма отсутствует в Norms-main, см. missing_norms_queue.json".

3. Check `paragraph_checks` in `norm_checks.json`:
   - `paragraph_verified: false` → fix wording using `actual_quote`.
     If `actual_quote: null` → `norm_status: "warning"`.
   - `paragraph_verified: true` → keep as-is.

4. For each updated finding record:
   - Original reference and wording
   - Updated reference and wording
   - Short explanation

## Output JSON Schema

The result must be a **complete copy** of findings with the following additions per-finding:

```json
{
  "meta": {
    "...all fields from 03_findings.json...",
    "norm_verification": {
      "verified_at": "<ISO datetime>",
      "total_norms_checked": 0,
      "norms_ok": 0,
      "norms_revised": 0,
      "findings_revised": ["F-002", "F-003"],
      "source": "norms_main_mcp"
    }
  },
  "findings": [
    {
      "...all fields from original...",
      "norm_verified": true,
      "norm_status": "ok|revised|warning",
      "norm_revision": {
        "original_norm": "старая ссылка",
        "revised_norm": "новая ссылка (или null)",
        "original_text": "старая формулировка (или null)",
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

1. Do not delete or add findings — only update existing ones.
2. For findings without norm issues: `norm_verified: true, norm_status: "ok", norm_revision: null`.
3. For revised findings: `norm_status: "revised"` + fill `norm_revision`.
4. For uncertain cases (no replacement found, clause text missing): `norm_status: "warning"`.
5. Preserve ALL original fields of each finding.
6. Write JSON via Write tool — do not print to chat.
