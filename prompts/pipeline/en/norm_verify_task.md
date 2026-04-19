> **OUTPUT LANGUAGE:** All text values in JSON output MUST be written in Russian.
> **PRIMARY OUTPUT:** a JSON file created via the `Write` tool. The chat response is secondary and can be a short confirmation. Do NOT treat "reply JSON in chat" as a substitute for creating the file.

# NORMATIVE QUOTE VERIFICATION — MCP Mode (authoritative Norms-main)

## Operating Mode

Work AUTONOMOUSLY. Do not ask questions.

**Document statuses are already determined by Python deterministically** from
`status_index.json` of the sibling project Norms-main (`/home/coder/projects/Norms/`).
Your task is ONLY to verify the text of specific clause quotes.

### Forbidden

- `WebSearch`, `WebFetch`, any internet requests
- Treating `norms/norms_db.json` or legacy caches as source of truth
- Modifying `status`, `edition_status`, `replacement_doc` of `checks[]` entries
  (they are authoritative)
- Guessing clause text if MCP did not return it

### Required

- Use MCP tools from the `norms` server:
  - `mcp__norms__get_paragraph_json(code, paragraph)` — exact clause lookup
  - `mcp__norms__semantic_search_json(query, top, code_filter)` — semantic search
  - `mcp__norms__get_norm_status(code)` — reference (resolve matched_code)
- If clause not found — honestly return `paragraph_verified: false` with `actual_quote: null`

## Project

- **ID:** {PROJECT_ID}

## Input Data

READ via Read tool:

1. **Preliminary `norm_checks.json`** — `{PROJECT_PATH}/_output/norm_checks.json`
   Already contains authoritative statuses from Norms-main. DO NOT overwrite.

2. **Paragraph reference cache** — `{BASE_DIR}/norms/norms_paragraphs.json`

### Verification assignment:
{LLM_WORK}

## Task

For each finding from the list above:

1. Read `{PROJECT_PATH}/_output/03_findings.json`, find the finding by ID,
   extract `norm` and `norm_quote`.

2. Call MCP: `mcp__norms__get_paragraph_json(code=<norm code>, paragraph=<clause number>)`
   - If `found: true` → compare `text` with `norm_quote`.
   - If `found: false` with `resolution_reason` = `paragraph_not_found` / `no_document_text` —
     try `mcp__norms__semantic_search_json(query=<short requirement>, code_filter=<code>)`
     to locate the clause number, then retry `get_paragraph_json`.

3. Result:
   - Semantic match → `paragraph_verified: true`, `actual_quote` = exact MCP text.
   - Mismatch → `paragraph_verified: false`, `actual_quote` = real text, `mismatch_details` set.
   - MCP returned nothing → `paragraph_verified: false`, `actual_quote: null`,
     `mismatch_details` = "clause not found in Norms-main".

Priority: КРИТИЧЕСКОЕ → ЭКОНОМИЧЕСКОЕ → others.

## Output JSON Schema

```json
{
  "meta": {
    "project_id": "{PROJECT_ID}",
    "check_date": "<ISO datetime>",
    "paragraphs_verified": 0,
    "source": "norms_main_mcp"
  },
  "checks": [],
  "paragraph_checks": [
    {
      "finding_id": "F-001",
      "norm": "СП 256.1325800.2016, п.14.9",
      "matched_code": "СП 256.1325800.2016",
      "claimed_quote": "Цитата из norm_quote замечания",
      "actual_quote": "Реальный текст пункта из MCP",
      "paragraph_verified": true,
      "mismatch_details": null,
      "verified_via": "norms_mcp_paragraph"
    }
  ]
}
```

**The `checks` field MUST be an empty list** — norm statuses are authoritative and
cannot be overwritten. Any status-change attempts will be discarded by Python during merge.

## Output — MANDATORY

You MUST create this file via the `Write` tool, exactly at this path:

```
{PROJECT_PATH}/_output/norm_checks_llm.json
```

This is the ONLY required artefact of this task. The chat response is optional.

### Non-negotiable invariants

1. The file MUST be created, even if the verification assignment is empty.
   In that case write a valid JSON with `"paragraph_checks": []`.
2. The file MUST use the EXACT absolute path above. Do NOT invent a different
   path. Do NOT use relative paths.
3. Call the `Write` tool BEFORE emitting any final chat message.
4. Do not verify norms not in the assignment.
5. Do not write to `norm_checks.json` — only to `norm_checks_llm.json`.
6. `checks` MUST be an empty list (`[]`).
7. If unsure about a clause text, leave `actual_quote: null` — do not guess.

After the `Write` call succeeds, a one-line chat confirmation is enough —
do NOT dump the JSON contents back into chat.
