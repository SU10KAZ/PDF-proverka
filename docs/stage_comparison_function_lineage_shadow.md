# Function Lineage Matcher v1 — production shadow contour

The confirmed Function Lineage architecture is integrated as a private,
non-materializing diagnostic of Stage Comparison.

## Runtime gates

- `AI_FUNCTION_LINEAGE_SHADOW_ENABLED=false` by default. The contour runs only
  for a `STANDARD` production run when this flag is explicitly enabled.
- `AI_FUNCTION_LINEAGE_MATERIALIZATION_ENABLED=false` is reserved for a future
  release. Current code records its requested state but has no materialization
  path and always persists `materialization.applied=false`.
- `FAST` and `DEEP` never enter this contour. In particular, `FAST` makes zero
  Function Lineage model calls.

The contour runs after production sheet passports and candidate search are
available and before TEXT/GRAPHIC content comparison. Its result is not read by
sheet scope, synthesis, review questions, engineer decisions, or the final
report.

## Artifacts and namespaces

The pair's private `production/` directory stores three separate files:

- `document_link_map.json` — namespace `DOCUMENT_LINK`;
- `function_lineage_map.json` — bounded candidates use
  `FUNCTIONAL_ANALOGUE`, verified lineages use `FUNCTION_LINEAGE`;
- `derived_sheet_map.json` — physical sheet relations derived only from stable
  function lineages.

`function_lineage_map.json` contains compact Function Passport v2 records,
atomic function fragments, Pass A/B verifier summaries, stability, unresolved
and rejected reasons, conflicts, human disagreements, model-call/token counts,
and runtime. It contains no model chain of thought, raw page excerpts, image
payloads, or server file paths.

Capacity is keyed by `RIGHT:<physical page>:<function fragment id>`. Therefore
different fragments on one RIGHT sheet may serve independent lineages, while
incompatible reuse of the same fragment fails closed.

## Diagnostics and failure behavior

The diagnostic result export for stage **«Сопоставление листов»** adds a
`function_lineage_shadow` section only when all available artifacts belong to
the requested run. Existing path/secret/binary redaction is applied to it.

Any model, verifier, builder, or shadow-persistence failure is isolated from the
main comparison. The contour records `shadow_status=FAILED` where persistence
is available and production continues with the original sheet scope and result.
Human mappings always have priority; only an explicitly namespaced manual
functional decision can create an `engineer_disagreements` diagnostic.
