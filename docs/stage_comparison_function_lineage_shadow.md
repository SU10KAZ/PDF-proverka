# Function Lineage Matcher v1 — production shadow contour

The confirmed Function Lineage architecture is integrated as a private,
non-materializing diagnostic of Stage Comparison.

## Runtime gates

- `AI_FUNCTION_LINEAGE_SHADOW_ENABLED=false` by default. This flag only arms
  the contour; it never opts all `STANDARD` runs in by itself.
- `AI_FUNCTION_LINEAGE_SHADOW_PAIR_ALLOWLIST=""` is a comma-separated, exact,
  case-sensitive list of production `pair_id` values.
- `AI_FUNCTION_LINEAGE_SHADOW_RUN_ALLOWLIST=""` is an optional comma-separated,
  exact, case-sensitive list of production `run_id` values.
- The contour runs only when the mode is `STANDARD`, the flag is enabled, and
  either the `pair_id` or the `run_id` is allowlisted. When both allowlists are
  empty it remains disabled and makes no Function Lineage model calls.
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

Function evidence has an explicit provenance type. Function-local fields use
`FRAGMENT_OWNED_EVIDENCE` with owner function and fragment IDs embedded in the
evidence identity. Fields copied directly from the compact physical-sheet
passport use `SHEET_SHARED_EVIDENCE`, have no function/fragment owner, and are
limited to the declared sheet-field allowlist. The verifier checks an exact
candidate evidence set and validates either the fragment owner or the explicit
shared sheet side/page and field. Merely being on the same physical page never
makes fragment-owned evidence valid for another fragment. The bounded selector
payload exposes this compact provenance and states the same non-transfer rule;
the model still cannot submit evidence or IDs outside the schema.

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

The run-bound production state stores `function_lineage_shadow` gate booleans
without copying allowlist identifiers. Its `diagnostic_reason` is one of:

- `SHADOW_DISABLED` — non-`STANDARD`, flag off, or both allowlists empty;
- `PAIR_NOT_ALLOWED` — a configured pair rollout boundary did not match;
- `RUN_NOT_ALLOWED` — a run-only rollout boundary did not match;
- `SHADOW_EXECUTED` — the allowed contour completed;
- `SHADOW_FAILED` — the allowed contour or its shadow-artifact persistence failed.

Executed shadow artifacts carry the same terminal diagnostic reason. The gate
is diagnostic only: no shadow artifact is read by the main comparison, and
materialization remains unimplemented even if its reserved flag is requested.
