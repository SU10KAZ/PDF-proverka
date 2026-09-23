# Stage 01 with OpenRouter disabled

For the existing GPT-5.4 + Codex Astra + Codex Sol ensemble, an explicit
`AUDIT_OPENROUTER_ENABLED=0` selects `TWO_MODEL_NO_OPENROUTER`. The GPT branch
is recorded as `SKIPPED_OPENROUTER_DISABLED` and is never dispatched. Astra
and Sol retain their original image, prompt and context. No replacement
provider is selected. Single-model and explicit secondary-leg profiles keep
their existing contracts and remain subject to the global OpenRouter gate.

The Sol Judge receives exactly two named result groups, including legitimate
empty detector findings. Its relationship schema uses left/right references;
GPT is absent from its input. The existing gap search runs inside the same
Judge invocation. With OpenRouter enabled, the existing three-detector dispatch
and Judge prompt/schema remain unchanged.

`01_blocks_analysis.json` → `stage01_meta.execution_receipt` and the run summary
record mode, effective OpenRouter flag, skipped branch, Judge inputs and counts
for OpenRouter/Astra/Sol/Judge/gap-search. Counts measure detector/reviewer
invocations, not tokens or transport retries; gap-search shares the Judge call.
The model-stage API and algorithm UI show GPT temporarily disabled and both
Codex detectors active.

Synthetic validation: `tests/test_stage01_openrouter_off.py` covers ON/OFF
completion, identical Codex inputs, no replacement provider, persisted receipts,
Judge matching and gap findings, real empty detector outputs, invalid flag,
deterministic review failure, UI rendering and frozen routing plans. Tests
block network connections and use fake providers; no real audit is launched.

Separate downstream inspection: the Full Codex reference routing plan has no
OpenRouter action after Stage 01. Other stage actions are byte-for-byte equal
between the two flag states in the regression test. Optimization retains its
existing Claude + Codex ensemble; this change does not alter later stages or
claim that a real full audit has completed. Custom model selections can still
explicitly require OpenRouter and will remain blocked by the global gate.
