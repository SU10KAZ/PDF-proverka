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

## Optional Claude leg (12.10.2026, on by default)

`STAGE01_CLAUDE_LEG_ENABLED` (default `true`; set `false` in the service env to
switch it off; test conftests force it off) adds a third, optional detector to
`TWO_MODEL_NO_OPENROUTER`: `STAGE01_CLAUDE_LEG_MODEL` (default
`claude-opus-5-5`) at `STAGE01_CLAUDE_LEG_EFFORT` (default `medium`), only for
`STAGE01_CLAUDE_LEG_DISCIPLINES` (default `AR,AI,EOM,SS,ITP`). It replaces the
value of the disabled OpenRouter leg; OpenRouter itself stays closed and the
gate is not involved.

- **Input** is identical to the Codex legs (system prompt, block context, image,
  response schema): `call_claude_for_block` mirrors `call_codex_for_block` and
  sends it through `claude_json_runner` (base64 image, `--system-prompt`, no
  Codex agent wrapper).
- **Judge.** Sol compares Claude (left) with the Codex group Astra + Sol (right):
  a comparison side may be a group of models (`dual_review._side_list`). Without
  the Claude leg the two-model prompt is byte-identical to before.
- **Optional.** A failed Claude call is recorded in `optional_legs_failed` and
  dropped before combining. The block stays a complete Astra + Sol block, and the
  leg never triggers `STAGE01_ABORT_ON_LEG_FAILURE_ENABLED`. A subscription or
  rate limit switches the leg off for the rest of the run
  (`claude_leg.disabled_reason`).
- **Accounting.** Receipt: `claude_leg_model`, `claude_calls`, `claude_failures`,
  `claude_leg_disabled_reason`; summary: `claude_leg` with tokens. The manager
  records Claude tokens separately (Claude subscription, notional cost) and
  subtracts them from the ensemble (Codex) record.
- **Not covered:** worker routing plans (`use_plan_ensemble`) and explicit
  secondary-leg profiles keep their frozen contracts and never get the leg.

Evidence and cost: `comparison/openrouter_leg_replacement/phase123_report.md`.
On 181 blocks Opus 5.5 medium recovered 34–47 % of the expert-accepted findings
lost with the OpenRouter leg; engineers accepted 35 % of its new findings. It
costs about $0.09–0.10 per block on the Claude subscription, about 4 % of the
weekly limit for the default disciplines. Tests: `tests/test_stage01_claude_leg.py`.
