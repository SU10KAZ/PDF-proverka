# AI Sheet Matcher — isolated research spike

Status: **complete**

This directory is an offline experiment over three frozen Stage Comparison
pairs. It is not imported by the backend or frontend, does not change
`production-sheet-matcher.v3`, does not change production thresholds, does not
write to pair run directories, and is not deployed.

The experiment has two deliberately separate phases:

1. `python -m experiments.ai_sheet_matcher.run candidate-audit`
2. `python -m experiments.ai_sheet_matcher.run experiment`

The second phase refuses to call a model unless `candidate_recall.json` exists
and matches the current frozen inputs. Candidate signature:
`4d28b97ca192ff37d852e3ef6a68358d930aa198e398373337277009f71e4eb0`.

Selector outputs contain only a frozen payload signature plus prebuilt task and
option IDs. Page numbers, evidence, values, and entity names cannot be emitted
by the model. Pass A and Pass B use byte-identical payloads; three independent
ephemeral calls are made for every mode/project/pass. A deterministic verifier
checks bindings, page bounds, direction, evidence refs, cardinality, complete
groups, map conflicts, stability, and saved engineer decisions. Any doubt is
blocked from materialization.

Artifacts:

- `candidate_recall.json` — top-5/top-10 audit before model calls;
- `model_runs.jsonl` — bounded outputs and call telemetry, without raw prompts;
- `decisions.jsonl` — per-task Pass A/B, final status, verifier, and evidence refs;
- `stability.json` — cold-run overlaps and map signatures;
- `metrics.json` — requested baseline/AI/safety/cost metrics;
- `experiment_report.md` — findings and rollout verdict.
