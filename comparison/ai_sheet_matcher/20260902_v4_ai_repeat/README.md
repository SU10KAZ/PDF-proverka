# AI Sheet Matcher repeat — Candidate Generator v4

Status: **complete**

This is an isolated offline repeat of commit `41d43625`.  Selector output,
Pass A/Pass B, three-cold-run unanimity, document-map review, verifier, and
human-priority materialization gate are retained.  Only the candidate source is
replaced by `research-candidate-generator.v4` from commit `7948c97d`.

The experiment does not import into the backend or frontend, does not write to
pair/run directories, does not change `production-sheet-matcher.v3`, UI,
engineer mappings, or the production pipeline, and has no deploy action.

Run in two gated phases:

1. `python -m experiments.ai_sheet_matcher_v4.run candidate-audit`
2. `python -m experiments.ai_sheet_matcher_v4.run experiment --model gpt-5.6-sol --effort medium`

Input signatures: `{"p19cd7f695a": "2bf124ed462e348eecc28c1cdf21f7a0405134ddcf979907c4f7cfd8eb258c61", "pb02de74a81": "5ec3e70ad5dd9dcbb89699584b813428fd337cd2bd882c52dae740c5b890f32f", "pe336037597": "58b9b3ea9edd77ca28aa76c52b1e2157856e2c09651fe21a84b94a5c45efc249"}`.

Artifacts:

- `experiment_report.md` — A/B findings, critical cases, safety verdict;
- `metrics.json` — project-level baseline/old/new metrics;
- `decisions.jsonl` — TEXT and final fallback decision traces;
- `model_runs.jsonl` — bounded outputs and call telemetry;
- `stability.json` — three-cold-run exact/map overlap and unstable relations;
- `manual_audit.json` — SUPPORTED/PARTIAL/UNSUPPORTED audit;
- `group_audit.json` — deterministic shortlist and post-shortlist recall;
- `cost_analysis.json` — TEXT/VISION call, token, runtime, and unit cost.
