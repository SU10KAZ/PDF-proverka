# Algorithm + Prompt Optimization Research

**Date:** 2026-05-20
**Scope:** Determine the best production audit algorithm for AuditManager Stage 01,
and whether the problem in multi-agent is the *architecture* or the *prompts*.
**Production impact:** NONE. This folder never modifies `backend/`, `webapp/`,
`frontend/`, `process_project.py`, or `manager.py`.

## Why this exists

The parent stand [`experiments/md_analysis_comparison/`](../README.md) compared
single-pass Opus (current AuditManager Stage 01) against a 6-agent + critic +
reviewer multi-agent pipeline on 8 cases. The headline result:

- multi-agent: better critical recall (1 missed vs 3), better cross-discipline,
  better completeness.
- multi-agent: ×3 false positives (218 vs 73), ×5.2 wall-clock.
- final composite score: current wins 7/8 cases, multi-agent 0/8.

The prior report ([final_comparison_report.md](../reports/final_comparison_report.md))
recommended a hybrid (current + completeness lens + cross_discipline lens +
extended critic + optional reviewer). But two open questions remained:

1. Is the multi-agent excess noise an **architectural property** (more LLMs →
   more hallucinations) or a **prompt-quality property** (the existing lens
   prompts are too permissive)?
2. Among the lens combinations actually worth running, **which prompts** should
   we use?

This sub-stand answers both.

## Constraints (hard rules)

- Production code is OFF LIMITS. We only ever read from production for context.
- All LLM calls go through `claude -p` subprocess on the Claude Code subscription.
- Models: only `claude-opus-4-7` and `claude-sonnet-4-6`.
- No OpenAI, no Gemini, no Ollama/VLLM, no external orchestrator APIs.
- All runners support `--skip-existing` and reuse cached outputs.
- All raw outputs go under `algorithm_research/logs/<runner>/<case>/<ts>.*`.

## Layout

```
algorithm_research/
├── README.md                          ← this file
├── baseline_analysis.md               ← what prior research already proved
├── hypotheses.md                      ← H1..H12 with expected evidence
├── algorithms/                        ← A0..A5 design specs
│   ├── A0_baseline_current.md
│   ├── A1_hybrid_lite.md
│   ├── A2_hybrid_cross_conditional.md
│   ├── A3_hybrid_critic_controlled.md
│   ├── A4_hybrid_production_candidate.md
│   └── A5_reduced_multi_agent.md
├── prompts/                           ← shared prompt fragments / templates
├── prompt_optimization/
│   ├── baseline_prompts/              ← copies of current parent prompts (frozen)
│   ├── optimized_prompts_v1/          ← Conservative Precision Prompts
│   ├── optimized_prompts_v2/          ← Balanced Engineering Prompts
│   ├── checklists/                    ← discipline completeness checklists
│   ├── prompt_diagnostics.md          ← what is wrong with baseline prompts
│   ├── prompt_diff.md                 ← v0 → v1 → v2 diffs
│   ├── prompt_ablation_results.md     ← empirical results
│   └── final_prompt_recommendations.md
├── runners/
│   ├── _common.py                     ← shared subprocess wrappers, cache logic
│   ├── algorithm_runner.py            ← generic A0..A5 dispatcher
│   ├── hybrid_lite_runner.py          ← A1
│   ├── hybrid_cross_runner.py         ← A2
│   ├── hybrid_critic_runner.py        ← A3
│   ├── hybrid_candidate_runner.py     ← A4
│   ├── reduced_multi_agent_runner.py  ← A5
│   ├── conditional_router.py          ← trigger-based cross_discipline router
│   └── class_dedup.py                 ← problem-class dedup
├── metrics/
│   ├── score_algorithms.py            ← unified scoring across 5 profiles
│   ├── dedup_quality.py
│   ├── noise_audit.py
│   ├── cost_model.py
│   └── discipline_matrix.py
├── results/<algorithm>/<case_id>.json ← raw algorithm outputs
├── reports/
│   ├── best_algorithm_report.md       ← final research report
│   └── final_verdict.md               ← one-page verdict
├── decision_matrix/
│   └── decision_matrix.md             ← engineering decision matrix
├── tests/
│   ├── test_class_dedup.py
│   ├── test_conditional_router.py
│   └── test_metrics_profiles.py
├── logs/<runner>/<case>/<ts>.*        ← raw stdout/stderr from claude -p
└── temp/                              ← scratch files for runners
```

## Reproducibility / commands

```bash
cd experiments/md_analysis_comparison/algorithm_research

# Run a single algorithm on a single case (cached by default):
python runners/algorithm_runner.py --algorithm A1 --case cross_01_eom_ov_loads

# Run all algorithms on all cases (skip what's already cached):
python runners/algorithm_runner.py --all --skip-existing

# Run a prompt ablation on selected cases:
python runners/algorithm_runner.py --algorithm A3 \
    --prompts baseline,v1,v2 --cases cross_01_eom_ov_loads ov_01_ventilation

# Score everything:
python metrics/score_algorithms.py
python metrics/discipline_matrix.py
python metrics/cost_model.py
```

## Stage outcomes (where to read what)

| Stage | Output file |
|---|---|
| Baseline interpretation | [baseline_analysis.md](baseline_analysis.md) |
| Hypotheses | [hypotheses.md](hypotheses.md) |
| Algorithm designs | [algorithms/](algorithms/) |
| Prompt issues | [prompt_optimization/prompt_diagnostics.md](prompt_optimization/prompt_diagnostics.md) |
| Optimized prompt v1 | [prompt_optimization/optimized_prompts_v1/](prompt_optimization/optimized_prompts_v1/) |
| Optimized prompt v2 | [prompt_optimization/optimized_prompts_v2/](prompt_optimization/optimized_prompts_v2/) |
| Class-level dedup spec | [runners/class_dedup.py](runners/class_dedup.py) |
| Conditional router spec | [runners/conditional_router.py](runners/conditional_router.py) |
| Prompt ablation | [prompt_optimization/prompt_ablation_results.md](prompt_optimization/prompt_ablation_results.md) |
| Final decision matrix | [decision_matrix/decision_matrix.md](decision_matrix/decision_matrix.md) |
| Final research report | [reports/best_algorithm_report.md](reports/best_algorithm_report.md) |
| Final verdict | [reports/final_verdict.md](reports/final_verdict.md) |
