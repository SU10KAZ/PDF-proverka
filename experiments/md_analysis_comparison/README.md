# MD-analysis comparison stand

Isolated test stand for comparing two approaches to auditing Markdown
design documentation:

1. **Current method** — single-pass Opus on full MD (mirrors AuditManager Stage 01).
2. **Multi-agent method** — N parallel specialist Sonnet agents (normative, calculations, contradictions, completeness, cross-discipline, safety) → Opus critic → Opus reviewer → Python dedup safety-net.

**All LLM calls go through `claude -p` subprocess on the Claude Code subscription.**
No OpenAI, no Gemini, no local LLMs, no other paid LLM APIs.

## Constraints

- This folder is fully isolated. Nothing here modifies production code in `backend/`, `webapp/`, `frontend/`, or `process_project.py`.
- All temp files stay under `temp/` and `logs/`.
- Production output directories (`projects/<name>/_output/`) are NEVER touched.

## Layout

```
experiments/md_analysis_comparison/
├── README.md                        ← you are here
├── configs/
│   └── config.py                    ← paths, model IDs, Claude CLI lookup
├── runners/
│   ├── _common.py                   ← `claude -p` subprocess helper
│   ├── unified_output_schema.py     ← Finding / RunResult dataclasses
│   ├── current_method_runner.py     ← single-pass Opus
│   └── multi_agent_method_runner.py ← 6 Sonnet agents + Opus critic + Opus reviewer
├── prompts/
│   ├── system/00_base.md            ← rules for every multi-agent agent
│   ├── current_method/text_analysis_task.md
│   ├── agents/                      ← normative, calculations, contradictions,
│   │                                  completeness, cross_discipline, safety
│   ├── critic/critic_task.md
│   └── reviewer/final_review_task.md
├── datasets/<case_id>/
│   ├── case.json                    ← id, discipline, md_file
│   ├── input.md                     ← MD to audit (Russian RD fragment)
│   └── ground_truth.json            ← expected findings, traps, hidden_contradictions
├── results/<case_id>/
│   ├── current.json                 ← current_method_runner output
│   └── multi_agent.json             ← multi_agent_method_runner output
├── comparison_outputs/
│   ├── per_case.json                ← detail metrics
│   ├── summary.json                 ← method aggregates
│   └── table.md                     ← final comparison table
├── scripts/
│   ├── compare_results.py           ← all 15 metrics
│   └── run_all.py                   ← orchestrator
├── reports/
│   └── final_comparison_report.md   ← architectural + empirical comparison
├── tests/
│   ├── test_schema.py
│   ├── test_compare.py
│   └── test_dataset_integrity.py
├── prompts/, logs/, temp/
└── comparison_outputs/
```

## Quick start

```bash
cd experiments/md_analysis_comparison

# unit tests (no Claude needed)
python tests/test_schema.py
python tests/test_compare.py
python tests/test_dataset_integrity.py

# one case, one method
python runners/current_method_runner.py --case eom_01_cable_sizing
python runners/multi_agent_method_runner.py --case eom_01_cable_sizing

# one case, multi-agent with subset of agents
python runners/multi_agent_method_runner.py --case eom_01_cable_sizing \
    --agents normative contradictions calculations

# all cases, both methods, then compare
python scripts/run_all.py --parallel-cases 2

# specific cases, skip existing results
python scripts/run_all.py --only eom_01_cable_sizing ov_01_ventilation --skip-existing

# only compare (results already computed)
python scripts/compare_results.py
```

## Dataset (8 cases)

| case_id | discipline | finding traits |
|---|---|---|
| eom_01_cable_sizing | ЭОМ | calc + normative + safety + contradiction |
| ov_01_ventilation | ОВ | calc + normative + cross-discipline + safety |
| vk_01_water_flow | ВК | calc + normative + cross-discipline (АПС) |
| ar_01_evacuation | АР | safety + normative + cross-discipline (ОВ) |
| kj_01_rebar | КЖ | calc + normative + hidden contradiction |
| ss_01_cabling | СС | normative + completeness + cross-discipline |
| multi_01_tz_vs_rd | MULTI | ТЗ vs РД (3 contradictions) |
| cross_01_eom_ov_loads | EOM↔OV | cross-discipline (load mismatch) |

Each case carries 6–8 expected findings, with ≥1 trap (false-positive bait),
≥1 hidden contradiction, and ≥1 cross-discipline issue.

To grow to 20 cases, create new folders under `datasets/<case_id>/` with the
same three files. `run_all.py` and `compare_results.py` auto-discover them.

## Models

| Stage | Model | Why |
|---|---|---|
| current_method (one call) | claude-opus-4-7 | Mirrors AuditManager Stage 01 |
| multi-agent / agents | claude-sonnet-4-6 | Bulk parallel work |
| multi-agent / critic | claude-opus-4-7 | Deep grounding judgement |
| multi-agent / reviewer | claude-opus-4-7 | Final synthesis |

Override via env: `EXP_MODEL_OPUS=...`, `EXP_MODEL_SONNET=...`.

## Metrics (compare_results.py)

For every (case × method):

1. total findings
2. matched-to-ground-truth (recall)
3. missed GT, **missed critical GT** (hard penalty)
4. false positives (incl. traps triggered)
5. internal duplicates
6. avg `norm_confidence`
7. avg `confidence`
8. fraction of findings with evidence quote
9. severity distribution
10. cross-discipline findings caught
11. hidden contradictions caught
12. noise score (FP / total)
13. weighted-recall score (severity- and criticality-weighted)
14. final score (recall − FP penalty − dupes penalty − missed-critical penalty)

Aggregate per method goes to `summary.json`. Per-case head-to-head goes
to `table.md`.

## Cost note

A single `claude -p` Opus call on a small MD takes ~2–4 minutes. Multi-agent
adds 6 parallel Sonnet calls + 2 Opus calls. For 8 cases × both methods,
plan for ~1–2 hours of compute, fully on the Claude Code subscription.
