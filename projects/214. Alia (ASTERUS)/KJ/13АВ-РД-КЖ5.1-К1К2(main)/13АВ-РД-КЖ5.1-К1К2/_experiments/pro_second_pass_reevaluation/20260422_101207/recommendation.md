# Pro Second-Pass Re-evaluation (offline, probe-informed)

## Scope

- No new API calls.
- Reused the original recall-hybrid run, Pro diagnostic, and baseline failmode probe.
- Question: can Gemini 3.1 Pro return to the candidate set for recall-oriented second pass on the small KJ project?

## Baseline anchors

- Observed Pro baseline: coverage 88.24%, missing 2, total findings 29, cost $0.8846.
- Claude second pass: coverage 100.00%, degraded 1, total findings 64, cost $0.5446.
- Pro low reasoning + heal ON: coverage 100.00%, total findings 14, cost $0.1810.
- Pro low reasoning + heal OFF: coverage 100.00%, total findings 13, cost $0.1785.

## Probe stability on the 2 flaky baseline blocks

- `4MQJ-6NXP-4YH`: successful high-reasoning observations 10 across modes A, B; findings distribution {3: 3, 2: 5, 4: 1, 1: 1}.
- `6DRC-7KQL-9TJ`: successful high-reasoning observations 10 across modes A, B; findings distribution {3: 2, 1: 5, 2: 3}.

## Probe-informed counterfactuals for baseline-style Pro (mode A = high reasoning + heal ON)

| Scenario | Coverage | Improved | Degraded | Added findings | Total findings | Approx cost USD | Winner vs Claude |
|----------|----------|----------|----------|----------------|----------------|-----------------|------------------|
| Pro high reasoning counterfactual (conservative) | 100.00% | 11 | 5 | +23 | 32 | $0.9495 | Claude (claude-opus-4-7) |
| Pro high reasoning counterfactual (median) | 100.00% | 11 | 5 | +23 | 32 | $1.0207 | Claude (claude-opus-4-7) |
| Pro high reasoning counterfactual (optimistic) | 100.00% | 12 | 4 | +25 | 35 | $0.9724 | Claude (claude-opus-4-7) |

## Recommendation

- Final call: `claude_still_best_small_kj`.
- Pro should stay in the candidate pool only in this config:
  - model: `google/gemini-3.1-pro-preview`
  - reasoning.effort: `high`
  - healing: `True`
  - parallelism: `2`
  - success rule: `len(block_analyses)==1 and block_id matches input`
  - empty-result retry: `True`

Why Pro remains a candidate:
- The original 2/17 missing baseline blocks were fully reproduced as successes in the confirmatory high-reasoning probe.
- Low reasoning fixed completeness but materially reduced findings and is not recall-safe.
- Healing OFF was not the differentiator; the stable signal was high reasoning on the flaky blocks.

Why Claude still wins on this small KJ benchmark:
- Median probe-informed Pro still trails Claude on findings (32 vs 64).
- Even optimistic probe-informed Pro trails Claude on added findings (+25 vs +46).
- Low-reasoning Pro variants underperform both median counterfactual Pro and Claude (14 / 13 findings).

Next step if a final confirmation is still needed:
- If Pro needs a final admission test, run only one narrow 17-block high-reasoning second-pass rerun with the strict single-block success rule and the current retry guardrails. Do not use low reasoning for recall-oriented second pass.

## Caveat
- Counterfactual quality metrics are exact only for the replaced probe blocks and the unchanged baseline blocks.
- Counterfactual cost is approximate: baseline estimated-cost placeholders were replaced with actual probe call costs.
- Counterfactual elapsed/token totals were intentionally left anchored to the observed baseline run to avoid false precision.
