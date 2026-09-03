# Function Lineage v2.6 — deterministic full-corpus regression

No model calls, no production state, no shadow, no materialization.

## Gates

| Gate | Result |
|---|---|
| `candidate_generation_byte_identical` | PASS |
| `candidate_partition_defects_zero` | PASS |
| `cross_granularity_competition_zero` | PASS |
| `false_capacity_conflicts_zero` | PASS |
| `model_calls_zero` | PASS |
| `no_new_conflicts_introduced` | PASS |
| `page_global_exclusivity_absent` | PASS |
| `raw_candidates_preserved` | PASS |
| `recall_unchanged` | PASS |
| `right_map_conflict_zero` | PASS |
| `scope_graph_deterministic` | PASS |
| `scope_safety_matches_frozen_baseline` | PASS |
| `scoped_transport_deterministic` | PASS |
| `search_failures_zero` | PASS |
| `sentinel_inputs_unchanged` | PASS |
| `true_conflicts_still_rejected` | PASS |
| `unknown_scope_fail_closed` | PASS |

All gates passed: **YES**.

## Recall baselines

Cases `19`; unchanged: `True`.

| Metric | @1 | @3 | @5 | @10 |
|---|---:|---:|---:|---:|
| raw_candidate_recall | 0.578947 | 0.684211 | 0.842105 | 0.947368 |
| scope_eligible_recall | 0.789474 | 0.842105 | 0.894737 | 0.947368 |

## Capacity replay of the frozen v2.5 selections

Model batches replayed `20`; conflicts before `9`; after `9`.
True conflicts still rejected `9`/`9`; false conflicts after `0`; newly introduced `0`.

The replay applies capacity to every task pair. The v2.5 harness skipped pairs whose source scopes intersected, so this run is strictly stricter, not looser.

## Population capacity sweep

Reachable collisions `17265`; rejected `16792`; licensed `473`.

| Licence | Count |
|---|---:|
| `DERIVED_COMPOSITE_OWNERSHIP` | 340 |
| `DERIVED_EXACT_CHILD_UNION` | 117 |
| `SAME_ATOMIC_OWNERSHIP` | 16 |

## Sentinels

Regression references only — never a mapping rule.

| Sentinel | Scope matches | Reference selectable | In transport | Candidates |
|---|---|---|---|---:|
| LEFT17 | True | True | True | 10 |
| LEFT18 | True | True | True | 4 |
| LEFT19 | True | True | True | 11 |
| LEFT20 DOMESTIC | True | True | True | 9 |
| LEFT20 FIRE | True | True | True | 9 |
| LEFT20 METERING | True | True | True | 9 |
| LEFT20 PARENT | True | True | True | 3 |
