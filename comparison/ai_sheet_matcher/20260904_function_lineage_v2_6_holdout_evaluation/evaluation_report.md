# Function Lineage v2.6 — independent holdout AI evaluation

Consented inference on frozen inputs. No deploy, no shadow, no materialization, no vision. The holdout sample shares no task with the v2.5 diagnostic set or the seven v2.4.2 controls.

## Consent

| Artifact | Consented SHA-256 | Observed |
|---|---|---|
| `holdout_population.json` | `95e640ed701acd564064d2310bbf6f3778eeff7f0a3dee6c2e8b6589a531754c` | MATCH |
| `holdout_sample.json` | `ab9cc617e9f2c8b38e632a3f72cff487c4060897ab50c8f1b1a22867bed6e589` | MATCH |
| `model_inputs.jsonl` | `c92b3fcc23dda3245a0f720127c26c9a05d870bb9fe5c38b0d3330a40201026d` | MATCH |

## Stability by corpus

| Corpus | Tasks | Stable 3/3 | 2/3 | 1/3 | 0/3 | Exact consistency | Stable NME | Pass disagreements |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| IOS1.1 | 12 | 4 | 4 | 2 | 2 | 4/12 (0.333333) | 1 | 3 |
| IOS2.1 | 12 | 4 | 4 | 3 | 1 | 4/12 (0.333333) | 2 | 3 |
| IOS3.1 | 12 | 8 | 1 | 3 | 0 | 8/12 (0.666667) | 2 | 1 |
| OVERALL | 36 | 16 | 9 | 8 | 3 | 16/36 (0.444444) | 5 | 7 |

## Stability by stratum

| Stratum | Tasks | Stable 3/3 | Exact consistency | Stable NME | Pass disagreements |
|---|---:|---:|---:|---:|---:|
| A | 7 | 6 | 0.857143 | 0 | 0 |
| B | 10 | 4 | 0.4 | 3 | 2 |
| C | 21 | 6 | 0.285714 | 2 | 6 |
| D | 10 | 5 | 0.5 | 3 | 1 |
| E | 0 | 0 | None | 0 | 0 |
| F | 10 | 5 | 0.5 | 3 | 1 |
| G | 21 | 6 | 0.285714 | 2 | 6 |
| H | 14 | 4 | 0.285714 | 2 | 3 |
| I | 8 | 3 | 0.375 | 1 | 2 |
| J | 11 | 5 | 0.454545 | 0 | 1 |
| K | 21 | 6 | 0.285714 | 2 | 6 |
| L | 25 | 10 | 0.4 | 4 | 6 |

## Stability by relation type

| Relation | Tasks | Stable 3/3 | Exact consistency | Stable NME |
|---|---:|---:|---:|---:|
| CONTINUED_1_TO_1 | 9 | 9 | 1.0 | 0 |
| SPLIT_1_TO_N | 0 | 0 | None | 0 |
| MERGED_N_TO_1 | 2 | 2 | 1.0 | 0 |
| FUNCTION_DISTRIBUTED | 0 | 0 | None | 0 |
| NEED_MORE_EVIDENCE | 5 | 5 | 1.0 | 5 |
| MIXED_RELATION | 10 | 0 | 0.0 | 0 |
| UNRESOLVED_OR_INVALID | 10 | 0 | 0.0 | 0 |

## Same-scope ambiguity

Ambiguous tasks `10`; stable 3/3 `4`; exact consistency `0.4`.

Stability is a model preference. It is not proof that the other eligible candidates are false.

## Reference classes

Authoritative determined rows `0`; alignment `None`.
Research-reference determined rows `5`; aligned `5`; rate `1.0` — hypothesis alignment, not precision.
DOCUMENT_LINK candidate occurrences `0`; used as functional truth: `NO`.

## Sentinels (reported separately)

Sentinel regression: **NO**.

| Sentinel | Expected v2.4.2 | Observed v2.6 | Status |
|---|---|---|---|
| LEFT17 | `lcand_cd6c87ed7f043a937b27` | `lcand_cd6c87ed7f043a937b27` | UNCHANGED |
| LEFT18 | `lcand_d9f1abdb7469869363ad` | `lcand_d9f1abdb7469869363ad` | UNCHANGED |
| LEFT19 | `lcand_26bcd544f168ff9ccea5` | `lcand_26bcd544f168ff9ccea5` | UNCHANGED |
| LEFT20 DOMESTIC | `lcand_1d1f175a30c34b88c6e0` | `lcand_1d1f175a30c34b88c6e0` | UNCHANGED |
| LEFT20 FIRE | `lcand_ebafe4012323c47ac349` | `lcand_ebafe4012323c47ac349` | UNCHANGED |
| LEFT20 METERING | `lcand_3e5e047c8b378f731c6b` | `lcand_3e5e047c8b378f731c6b` | UNCHANGED |
| LEFT20 PARENT | `lcand_9c617494b14c2b922d3f` | `lcand_9c617494b14c2b922d3f` | UNCHANGED |

## Safety

Unsupported accepted matches `0`; verifier rejection tasks `0`; capacity errors `12` (FUNCTION_FRAGMENT_CONFLICT `12`, RIGHT_MAP_CONFLICT `0`); technical failures `0`.

Every observed conflict was classified deterministically: true `12`, false `0`. Of the true ones, `1` assert incompatible merge arity onto one fragment and `11` have no candidate that could express the convergence at all — a representability gap, resolved fail-closed.

| Root cause | Conflicts |
|---|---:|
| `A_TRUE_FUNCTION_FRAGMENT_CONFLICT` | 12 |
| `B_HIERARCHICAL_DUPLICATE` | 0 |
| `B_LICENSED_EXACT_CHILD_UNION` | 0 |
| `C_TASK_DUPLICATION` | 0 |
| `D_FRAGMENTATION_DEFECT` | 0 |
| `E_CANDIDATE_DEFECT` | 0 |
| `F_CAPACITY_ACCOUNTING_DEFECT` | 0 |
| `G_UNKNOWN` | 0 |

## Reproducibility

Stable 3/3 `16`/`36` = `0.444444`; cross-cold exact consistency `0.444444`. Thresholds `0.9` / `0.85` (pre-existing v2.5 verdict-A thresholds).

Tasks publishing a decision without full unanimity: `0`.

### Where the instability comes from

| Cause | Tasks |
|---|---:|
| `CAPACITY_REJECTION_IN_SOME_REPEAT` | 17 |
| `PASS_DISAGREEMENT_IN_SOME_REPEAT` | 3 |
| `STABLE_3_OF_3` | 16 |

Published stable 3/3 `16`; the same tasks judged on their own A==B answer alone, ignoring any contest raised by another task, would be `21` (`0.583333`). `5` task(s) repeated one identical answer three times and published nothing only because another task claimed the same fragment.

That ceiling is still far below the threshold, so the reproducibility gap is a property of the bounded selector on independent hard tasks, not an artifact of capacity accounting. Changing the capacity stage could not close it.

## Cost

Planned / recorded / successful: `110` / `110` / `110`. Wall time `323401 ms`; model runtime `1098488 ms`. TELEMETRY_DEFECT: successful inference returned zero tokens; zero is not interpreted as zero cost.

## GO / NO-GO gates

| Gate | Result |
|---|---|
| `all_requests_successful` | PASS |
| `no_false_capacity_conflicts` | PASS |
| `no_page_global_capacity` | PASS |
| `no_technical_failures` | PASS |
| `right_map_conflict_zero` | PASS |
| `run_completed` | PASS |
| `sentinels_do_not_regress` | PASS |
| `strong_reproducibility` | FAIL |
| `unstable_tasks_publish_no_decision` | PASS |
| `unsupported_accepted_zero` | PASS |

All gates passed: **NO**.

**DO NOT DEPLOY. DO NOT ENABLE SHADOW.** Production authorization is a separate, explicit decision.
