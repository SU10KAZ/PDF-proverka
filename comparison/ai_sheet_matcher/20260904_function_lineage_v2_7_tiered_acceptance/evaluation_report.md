# Function Lineage v2.7 — tiered acceptance evaluation

Consented inference on frozen inputs. No deploy, no shadow, no materialization, no vision. Capacity is resolved once, globally, after two-pass consensus.

## Consent

| Artifact | Consented SHA-256 | Observed |
|---|---|---|
| `acceptance_population.json` | `340da5a123db4d74df6b37b05b3b66931df90afea3268b658cc1c704a005615e` | MATCH |
| `acceptance_sample.json` | `f1e900b6068337196e632b018287e6495b429879e6faf840aa27bf05dce0d23f` | MATCH |
| `model_inputs.jsonl` | `173d2be7a3b2fe291263edc8bc786d89887a39fd92ac156b4d45d1241243dfd9` | MATCH |

## Tier results (primary view: capacity resolved per tier)

| Tier | Decides GO | Tasks | Reproducible 3/3 | Rate | Auto matches | Stable NME | Unstable |
|---|---|---:|---:|---:|---:|---:|---:|
| `AUTO_ONE_TO_ONE` | YES | 33 | 22 | 0.666667 | 14 | 2 | 11 |
| `AUTO_MERGED` | YES | 54 | 46 | 0.851852 | 8 | 18 | 8 |
| `HARD_DIAGNOSTIC` | no | 12 | 9 | 0.75 | 4 | 4 | 3 |

## Secondary view (capacity across every tier, production-like)

| Tier | Reproducible 3/3 | Rate | Auto matches |
|---|---:|---:|---:|
| `AUTO_ONE_TO_ONE` | 23 | 0.69697 | 14 |
| `AUTO_MERGED` | 46 | 0.851852 | 8 |
| `HARD_DIAGNOSTIC` | 9 | 0.75 | 2 |

## Safety

Unsupported accepted `0`; verifier rejection tasks `0`; technical failures `0`; RIGHT_MAP_CONFLICT `0`.
Capacity conflicts `29` — true `29`, false `0`.
Permutation invariance: `22` groups × `25` shuffles, changes `0`.

## Sentinels

Sentinel regression: **YES**.

| Sentinel | Expected v2.4.2 | Observed v2.7 | Status |
|---|---|---|---|
| LEFT17 | `lcand_cd6c87ed7f043a937b27` | `lcand_cd6c87ed7f043a937b27` | UNCHANGED |
| LEFT18 | `lcand_d9f1abdb7469869363ad` | `lcand_d9f1abdb7469869363ad` | UNCHANGED |
| LEFT19 | `lcand_26bcd544f168ff9ccea5` | `lcand_26bcd544f168ff9ccea5` | UNCHANGED |
| LEFT20 DOMESTIC | `lcand_1d1f175a30c34b88c6e0` | `lcand_1d1f175a30c34b88c6e0` | UNCHANGED |
| LEFT20 FIRE | `lcand_ebafe4012323c47ac349` | `lcand_ebafe4012323c47ac349` | UNCHANGED |
| LEFT20 METERING | `lcand_3e5e047c8b378f731c6b` | `PASS_DISAGREEMENT` | CHANGED |
| LEFT20 PARENT | `lcand_9c617494b14c2b922d3f` | `lcand_9c617494b14c2b922d3f` | UNCHANGED |

## GO / NO-GO

### `AUTO_ONE_TO_ONE` — FAIL

| Gate | Result |
|---|---|
| `all_requests_successful` | PASS |
| `batch_permutation_changes_zero` | PASS |
| `cross_cold_consistency_threshold` | FAIL |
| `experiment_valid` | PASS |
| `false_capacity_conflicts_zero` | PASS |
| `no_technical_failures` | PASS |
| `produces_at_least_one_auto_match` | PASS |
| `right_map_conflict_zero` | PASS |
| `sentinels_do_not_regress` | FAIL |
| `stable_3_of_3_threshold` | FAIL |
| `unsupported_accepted_zero` | PASS |

### `AUTO_MERGED` — FAIL

| Gate | Result |
|---|---|
| `all_requests_successful` | PASS |
| `batch_permutation_changes_zero` | PASS |
| `cross_cold_consistency_threshold` | PASS |
| `experiment_valid` | PASS |
| `false_capacity_conflicts_zero` | PASS |
| `no_technical_failures` | PASS |
| `produces_at_least_one_auto_match` | PASS |
| `right_map_conflict_zero` | PASS |
| `sentinels_do_not_regress` | FAIL |
| `stable_3_of_3_threshold` | FAIL |
| `unsupported_accepted_zero` | PASS |

Relations earning automatic publication: `none`.

**Verdict: NOT_READY.**

The HARD set never decides the product question; it is reported for diagnosis only.

**DO NOT DEPLOY. DO NOT ENABLE SHADOW.** Production authorization is a separate, explicit decision.
