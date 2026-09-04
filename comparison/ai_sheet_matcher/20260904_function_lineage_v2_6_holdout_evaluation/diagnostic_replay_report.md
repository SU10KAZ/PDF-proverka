# Function Lineage v2.6 — diagnostic replay of the consented holdout

**DIAGNOSTIC ONLY.** No model call was made (`model_calls = 0`). The sample has already been seen, so these numbers diagnose the Phase A architecture change and are never acceptance evidence.

Capacity stage: `POST_CONSENSUS_GLOBAL`. Run semantics: one cold repeat is one production-equivalent run.

## Effect of removing batch-dependent capacity

Stable 3/3 before `16`/`36`; after `20`/`36` (ceiling `0.555556`).

Recovered purely by the architecture fix: `4`; lost: `0`.
Still unstable `16`, of which model-level `13` and still capacity-contested `3`.

## Conflicts after the change

Distinct conflicts `3`; true `3`; false `0`.

| Root cause | Conflicts |
|---|---:|
| `A_TRUE_FUNCTION_FRAGMENT_CONFLICT` | 3 |
| `B_HIERARCHICAL_DUPLICATE` | 0 |
| `B_LICENSED_EXACT_CHILD_UNION` | 0 |
| `C_TASK_DUPLICATION` | 0 |
| `D_FRAGMENTATION_DEFECT` | 0 |
| `E_CANDIDATE_DEFECT` | 0 |
| `F_CAPACITY_ACCOUNTING_DEFECT` | 0 |
| `G_UNKNOWN` | 0 |

## Tasks whose outcome changed

| Task | Corpus | Candidates | Before | After |
|---|---|---:|---|---|
| `fstask_4f157f074b1933ff5791` | IOS1.1 | 10 | unstable (STABLE_MATCH,CAPACITY_REJECTION,STABLE_MATCH) | stable (STABLE_CLAIM,STABLE_CLAIM,STABLE_CLAIM) |
| `fstask_a3ed973e1b3de0f531a9` | IOS2.1 | 11 | unstable (STABLE_MATCH,CAPACITY_REJECTION,STABLE_MATCH) | stable (STABLE_CLAIM,STABLE_CLAIM,STABLE_CLAIM) |
| `fstask_b324a03b9b6fde243a7a` | IOS1.1 | 1 | unstable (STABLE_MATCH,CAPACITY_REJECTION,STABLE_MATCH) | stable (STABLE_CLAIM,STABLE_CLAIM,STABLE_CLAIM) |
| `fstask_d548be4d6014fc816459` | IOS3.1 | 3 | unstable (STABLE_MATCH,CAPACITY_REJECTION,CAPACITY_REJECTION) | stable (STABLE_CLAIM,STABLE_CLAIM,STABLE_CLAIM) |
