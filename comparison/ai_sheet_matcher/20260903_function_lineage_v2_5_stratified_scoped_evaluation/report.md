# Function Lineage v2.5 — stratified scoped corpus AI evaluation

Frozen v2.4 scope graph `6d2e7a5e4710765f0b5b8450c73c31431e070d13` and v2.4.1 transport `edcaea0b997330b744f2c479783b9c3ced5e29ae`; model `gpt-5.6-sol/low`.
No deploy, no shadow, no materialization, no vision. The seven IOS2.1 controls are reported only as sentinels and are excluded from headline metrics.

## Sample and corpus stability

| Corpus | Tasks | Stable 3/3 | Stable 2/3 | Stable 1/3 | Stable 0/3 | Exact cross-cold consistency | Stable NME | Disagreement repeats |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| IOS1.1 | 12 | 6 | 1 | 4 | 1 | 6/12 (0.5) | 3 | 4 |
| IOS2.1 | 12 | 9 | 0 | 1 | 2 | 9/12 (0.75) | 5 | 2 |
| IOS3.1 | 12 | 8 | 1 | 1 | 2 | 8/12 (0.666667) | 0 | 2 |
| **OVERALL** | **36** | **23** | **2** | **6** | **5** | **23/36 (0.638889)** | **8** | **8** |

Exact distribution is 12/12/12; redistribution was not required.

## Requested strata

| Stratum | Eligible new population | Selected | Stable 3/3 | Exact consistency | Stable NME | Pass disagreements |
|---|---:|---:|---:|---:|---:|---:|
| A | 35 | 7 | 7 | 1.0 | 0 | 0 |
| B | 55 | 17 | 9 | 0.529412 | 5 | 6 |
| C | 99 | 31 | 19 | 0.612903 | 6 | 8 |
| D | 67 | 3 | 2 | 0.666667 | 2 | 0 |
| E | 0 | 0 | 0 | None | 0 | 0 |
| F | 63 | 3 | 2 | 0.666667 | 2 | 0 |
| G | 99 | 31 | 19 | 0.612903 | 6 | 8 |
| H | 34 | 16 | 8 | 0.5 | 3 | 3 |
| I | 29 | 11 | 9 | 0.818182 | 2 | 0 |
| J | 36 | 7 | 5 | 0.714286 | 0 | 0 |
| K | 85 | 31 | 19 | 0.612903 | 6 | 8 |
| L | 134 | 31 | 19 | 0.612903 | 6 | 8 |

Stratum E has no NEW eligible task: all frozen FUNCTION_DISTRIBUTED candidates belong to the excluded LEFT20 PARENT sentinel. Its result appears only in the sentinel section.

## Stable results by relation type

| Relation | Tasks | Stable 3/3 | Exact consistency | Stable NME |
|---|---:|---:|---:|---:|
| CONTINUED_1_TO_1 | 14 | 14 | 1.0 | 0 |
| SPLIT_1_TO_N | 1 | 1 | 1.0 | 0 |
| MERGED_N_TO_1 | 0 | 0 | None | 0 |
| FUNCTION_DISTRIBUTED | 0 | 0 | None | 0 |
| NEED_MORE_EVIDENCE | 8 | 8 | 1.0 | 8 |
| MIXED_RELATION | 5 | 0 | 0.0 | 0 |
| UNRESOLVED_OR_INVALID | 8 | 0 | 0.0 | 0 |

## Same-scope ambiguity

Ambiguous tasks: `17`; stable 3/3: `9`; exact cross-cold consistency: `0.529412`.
Stability is only a model preference. It is not treated as proof that other eligible candidates are false.

| Task | Corpus | Distribution | Stable preference | Evidence signatures |
|---|---|---|---|---|
| `fstask_4d9574b20321aa38f23d` | IOS1.1 | `{"lcand_6e4eb8d958b2cf466faa": 2, "lcand_f432debb2bb406cbb096": 4}` | `None` | `{"15f2a48be7774d7e60534302382029d560611e1e9dfa097b68ca28cbd550f8c5": 1}` |
| `fstask_7e4e4b58356e4f914964` | IOS1.1 | `{"lcand_20554e7021714e365212": 4, "lcand_b3991a06e627f7db1a23": 2}` | `None` | `{"f4deac6aa28bbefacfdf2ef9c0f5165c5d2ec5e4adad2d9f96f867bd700ac5f2": 1}` |
| `fstask_06a839169f0d2c3d7816` | IOS1.1 | `{"NEED_MORE_EVIDENCE": 6}` | `NEED_MORE_EVIDENCE` | `{"NEED_MORE_EVIDENCE": 3}` |
| `fstask_b3e69e0c000d492b9a76` | IOS2.1 | `{"NEED_MORE_EVIDENCE": 6}` | `NEED_MORE_EVIDENCE` | `{"NEED_MORE_EVIDENCE": 3}` |
| `fstask_697410d55ee3093927ad` | IOS2.1 | `{"NEED_MORE_EVIDENCE": 2, "lcand_74ed239a14c9531423c2": 4}` | `None` | `{"8db8b61df661986ac63e853c7e65f17d03c11097314b922e2cc5cc5cc26b6032": 1}` |
| `fstask_f19c61a2e4d498be58ad` | IOS2.1 | `{"NEED_MORE_EVIDENCE": 6}` | `NEED_MORE_EVIDENCE` | `{"NEED_MORE_EVIDENCE": 3}` |
| `fstask_45d9665909f03e932943` | IOS2.1 | `{"NEED_MORE_EVIDENCE": 6}` | `NEED_MORE_EVIDENCE` | `{"NEED_MORE_EVIDENCE": 3}` |
| `fstask_5d50a0294eb6f569e768` | IOS2.1 | `{"lcand_18ca738ce8ff6250b6e4": 6}` | `None` | `{}` |
| `fstask_ef16fc80e9ae139b6942` | IOS2.1 | `{"NEED_MORE_EVIDENCE": 6}` | `NEED_MORE_EVIDENCE` | `{"NEED_MORE_EVIDENCE": 3}` |
| `fstask_1df7656d1f3d874ae15f` | IOS2.1 | `{"lcand_d15736a68bae9ca6553a": 6}` | `None` | `{}` |
| `fstask_83c8d15fdd36e01a4791` | IOS3.1 | `{"lcand_af7509a70f6cc44a928d": 6}` | `lcand_af7509a70f6cc44a928d` | `{"67fac28594c18de8a98fc061bd719f75a99002f9a8dc88e89b67baa58d9db429": 3}` |
| `fstask_5a5c988dc155556ac61f` | IOS3.1 | `{"lcand_8053bd149cba91ed9543": 6}` | `None` | `{}` |
| `fstask_0ae08ce1da6851ce56be` | IOS3.1 | `{"NEED_MORE_EVIDENCE": 4, "lcand_944a14849413357fb6d9": 2}` | `None` | `{"NEED_MORE_EVIDENCE": 1}` |
| `fstask_252ad33d332bede66a14` | IOS3.1 | `{"lcand_721f59bd1df11955bc2d": 6}` | `lcand_721f59bd1df11955bc2d` | `{"358b2c7aed88a2ae88ff0ca80748c9d55e3f4661efb442ab28ae7abff3a039ab": 3}` |
| `fstask_ec9c3f8cde24d5e191be` | IOS3.1 | `{"lcand_d663ff5d4eb140c74277": 6}` | `lcand_d663ff5d4eb140c74277` | `{"51c8926c7bce98f73a54245638cd54e43d17a13af5ee3113da81763bd66a3702": 3}` |
| `fstask_2ff8b94e8fd3ad57fa6f` | IOS3.1 | `{"NEED_MORE_EVIDENCE": 5, "lcand_5b0cbae4d1df63e10e96": 1}` | `None` | `{"NEED_MORE_EVIDENCE": 2}` |
| `fstask_863a24370ae95db4b997` | IOS3.1 | `{"lcand_a1b6b976bddcb5c0cac4": 6}` | `lcand_a1b6b976bddcb5c0cac4` | `{"b172b60f548fe1cd10b7f0e9e7de31dc05bd435df8ef63c86aa83e6e06f0f93d": 3}` |

## EXACT_CHILD_UNION

Outcome counts: `{"child incomplete": 2, "parent NME": 2}`. The normal model selector ran first; model bypass was never used.

| Corpus | Parent task | Candidate | Outcome | Deterministic union fields |
|---|---|---|---|---|
| IOS1.1 | `fstask_86975010b48a1807aec0` | `lcand_7f74c4622a69e484cd63` | parent NME | `{"capacity_keys": true, "right_fragment_ids": true, "right_function_ids": true, "right_physical_pages": true}` |
| IOS2.1 | `fstask_bbd391b7d2e8853fce21` | `lcand_39cf0ff0221b33018c92` | parent NME | `{"capacity_keys": true, "right_fragment_ids": true, "right_function_ids": true, "right_physical_pages": true}` |
| IOS3.1 | `fstask_8a95cf739271a7cf4303` | `lcand_17ea889165f00c8c3688` | child incomplete | `{"capacity_keys": true, "right_fragment_ids": true, "right_function_ids": true, "right_physical_pages": true}` |
| IOS3.1 | `fstask_8a95cf739271a7cf4303` | `lcand_dd31db9247928e362d4c` | child incomplete | `{"capacity_keys": true, "right_fragment_ids": true, "right_function_ids": true, "right_physical_pages": true}` |

## NON_DECOMPOSABLE_GROUP

Selected tasks `31`; stable 3/3 `19`; exact consistency `0.612903`.

## Reference classes

Authoritative functional references: `0`; alignment is `N/A` because the frozen corpus contains no genuinely authoritative functional mapping.
Research-reference determined rows `7`; aligned `6`; rate `0.857143`. This is hypothesis alignment, not precision.
DOCUMENT_LINK candidate occurrences `38`; used as functional truth: `NO`. NO_REFERENCE tasks `26`.

## Sentinels (excluded from headline)

Sentinel regression: **NO**.

| Sentinel | Expected v2.4.2 | Observed v2.5 | Status |
|---|---|---|---|
| LEFT17 | `lcand_cd6c87ed7f043a937b27` | `lcand_cd6c87ed7f043a937b27` | UNCHANGED |
| LEFT18 | `lcand_d9f1abdb7469869363ad` | `lcand_d9f1abdb7469869363ad` | UNCHANGED |
| LEFT19 | `lcand_26bcd544f168ff9ccea5` | `lcand_26bcd544f168ff9ccea5` | UNCHANGED |
| LEFT20 DOMESTIC | `lcand_1d1f175a30c34b88c6e0` | `lcand_1d1f175a30c34b88c6e0` | UNCHANGED |
| LEFT20 FIRE | `lcand_ebafe4012323c47ac349` | `lcand_ebafe4012323c47ac349` | UNCHANGED |
| LEFT20 METERING | `lcand_3e5e047c8b378f731c6b` | `lcand_3e5e047c8b378f731c6b` | UNCHANGED |
| LEFT20 PARENT | `lcand_9c617494b14c2b922d3f` | `lcand_9c617494b14c2b922d3f` | UNCHANGED |

## Safety and technical quality

Unsupported accepted matches `0`; verifier rejects `0`; capacity defects `9`; RIGHT_MAP_CONFLICT `0`; FUNCTION_FRAGMENT_CONFLICT `9`.
Cross-granularity selectable competition before/after: `0` / `0`. Raw candidates preserved: `True`.
Model/schema failure tasks `0`; stable NME `8`; PASS_DISAGREEMENT repeats `8`.

## Cost and runtime

Planned / attempted / successful requests: `110` / `110` / `110`.
Model runtime `1064002 ms`; wall time `304343 ms`; shards `21`; average tasks/request `2.090909`.
Prompt characters median/p95/max: `220912.0` / `249554` / `249554`.
Input/output/total tokens: `0` / `0` / `0`. TELEMETRY_DEFECT: successful inference returned usage={} / zero tokens; zero is not interpreted as zero cost.

## Verdict

**D — scope/candidate/verifier/capacity architecture has a new safety defect.**

Ready to prepare a production integration candidate for shadow-only validation: `NO`.

Even if verdict A: **DO NOT DEPLOY. DO NOT ENABLE SHADOW.**
