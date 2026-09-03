# Function Lineage v2.6 — capacity conflict forensics (Phase 1)

Deterministic replay of the frozen v2.5 stratified evaluation. No model calls, no production state, no shadow.

## Observed v2.5 conflicts

Unique conflicts `9`; observation repeats `22`; true `9`; false `0`.

| Root cause | Unique | Repeats |
|---|---:|---:|
| `A_TRUE_FUNCTION_FRAGMENT_CONFLICT` | 9 | 22 |
| `B_HIERARCHICAL_DUPLICATE` | 0 | 0 |
| `B_LICENSED_EXACT_CHILD_UNION` | 0 | 0 |
| `C_TASK_DUPLICATION` | 0 | 0 |
| `D_FRAGMENTATION_DEFECT` | 0 | 0 |
| `E_CANDIDATE_DEFECT` | 0 | 0 |
| `F_CAPACITY_ACCOUNTING_DEFECT` | 0 | 0 |
| `G_UNKNOWN` | 0 | 0 |

### Reconstructed conflicts

| Corpus | Capacity key | Claim A | Claim B | Scope relation | Candidate relation | Class |
|---|---|---|---|---|---|---|
| IOS1.1 | `RIGHT:21:frag_dcbcc7c1908e0da0e5d5` | `lcand_b3991a06e627f7db1a23` (fstask_7e4e4b58356e4f914964) | `lcand_c92124143ea0c7d53bfc` (fstask_df744d02a942d8e7d21a) | UNRELATED | true incompatible reuse | `A_TRUE_FUNCTION_FRAGMENT_CONFLICT` |
| IOS1.1 | `RIGHT:22:frag_5f3ecfecf91bfc8ec296` | `lcand_b3991a06e627f7db1a23` (fstask_7e4e4b58356e4f914964) | `lcand_c92124143ea0c7d53bfc` (fstask_df744d02a942d8e7d21a) | UNRELATED | true incompatible reuse | `A_TRUE_FUNCTION_FRAGMENT_CONFLICT` |
| IOS1.1 | `RIGHT:23:frag_7f1231a2b6d8b6b825b2` | `lcand_b3991a06e627f7db1a23` (fstask_7e4e4b58356e4f914964) | `lcand_c92124143ea0c7d53bfc` (fstask_df744d02a942d8e7d21a) | UNRELATED | true incompatible reuse | `A_TRUE_FUNCTION_FRAGMENT_CONFLICT` |
| IOS1.1 | `RIGHT:31:frag_d32528e9a56846d4b410` | `lcand_cffa286418ad3a7abac4` (fstask_9731937fb58229e8643c) | `lcand_7bc850b75a2d7210f7fb` (fstask_a23d1bcd6ec923c52382) | UNRELATED | true incompatible reuse | `A_TRUE_FUNCTION_FRAGMENT_CONFLICT` |
| IOS1.1 | `RIGHT:31:frag_d32528e9a56846d4b410` | `lcand_f192ade4e03bdb685b07` (fstask_9731937fb58229e8643c) | `lcand_7bc850b75a2d7210f7fb` (fstask_a23d1bcd6ec923c52382) | UNRELATED | true incompatible reuse | `A_TRUE_FUNCTION_FRAGMENT_CONFLICT` |
| IOS3.1 | `RIGHT:13:frag_9d3c2eb5c2fd4e6351da` | `lcand_5b0cbae4d1df63e10e96` (fstask_2ff8b94e8fd3ad57fa6f) | `lcand_17ea889165f00c8c3688` (fstask_8a95cf739271a7cf4303) | UNRELATED | true incompatible reuse | `A_TRUE_FUNCTION_FRAGMENT_CONFLICT` |
| IOS3.1 | `RIGHT:13:frag_9d3c2eb5c2fd4e6351da` | `lcand_5b0cbae4d1df63e10e96` (fstask_2ff8b94e8fd3ad57fa6f) | `lcand_8053bd149cba91ed9543` (fstask_5a5c988dc155556ac61f) | UNRELATED | true incompatible reuse | `A_TRUE_FUNCTION_FRAGMENT_CONFLICT` |
| IOS3.1 | `RIGHT:13:frag_9d3c2eb5c2fd4e6351da` | `lcand_8053bd149cba91ed9543` (fstask_5a5c988dc155556ac61f) | `lcand_17ea889165f00c8c3688` (fstask_8a95cf739271a7cf4303) | UNRELATED | true incompatible reuse | `A_TRUE_FUNCTION_FRAGMENT_CONFLICT` |
| IOS2.1 | `RIGHT:47:frag_32c9267326e8332879fe` | `lcand_d15736a68bae9ca6553a` (fstask_1df7656d1f3d874ae15f) | `lcand_18ca738ce8ff6250b6e4` (fstask_5d50a0294eb6f569e768) | UNRELATED | true incompatible reuse | `A_TRUE_FUNCTION_FRAGMENT_CONFLICT` |

## Latent conflict surface (whole 213-task scoped population)

Scoped task pairs examined `8314`; pairs with a reachable capacity collision `2756`; classified collisions `17265`.

A reachable collision is a pair of candidates two scoped tasks *may* both select. `A_TRUE_FUNCTION_FRAGMENT_CONFLICT` therefore measures the surface the verifier must guard, not a defect count. Every other class is a claim pair the current accounting would reject although the frozen deterministic evidence proves the two claims are one composed mapping.

| Root cause | Reachable collisions |
|---|---:|
| `A_TRUE_FUNCTION_FRAGMENT_CONFLICT` | 16746 |
| `B_HIERARCHICAL_DUPLICATE` | 251 |
| `B_LICENSED_EXACT_CHILD_UNION` | 117 |
| `C_TASK_DUPLICATION` | 0 |
| `D_FRAGMENTATION_DEFECT` | 151 |
| `E_CANDIDATE_DEFECT` | 0 |
| `F_CAPACITY_ACCOUNTING_DEFECT` | 0 |
| `G_UNKNOWN` | 0 |

| Scope relation | `A_TRUE_FUNCTION_FRAGMENT_CONFLICT` | `B_HIERARCHICAL_DUPLICATE` | `B_LICENSED_EXACT_CHILD_UNION` | `C_TASK_DUPLICATION` | `D_FRAGMENTATION_DEFECT` | `E_CANDIDATE_DEFECT` | `F_CAPACITY_ACCOUNTING_DEFECT` | `G_UNKNOWN` |
|---|---|---|---|---|---|---|---|---|
| SAME_SCOPE | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| PARENT_CHILD | 0 | 251 | 0 | 0 | 102 | 0 | 0 | 0 |
| OVERLAP | 0 | 0 | 0 | 0 | 49 | 0 | 0 | 0 |
| UNRELATED | 16746 | 0 | 117 | 0 | 0 | 0 | 0 | 0 |
| UNKNOWN | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |

## EXACT_CHILD_UNION exposure

Certified groups `120`; groups whose unrelated sibling children can collide on capacity `117`; sibling pairs `117`.

## Ranked root causes

1. **LATENT_FALSE_CONFLICT_BETWEEN_CERTIFIED_EXACT_UNION_SIBLINGS** (`B_LICENSED_EXACT_CHILD_UNION`) — observed `0`, latent `117`.
   Two atomic child lineages of one certified EXACT_CHILD_UNION are co-owners of one composed mapping, but capacity accounting keys on candidate_id, so their compatible claims are rejected as a conflict.
2. **PARENT_CHILD_DOUBLE_CONSUMPTION_HANDLED_ONLY_BY_A_HARNESS_HEURISTIC** (`B_HIERARCHICAL_DUPLICATE`) — observed `0`, latent `251`.
   The v2.5 harness skipped every task pair with intersecting source components, so parent/child double consumption never surfaced; the production verifier has no equivalent rule.
3. **TRUE_CONFLICTS_SCORED_AS_AN_ARCHITECTURE_SAFETY_DEFECT** (`A_TRUE_FUNCTION_FRAGMENT_CONFLICT`) — observed `9`, latent `16746`.
   The verifier correctly rejected mutually exclusive claims, but the verdict rule counts any conflict as a defect, so correct fail-closed behaviour produced verdict D.
