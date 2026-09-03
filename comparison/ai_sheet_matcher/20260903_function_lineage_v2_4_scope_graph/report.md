# Function Lineage v2.4 — deterministic Function Scope Graph

## Execution boundary

- Frozen candidate/evaluation record: `94eb48b8`; V2.3 parent: `ef186278`.
- Production reference only: `4d489bf9033ad40c40099fe5e1436493bc56c0ed` / `ui-real-4d489bf9`.
- New model calls: `0`; vision: `0`; candidate regeneration: `0`; selector prompt changes: `0`.
- Deploy, shadow wiring, materialization, and production matcher changes: not performed.

## LEFT20 control

Parent composite scope: `fscope_d1faafb9db1c9aca8074` with roles `DOMESTIC_PRESSURE_BOOST, FIRE_PRESSURE_BOOST, INCOMING_METERING`.

| Candidate | Child scope | Relation to child | Relation to parent | Child eligible | Parent eligible |
|---|---|---|---|---:|---:|
| R26 `lcand_1d1f175a30c34b88c6e0` | `fscope_90a63adbb11d34d61f4b` (DOMESTIC_PRESSURE_BOOST) | `EXACT_SCOPE` | `STRICT_SUBSET` | `True` | `False` |
| R28 `lcand_ebafe4012323c47ac349` | `fscope_472f43e47a98f8cb7b35` (FIRE_PRESSURE_BOOST) | `EXACT_SCOPE` | `STRICT_SUBSET` | `True` | `False` |
| R29 `lcand_3e5e047c8b378f731c6b` | `fscope_2bb6cd1e14a1c59c591e` (INCOMING_METERING) | `EXACT_SCOPE` | `STRICT_SUBSET` | `True` | `False` |

Distributed `[26,28,29]` `lcand_9c617494b14c2b922d3f` is `EXACT_SCOPE` for the parent and `EXACT_CHILD_UNION`. Exact child union: `YES`.

Composite lineage can be derived **SOMETIMES**: only when every required source child has an independently complete 1→1 mapping, their exact mapping union equals the group, and exact fragment capacity keys are preserved. SPLIT 1→N is not mistaken for a composite merely because it has several RIGHT targets.

## LEFT19 negative control

R30 `lcand_26bcd544f168ff9ccea5` and R25 `lcand_c725393a11cb3b17ed2d` have the same scope: `YES`. Both remain selector-eligible: `True`. Ambiguity remains: `YES`. The frozen 6/6 preference is context only, never deterministic truth.

## Corpus metrics

| Corpus | Component scopes | Composite scopes | Unknown scopes | Parent/child | EXACT | SUBSET | SUPERSET | OVERLAP | UNKNOWN |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| IOS1.1 | 61 | 33 | 0 | 66 | 599 | 453 | 46 | 35 | 0 |
| IOS2.1 | 58 | 23 | 0 | 47 | 644 | 327 | 48 | 37 | 0 |
| IOS3.1 | 26 | 12 | 0 | 24 | 218 | 90 | 34 | 30 | 0 |
| **Total** | **145** | **68** | **0** | **137** | **1461** | **870** | **128** | **102** | **0** |

Selector tasks before/after: `145` / `213`. Cross-granularity task competitions before/after: `99` / `0`; candidate-pair competitions: `1650` / `0`.

| Corpus | Tasks before | Tasks after | Cross-scope tasks before | Cross-scope tasks after |
|---|---:|---:|---:|---:|
| IOS1.1 | 61 | 94 | 48 | 0 |
| IOS2.1 | 58 | 81 | 35 | 0 |
| IOS3.1 | 26 | 38 | 16 | 0 |

## Recall and safety

| Metric | R@1 | R@3 | R@5 | R@10 |
|---|---:|---:|---:|---:|
| RAW CANDIDATE RECALL | 0.578947 | 0.684211 | 0.842105 | 0.947368 |
| SCOPE-ELIGIBLE RECALL | 0.789474 | 0.842105 | 0.894737 | 0.947368 |

| Corpus / metric | R@1 | R@3 | R@5 | R@10 |
|---|---:|---:|---:|---:|
| IOS1.1 RAW | 0.166667 | 0.5 | 0.833333 | 1.0 |
| IOS1.1 SCOPE-ELIGIBLE | 0.833333 | 0.833333 | 1.0 | 1.0 |
| IOS2.1 RAW | 0.75 | 0.75 | 0.875 | 0.875 |
| IOS2.1 SCOPE-ELIGIBLE | 0.75 | 0.875 | 0.875 | 0.875 |
| IOS3.1 RAW | 0.8 | 0.8 | 0.8 | 1.0 |
| IOS3.1 SCOPE-ELIGIBLE | 0.8 | 0.8 | 0.8 | 1.0 |

Scope-eligible recall is a separate filtered-task diagnostic, not a claim that recall improved by deleting alternatives. All frozen candidates and evidence references remain persisted; candidate loss is `0`.

Group derivability counts: EXACT_CHILD_UNION `120`, NON_DECOMPOSABLE_GROUP `311`, PARTIAL_CHILD_UNION `8`, UNKNOWN `0`.

| Corpus | EXACT_CHILD_UNION | NON_DECOMPOSABLE_GROUP | PARTIAL_CHILD_UNION | UNKNOWN |
|---|---:|---:|---:|---:|
| IOS1.1 | 43 | 139 | 3 | 0 |
| IOS2.1 | 43 | 138 | 5 | 0 |
| IOS3.1 | 34 | 34 | 0 | 0 |

Search failures: `0`. Frozen group-generation failures: `2`. Capacity-key defects: `0`. RIGHT_MAP_CONFLICT: `0`. Capacity remains RIGHT physical page + exact function fragment; page-global exclusivity is `False`.

## Deterministic replay

Two independent in-process builds are byte-identical before write.
- `candidate_scope_membership.json`: `a28cf9700151b98e7a5b1206dee08b3f9f17c59037510f57861736de9a1b0dba`
- `function_scope_graph.json`: `f86c2911d8ac8c3d65295d3f5c43bb9befb53c7ca99716e2979f9ae2574aec29`
- `group_derivability_audit.json`: `9bd14758895dca67f1397f56f8149f94362fc6c2a70c0cbe54f863c4eca10812`
- `ios21_scope_forensics.json`: `57e6456dba5d3166083b7f2980f9fe94fb5c9467a198c62658f821f038f52d1b`
- `scope_metrics.json`: `23dce5a1f23b33c509b6e6b9015369011e3d7acfe56d27a16152f605d1f3e4a4`
- `selector_tasks_scoped.json`: `58432c19caa92b01e26e52d366a0344e1a63768b39f27ae83639d078ce2fa1af`

## Verdict

**A — explicit Function Scope Graph resolves provable cross-granularity competition without candidate loss or capacity regression.** Ready only for another isolated critical AI smoke on frozen scoped tasks.

Even with verdict A: **NO DEPLOY. NO SHADOW.**

Model calls = `0`.
