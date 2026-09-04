# Function Lineage v2.7 — deterministic instance / series identity

Phase 1 forensics. No model calls. Physical page numbers and graphic sheet numbers are provenance only and never establish identity or a match; a missing fact stays UNKNOWN and is never a mismatch.

## Identity coverage per corpus

| Corpus | Functions | PROVEN | PARTIAL | UNKNOWN |
|---|---:|---:|---:|---:|
| IOS1.1 | 139 | 73 | 17 | 49 |
| IOS2.1 | 124 | 7 | 82 | 35 |
| IOS3.1 | 50 | 0 | 37 | 13 |

## Which documented facts exist at all

| Corpus | mark | level | zone | section | floors | serviced_object |
|---|---|---|---|---|---|---|
| IOS1.1 | 73 | 0 | 29 | 0 | 27 | 3 |
| IOS2.1 | 7 | 56 | 70 | 20 | 83 | 57 |
| IOS3.1 | 0 | 32 | 33 | 2 | 37 | 30 |

## Same-class clusters

A cluster is every function of one side sharing class and document role — exactly the situation where the selector had to guess.

| Classification | Clusters |
|---|---:|
| `UNIQUELY_IDENTIFIED` | 8 |
| `PARTIALLY_IDENTIFIED` | 6 |
| `INDISTINGUISHABLE` | 20 |
| `CONTRADICTORY` | 5 |
| `UNKNOWN` | 14 |

| Corpus | Clusters | Breakdown |
|---|---:|---|
| IOS1.1 | 19 | `{'CONTRADICTORY': 5, 'INDISTINGUISHABLE': 9, 'UNKNOWN': 5}` |
| IOS2.1 | 26 | `{'INDISTINGUISHABLE': 10, 'PARTIALLY_IDENTIFIED': 4, 'UNIQUELY_IDENTIFIED': 3, 'UNKNOWN': 9}` |
| IOS3.1 | 8 | `{'INDISTINGUISHABLE': 1, 'PARTIALLY_IDENTIFIED': 2, 'UNIQUELY_IDENTIFIED': 5}` |

## Does identity resolve the contested clusters? (diagnostic)

Read-only over responses already recorded under consent. Never acceptance evidence.

Contested clusters examined `12`; resolved by a matching mark **`0`**.

| Outcome | Clusters |
|---|---:|
| `NO_IDENTITY_SIGNAL` | 8 |
| `PARTIAL_SOME_MARKS_MISSING` | 3 |
| `RESOLVED_BY_MATCHING_MARK` | 0 |
| `SEPARABLE_LEFT_ONLY` | 1 |

## Could an identity-certified 1:1 tier exist at all?

Non-sentinel tasks `206`; of them purely CONTINUED_1_TO_1 `40`.

* with no capacity contention against any sibling task: **`1`**
* with a proven instance identity on both sides: **`0`**

certification needs an instance that cannot be confused with a sibling; contention with another task's inventory is exactly that confusion
