# Function Lineage deterministic candidate coverage

Algorithm: `function-lineage-matcher.v1.2-deterministic`.
Production baseline: `5eb6fa144c3124e8926f5e8c69c546827b878ff8` (`ui-real-5eb6fa14`).

This run stopped before selector/model execution. Model calls: `0`; deploy: `false`; materialization: `false`.

## Architecture

Existing same-version Markdown is converted deterministically into compact, provenance-ready function facts. Function fragments search the complete RIGHT function corpus through independent functional channels; Sheet Matcher edges, titles, and physical proximity are supporting signals only. Bounded 1:1, 1:N, N:1, and FUNCTION_DISTRIBUTED candidates retain exact-fragment capacity keys.

## Candidate recall

| Project | R@1 | R@3 | R@5 | R@10 | single R@10 | group recall | DOCUMENT_LINK recall | median / p95 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| ИОС 1.1 | 0.166667 | 0.5 | 0.833333 | 1.0 | 1.0 | 1.0 | 0.045455 | 12 / 12.0 |
| ИОС 3.1 | 0.8 | 0.8 | 0.8 | 1.0 | 1.0 | None | 0.0 | 9.0 / 12.0 |
| ИОС 2.1 | 0.75 | 0.75 | 0.875 | 0.875 | 1.0 | 1.0 | 0.0 | 12.0 / 12.0 |

Overall FUNCTIONAL_ANALOGUE recall: R@1 `0.578947`, R@3 `0.684211`, R@5 `0.842105`, R@10 `0.947368`.

### Single-page recall

| Project | R@1 | R@3 | R@5 | R@10 |
|---|---:|---:|---:|---:|
| ИОС 1.1 | 0.0 | 0.0 | 0.0 | 1.0 |
| ИОС 3.1 | 0.8 | 0.8 | 0.8 | 1.0 |
| ИОС 2.1 | 1.0 | 1.0 | 1.0 | 1.0 |

DOCUMENT_LINK and FUNCTIONAL_ANALOGUE are measured in separate namespaces; documentary links do not admit or exclude functional candidates.

## IOS2.1 controls

- left17_right27: present `True`, rank `1`.
- left18_right24: present `True`, rank `1`.
- left19_right25: present `True`, rank `2`.
- left19_right30: present `True`, rank `1`.
- LEFT20 → [26,28,29]: `lcand_9c617494b14c2b922d3f`, rank `1`, present `True`.

R30 remains in the LEFT19 candidate set; no page-global exclusivity was applied.

## Production integration drift

- LEFT 17: lost `serviced_object, corpus, zone, floors, consumers, upstream, downstream`; restored `consumers, corpus, downstream, floors, serviced_object, upstream, zone`.
- LEFT 18: lost `serviced_object, corpus, zone, floors, consumers, upstream, downstream, equipment_roles`; restored `consumers, corpus, downstream, equipment_roles, floors, serviced_object, upstream, zone`.
- LEFT 19: lost `serviced_object, corpus, zone, floors, consumers, upstream, downstream`; restored `consumers, corpus, downstream, floors, serviced_object, upstream, zone`.
- LEFT 20: lost `consumers, upstream, downstream`; restored `consumers, downstream, upstream`.

Full passports, candidates, channels, ranks, and provenance are in `stage_comparison_ios21.json`.

## Safety verdict

Search failures: `0`. Group-generation failures: `0`. New false conflicts: `0`.

Verdict: **A — deterministic candidate layer готов к isolated AI repeat**
