# G2.3 — SYSTEM_GRAPH comparison result

## Verdict

- Overall: `CHANGED`.
- Level A: `BACKBONE_PRESERVED`.
- Level B: `FUNCTIONS_UNCERTAIN`.
- Matched pairs: 50.
- Geometry identity weight: `0.0`.
- Contract valid: `True`.

## Changes

| Level | Type | Confidence | Summary |
|---|---|---:|---|
| A | `DETAIL_LEVEL_INCREASED` | 0.940 | Источник показан подробнее без смены функционального пути: ТП1 (UPSTREAM_TP_CONNECTION) → Т1 (TRANSFORMER_EXPLICIT). |
| A | `DETAIL_LEVEL_INCREASED` | 0.940 | Источник показан подробнее без смены функционального пути: ТП2 (UPSTREAM_TP_CONNECTION) → Т2 (TRANSFORMER_EXPLICIT). |
| B | `UNCERTAIN_STRUCTURAL_CHANGE` | 0.350 | Число распознанных резервных отходящих различается, но уверенности идентификации недостаточно для утверждения об изменении: 2 → 0. |
| C | `GROUP_COUNT_CHANGED` | 0.867 | Количество отходящих аппаратов изменилось: 30 → 27. |
| C | `NODE_TYPE_CHANGED` | 0.920 | Тип сопоставленного узла изменился: QF3 (CIRCUIT_BREAKER) → QS1 (SWITCH_DISCONNECTOR). |
| C | `UNCERTAIN_STRUCTURAL_CHANGE` | 0.490 | Для части узлов соответствие недостаточно надёжно; удаление или добавление не утверждается. |

## Preserved functions

- `COMPENSATION_GROUP`: preserved
- `METERING_GROUP`: preserved
- `SERVICE_GROUP`: preserved

Labels of functional-group implementations are intentionally ignored when the
same role remains attached to the same functional section.

## Detail versus change

Two source paths are classified as `DETAIL_LEVEL_INCREASED`; their expanded
right-side subgraphs are consumed by the detail pass and are not emitted as
`NODE_ADDED`. The section tie remains the same functional tie, while its grounded
device subtype changes and is therefore `NODE_TYPE_CHANGED`.

## Repeated outgoing group

The outgoing-device group changes from 30 to 27 and yields one
`GROUP_COUNT_CHANGED`. Reordered/partially unresolved branch identities do not
yield mass removal/addition. Counts in this result: `NODE_REMOVED=0`,
`NODE_ADDED=0`.

## Uncertainty

Reserve recognition and unresolved individual outgoing correspondences remain
`UNCERTAIN_STRUCTURAL_CHANGE`; neither is promoted to a proven removal/addition.

## Verification

- Comparator negative/real suite: `10 passed`.
- G2.2 profile/source-kind regressions: `23 passed`.
- Classic Vectograf: `57 passed, 23 skipped`.
- Stage Comparison: `300 passed`.

## Boundaries

This artifact compares ready JSON graphs only. It performs no PDF extraction,
Vision, UI work, Stage Comparison integration, or GraphicChangeLedger integration.
