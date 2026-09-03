# Function Lineage v2 — isolated AI repeat

Source candidate commit: `2bcb832f51c46867c56d49d81549d9cac5918e96`. Production baseline: `5eb6fa144c3124e8926f5e8c69c546827b878ff8` (`ui-real-5eb6fa14`).

Research only: no production run, no shadow enablement, no deploy, no materialization, no Vision.

Experiment validity: **NOT VALID** for selector repeatability. All 18 isolated
calls were rejected before inference by the transport's 1,048,576-character
input limit: IOS1.1 `3,039,154`, IOS3.1 `1,086,687`, IOS2.1 `3,193,533`.
No prompt, candidate, ordering, passport, verifier, or model setting was changed
and no retry with a modified input was made.

## Project metrics

| Project | Candidate tasks | Stable tasks | Stable lineages | Stable % | NME | Pass disagreement | Verifier reject | Model/schema fail |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| ИОС 1.1 | 61 | 0 | 0 | 0.0 | 0 | 0 | 0 | 61 |
| ИОС 3.1 | 26 | 0 | 0 | 0.0 | 0 | 0 | 0 | 26 |
| ИОС 2.1 | 58 | 0 | 0 | 0.0 | 0 | 0 | 0 | 58 |

Stable % is stable task decisions / candidate-bearing tasks; stable lineages are unique candidate IDs.

## IOS2.1 controls

- LEFT17 → R27: present `True`, best rank `1`, stable target tasks `0`.
- LEFT18 → R24: present `True`, best rank `1`, stable target tasks `0`.
- LEFT19 ambiguity distribution: not observable; all six intended observations
  failed before inference (`R30: 0`, `R25: 0`, `NEED_MORE_EVIDENCE: 0`,
  `MODEL_START_FAILURE: 6`).
- LEFT20 group `lcand_9c617494b14c2b922d3f`: present `True`, stable owner tasks `0/3`, exact capacities `True`.

## Safety and cost

Unsupported accepted matches: `0`. FUNCTION_FRAGMENT_CONFLICT: `0`. RIGHT_MAP_CONFLICT: `0`.

Model calls: `18`; successful: `0`; wall time: `5448 ms`; model runtime sum: `13746 ms`; reported tokens: `0`.

Telemetry defect: `False` (there were no successful calls). Reported tokens are
`0` because inference never started; the apparent `-100%` token delta is not a
quality/cost comparison with a completed run.

## Verdict

**B — candidate coverage is restored, but the selector input is not bounded to
the available transport context. Repeatability remains unmeasured.**

This verdict does not authorize deployment or shadow enablement.
