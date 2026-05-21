# A2 — Hybrid Cross Conditional (Current + Completeness + Conditional cross_discipline)

**Hypothesis:** [H2](../hypotheses.md#h2-hybrid-cross-a2).

A1 plus a **conditionally executed** `cross_discipline` Sonnet lens. The
conditional router scans the MD for cross-discipline trigger markers
before deciding to call the lens (saves ~50% of the cross-discipline
spend on cases that don't need it).

## Architecture

```
                      ┌────────────────────────┐
                  ┌──►│ [Opus] current_method  │──► current_findings[]
                  │   └────────────────────────┘
   MD ───────────┤
                  │   ┌────────────────────────┐
                  ├──►│ [Sonnet] completeness  │──► comp_findings[]
                  │   └────────────────────────┘
                  │
                  │           Trigger router
                  │           ┌────────┐
                  └──► [py] │ check  │──┬─► no trigger → []
                              │ triggers│  │
                              └────────┘  └─► trigger → [Sonnet] cross_discipline
                                                                       │
                                                                       ▼
                                                          xd_findings[]
                                                                       │
                                                                       ▼
                                              Python merge + class dedup
                                                                       │
                                                                       ▼
                                                          merged_findings[]
```

## Cost model

| Resource | Per case (no XD trigger) | Per case (XD trigger) |
|---|---|---|
| Opus calls | 1 | 1 |
| Sonnet calls | 1 | 2 |
| Critic calls | 0 | 0 |
| Reviewer calls | 0 | 0 |
| Expected wall-clock | 200–250 s | 280–350 s |

Expected trigger rate on 8 cases (estimated from MD scan, see
[../runners/conditional_router.py](../runners/conditional_router.py)):

| Case | Triggered? |
|---|---|
| ar_01_evacuation | yes (refs ОВ) |
| cross_01_eom_ov_loads | yes (explicit ЭОМ↔ОВ) |
| eom_01_cable_sizing | yes (refs ОВ loads, ИТП) |
| kj_01_rebar | no (intra-section) |
| multi_01_tz_vs_rd | yes (ТЗ vs РД itself) |
| ov_01_ventilation | yes (refs АР, ЭОМ, ГАЗ) |
| ss_01_cabling | yes (refs ЭОМ, АПС) |
| vk_01_water_flow | yes (refs АПС) |

7/8 cases trigger. The router pays off most when scaled to a larger
dataset where pure-calculation or pure-architecture cases dominate.

## Expected strengths

- Covers the `cross_01_eom_ov_loads`-style critical-recall gap.
- Saves cross-discipline cost on pure-calc cases.
- Same simplicity as A1 with one more leg.

## Expected weaknesses

- No critic — the cross_discipline lens's tendency to enumerate variations
  of the same problem class is still present.
- Trigger router is heuristic; misses on cases whose cross-discipline issue
  is unstated (e.g. cable cross-section implies penetrations into walls
  but MD doesn't say so).

## Prompt set

- Current method (unchanged).
- Completeness lens (v0 / v1 / v2 selectable).
- Cross-discipline lens (v0 / v1 / v2 selectable).

## Routing rules (cross_discipline)

Run if **any** of:
- MD contains any of: `смеж`, `ОВ`, `ЭОМ`, `ВК`, `СС`, `АПС`, `задание`,
  `пусковой ток`, `тепловая нагрузка`, `отверстие`, `проходка`,
  `электропитание`, `закладные`, `автоматика`, `ТЗ vs`, `согласован`,
  `смежн.`, `по заданию`, `закладны`, `вентоборудование`.
- The case discipline is in {EOM, OV, VK, TX, ITP, SS, AR, KJ, MULTI}.

Logic implemented in [`../runners/conditional_router.py`](../runners/conditional_router.py).

## Dedup strategy

Same as A1 — class-level dedup with priority:
1. Drop XD findings that duplicate Current findings.
2. Drop Completeness findings that duplicate Current findings.
3. Drop XD/Completeness mutual duplicates (keep XD, since cross-discipline
   evidence is more actionable).
4. Internal dedup within Completeness and XD.

## Outputs

`algorithm_research/results/A2_hybrid_cross_conditional__<prompt>/<case_id>.json`.

## Decision criteria

| Outcome | Action |
|---|---|
| A2 catches the 2 missed critical on cross_01 AND FP increase ≤ +8 | Promote over A1 |
| A2 catches the 2 critical but FP > +15 | Combine A2 with A3 critic |
| A2 fails to catch the 2 critical | Investigate XD prompt; do not promote |
