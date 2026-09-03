# Function Lineage v2.4.1 — scoped selector transport

## Boundary

- Scope source: `6d2e7a5e4710765f0b5b8450c73c31431e070d13` (V2.4 verdict A).
- Production reference only: `4d489bf9033ad40c40099fe5e1436493bc56c0ed` / `ui-real-4d489bf9`.
- Model calls `0`; deploy `NO`; shadow `NO`; materialization `NO`; vision `NO`.
- Pre-scope selector shards are explicitly forbidden as future AI input and were not read.

## Scoped transport

`213` atomic selector tasks each carry one exact FunctionScope and only its eligible `EXACT_SCOPE` candidates.
All raw candidates remain in frozen forensic sources and the exact-scope task partition: `1461/1461`; model-input occurrences `1461` and unique IDs `1461`.
`STRICT_SUBSET`, `STRICT_SUPERSET`, and `OVERLAP` IDs are retained in V2.4 forensic artifacts but are absent from foreign selectable candidate lists.
Target/hard gate: `250000` / `350000` characters; shards `84`; over target `0`; over hard gate `0`; oversized atomic tasks `0`.
Silent truncations `0`; candidate-list truncations `0`. Verbose facts use explicit compaction markers with full-value SHA-256.

## Recall

| Metric | R@1 | R@3 | R@5 | R@10 | No regression |
|---|---:|---:|---:|---:|---:|
| RAW | 0.578947 | 0.684211 | 0.842105 | 0.947368 | True |
| SCOPE-ELIGIBLE | 0.789474 | 0.842105 | 0.894737 | 0.947368 | True |

## Safety

Cross-granularity selectable competition `0`; RIGHT_MAP_CONFLICT `0`; capacity defects `0`; search failures `0`.
Frozen pre-existing group-generation failure diagnostics retained: `2` (not transport defects).
Projection errors `0`; provider-schema problems `0`; `oneOf` present `False`; parser fail-closed `True`.

## IOS2.1 future isolated scoped smoke IDs

| Control | task_id | scope_id | Kind |
|---|---|---|---|
| LEFT17 | `fstask_e626d29f5317c598bf32` | `fscope_2b9be69ab7ab0329c05c` | `COMPONENT` |
| LEFT18 | `fstask_3778375037ec99747b0c` | `fscope_a0958d87cf3434c11438` | `COMPONENT` |
| LEFT19 | `fstask_135321825e7b00340f49` | `fscope_d218cf99622ef6ffff16` | `COMPONENT` |
| LEFT20 DOMESTIC child | `fstask_c289ca22f53fcdbe6f99` | `fscope_90a63adbb11d34d61f4b` | `COMPONENT` |
| LEFT20 FIRE child | `fstask_263e2f49af1b34aafb1c` | `fscope_472f43e47a98f8cb7b35` | `COMPONENT` |
| LEFT20 METERING child | `fstask_3412531f08348a502fc6` | `fscope_2bb6cd1e14a1c59c591e` | `COMPONENT` |
| LEFT20 composite parent | `fstask_329baf4983e5d00118f2` | `fscope_d1faafb9db1c9aca8074` | `COMPOSITE` |

LEFT20 child tasks keep R26/R28/R29 eligible in DOMESTIC/FIRE/METERING respectively. The composite parent keeps `[26,28,29]` eligible while those singletons are not selectable there. LEFT19 keeps R30 and R25 together; LEFT17 R27 and LEFT18 R24 remain eligible.

## Deterministic replay

Two independent full builds are required to be byte-identical before any artifact is written; mismatch fails the command.

## Verdict

**A — scoped bounded transport готов к isolated scoped AI smoke.**

Even with verdict A: **NO MODEL CALLS. NO DEPLOY. NO SHADOW.**
