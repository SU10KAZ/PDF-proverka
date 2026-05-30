# Claude stage 02 block_batch — A/B matrix summary
Experiment dir: `/home/coder/projects/PDF-proverka/projects/214. Alia (ASTERUS)/KJ/13АВ-РД-КЖ5.17-23.1-К2 (1) (1).pdf/_experiments/block_batch_final/20260420_155609`
Generated: 2026-04-20T17:02:06
Runs: **4**

## Plan stats
| run_id | profile | parallelism | total_batches | avg | max | max_heavy | heavy/normal/light |
|---|---|---|---|---|---|---|---|
| baseline_p3 | baseline | 3 | 33 | 6.52 | 8 | 2 | 11/151/53 |
| aggressive_p3 | aggressive | 3 | 31 | 6.94 | 10 | 2 | 11/151/53 |
| baseline_p3_subset | baseline | 3 | 10 | 6 | 8 | 3 | 11/36/13 |
| aggressive_p3_subset | aggressive | 3 | 9 | 6.67 | 10 | 3 | 11/36/13 |

## Runtime + Quality (real runs only)
| run_id | elapsed_s | coverage% | unreadable% | fail | total_findings | findings/100 |
|---|---|---|---|---|---|---|
| baseline_p3 | 1979.25 | 100.0 | 0.0 | 0 | 168 | 78.14 |
| aggressive_p3 | 1790.01 | 100.0 | 0.0 | 0 | 186 | 86.51 |
| baseline_p3_subset | 180.83 | 30.0 | 0.0 | 10 | 24 | 133.33 |
| aggressive_p3_subset | 7.63 | 0.0 | 0.0 | 9 | 0 | 0.0 |

## Winner
- **Production recommendation:** `baseline_p3` (profile=baseline, parallelism=3)
- Fastest: `aggressive_p3`
- Best quality: `aggressive_p3`
- Reason: quality gate subset не пройден: ['total_findings_aggr_>=_95%_baseline', 'blocks_with_findings_aggr_>=_95%_baseline']
