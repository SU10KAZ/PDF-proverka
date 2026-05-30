# Claude stage 02 block_batch — A/B matrix summary
Experiment dir: `/home/coder/projects/PDF-proverka/projects/214. Alia (ASTERUS)/KJ/13АВ-РД-КЖ5.17-23.1-К2 (1) (1).pdf/_experiments/block_batch_ab/20260420_133050`
Generated: 2026-04-20T15:12:24
Runs: **9**

## Plan stats
| run_id | profile | parallelism | total_batches | avg | max | max_heavy | heavy/normal/light |
|---|---|---|---|---|---|---|---|
| conservative_p1 | conservative | 1 | 41 | 5.24 | 6 | 2 | 11/151/53 |
| conservative_p2 | conservative | 2 | 41 | 5.24 | 6 | 2 | 11/151/53 |
| conservative_p3 | conservative | 3 | 41 | 5.24 | 6 | 2 | 11/151/53 |
| baseline_p1 | baseline | 1 | 33 | 6.52 | 8 | 2 | 11/151/53 |
| baseline_p2 | baseline | 2 | 33 | 6.52 | 8 | 2 | 11/151/53 |
| baseline_p3 | baseline | 3 | 33 | 6.52 | 8 | 2 | 11/151/53 |
| aggressive_p1 | aggressive | 1 | 31 | 6.94 | 10 | 2 | 11/151/53 |
| aggressive_p2 | aggressive | 2 | 31 | 6.94 | 10 | 2 | 11/151/53 |
| aggressive_p3 | aggressive | 3 | 31 | 6.94 | 10 | 2 | 11/151/53 |

## Runtime + Quality (real runs only)
| run_id | elapsed_s | coverage% | unreadable% | fail | total_findings | findings/100 |
|---|---|---|---|---|---|---|
| conservative_p1 | 1054.74 | 13.49 | 0.0 | 0 | 27 | 93.1 |
| conservative_p2 | 622.77 | 13.49 | 0.0 | 0 | 18 | 62.07 |
| conservative_p3 | 375.22 | 13.49 | 0.0 | 0 | 26 | 89.66 |
| baseline_p1 | 1003.47 | 16.28 | 0.0 | 0 | 20 | 57.14 |
| baseline_p2 | 542.29 | 16.28 | 0.0 | 0 | 28 | 80.0 |
| baseline_p3 | 433.73 | 16.28 | 0.0 | 0 | 30 | 85.71 |
| aggressive_p1 | 1126.77 | 18.14 | 0.0 | 0 | 43 | 110.26 |
| aggressive_p2 | 545.79 | 18.14 | 0.0 | 0 | 33 | 84.62 |
| aggressive_p3 | 389.23 | 18.14 | 0.0 | 0 | 38 | 97.44 |

## Winner
- **Production recommendation:** `conservative_p3` (profile=conservative, parallelism=3)
- Fastest: `conservative_p3`
- Best quality: `aggressive_p1`
- Reason: НЕТ runs с coverage=100% — victor выбран из всего пула по elapsed.
- Исключено из-за coverage < 100%: ['conservative_p1', 'conservative_p2', 'conservative_p3', 'baseline_p1', 'baseline_p2', 'baseline_p3', 'aggressive_p1', 'aggressive_p2', 'aggressive_p3']
