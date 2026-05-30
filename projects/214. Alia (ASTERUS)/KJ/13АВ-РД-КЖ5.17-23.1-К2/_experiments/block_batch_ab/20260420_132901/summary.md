# Claude stage 02 block_batch — A/B matrix summary
Experiment dir: `/home/coder/projects/PDF-proverka/projects/214. Alia (ASTERUS)/KJ/13АВ-РД-КЖ5.17-23.1-К2 (1) (1).pdf/_experiments/block_batch_ab/20260420_132901`
Generated: 2026-04-20T13:29:53
Runs: **1**

## Plan stats
| run_id | profile | parallelism | total_batches | avg | max | max_heavy | heavy/normal/light |
|---|---|---|---|---|---|---|---|
| baseline_p1 | baseline | 1 | 33 | 6.52 | 8 | 2 | 11/151/53 |

## Runtime + Quality (real runs only)
| run_id | elapsed_s | coverage% | unreadable% | fail | total_findings | findings/100 |
|---|---|---|---|---|---|---|
| baseline_p1 | 52.22 | 0.47 | 0.0 | 0 | 1 | 100.0 |

## Winner
- **Production recommendation:** `baseline_p1` (profile=baseline, parallelism=1)
- Fastest: `baseline_p1`
- Best quality: `baseline_p1`
- Reason: НЕТ runs с coverage=100% — victor выбран из всего пула по elapsed.
- Исключено из-за coverage < 100%: ['baseline_p1']
