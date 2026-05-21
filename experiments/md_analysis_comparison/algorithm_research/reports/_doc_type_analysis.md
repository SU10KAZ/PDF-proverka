# Per-document_type Analysis

Aggregated by document_type across the cases done so far.

## full_rd

| Algorithm | cases | matched | missed_crit | FP | total | avg_strict | avg_balanced |
|---|---|---|---|---|---|---|---|
| A0_baseline_current__baseline | 6 | 38 | 1 | 65 | 105 | 49.8 | 72.3 |
| A0_phase0_classdedup__baseline | 6 | 38 | 1 | 65 | 105 | 49.8 | 72.3 |
| A0_phase0_combined__baseline | 6 | 38 | 1 | 65 | 105 | 49.8 | 72.3 |
| A0_phase0_fuzzydedup__baseline | 6 | 38 | 1 | 65 | 105 | 49.8 | 72.3 |
| A1_hybrid_lite__v1 | 1 | 6 | 0 | 3 | 9 | 88.0 | 94.0 |
| A1_hybrid_lite__v2 | 16 | 77 | 1 | 251 | 335 | 25.8 | 57.5 |
| replay_class_dedup__baseline | 6 | 38 | 1 | 166 | 206 | -18.5 | 38.7 |
| replay_fuzzy_dedup__baseline | 6 | 38 | 1 | 134 | 174 | 3.8 | 49.3 |

## audit_comparison

| Algorithm | cases | matched | missed_crit | FP | total | avg_strict | avg_balanced |
|---|---|---|---|---|---|---|---|
| A0_baseline_current__baseline | 1 | 4 | 2 | 3 | 10 | 25.1 | 41.1 |
| A0_phase0_classdedup__baseline | 1 | 4 | 2 | 3 | 10 | 25.1 | 41.1 |
| A0_phase0_combined__baseline | 1 | 4 | 2 | 3 | 10 | 25.1 | 41.1 |
| A0_phase0_fuzzydedup__baseline | 1 | 4 | 2 | 3 | 10 | 25.1 | 41.1 |
| A1_hybrid_lite__v1 | 1 | 6 | 0 | 7 | 14 | 57.7 | 71.7 |
| A1_hybrid_lite__v2 | 3 | 10 | 2 | 16 | 29 | 51.4 | 65.4 |
| replay_class_dedup__baseline | 1 | 7 | 0 | 20 | 27 | 20.0 | 60.0 |
| replay_fuzzy_dedup__baseline | 1 | 7 | 0 | 20 | 27 | 20.0 | 60.0 |

## tz_vs_rd

| Algorithm | cases | matched | missed_crit | FP | total | avg_strict | avg_balanced |
|---|---|---|---|---|---|---|---|
| A0_baseline_current__baseline | 1 | 7 | 0 | 5 | 12 | 80.0 | 90.0 |
| A0_phase0_classdedup__baseline | 1 | 7 | 0 | 5 | 12 | 80.0 | 90.0 |
| A0_phase0_combined__baseline | 1 | 7 | 0 | 5 | 12 | 80.0 | 90.0 |
| A0_phase0_fuzzydedup__baseline | 1 | 7 | 0 | 5 | 12 | 80.0 | 90.0 |
| A1_hybrid_lite__v2 | 2 | 12 | 0 | 25 | 37 | 50.0 | 75.0 |
| replay_class_dedup__baseline | 1 | 7 | 0 | 32 | 39 | -30.0 | 36.0 |
| replay_fuzzy_dedup__baseline | 1 | 7 | 0 | 25 | 32 | 0.0 | 50.0 |

## specification_only

| Algorithm | cases | matched | missed_crit | FP | total | avg_strict | avg_balanced |
|---|---|---|---|---|---|---|---|
| A1_hybrid_lite__v2 | 3 | 9 | 0 | 34 | 43 | 53.3 | 77.3 |

