# Per-Block Side-by-Side

| profile | block_id | verdict | ref_findings | cand_findings | ref_kv | cand_kv | ref_sum_len | cand_sum_len | reasons |
|---------|----------|---------|--------------|---------------|--------|---------|-------------|--------------|---------|
| b4 | 3TFL-TNRF-7G6 | likely improved | 1 | 1 | 55 | 61 | 127 | 116 | kv_adequacy_improved |
| b4 | 66NE-7DYY-GQN | equivalent | 1 | 1 | 46 | 33 | 104 | 108 | no_obvious_regression |
| b4 | 9DH3-MEUR-DR4 | likely degraded | 3 | 0 | 19 | 0 | 113 | 0 | missing_analysis |
| b4 | 947C-9UJT-RYU | likely degraded | 1 | 0 | 23 | 0 | 108 | 0 | missing_analysis |
| b4 | 9PPJ-YP6U-HV6 | equivalent | 3 | 1 | 14 | 9 | 151 | 90 | no_obvious_regression |
| b4 | 97AH-VUFP-LJJ | equivalent | 5 | 1 | 24 | 13 | 123 | 105 | no_obvious_regression |
| b4 | 4CFG-LM4Y-7H4 | equivalent | 1 | 1 | 23 | 19 | 114 | 108 | no_obvious_regression |
| b4 | 66M4-Y69W-VCG | likely improved | 2 | 1 | 14 | 20 | 109 | 75 | kv_adequacy_improved |
| b4 | 6PPA-4DDX-6FR | likely degraded | 2 | 1 | 13 | 16 | 186 | 85 | generic_or_short_summary |
| b4 | 6R7N-7LRD-AUR | equivalent | 2 | 1 | 27 | 25 | 145 | 92 | no_obvious_regression |
| b4 | 7DJ9-EQQ3-QMK | likely degraded | 3 | 1 | 38 | 10 | 81 | 108 | kv_collapse |
| b4 | 6P9T-Q7GT-N9J | likely degraded | 4 | 1 | 32 | 12 | 169 | 105 | kv_collapse |
| b2 | 3TFL-TNRF-7G6 | uncertain | 1 | 3 | 55 | 13 | 127 | 99 | kv_collapse |
| b2 | 66NE-7DYY-GQN | uncertain | 1 | 2 | 46 | 21 | 104 | 103 | kv_collapse |
| b2 | 9DH3-MEUR-DR4 | uncertain | 3 | 1 | 19 | 59 | 113 | 142 | noisy_kv_inflation |
| b2 | 947C-9UJT-RYU | equivalent | 1 | 1 | 23 | 23 | 108 | 110 | no_obvious_regression |
| b2 | 9PPJ-YP6U-HV6 | likely improved | 3 | 2 | 14 | 23 | 151 | 105 | kv_adequacy_improved |
| b2 | 97AH-VUFP-LJJ | equivalent | 5 | 2 | 24 | 17 | 123 | 127 | no_obvious_regression |
| b2 | 4CFG-LM4Y-7H4 | equivalent | 1 | 1 | 23 | 13 | 114 | 79 | no_obvious_regression |
| b2 | 66M4-Y69W-VCG | equivalent | 2 | 1 | 14 | 14 | 109 | 65 | no_obvious_regression |
| b2 | 6PPA-4DDX-6FR | likely degraded | 2 | 1 | 13 | 7 | 186 | 85 | generic_or_short_summary |
| b2 | 6R7N-7LRD-AUR | likely degraded | 2 | 2 | 27 | 6 | 145 | 92 | kv_collapse |
| b2 | 7DJ9-EQQ3-QMK | equivalent | 3 | 2 | 38 | 30 | 81 | 72 | no_obvious_regression |
| b2 | 6P9T-Q7GT-N9J | equivalent | 4 | 2 | 32 | 32 | 169 | 122 | no_obvious_regression |
