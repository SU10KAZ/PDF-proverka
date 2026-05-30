# Audit Smoke Side-by-Side

| block_id | verdict | ref_findings | cand_findings | ref_kv | cand_kv | ref_sum_len | cand_sum_len | reasons |
|----------|---------|--------------|---------------|--------|---------|-------------|--------------|---------|
| 3TFL-TNRF-7G6 | likely degraded | 1 | 1 | 55 | 5 | 127 | 74 | kv_collapse |
| 66NE-7DYY-GQN | likely degraded | 1 | 1 | 46 | 8 | 104 | 84 | kv_collapse |
| 9DH3-MEUR-DR4 | equivalent | 3 | 1 | 19 | 14 | 113 | 97 | no_obvious_regression |
| 947C-9UJT-RYU | likely degraded | 1 | 1 | 23 | 6 | 108 | 65 | kv_collapse |
| 9PPJ-YP6U-HV6 | likely degraded | 3 | 1 | 14 | 6 | 151 | 84 | kv_collapse |
| 97AH-VUFP-LJJ | likely degraded | 5 | 1 | 24 | 11 | 123 | 55 | kv_collapse,generic_or_short_summary |
| 4CFG-LM4Y-7H4 | likely degraded | 1 | 1 | 23 | 11 | 114 | 69 | kv_collapse |
| 66M4-Y69W-VCG | likely degraded | 2 | 1 | 14 | 12 | 109 | 52 | generic_or_short_summary |
| 6PPA-4DDX-6FR | likely degraded | 2 | 1 | 13 | 11 | 186 | 52 | generic_or_short_summary |
| 6R7N-7LRD-AUR | likely degraded | 2 | 1 | 27 | 9 | 145 | 47 | kv_collapse,generic_or_short_summary |
| 7DJ9-EQQ3-QMK | likely degraded | 3 | 1 | 38 | 9 | 81 | 53 | kv_collapse,generic_or_short_summary |
| 6P9T-Q7GT-N9J | likely degraded | 4 | 1 | 32 | 20 | 169 | 56 | generic_or_short_summary |
