# Per-Block Side-by-Side

| phase | run_id | block_id | class | ref_findings | cand_findings | ref_kv | cand_kv | ref_sum_len | cand_sum_len | reasons |
|-------|--------|----------|-------|--------------|---------------|--------|---------|-------------|--------------|---------|
| batch_screening | b2 | 6DRC-7KQL-9TJ | likely_improved | 1 | 3 | 4 | 4 | 159 | 126 | useful_findings_improved |
| batch_screening | b2 | 4MQJ-6NXP-4YH | likely_equivalent | 2 | 2 | 13 | 15 | 88 | 98 | no_obvious_regression |
| batch_screening | b2 | RELE-MX3A-MEN | likely_degraded | 1 | 1 | 10 | 32 | 139 | 97 | noisy_kv_inflation |
| batch_screening | b2 | DTGF-MYHX-PPD | likely_improved | 2 | 1 | 13 | 21 | 123 | 118 | kv_adequacy_improved |
| batch_screening | b2 | 4WTD-JFKA-JLE | likely_improved | 1 | 2 | 13 | 19 | 131 | 163 | useful_findings_improved,kv_adequacy_improved |
| batch_screening | b2 | 9GNP-D7CE-RYM | likely_improved | 4 | 4 | 11 | 11 | 122 | 152 | summary_specificity_improved |
| batch_screening | b2 | 4UTW-PPGP-VEN | likely_improved | 2 | 2 | 20 | 17 | 87 | 150 | summary_specificity_improved |
| batch_screening | b2 | TULW-YCYU-YVQ | likely_equivalent | 1 | 1 | 14 | 17 | 108 | 86 | no_obvious_regression |
| batch_screening | b4 | 6DRC-7KQL-9TJ | likely_improved | 1 | 3 | 4 | 26 | 159 | 101 | useful_findings_improved,kv_adequacy_improved |
| batch_screening | b4 | 4MQJ-6NXP-4YH | likely_improved | 2 | 2 | 13 | 13 | 88 | 116 | summary_specificity_improved |
| batch_screening | b4 | RELE-MX3A-MEN | likely_equivalent | 1 | 1 | 10 | 11 | 139 | 122 | no_obvious_regression |
| batch_screening | b4 | DTGF-MYHX-PPD | likely_degraded | 2 | 2 | 13 | 6 | 123 | 113 | kv_collapse |
| batch_screening | b4 | 4WTD-JFKA-JLE | likely_equivalent | 1 | 1 | 13 | 16 | 131 | 104 | no_obvious_regression |
| batch_screening | b4 | 9GNP-D7CE-RYM | likely_equivalent | 4 | 1 | 11 | 10 | 122 | 97 | no_obvious_regression |
| batch_screening | b4 | 4UTW-PPGP-VEN | likely_equivalent | 2 | 1 | 20 | 18 | 87 | 93 | no_obvious_regression |
| batch_screening | b4 | TULW-YCYU-YVQ | likely_improved | 1 | 1 | 14 | 28 | 108 | 86 | kv_adequacy_improved |
| batch_screening | b6 | 6DRC-7KQL-9TJ | likely_improved | 1 | 1 | 4 | 8 | 159 | 118 | kv_adequacy_improved |
| batch_screening | b6 | 4MQJ-6NXP-4YH | likely_equivalent | 2 | 2 | 13 | 11 | 88 | 116 | no_obvious_regression |
| batch_screening | b6 | RELE-MX3A-MEN | likely_improved | 1 | 2 | 10 | 8 | 139 | 126 | useful_findings_improved |
| batch_screening | b6 | DTGF-MYHX-PPD | likely_equivalent | 2 | 2 | 13 | 15 | 123 | 114 | no_obvious_regression |
| batch_screening | b6 | 4WTD-JFKA-JLE | likely_improved | 1 | 3 | 13 | 29 | 131 | 107 | useful_findings_improved,kv_adequacy_improved |
| batch_screening | b6 | 9GNP-D7CE-RYM | likely_equivalent | 4 | 1 | 11 | 11 | 122 | 141 | no_obvious_regression |
| batch_screening | b6 | 4UTW-PPGP-VEN | likely_degraded | 2 | 1 | 20 | 8 | 87 | 101 | kv_collapse |
| batch_screening | b6 | TULW-YCYU-YVQ | likely_equivalent | 1 | 1 | 14 | 17 | 108 | 107 | no_obvious_regression |
| resolution_screening | r1000 | RELE-MX3A-MEN | likely_improved | 1 | 2 | 10 | 13 | 139 | 171 | useful_findings_improved |
| resolution_screening | r1000 | 4WTD-JFKA-JLE | likely_improved | 1 | 2 | 13 | 20 | 131 | 148 | useful_findings_improved,kv_adequacy_improved |
| resolution_screening | r1000 | 4UTW-PPGP-VEN | likely_degraded | 2 | 2 | 20 | 10 | 87 | 131 | kv_collapse |
| resolution_screening | r1000 | 6DRC-7KQL-9TJ | likely_improved | 1 | 3 | 4 | 12 | 159 | 141 | useful_findings_improved,kv_adequacy_improved |
| resolution_screening | r1000 | 4JYF-HEKU-VVV | likely_equivalent | 2 | 2 | 18 | 20 | 119 | 99 | no_obvious_regression |
| resolution_screening | r1000 | 46LP-7CN7-GG6 | likely_degraded | 3 | 1 | 106 | 18 | 120 | 149 | kv_collapse |
