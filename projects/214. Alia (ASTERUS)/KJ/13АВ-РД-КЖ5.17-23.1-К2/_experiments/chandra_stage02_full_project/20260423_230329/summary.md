# Chandra Stage02 Full Project

- Model: `qwen/qwen3.6-35b-a3b`
- Total blocks in index: `215`
- Prepared images: `215`
- Stage-02 baseline blocks: `209`
- Prompt variant: `compact_recall`
- Max output tokens: `900`
- Avg elapsed: `5.06` sec/block
- Median elapsed: `5.16` sec/block
- Parse ok: `209` / `215`
- Blocks with findings: candidate `161`, baseline `14`
- Blocks where baseline had findings but candidate had none: `1`
- Blocks where candidate had findings but baseline had none: `148`
- Verdict counts vs stage-02 baseline: `{'uncertain': 148, 'fail_parse': 6, 'likely_improved': 53, 'likely_degraded': 1, 'baseline_missing': 6, 'equivalent': 1}`

| Verdict | Count |
|---|---:|
| `baseline_missing` | 6 |
| `equivalent` | 1 |
| `fail_parse` | 6 |
| `likely_degraded` | 1 |
| `likely_improved` | 53 |
| `uncertain` | 148 |

## Shortlists

- Parse fails: 66NE-7DYY-GQN, 3TFL-TNRF-7G6, 6R7N-7LRD-AUR, 9DH3-MEUR-DR4, 9PPJ-YP6U-HV6, 947C-9UJT-RYU
- Likely degraded: 9PCA-GQGT-JTN
- Uncertain: A9M4-EJ7R-MKH, 9RD7-EPAA-NJQ, 7EN4-ATAF-MM7, 4PJJ-YHH6-KK7, LFYA-MR6V-D36, H3VM-7JM6-6HM, CWVU-XLAY-GHP, 69E6-U9HN-NQE, C3EQ-CLF7-JMK, 33N9-7JYR-4JQ, AC3T-RYW4-UD4, 6WQH-PM9T-C4Y, 74WW-EAML-LKD, 4LY3-Y3PU-YTQ, TGNP-L334-33C, 6EXK-YA6J-KPU, 6HAF-43VX-V6V, 6P9T-Q7GT-N9J, 9C4E-9M37-MD4, 4MJP-RPV6-4G3
