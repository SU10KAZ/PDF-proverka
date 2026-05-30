# Audit Set Reuse Report

- Reused experiment dir: `/home/coder/projects/PDF-proverka/projects/214. Alia (ASTERUS)/KJ/13АВ-РД-КЖ5.17-23.1-К2 (1) (1).pdf/_experiments/pro_b6_r800_big_project_validation/20260422_210243`
- Reused timestamp: `20260422_210243`
- PDF: `13АВ-РД-КЖ5.17-23.1-К2 (1) (1).pdf`
- Project dir: `/home/coder/projects/PDF-proverka/projects/214. Alia (ASTERUS)/KJ/13АВ-РД-КЖ5.17-23.1-К2 (1) (1).pdf`
- Block source: `_output`
- Audit set size: 12
- Exact block IDs: `3TFL-TNRF-7G6`, `66NE-7DYY-GQN`, `9DH3-MEUR-DR4`, `947C-9UJT-RYU`, `9PPJ-YP6U-HV6`, `97AH-VUFP-LJJ`, `4CFG-LM4Y-7H4`, `66M4-Y69W-VCG`, `6PPA-4DDX-6FR`, `6R7N-7LRD-AUR`, `7DJ9-EQQ3-QMK`, `6P9T-Q7GT-N9J`
- New audit set created: no
- Reference rerun: no

## Reused Single-Block Reference
- Coverage: 100.00%
- Missing after reference run: 0
- Reasoning / parallelism / resolution: `high` / 2 / `r800`

## Prior b6 Smoke Used Only As Cost Prior
- Prior b6 smoke total cost: $0.2429
- Prior b6 smoke survived: False

## Source Audit Manifest
# Audit Set Manifest

- Total audit blocks: 12
- Heavy: 6
- Normal dense: 4
- Historical risky / heuristic: 2

## Historical sources
- `/home/coder/projects/PDF-proverka/projects/214. Alia (ASTERUS)/KJ/13АВ-РД-КЖ5.17-23.1-К2 (1) (1).pdf/_experiments/gemini_openrouter_stage02_singleblock/20260421_211303/escalation_sample_block_ids.json`
- `/home/coder/projects/PDF-proverka/projects/214. Alia (ASTERUS)/KJ/13АВ-РД-КЖ5.17-23.1-К2 (1) (1).pdf/_experiments/gemini_openrouter_stage02_budget/20260421_184420/pro_fallback_sample_ids.json`

| block_id | page | risk | size_kb | ocr_text_len | bucket | reason |
|----------|------|------|---------|--------------|--------|--------|
| 3TFL-TNRF-7G6 | 8 | heavy | 605.6 | 1227 | heavy | top_heavy_complexity_rank_1 |
| 66NE-7DYY-GQN | 7 | heavy | 476.2 | 1108 | heavy | top_heavy_complexity_rank_2 |
| 9DH3-MEUR-DR4 | 9 | heavy | 105.8 | 1495 | heavy | top_heavy_complexity_rank_3 |
| 947C-9UJT-RYU | 16 | heavy | 93.3 | 1971 | heavy | top_heavy_complexity_rank_4 |
| 9PPJ-YP6U-HV6 | 16 | heavy | 92.3 | 1956 | heavy | top_heavy_complexity_rank_5 |
| 97AH-VUFP-LJJ | 9 | heavy | 71.1 | 1070 | heavy | top_heavy_complexity_rank_6 |
| 4CFG-LM4Y-7H4 | 9 | normal | 77.7 | 1102 | normal_dense | top_normal_dense_rank_1 |
| 66M4-Y69W-VCG | 14 | normal | 71.8 | 1266 | normal_dense | top_normal_dense_rank_2 |
| 6PPA-4DDX-6FR | 13 | normal | 70.9 | 1091 | normal_dense | top_normal_dense_rank_3 |
| 6R7N-7LRD-AUR | 9 | normal | 70.1 | 1122 | normal_dense | top_normal_dense_rank_4 |
| 7DJ9-EQQ3-QMK | 9 | heavy | 63.3 | 1314 | historical_risky | historical_problematic_rank_1 |
| 6P9T-Q7GT-N9J | 7 | heavy | 53.3 | 1219 | historical_risky | historical_problematic_rank_2 |
