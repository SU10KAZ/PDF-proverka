# signoise probe 8 — minimal sub-contract that reproduces every Track A verdict

Reproduce: `python -m experiments.stage_comparison_vector_architecture_opus.probes.signoise_08_minimal_contract`

Pairs where the stripped payload changed ANY part of the output: **0/10** []

Corpus: 119,671,126 B full -> 43,299,373 B minimal (**63.8 %** of VectorBlockDescription v0.1 is unread by the comparator).

| pair | status (full) | status (minimal) | output identical | full B | minimal B | reduction |
|---|---|---|:--:|---:|---:|---:|
| ss_scheme_text_changed | STRUCTURE_SAME_VALUES_CHANGED | STRUCTURE_SAME_VALUES_CHANGED | YES | 399,997 | 124,919 | 68.8 % |
| ss_plan_dense | NEAR_IDENTICAL | NEAR_IDENTICAL | YES | 18,758,717 | 7,335,438 | 60.9 % |
| ss_simple_node | IDENTICAL | IDENTICAL | YES | 50,178 | 9,920 | 80.2 % |
| ss_table_graphic | NEAR_IDENTICAL | NEAR_IDENTICAL | YES | 366,559 | 133,340 | 63.6 % |
| ar_plan | NEAR_IDENTICAL | NEAR_IDENTICAL | YES | 19,523,614 | 6,961,140 | 64.3 % |
| ar_wall_sections | NEAR_IDENTICAL | NEAR_IDENTICAL | YES | 24,883,658 | 8,932,730 | 64.1 % |
| vk_plan | NEAR_IDENTICAL | NEAR_IDENTICAL | YES | 4,148,607 | 1,438,211 | 65.3 % |
| vk_nodes | NEAR_IDENTICAL | NEAR_IDENTICAL | YES | 25,249,908 | 8,976,941 | 64.4 % |
| vk_node_plan | NEAR_IDENTICAL | NEAR_IDENTICAL | YES | 25,156,385 | 9,002,370 | 64.2 % |
| eom_singleline_changed | STRUCTURE_CHANGED | STRUCTURE_CHANGED | YES | 1,133,503 | 384,364 | 66.1 % |
