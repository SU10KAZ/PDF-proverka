# signoise probe 5 — verdict stability under 0.5 % / 2 % bbox perturbation

Reproduce: `python -m experiments.stage_comparison_vector_architecture_opus.probes.signoise_05_perturbation`

The RIGHT block is re-extracted with a perturbed crop; LEFT is the unchanged Track A description.

`cross` = left vs perturbed right (real pair). `self` = left vs perturbed LEFT, i.e. the same block of the same PDF compared with itself under crop jitter — the pure false-positive floor.

| mode/pair | perturbation | status | diff lines | geometry | text | topology | patterns | prims | segs | texts |
|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| cross/ss_simple_node | `identity` | **IDENTICAL** | 0 | 1.0000 | 1.0000 | 1.0000 | 1.0000 | 2 | 45 | 31 |
| cross/ss_simple_node | `shift_0.5` | **IDENTICAL** | 0 | 1.0000 | 1.0000 | 1.0000 | 1.0000 | 2 | 45 | 31 |
| cross/ss_simple_node | `shift_2` | **IDENTICAL** | 0 | 1.0000 | 1.0000 | 1.0000 | 1.0000 | 2 | 45 | 31 |
| cross/ss_simple_node | `scale_2` | **IDENTICAL** | 0 | 1.0000 | 1.0000 | 1.0000 | 1.0000 | 2 | 45 | 31 |
| cross/ss_simple_node | `scale_-2` | **IDENTICAL** | 0 | 1.0000 | 1.0000 | 1.0000 | 1.0000 | 2 | 45 | 31 |
| cross/ss_scheme_text_changed | `identity` | **STRUCTURE_SAME_VALUES_CHANGED** | 12 | 0.8664 | 0.9137 | 0.7327 | 0.2000 | 159 | 1190 | 97 |
| cross/ss_scheme_text_changed | `shift_0.5` | **STRUCTURE_CHANGED** | 12 | 0.8564 | 0.9455 | 0.7327 | 0.2000 | 159 | 1190 | 97 |
| cross/ss_scheme_text_changed | `shift_2` | **STRUCTURE_CHANGED** | 11 | 0.8055 | 0.9137 | 0.7320 | 0.2000 | 160 | 1191 | 97 |
| cross/ss_scheme_text_changed | `scale_2` | **STRUCTURE_CHANGED** | 12 | 0.8650 | 0.9455 | 0.5939 | 0.2000 | 160 | 1193 | 97 |
| cross/ss_scheme_text_changed | `scale_-2` | **STRUCTURE_SAME_VALUES_CHANGED** | 12 | 0.8584 | 0.9137 | 0.7328 | 0.2000 | 158 | 1189 | 97 |
| cross/eom_singleline_changed | `identity` | **STRUCTURE_CHANGED** | 25 | 0.1739 | 0.2506 | 0.6102 | 0.0000 | 164 | 1868 | 299 |
| cross/eom_singleline_changed | `shift_0.5` | **STRUCTURE_CHANGED** | 25 | 0.1849 | 0.2565 | 0.6078 | 0.0000 | 163 | 1867 | 299 |
| cross/eom_singleline_changed | `shift_2` | **STRUCTURE_CHANGED** | 25 | 0.1928 | 0.2500 | 0.6078 | 0.0000 | 163 | 1867 | 305 |
| cross/eom_singleline_changed | `scale_2` | **STRUCTURE_CHANGED** | 23 | 0.1416 | 0.2620 | 0.6190 | 0.0000 | 218 | 2252 | 308 |
| cross/eom_singleline_changed | `scale_-2` | **STRUCTURE_CHANGED** | 25 | 0.1915 | 0.2565 | 0.6131 | 0.0000 | 163 | 1857 | 299 |
| cross/ss_table_graphic | `identity` | **NEAR_IDENTICAL** | 10 | 0.9965 | 0.9292 | 0.8611 | 1.0000 | 7 | 1599 | 58 |
| cross/ss_table_graphic | `shift_0.5` | **NEAR_IDENTICAL** | 8 | 0.9962 | 0.9784 | 0.8611 | 1.0000 | 7 | 1599 | 53 |
| cross/ss_table_graphic | `shift_2` | **STRUCTURE_CHANGED** | 7 | 0.8089 | 0.9912 | 0.8666 | 1.0000 | 7 | 1598 | 55 |
| cross/ss_table_graphic | `scale_2` | **STRUCTURE_CHANGED** | 6 | 0.9339 | 0.8842 | 0.9519 | 1.0000 | 12 | 1804 | 64 |
| cross/ss_table_graphic | `scale_-2` | **NEAR_IDENTICAL** | 7 | 0.9965 | 0.9462 | 0.8660 | 1.0000 | 7 | 1599 | 51 |
| self/ss_simple_node | `identity` | **IDENTICAL** | 0 | 1.0000 | 1.0000 | 1.0000 | 1.0000 | 2 | 45 | 31 |
| self/ss_simple_node | `shift_0.5` | **IDENTICAL** | 0 | 1.0000 | 1.0000 | 1.0000 | 1.0000 | 2 | 45 | 31 |
| self/ss_simple_node | `shift_2` | **IDENTICAL** | 0 | 1.0000 | 1.0000 | 1.0000 | 1.0000 | 2 | 45 | 31 |
| self/ss_simple_node | `scale_2` | **IDENTICAL** | 0 | 1.0000 | 1.0000 | 1.0000 | 1.0000 | 2 | 45 | 31 |
| self/ss_simple_node | `scale_-2` | **IDENTICAL** | 0 | 1.0000 | 1.0000 | 1.0000 | 1.0000 | 2 | 45 | 31 |
| self/ss_scheme_text_changed | `identity` | **IDENTICAL** | 0 | 1.0000 | 1.0000 | 1.0000 | 1.0000 | 39 | 710 | 100 |
| self/ss_scheme_text_changed | `shift_0.5` | **NEAR_IDENTICAL** | 0 | 0.9986 | 1.0000 | 1.0000 | 1.0000 | 39 | 710 | 100 |
| self/ss_scheme_text_changed | `shift_2` | **NEAR_IDENTICAL** | 1 | 0.9979 | 1.0000 | 0.9991 | 1.0000 | 40 | 711 | 100 |
| self/ss_scheme_text_changed | `scale_2` | **NEAR_IDENTICAL** | 1 | 0.9986 | 1.0000 | 0.9331 | 1.0000 | 39 | 712 | 100 |
| self/ss_scheme_text_changed | `scale_-2` | **NEAR_IDENTICAL** | 2 | 0.9993 | 1.0000 | 0.8880 | 1.0000 | 38 | 709 | 100 |
| self/eom_singleline_changed | `identity` | **IDENTICAL** | 0 | 1.0000 | 1.0000 | 1.0000 | 1.0000 | 704 | 3297 | 73 |
| self/eom_singleline_changed | `shift_0.5` | **NEAR_IDENTICAL** | 4 | 0.9980 | 0.9863 | 0.9900 | 1.0000 | 714 | 3308 | 73 |
| self/eom_singleline_changed | `shift_2` | **NEAR_IDENTICAL** | 5 | 0.9983 | 0.9510 | 0.9790 | 0.9167 | 721 | 3210 | 70 |
| self/eom_singleline_changed | `scale_2` | **NEAR_IDENTICAL** | 6 | 0.9959 | 0.9664 | 0.9749 | 0.6829 | 720 | 3315 | 76 |
| self/eom_singleline_changed | `scale_-2` | **NEAR_IDENTICAL** | 3 | 0.9974 | 0.9790 | 0.9932 | 0.6667 | 696 | 3260 | 70 |
| self/ss_table_graphic | `identity` | **IDENTICAL** | 0 | 1.0000 | 1.0000 | 1.0000 | 1.0000 | 11 | 1609 | 54 |
| self/ss_table_graphic | `shift_0.5` | **NEAR_IDENTICAL** | 7 | 0.9965 | 0.9358 | 0.8679 | 1.0000 | 7 | 1598 | 55 |
| self/ss_table_graphic | `shift_2` | **STRUCTURE_SAME_VALUES_CHANGED** | 9 | 0.9965 | 0.9147 | 0.8679 | 1.0000 | 7 | 1598 | 54 |
| self/ss_table_graphic | `scale_2` | **STRUCTURE_CHANGED** | 7 | 0.9461 | 0.8793 | 0.9444 | 1.0000 | 12 | 1818 | 62 |
| self/ss_table_graphic | `scale_-2` | **NEAR_IDENTICAL** | 7 | 0.9965 | 0.9803 | 0.8556 | 1.0000 | 7 | 1598 | 53 |
