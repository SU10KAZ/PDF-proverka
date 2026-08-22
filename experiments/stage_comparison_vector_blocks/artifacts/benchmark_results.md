# Reproducible benchmark result

- Pairs: 10
- Blocks: 20
- Total extraction time: 123.611 s
- Total comparison time: 37.930 s

## Comparator statuses

- IDENTICAL: 1
- NEAR_IDENTICAL: 7
- STRUCTURE_CHANGED: 1
- STRUCTURE_SAME_VALUES_CHANGED: 1

## Pairs

| Pair | Discipline | Type | Status | Geometry | Text | Topology |
|---|---|---|---|---:|---:|---:|
| ss_scheme_text_changed | SS | low-current scheme; text-heavy; repeated symbols | STRUCTURE_SAME_VALUES_CHANGED | 0.866 | 0.914 | 0.733 |
| ss_plan_dense | SS | dense plan; complex block; repeated symbols | NEAR_IDENTICAL | 1.000 | 1.000 | 0.996 |
| ss_simple_node | SS | simple engineering node | IDENTICAL | 1.000 | 1.000 | 1.000 |
| ss_table_graphic | SS | table plus engineering graphic; text-heavy | NEAR_IDENTICAL | 0.997 | 0.929 | 0.861 |
| ar_plan | AR | architectural plan; repeated elements | NEAR_IDENTICAL | 1.000 | 1.000 | 0.999 |
| ar_wall_sections | AR | architectural sections; repeated details | NEAR_IDENTICAL | 1.000 | 1.000 | 0.967 |
| vk_plan | VK | plumbing plan; labels and repeated symbols | NEAR_IDENTICAL | 0.993 | 0.563 | 0.970 |
| vk_nodes | VK | plumbing engineering nodes; tall complex block | NEAR_IDENTICAL | 0.991 | 0.443 | 0.980 |
| vk_node_plan | VK | plumbing node and plan; mixed geometry | NEAR_IDENTICAL | 0.995 | 0.421 | 0.893 |
| eom_singleline_changed | EOM | electrical single-line scheme; repeated devices; table plus graphic | STRUCTURE_CHANGED | 0.174 | 0.251 | 0.610 |
