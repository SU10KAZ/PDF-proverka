# signoise probe 3 — redundancy between VectorBlockDescription fields

Reproduce: `python -m experiments.stage_comparison_vector_architecture_opus.probes.signoise_03_redundancy`

n = 20 blocks.

## Suspected-duplicate field pairs

| field A | field B | Pearson r | Spearman ρ | identical values? | max relative gap |
|---|---|---:|---:|:--:|---:|
| `primitive_count` | `total_segment_count` | 0.1039 | 0.7802 | no | 0.9956 |
| `primitive_count` | `size_l0_bytes` | 0.9206 | 0.9692 | no | 0.9999 |
| `total_segment_count` | `size_l0_bytes` | 0.4839 | 0.8566 | no | 0.9971 |
| `total_segment_count` | `size_l1_bytes` | 0.4153 | 0.8551 | no | 0.9971 |
| `node_count` | `edge_count` | 0.9038 | 0.9142 | no | 0.6754 |
| `node_count` | `segments_total` | 0.6523 | 0.8476 | no | 0.9271 |
| `edge_count` | `segments_total` | 0.7387 | 0.8566 | no | 0.9054 |
| `node_count` | `segments_used` | 0.7762 | 0.8892 | no | 0.7994 |
| `edge_count` | `segments_used` | 0.9461 | 0.9005 | no | 0.6448 |
| `endpoints` | `node_count` | 0.4214 | 0.5999 | no | 0.9827 |
| `branch_points` | `t_junctions` | 0.9896 | 0.9985 | no | 0.7088 |
| `text_items` | `anchors` | 1.0000 | 1.0000 | YES | 0.0000 |
| `text_items` | `labels` | 0.7702 | 0.8917 | no | 0.6975 |
| `engineering_values` | `dimensions` | 1.0000 | 1.0000 | YES | 0.0000 |
| `connected_components` | `primitive_count` | -0.3232 | 0.1306 | no | 0.9980 |
| `size_l2_bytes` | `text_items` | 0.9533 | 0.9864 | no | 0.9982 |
| `size_l3_bytes` | `text_items` | 0.9027 | 0.9653 | no | 0.9841 |
| `size_l3_bytes` | `repeated_elements` | 0.6022 | 0.6907 | no | 1.0000 |

## All count-field pairs with |Spearman| >= 0.9

| A | B | ρ |
|---|---|---:|
| `total_segment_count` | `segments_total` | 1.000 |
| `closed_paths` | `closed_contours` | 1.000 |
| `text_items` | `anchors` | 1.000 |
| `engineering_values` | `dimensions` | 1.000 |
| `branch_points` | `t_junctions` | 0.998 |
| `size_l0_bytes` | `size_l1_bytes` | 0.994 |
| `primitive_count` | `stroke_paths` | 0.987 |
| `text_items` | `size_l2_bytes` | 0.986 |
| `anchors` | `size_l2_bytes` | 0.986 |
| `size_l2_bytes` | `size_l3_bytes` | 0.976 |
| `primitive_count` | `size_l0_bytes` | 0.969 |
| `primitive_count` | `size_l1_bytes` | 0.968 |
| `text_items` | `size_l3_bytes` | 0.965 |
| `anchors` | `size_l3_bytes` | 0.965 |
| `connected_components` | `endpoints` | 0.950 |
| `filled_paths` | `closed_paths` | 0.949 |
| `filled_paths` | `closed_contours` | 0.949 |
| `stroke_paths` | `size_l0_bytes` | 0.941 |
| `stroke_paths` | `size_l1_bytes` | 0.941 |
| `total_segment_count` | `segments_used` | 0.936 |
| `segments_total` | `segments_used` | 0.936 |
| `edge_count` | `t_junctions` | 0.935 |
| `edge_count` | `branch_points` | 0.932 |
| `segments_used` | `size_l0_bytes` | 0.926 |
| `segments_used` | `size_l1_bytes` | 0.925 |
| `segments_used` | `x_crossings_unconnected` | 0.924 |
| `closed_paths` | `repeated_elements` | 0.914 |
| `node_count` | `edge_count` | 0.914 |
| `closed_contours` | `repeated_elements` | 0.914 |
| `node_count` | `t_junctions` | 0.910 |
| `edge_count` | `x_crossings_unconnected` | 0.910 |
| `segments_used` | `branch_points` | 0.910 |
| `segments_used` | `t_junctions` | 0.910 |
| `segments_used` | `hatch_like_structures` | 0.907 |
| `node_count` | `branch_points` | 0.905 |
| `x_crossings_unconnected` | `size_l2_bytes` | 0.905 |
| `x_crossings_unconnected` | `size_l3_bytes` | 0.902 |
| `edge_count` | `segments_used` | 0.900 |

## structural_signature levels over all C(20,2) = 190 cross pairings

- pairings where level_2 equality and level_3 equality AGREE: **190/190**
- pairings where they DISAGREE: **0/190**

| (l1_eq, l2_eq, l3_eq) | pairings |
|---|---:|
| (False, False, False) | 189 |
| (True, True, True) | 1 |

## The 10 benchmark pairs (true counterparts)

| pair | l1 equal | l2 equal | l3 equal |
|---|:--:|:--:|:--:|
| ar_plan | False | False | False |
| ar_wall_sections | False | False | False |
| eom_singleline_changed | False | False | False |
| ss_plan_dense | False | False | False |
| ss_scheme_text_changed | False | False | False |
| ss_simple_node | True | True | True |
| ss_table_graphic | False | False | False |
| vk_node_plan | False | False | False |
| vk_nodes | False | False | False |
| vk_plan | False | False | False |
