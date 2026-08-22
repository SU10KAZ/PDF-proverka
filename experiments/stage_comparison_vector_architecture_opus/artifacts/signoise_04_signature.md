# signoise probe 4 — is structural_signature usable for candidate search?

Reproduce: `python -m experiments.stage_comparison_vector_architecture_opus.probes.signoise_04_signature`
(needs `signoise_03_block_features.json` from probe 3)

## A. Exact hash keys over the 20 blocks (10 true П↔РД counterpart pairs)

| key | distinct values / 20 | true pairs recovered | cross-pair collisions | largest bucket |
|---|---:|---:|---:|---:|
| `structural_signature.l1` | 19 | 1/10 (recall 0.10) | 0 | 2 |
| `structural_signature.l2` | 19 | 1/10 (recall 0.10) | 0 | 2 |
| `structural_signature.l3` | 19 | 1/10 (recall 0.10) | 0 | 2 |
| `level_3_payload.primitive_types only` | 16 | 4/10 (recall 0.40) | 0 | 2 |
| `level_3_payload.degree_histogram only` | 19 | 1/10 (recall 0.10) | 0 | 2 |
| `signoise coarse bucket hash` | 13 | 7/10 (recall 0.70) | 0 | 2 |

## B. Tolerant nearest-neighbour retrieval with a coarse descriptor (signoise design)

34 dims: 5-bin angle histogram + 4x4 spatial occupancy of segment midpoints + log(segments/components/texts) + endpoint/branch/T-junction/closed ratios + text-category shares + mean & p90 segment length + aspect ratio. Z-scored across the 20 blocks, Euclidean distance.

| descriptor | top-1 accuracy | mean rank of true counterpart | median | worst |
|---|---:|---:|---:|---:|
| `full` | 19/20 = 0.95 | 1.05 | 1 | 2 |
| `angles_only` | 18/20 = 0.90 | 1.85 | 1 | 11 |
| `grid_only` | 18/20 = 0.90 | 1.7 | 1 | 10 |
| `counts_only` | 18/20 = 0.90 | 1.3 | 1 | 5 |
| `topology_only` | 17/20 = 0.85 | 1.4 | 1 | 5 |
| `no_grid` | 20/20 = 1.00 | 1.0 | 1 | 1 |
| `no_counts` | 18/20 = 0.90 | 1.2 | 1 | 3 |

### full descriptor, per query

| query block | true counterpart | rank | nearest neighbour found |
|---|---|---:|---|
| ar_plan/left | ar_plan/right | 1 | ar_plan/right |
| ar_plan/right | ar_plan/left | 1 | ar_plan/left |
| ar_wall_sections/left | ar_wall_sections/right | 1 | ar_wall_sections/right |
| ar_wall_sections/right | ar_wall_sections/left | 1 | ar_wall_sections/left |
| eom_singleline_changed/left | eom_singleline_changed/right | 1 | eom_singleline_changed/right |
| eom_singleline_changed/right | eom_singleline_changed/left | 2 | vk_node_plan/left |
| ss_plan_dense/left | ss_plan_dense/right | 1 | ss_plan_dense/right |
| ss_plan_dense/right | ss_plan_dense/left | 1 | ss_plan_dense/left |
| ss_scheme_text_changed/left | ss_scheme_text_changed/right | 1 | ss_scheme_text_changed/right |
| ss_scheme_text_changed/right | ss_scheme_text_changed/left | 1 | ss_scheme_text_changed/left |
| ss_simple_node/left | ss_simple_node/right | 1 | ss_simple_node/right |
| ss_simple_node/right | ss_simple_node/left | 1 | ss_simple_node/left |
| ss_table_graphic/left | ss_table_graphic/right | 1 | ss_table_graphic/right |
| ss_table_graphic/right | ss_table_graphic/left | 1 | ss_table_graphic/left |
| vk_node_plan/left | vk_node_plan/right | 1 | vk_node_plan/right |
| vk_node_plan/right | vk_node_plan/left | 1 | vk_node_plan/left |
| vk_nodes/left | vk_nodes/right | 1 | vk_nodes/right |
| vk_nodes/right | vk_nodes/left | 1 | vk_nodes/left |
| vk_plan/left | vk_plan/right | 1 | vk_plan/right |
| vk_plan/right | vk_plan/left | 1 | vk_plan/left |
