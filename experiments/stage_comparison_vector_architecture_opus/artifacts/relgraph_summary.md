# relgraph derived summary (all numbers measured)

## Crop noise floor per block

min similarity over {jitter 2 %, jitter 5 %, crop-edge 10 %} x {frame-only, re-extract}

| block | cov@0.005 | cov@0.01 | relG3 | relG1 | relG0 | entity | text |
|---|---|---|---|---|---|---|---|
| eom_singleline_changed | 0.1094 | 0.3883 | 0.5763 | 0.7420 | 0.8833 | 0.7785 | 0.8354 |
| ss_plan_dense | 0.5373 | 0.8570 | 0.1158 | 0.7009 | 0.7529 | 0.5657 | 0.8112 |
| ss_scheme_text_changed | 0.3021 | 0.4824 | 0.0459 | 0.7897 | 0.8422 | 0.1342 | 0.9118 |

## eom operating margin (same block: crop noise floor vs real pair)

| metric | crop noise floor | real changed pair | margin | headroom ratio |
|---|---:|---:|---:|---:|
| cov@0.005 | 0.1094 | 0.0698 | +0.0396 | 0.045 |
| cov@0.01 | 0.3883 | 0.1739 | +0.2144 | 0.35 |
| relG3 | 0.5763 | 0.0850 | +0.4913 | 1.16 |
| relG1 | 0.7420 | 0.3247 | +0.4174 | 1.618 |
| relG0 | 0.8833 | 0.6594 | +0.2238 | 1.918 |

## Frame-attributable fraction of the observed drop

(1 - control) / (1 - real); control = identical content, frame stretched by the pair's own aspect mismatch

| pair | distortion | cov@0.005 | cov@0.01 | rel_G3 | rel_G1 | rel_G0 | entity | text |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| eom_singleline_changed | x1.13713 | 0.9182 | 0.7635 | 0.8657 | 0.264 | 0.5034 | 0.8236 | 0.0 |
| ss_scheme_text_changed | x0.9931 | 0.0 | 0.0 | 0.0331 | 0.0118 | 0.0144 | 0.0 | 0.0 |
| vk_nodes | x1.03079 | 0.9728 | 0.014 | 0.6357 | 0.3834 | 0.3797 | 0.4054 | 0.0 |

## Payload size, 20 blocks, estimated tokens

- L0_raw: 16156271
- L1_normalized_primitives: 15385253
- L2_groups_topology: 378582
- L3_compact: 43757
- RG3_full_class: 309791
- RG1_shape_class: 24027
- RG1_plus_texts: 40072

## Benchmark coordinate validity

- pairs with correct coordinates: ['ar_plan', 'ar_wall_sections', 'ss_plan_dense', 'ss_scheme_text_changed', 'ss_simple_node', 'ss_table_graphic']
- pairs corrupted by page rotation: ['eom_singleline_changed', 'vk_node_plan', 'vk_nodes', 'vk_plan']
