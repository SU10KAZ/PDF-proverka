# signoise probe 9 — style palette and level-3 payload composition

Reproduce: `python -m experiments.stage_comparison_vector_architecture_opus.probes.signoise_09_style_palette`

## A. `primitive.style` (27.0 % of the whole corpus) is ~247x redundant

160,221 primitives carry 30,899,153 B of style dicts drawn from 660 distinct values (summed per block) = 125,208 B of palette; compression factor **246.8x** before indices.

| block | primitives | distinct styles | style bytes | palette bytes |
|---|---:|---:|---:|---:|
| ar_plan/left | 14,800 | 42 | 2,847,024 | 7,957 |
| ar_plan/right | 14,799 | 42 | 2,846,833 | 7,957 |
| ar_wall_sections/left | 20,000 | 15 | 3,825,032 | 2,771 |
| ar_wall_sections/right | 20,000 | 15 | 3,825,032 | 2,771 |
| eom_singleline_changed/left | 704 | 8 | 134,338 | 1,516 |
| eom_singleline_changed/right | 164 | 5 | 28,669 | 871 |
| ss_plan_dense/left | 1,604 | 17 | 285,635 | 3,008 |
| ss_plan_dense/right | 1,604 | 17 | 285,635 | 3,008 |
| ss_scheme_text_changed/left | 39 | 8 | 6,827 | 1,402 |
| ss_scheme_text_changed/right | 159 | 9 | 26,747 | 1,568 |
| ss_simple_node/left | 2 | 2 | 349 | 349 |
| ss_simple_node/right | 2 | 2 | 349 | 349 |
| ss_table_graphic/left | 11 | 5 | 1,924 | 874 |
| ss_table_graphic/right | 7 | 5 | 1,224 | 874 |
| vk_node_plan/left | 20,000 | 112 | 3,887,002 | 21,462 |
| vk_node_plan/right | 20,000 | 115 | 3,884,635 | 22,047 |
| vk_nodes/left | 20,000 | 106 | 3,890,696 | 20,495 |
| vk_nodes/right | 20,000 | 100 | 3,891,347 | 19,317 |
| vk_plan/left | 3,163 | 17 | 615,009 | 3,223 |
| vk_plan/right | 3,163 | 18 | 614,846 | 3,389 |

## B. What the level-3 payload (the thing Track A actually sent to the model) is made of

20 blocks, 179,060 B total.

| key | bytes | share |
|---|---:|---:|
| `texts` | 132,233 | 73.85 % |
| `signatures` | 17,534 | 9.79 % |
| `patterns` | 13,572 | 7.58 % |
| `topology` | 8,503 | 4.75 % |
| `summary` | 4,940 | 2.76 % |
| `hatch_candidates` | 1,938 | 1.08 % |
| `quality` | 460 | 0.26 % |
