# signoise probe 7 — information content of `anchors`

Reproduce: `python -m experiments.stage_comparison_vector_architecture_opus.probes.signoise_07_anchor_information`

For up to 300 text spans per block (left side), the number of DISTINCT primitives whose distance to the span is within 0.002 of the nearest one — i.e. how many objects the anchor could equally well have picked.

| block | texts | mean tied primitives | median | % spans with ambiguous nearest | mean primitives within 0.035 | distinct anchor targets | top target share |
|---|---:|---:|---:|---:|---:|---:|---:|
| ar_plan | 836 | 2.583 | 2 | 68.3 % | 173.47 | 574 | 0.0144 |
| ar_wall_sections | 119 | 2.605 | 1 | 49.6 % | 54.19 | 108 | 0.0261 |
| eom_singleline_changed | 73 | 0.973 | 1 | 5.5 % | 2.34 | 32 | 0.0862 |
| ss_plan_dense | 522 | 2.947 | 3 | 94.3 % | 20.88 | 270 | 0.0249 |
| ss_scheme_text_changed | 100 | 1.07 | 1 | 12.0 % | 1.6 | 23 | 0.1461 |
| ss_simple_node | 31 | 0.903 | 1 | 0.0 % | 0.87 | 2 | 0.8148 |
| ss_table_graphic | 54 | 1.0 | 1 | 1.8 % | 1.5 | 4 | 0.6 |
| vk_node_plan | 243 | 1.802 | 2 | 76.5 % | 61.87 | 213 | 0.0165 |
| vk_nodes | 421 | 1.79 | 2 | 51.0 % | 185.79 | 300 | 0.0095 |
| vk_plan | 232 | 2.211 | 2 | 94.8 % | 20.59 | 203 | 0.0306 |
