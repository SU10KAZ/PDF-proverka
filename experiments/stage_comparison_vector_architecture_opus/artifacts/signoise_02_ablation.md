# signoise probe 2 — ablation of field groups against the Track A comparator

Reproduce: `python -m experiments.stage_comparison_vector_architecture_opus.probes.signoise_02_ablation`

Baseline statuses: ss_scheme_text_changed=STRUCTURE_SAME_VALUES_CHANGED, ss_plan_dense=NEAR_IDENTICAL, ss_simple_node=IDENTICAL, ss_table_graphic=NEAR_IDENTICAL, ar_plan=NEAR_IDENTICAL, ar_wall_sections=NEAR_IDENTICAL, vk_plan=NEAR_IDENTICAL, vk_nodes=NEAR_IDENTICAL, vk_node_plan=NEAR_IDENTICAL, eom_singleline_changed=STRUCTURE_CHANGED

| ablation | what was neutralised | pairs whose STATUS changed | pairs whose `differences` text changed | pairs whose any score changed |
|---|---|---:|---:|---:|
| `anchors_blank` | anchors = [] on both sides | **0/10**  | 0/10  | 0/10 |
| `repeated_elements_blank` | repeated_elements = [] on both sides | **0/10**  | 8/10 ss_scheme_text_changed,ss_plan_dense,ar_plan,ar_wall_sections,vk_plan,vk_nodes,vk_node_plan,eom_singleline_changed | 8/10 |
| `hatch_blank` | hatch_like_structures = [] | **0/10**  | 0/10  | 0/10 |
| `dimensions_labels_blank` | dimensions = labels = [] | **0/10**  | 0/10  | 0/10 |
| `size_metrics_blank` | size_metrics = {} | **0/10**  | 0/10  | 0/10 |
| `primitive_summary_equalized` | right.primitive_summary.primitive_count forced equal to left | **0/10**  | 4/10 ss_scheme_text_changed,ss_table_graphic,ar_plan,eom_singleline_changed | 0/10 |
| `texts_blank` | texts = [] on both sides | **1/10** ss_scheme_text_changed | 6/10 ss_scheme_text_changed,ss_table_graphic,vk_plan,vk_nodes,vk_node_plan,eom_singleline_changed | 6/10 |
| `texts_categories_flattened` | every text category forced to 'label' | **0/10**  | 4/10 vk_plan,vk_nodes,vk_node_plan,eom_singleline_changed | 0/10 |
| `texts_positions_zeroed` | text x_norm/y_norm forced to 0 | **1/10** ss_scheme_text_changed | 5/10 ss_table_graphic,vk_plan,vk_nodes,vk_node_plan,eom_singleline_changed | 4/10 |
| `topology_forced_1.0` | monkeypatch _topology_diff -> similarity 1.0 | **0/10**  | 7/10 ss_scheme_text_changed,ss_table_graphic,ar_wall_sections,vk_plan,vk_nodes,vk_node_plan,eom_singleline_changed | 9/10 |
| `topology_forced_0.0` | monkeypatch _topology_diff -> similarity 0.0 | **7/10** ss_plan_dense,ss_table_graphic,ar_plan,ar_wall_sections,vk_plan,vk_nodes,vk_node_plan | 10/10 ss_scheme_text_changed,ss_plan_dense,ss_simple_node,ss_table_graphic,ar_plan,ar_wall_sections,vk_plan,vk_nodes,vk_node_plan,eom_singleline_changed | 10/10 |
| `topology_equalized` | right topology counts copied from left (9 comparator keys) | **0/10**  | 7/10 ss_scheme_text_changed,ss_table_graphic,ar_wall_sections,vk_plan,vk_nodes,vk_node_plan,eom_singleline_changed | 9/10 |
| `signature_l1_broken` | level_1_exact_vector forced unequal | **1/10** ss_simple_node | 0/10  | 1/10 |
| `signature_l2_l3_blank` | level_2 / level_3 signatures forced equal-and-meaningless | **0/10**  | 0/10  | 9/10 |
| `primitive_style_blank` | every primitive.style = {} (27.0 % of corpus bytes) | **0/10**  | 0/10  | 0/10 |
| `primitive_raw_blank` | every primitive.raw = {} (22.8 % of corpus bytes) | **0/10**  | 0/10  | 0/10 |
| `primitive_provenance_blank` | primitive.source_kinds / item_indexes = [] (6.6 % of corpus bytes) | **0/10**  | 0/10  | 0/10 |
| `extraction_item_counts_blank` | geometry.extraction.source_item_counts = {} (kills encoding-rewrite heuristic) | **1/10** ss_scheme_text_changed | 0/10  | 1/10 |
| `quality_notes_ambiguities_blank` | quality_notes / ambiguities / coordinate_system / source emptied | **0/10**  | 0/10  | 0/10 |
