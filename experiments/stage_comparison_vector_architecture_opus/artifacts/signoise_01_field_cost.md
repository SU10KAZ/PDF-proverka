# signoise probe 1 — field byte cost of VectorBlockDescription v0.1

Reproduce: `python -m experiments.stage_comparison_vector_architecture_opus.probes.signoise_01_field_cost`

Corpus: 20 descriptions, 119,671,126 bytes of compact JSON (114.1 MiB).

## Top-level keys

| key | bytes (20 blocks) | share % |
|---|---:|---:|
| `geometry` | 116,603,960 | 97.4370 |
| `texts` | 1,311,348 | 1.0958 |
| `anchors` | 687,850 | 0.5748 |
| `labels` | 265,784 | 0.2221 |
| `topology` | 225,406 | 0.1884 |
| `size_metrics` | 186,368 | 0.1557 |
| `repeated_elements` | 170,113 | 0.1422 |
| `dimensions` | 94,117 | 0.0786 |
| `hatch_like_structures` | 70,145 | 0.0586 |
| `structural_signature` | 17,714 | 0.0148 |
| `ambiguities` | 10,040 | 0.0084 |
| `source` | 8,847 | 0.0074 |
| `coordinate_system` | 7,000 | 0.0058 |
| `primitive_summary` | 5,120 | 0.0043 |
| `quality_notes` | 1,330 | 0.0011 |
| `bbox_norm_on_page` | 1,048 | 0.0009 |
| `block_id` | 991 | 0.0008 |
| `bbox` | 947 | 0.0008 |
| `schema_version` | 920 | 0.0008 |
| `vector_quality` | 580 | 0.0005 |
| `polygon_norm_on_page` | 560 | 0.0005 |
| `research_only` | 420 | 0.0004 |
| `page_index` | 309 | 0.0003 |
| `page` | 189 | 0.0002 |

## `ambiguities` sub-keys (9,720 B, 0.008 % of corpus)

| sub-key | bytes | % of corpus | % of group |
|---|---:|---:|---:|
| `<scalar>` | 9,720 | 0.0081 | 100.00 |

## `anchors` sub-keys (676,756 B, 0.566 % of corpus)

| sub-key | bytes | % of corpus | % of group |
|---|---:|---:|---:|
| `geometry_id` | 163,582 | 0.1367 | 24.17 |
| `relation` | 161,442 | 0.1349 | 23.86 |
| `distance_norm` | 129,275 | 0.1080 | 19.10 |
| `text_id` | 112,057 | 0.0936 | 16.56 |
| `confidence` | 110,400 | 0.0923 | 16.31 |

## `coordinate_system` sub-keys (6,560 B, 0.005 % of corpus)

| sub-key | bytes | % of corpus | % of group |
|---|---:|---:|---:|
| `formula` | 1,860 | 0.0016 | 28.35 |
| `normalization_does_not_use` | 1,620 | 0.0014 | 24.70 |
| `normalization_removes` | 1,420 | 0.0012 | 21.65 |
| `normalized` | 880 | 0.0007 | 13.41 |
| `raw` | 780 | 0.0007 | 11.89 |

## `dimensions` sub-keys (92,849 B, 0.078 % of corpus)

| sub-key | bytes | % of corpus | % of group |
|---|---:|---:|---:|
| `classification` | 28,800 | 0.0241 | 31.02 |
| `bbox_norm` | 21,829 | 0.0182 | 23.51 |
| `text` | 18,001 | 0.0150 | 19.39 |
| `geometry_id` | 14,340 | 0.0120 | 15.44 |
| `text_id` | 9,879 | 0.0083 | 10.64 |

## `geometry` sub-keys (116,603,700 B, 97.437 % of corpus)

| sub-key | bytes | % of corpus | % of group |
|---|---:|---:|---:|
| `primitives` | 116,599,942 | 97.4336 | 100.00 |
| `extraction` | 3,758 | 0.0031 | 0.00 |

## `geometry.primitives[]` sub-keys (116,279,200 B, 97.166 % of corpus)

| sub-key | bytes | % of corpus | % of group |
|---|---:|---:|---:|
| `style` | 32,341,142 | 27.0250 | 27.81 |
| `raw` | 27,299,148 | 22.8118 | 23.48 |
| `normalized` | 24,215,069 | 20.2347 | 20.82 |
| `source_kinds` | 4,083,087 | 3.4119 | 3.51 |
| `item_indexes` | 3,823,323 | 3.1949 | 3.29 |
| `angle_degrees` | 3,682,798 | 3.0774 | 3.17 |
| `id` | 3,579,683 | 2.9913 | 3.08 |
| `length_norm` | 3,500,178 | 2.9248 | 3.01 |
| `drawing_index` | 3,487,247 | 2.9140 | 3.00 |
| `segment_count` | 2,885,372 | 2.4111 | 2.48 |
| `length` | 2,717,847 | 2.2711 | 2.34 |
| `closed` | 2,400,924 | 2.0063 | 2.06 |
| `type` | 2,263,382 | 1.8913 | 1.95 |

## `hatch_like_structures` sub-keys (68,715 B, 0.057 % of corpus)

| sub-key | bytes | % of corpus | % of group |
|---|---:|---:|---:|
| `interpretation` | 24,516 | 0.0205 | 35.68 |
| `candidate_id` | 13,546 | 0.0113 | 19.71 |
| `median_length_norm` | 13,118 | 0.0110 | 19.09 |
| `segment_count` | 8,889 | 0.0074 | 12.94 |
| `angle_degrees` | 8,646 | 0.0072 | 12.58 |

## `labels` sub-keys (259,612 B, 0.217 % of corpus)

| sub-key | bytes | % of corpus | % of group |
|---|---:|---:|---:|
| `bbox_norm` | 135,489 | 0.1132 | 52.19 |
| `text` | 62,816 | 0.0525 | 24.20 |
| `text_id` | 61,307 | 0.0512 | 23.61 |

## `primitive_summary` sub-keys (4,680 B, 0.004 % of corpus)

| sub-key | bytes | % of corpus | % of group |
|---|---:|---:|---:|
| `primitive_types` | 1,246 | 0.0010 | 26.62 |
| `total_segment_count` | 545 | 0.0005 | 11.65 |
| `connected_components` | 525 | 0.0004 | 11.22 |
| `engineering_values` | 469 | 0.0004 | 10.02 |
| `primitive_count` | 452 | 0.0004 | 9.66 |
| `stroke_paths` | 391 | 0.0003 | 8.35 |
| `closed_paths` | 359 | 0.0003 | 7.67 |
| `filled_paths` | 359 | 0.0003 | 7.67 |
| `text_items` | 334 | 0.0003 | 7.14 |

## `quality_notes` sub-keys (960 B, 0.001 % of corpus)

| sub-key | bytes | % of corpus | % of group |
|---|---:|---:|---:|
| `<scalar>` | 960 | 0.0008 | 100.00 |

## `repeated_elements` sub-keys (168,690 B, 0.141 % of corpus)

| sub-key | bytes | % of corpus | % of group |
|---|---:|---:|---:|
| `instances` | 109,462 | 0.0915 | 64.89 |
| `pattern_id` | 17,604 | 0.0147 | 10.44 |
| `primitive_type` | 14,027 | 0.0117 | 8.32 |
| `instances_truncated` | 13,690 | 0.0114 | 8.12 |
| `segment_count` | 8,933 | 0.0075 | 5.30 |
| `count` | 4,974 | 0.0042 | 2.95 |

## `size_metrics` sub-keys (186,028 B, 0.155 % of corpus)

| sub-key | bytes | % of corpus | % of group |
|---|---:|---:|---:|
| `compact_payload` | 179,440 | 0.1499 | 96.46 |
| `level_1_normalized_primitives` | 1,786 | 0.0015 | 0.96 |
| `level_3_compact_description` | 1,633 | 0.0014 | 0.88 |
| `level_2_groups_topology` | 1,600 | 0.0013 | 0.86 |
| `level_0_raw_vector` | 1,569 | 0.0013 | 0.84 |

## `source` sub-keys (8,627 B, 0.007 % of corpus)

| sub-key | bytes | % of corpus | % of group |
|---|---:|---:|---:|
| `pdf` | 3,362 | 0.0028 | 38.97 |
| `pdf_sha256` | 1,600 | 0.0013 | 18.55 |
| `excluded_sources` | 1,400 | 0.0012 | 16.23 |
| `source_layers` | 1,340 | 0.0011 | 15.53 |
| `page_height` | 471 | 0.0004 | 5.46 |
| `page_width` | 454 | 0.0004 | 5.26 |

## `structural_signature` sub-keys (17,214 B, 0.014 % of corpus)

| sub-key | bytes | % of corpus | % of group |
|---|---:|---:|---:|
| `level_3_payload` | 11,534 | 0.0096 | 67.00 |
| `level_2_normalized_geometry` | 1,940 | 0.0016 | 11.27 |
| `level_3_structural_topology` | 1,940 | 0.0016 | 11.27 |
| `level_1_exact_vector` | 1,800 | 0.0015 | 10.46 |

## `texts` sub-keys (1,300,294 B, 1.087 % of corpus)

| sub-key | bytes | % of corpus | % of group |
|---|---:|---:|---:|
| `bbox` | 268,131 | 0.2241 | 20.62 |
| `bbox_norm` | 247,080 | 0.2065 | 19.00 |
| `category` | 112,815 | 0.0943 | 8.68 |
| `font` | 111,839 | 0.0935 | 8.60 |
| `text` | 106,030 | 0.0886 | 8.15 |
| `font_size` | 98,331 | 0.0822 | 7.56 |
| `x_norm` | 91,626 | 0.0766 | 7.05 |
| `y_norm` | 91,616 | 0.0766 | 7.05 |
| `rotation` | 87,904 | 0.0735 | 6.76 |
| `id` | 84,922 | 0.0710 | 6.53 |

## `topology` sub-keys (225,146 B, 0.188 % of corpus)

| sub-key | bytes | % of corpus | % of group |
|---|---:|---:|---:|
| `components` | 212,297 | 0.1774 | 94.29 |
| `degree_histogram` | 6,143 | 0.0051 | 2.73 |
| `x_crossings_unconnected` | 601 | 0.0005 | 0.27 |
| `components_truncated` | 570 | 0.0005 | 0.25 |
| `crossings_truncated` | 558 | 0.0005 | 0.25 |
| `connected_components` | 525 | 0.0004 | 0.23 |
| `tolerance_norm` | 480 | 0.0004 | 0.21 |
| `segments_capped` | 470 | 0.0004 | 0.21 |
| `segments_total` | 445 | 0.0004 | 0.20 |
| `closed_contours` | 419 | 0.0004 | 0.19 |
| `segments_used` | 415 | 0.0003 | 0.18 |
| `nested_contours` | 412 | 0.0003 | 0.18 |
| `branch_points` | 410 | 0.0003 | 0.18 |
| `t_junctions` | 377 | 0.0003 | 0.17 |
| `edge_count` | 355 | 0.0003 | 0.16 |
| `node_count` | 355 | 0.0003 | 0.16 |
| `endpoints` | 314 | 0.0003 | 0.14 |

## Per-block totals

| block | compact bytes | primitives | texts | segments | L3 compact bytes |
|---|---:|---:|---:|---:|---:|
| ar_plan/left | 9,761,787 | 14,800 | 836 | 18,139 | 19,061 |
| ar_plan/right | 9,761,827 | 14,799 | 836 | 18,138 | 19,078 |
| ar_wall_sections/left | 12,444,223 | 20,000 | 119 | 20,324 | 5,314 |
| ar_wall_sections/right | 12,439,435 | 20,000 | 119 | 20,324 | 5,250 |
| eom_singleline_changed/left | 712,733 | 704 | 73 | 3,297 | 4,579 |
| eom_singleline_changed/right | 420,770 | 164 | 299 | 1,868 | 8,040 |
| ss_plan_dense/left | 9,384,761 | 1,604 | 522 | 88,752 | 13,194 |
| ss_plan_dense/right | 9,373,956 | 1,604 | 522 | 88,609 | 13,193 |
| ss_scheme_text_changed/left | 146,016 | 39 | 100 | 710 | 3,381 |
| ss_scheme_text_changed/right | 253,981 | 159 | 97 | 1,190 | 3,701 |
| ss_simple_node/left | 25,089 | 2 | 31 | 45 | 1,516 |
| ss_simple_node/right | 25,089 | 2 | 31 | 45 | 1,516 |
| ss_table_graphic/left | 183,711 | 11 | 54 | 1,609 | 2,648 |
| ss_table_graphic/right | 182,848 | 7 | 58 | 1,599 | 2,708 |
| vk_node_plan/left | 12,571,242 | 20,000 | 243 | 20,677 | 13,103 |
| vk_node_plan/right | 12,585,143 | 20,000 | 237 | 20,817 | 12,998 |
| vk_nodes/left | 12,638,152 | 20,000 | 421 | 20,086 | 16,362 |
| vk_nodes/right | 12,611,756 | 20,000 | 360 | 20,087 | 15,911 |
| vk_plan/left | 2,071,522 | 3,163 | 232 | 3,180 | 8,226 |
| vk_plan/right | 2,077,085 | 3,163 | 237 | 3,183 | 9,281 |
