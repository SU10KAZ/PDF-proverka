# Human validation

Raster crops were used only to validate the vector-only result. They were not read by the extractor or comparator.

| Pair | Human verdict | Comparator verdict | Correctness | Why |
|---|---|---|---|---|
| ss_scheme_text_changed | STRUCTURE_SAME_VALUES_CHANGED | STRUCTURE_SAME_VALUES_CHANGED | CORRECT | Same connections; OSPD/camera/room labels changed. |
| ss_plan_dense | NEAR_IDENTICAL | NEAR_IDENTICAL | CORRECT | Same dense plan; crop height/PDF precision differs. |
| ss_simple_node | IDENTICAL | IDENTICAL | CORRECT | Exact small vector node. |
| ss_table_graphic | NEAR_IDENTICAL | NEAR_IDENTICAL | CORRECT | Same detail/table; padding and span splitting differ. |
| ar_plan | NEAR_IDENTICAL | NEAR_IDENTICAL | CORRECT | Same plan; one low-level line command differs. |
| ar_wall_sections | NEAR_IDENTICAL | NEAR_IDENTICAL | CORRECT | Same repeated sections at 0.5% tolerance. |
| vk_plan | NEAR_IDENTICAL | NEAR_IDENTICAL | CORRECT | Same routes with minor presentation/crop noise. |
| vk_nodes | STRUCTURE_SAME_VALUES_CHANGED | NEAR_IDENTICAL | PARTIALLY_CORRECT | RIGHT adds notes and a −0.034 annotation; embedded-font text is undecodable. |
| vk_node_plan | NEAR_IDENTICAL | NEAR_IDENTICAL | PARTIALLY_CORRECT | Geometry agrees, but vector text cannot confirm values. |
| eom_singleline_changed | STRUCTURE_CHANGED | STRUCTURE_CHANGED | CORRECT | Generalized circuits become four explicit branches. |

Totals: **8 correct, 2 partially correct, 0 wrong**.
