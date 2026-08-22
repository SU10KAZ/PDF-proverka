# Human validation — graphic-only scope

Ground truth was fixed by manual side-by-side raster inspection; model output was not used as truth. Text and table-content differences were deliberately removed from graphic GT.

| Pair | Human graphic GT | Deterministic | Route | Note |
|---|---|---|---|---|
| ss_scheme_text_changed | NO_GRAPHIC_CHANGE | GRAPHIC_CHANGE | GRAPHIC_HYBRID | Text differences are handled by the separate text pipeline and are not graphic GT. |
| ss_plan_dense | NO_GRAPHIC_CHANGE | GRAPHIC_CHANGE | GRAPHIC_HYBRID | No manually confirmed graphical-object change. |
| ss_simple_node | NO_GRAPHIC_CHANGE | NO_GRAPHIC_CHANGE | GRAPHIC_VECTOR_OK | No manually confirmed graphical-object change. |
| ss_table_graphic | NO_GRAPHIC_CHANGE | GRAPHIC_CHANGE | GRAPHIC_HYBRID | No manually confirmed graphical-object change. |
| ar_plan | NO_GRAPHIC_CHANGE | GRAPHIC_CHANGE | GRAPHIC_HYBRID | No manually confirmed graphical-object change. |
| ar_wall_sections | NO_GRAPHIC_CHANGE | GRAPHIC_CHANGE | GRAPHIC_HYBRID | No manually confirmed graphical-object change. |
| vk_plan | GRAPHIC_CHANGE | GRAPHIC_CHANGE | GRAPHIC_HYBRID | No manually confirmed graphical-object change. |
| vk_nodes | GRAPHIC_CHANGE | GRAPHIC_CHANGE | GRAPHIC_HYBRID | No manually confirmed graphical-object change. |
| vk_node_plan | NO_GRAPHIC_CHANGE | GRAPHIC_CHANGE | GRAPHIC_HYBRID | No manually confirmed graphical-object change. |
| ss_crop_mismatch_page07 | UNSURE | GRAPHIC_CHANGE | GRAPHIC_HYBRID | Prepared blocks cover different semantic extents; a block matcher or remapping would be needed, and is out of scope. |
| ss_plan_page09 | NO_GRAPHIC_CHANGE | NO_GRAPHIC_CHANGE | GRAPHIC_HYBRID | No manually confirmed graphical-object change. |
| ss_plan_page11 | NO_GRAPHIC_CHANGE | GRAPHIC_CHANGE | GRAPHIC_HYBRID | No manually confirmed graphical-object change. |
| ss_plan_page12 | NO_GRAPHIC_CHANGE | NO_GRAPHIC_CHANGE | GRAPHIC_HYBRID | No manually confirmed graphical-object change. |
| ss_plan_page13 | NO_GRAPHIC_CHANGE | GRAPHIC_CHANGE | GRAPHIC_HYBRID | No manually confirmed graphical-object change. |
| ss_plan_page14 | NO_GRAPHIC_CHANGE | GRAPHIC_CHANGE | GRAPHIC_HYBRID | No manually confirmed graphical-object change. |
| ss_detail_page17 | NO_GRAPHIC_CHANGE | NO_GRAPHIC_CHANGE | GRAPHIC_VECTOR_OK | No manually confirmed graphical-object change. |
| ss_table_page19 | NO_GRAPHIC_CHANGE | NO_GRAPHIC_CHANGE | GRAPHIC_VECTOR_OK | No manually confirmed graphical-object change. |
| vk_plan_page07 | NO_GRAPHIC_CHANGE | GRAPHIC_CHANGE | GRAPHIC_HYBRID | No manually confirmed graphical-object change. |
| vk_plan_page08 | NO_GRAPHIC_CHANGE | GRAPHIC_CHANGE | GRAPHIC_HYBRID | No manually confirmed graphical-object change. |
| vk_plan_page10 | NO_GRAPHIC_CHANGE | GRAPHIC_CHANGE | GRAPHIC_HYBRID | No manually confirmed graphical-object change. |
| vk_nodes_page11 | NO_GRAPHIC_CHANGE | GRAPHIC_CHANGE | GRAPHIC_HYBRID | No manually confirmed graphical-object change. |
| vk_diagrams_page16 | NO_GRAPHIC_CHANGE | GRAPHIC_CHANGE | GRAPHIC_HYBRID | No manually confirmed graphical-object change. |
| vk_axono_page17 | NO_GRAPHIC_CHANGE | GRAPHIC_CHANGE | GRAPHIC_HYBRID | Text differences are handled by the separate text pipeline and are not graphic GT. |
| vk_diagrams_page18 | NO_GRAPHIC_CHANGE | GRAPHIC_CHANGE | GRAPHIC_HYBRID | No manually confirmed graphical-object change. |
| vk_axono_page20 | NO_GRAPHIC_CHANGE | GRAPHIC_CHANGE | GRAPHIC_HYBRID | No manually confirmed graphical-object change. |
| ar_plan_page05 | NO_GRAPHIC_CHANGE | GRAPHIC_CHANGE | GRAPHIC_HYBRID | No manually confirmed graphical-object change. |
| ar_plan_page07 | NO_GRAPHIC_CHANGE | GRAPHIC_CHANGE | GRAPHIC_HYBRID | No manually confirmed graphical-object change. |
| ar_plan_page08 | NO_GRAPHIC_CHANGE | GRAPHIC_CHANGE | GRAPHIC_HYBRID | No manually confirmed graphical-object change. |
| ar_plan_page10 | NO_GRAPHIC_CHANGE | GRAPHIC_CHANGE | GRAPHIC_HYBRID | No manually confirmed graphical-object change. |
| ar_plan_page11 | NO_GRAPHIC_CHANGE | GRAPHIC_CHANGE | GRAPHIC_HYBRID | No manually confirmed graphical-object change. |
| ar_plan_page12 | NO_GRAPHIC_CHANGE | GRAPHIC_CHANGE | GRAPHIC_HYBRID | No manually confirmed graphical-object change. |
| ar_plan_page13 | NO_GRAPHIC_CHANGE | GRAPHIC_CHANGE | GRAPHIC_HYBRID | No manually confirmed graphical-object change. |
| ar_plan_page16 | NO_GRAPHIC_CHANGE | GRAPHIC_CHANGE | GRAPHIC_HYBRID | No manually confirmed graphical-object change. |
| ov_plan_floor04 | NO_GRAPHIC_CHANGE | GRAPHIC_CHANGE | GRAPHIC_HYBRID | No manually confirmed graphical-object change. |
| ov_plan_floor05 | NO_GRAPHIC_CHANGE | GRAPHIC_CHANGE | GRAPHIC_HYBRID | No manually confirmed graphical-object change. |
| ov_plan_floor06 | NO_GRAPHIC_CHANGE | GRAPHIC_CHANGE | GRAPHIC_HYBRID | No manually confirmed graphical-object change. |
| ov_plan_floor07 | GRAPHIC_CHANGE | GRAPHIC_CHANGE | GRAPHIC_HYBRID | No manually confirmed graphical-object change. |
| ov_equipment_table | NO_GRAPHIC_CHANGE | NO_GRAPHIC_CHANGE | GRAPHIC_VECTOR_OK | No manually confirmed graphical-object change. |

The corpus limitation is material: only three pairs (`vk_plan`, `vk_nodes`, `ov_plan_floor07`) are manually confirmed graphical-change positives. `ss_crop_mismatch_page07` is UNSURE because existing prepared blocks cover different semantic extents.
