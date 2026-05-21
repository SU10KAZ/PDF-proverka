# Phase 0 / Phase 1 Comparison

## 1. Aggregate Comparison

| Algorithm | Cases | matched_gt | missed_crit | FP | dupes | beyond | strict | recall | balanced | cost_aware | human | avg_sec |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| A0_baseline_current__baseline | 8 | 49 | 3 | 73 | 0 | 0 | 50.5 | 79.8 | 70.6 | 50.4 | 79.8 | 158.7 |
| A0_phase0_classdedup__baseline | 8 | 49 | 3 | 73 | 0 | 0 | 50.5 | 79.8 | 70.6 | 50.4 | 79.8 | 158.7 |
| A0_phase0_combined__baseline | 8 | 49 | 3 | 73 | 0 | 0 | 50.5 | 79.8 | 70.6 | 50.4 | 79.8 | 158.7 |
| A0_phase0_fuzzydedup__baseline | 8 | 49 | 3 | 73 | 0 | 0 | 50.5 | 79.8 | 70.6 | 50.4 | 79.8 | 158.7 |
| A1_hybrid_lite__v1 | 2 | 12 | 0 | 10 | 0 | 0 | 72.8 | 87.8 | 82.8 | 66.4 | 87.8 | 329.2 |
| A1_hybrid_lite__v2 | 24 | 108 | 3 | 326 | 2 | 0 | 34.5 | 76.0 | 62.4 | 32.0 | 75.9 | 244.1 |
| replay_class_dedup__baseline | 8 | 52 | 1 | 218 | 4 | 0 | -15.1 | 68.2 | 41.0 | -44.0 | 67.8 | 776.8 |
| replay_fuzzy_dedup__baseline | 8 | 52 | 1 | 179 | 0 | 0 | 5.4 | 73.1 | 50.8 | -23.4 | 73.1 | 776.8 |

## 2. Coverage matrix (case × algorithm)

| Case | A0_baseline_current__baseline | A0_phase0_classdedup__baseline | A0_phase0_combined__baseline | A0_phase0_fuzzydedup__baseline | A1_hybrid_lite__v1 | A1_hybrid_lite__v2 | replay_class_dedup__baseline | replay_fuzzy_dedup__baseline |
|---|---|---|---|---|---|---|---|---|
| ar_01_evacuation | ✓ | ✓ | ✓ | ✓ | · | ✓ | ✓ | ✓ |
| ar_02_facade_thermal | · | · | · | · | · | ✓ | · | · |
| ar_03_balcony_glazing | · | · | · | · | · | ✓ | · | · |
| cross_01_eom_ov_loads | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| cross_02_kj_ar_opening | · | · | · | · | · | ✓ | · | · |
| eom_01_cable_sizing | ✓ | ✓ | ✓ | ✓ | · | ✓ | ✓ | ✓ |
| eom_02_grounding | · | · | · | · | · | ✓ | · | · |
| eom_03_low_voltage_selectivity | · | · | · | · | · | ✓ | · | · |
| kj_01_rebar | ✓ | ✓ | ✓ | ✓ | · | ✓ | ✓ | ✓ |
| kj_02_slab_punching | · | · | · | · | · | ✓ | · | · |
| kj_03_foundation_audit | · | · | · | · | · | ✓ | · | · |
| km_01_truss_design | · | · | · | · | · | ✓ | · | · |
| km_02_metal_protection_spec | · | · | · | · | · | ✓ | · | · |
| km_03_connections | · | · | · | · | · | ✓ | · | · |
| multi_01_tz_vs_rd | ✓ | ✓ | ✓ | ✓ | · | ✓ | ✓ | ✓ |
| ov_01_ventilation | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| ov_02_smoke_protection | · | · | · | · | · | ✓ | · | · |
| ov_03_heating_calc | · | · | · | · | · | ✓ | · | · |
| ss_01_cabling | ✓ | ✓ | ✓ | ✓ | · | ✓ | ✓ | ✓ |
| ss_02_fire_alarm | · | · | · | · | · | ✓ | · | · |
| ss_03_access_integration | · | · | · | · | · | ✓ | · | · |
| vk_01_water_flow | ✓ | ✓ | ✓ | ✓ | · | ✓ | ✓ | ✓ |
| vk_02_sewage | · | · | · | · | · | ✓ | · | · |
| vk_03_hot_water_tz | · | · | · | · | · | ✓ | · | · |

## 3. Coverage gaps

- Total dataset cases:  **24**
- Cases with at least one algorithm output: **24**
- Cases with zero algorithm output: **0**
