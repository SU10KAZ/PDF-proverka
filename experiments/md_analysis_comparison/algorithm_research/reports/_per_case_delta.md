# Per-Case Delta (A0 vs A1-v2)

Side-by-side A0 baseline vs A1-v2 candidate per case.
Only cases present in BOTH algorithms are shown.

| Case | doc_type | matched (A0→v2) | missed_crit (A0→v2) | FP (A0→v2) | total (A0→v2) | strict (A0→v2) | Δstrict |
|---|---|---|---|---|---|---|---|
| ar_01_evacuation | full_rd | 6→7 | 0→0 | 9→21 | 16→28 | 49.7→16.0 | -33.7 |
| cross_01_eom_ov_loads | audit_comparison | 4→5 | 2→1 | 3→6 | 10→13 | 25.1→37.4 | +12.3 |
| eom_01_cable_sizing | full_rd | 6→6 | 0→0 | 20→19 | 26→25 | 20.0→24.0 | +4.0 |
| kj_01_rebar | full_rd | 7→7 | 0→0 | 8→7 | 15→14 | 68.0→72.0 | +4.0 |
| multi_01_tz_vs_rd | tz_vs_rd | 7→7 | 0→0 | 5→16 | 12→23 | 80.0→36.0 | -44.0 |
| ov_01_ventilation | full_rd | 5→6 | 1→0 | 10→17 | 16→23 | 33.3→32.0 | -1.3 |
| ss_01_cabling | full_rd | 7→7 | 0→0 | 9→12 | 16→19 | 64.0→52.0 | -12.0 |
| vk_01_water_flow | full_rd | 7→7 | 0→0 | 9→21 | 16→28 | 64.0→16.0 | -48.0 |

## Cases with A1-v2 only (no A0 baseline)

| Case | doc_type | matched | missed_crit | FP | total | strict |
|---|---|---|---|---|---|---|
| ar_02_facade_thermal | full_rd | 3 | 0 | 17 | 21 | 7.0 |
| ar_03_balcony_glazing | specification_only | 3 | 0 | 19 | 22 | 22.0 |
| cross_02_kj_ar_opening | audit_comparison | 2 | 1 | 2 | 5 | 48.7 |
| eom_02_grounding | full_rd | 5 | 0 | 20 | 25 | 20.0 |
| eom_03_low_voltage_selectivity | full_rd | 1 | 1 | 11 | 15 | -29.0 |
| kj_02_slab_punching | full_rd | 4 | 0 | 15 | 19 | 40.0 |
| kj_03_foundation_audit | audit_comparison | 3 | 0 | 8 | 11 | 68.0 |
| km_01_truss_design | full_rd | 4 | 0 | 19 | 24 | 4.0 |

## v1 vs v2 head-to-head (cases with both)

| Case | matched (v1→v2) | missed_crit (v1→v2) | FP (v1→v2) | strict (v1→v2) |
|---|---|---|---|---|
| cross_01_eom_ov_loads | 6→5 | 0→1 | 7→6 | 57.7→37.4 |
| ov_01_ventilation | 6→6 | 0→0 | 3→17 | 88.0→32.0 |
