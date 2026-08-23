### [T1] Объём набора [CF]

строк 3074, носителей 58, дисциплин 12, сравнений выполнено 2886, пропущено движком 164, ошибок обвязки 24

### [T2] Матрица M2 «что было → что сказала система» [CF]

| истина ↓ / ответ → | молчание | MOVED в нужном месте | в нужном месте, не назван переносом | только в другом месте | находки (bbox истины нет) | UNKNOWN | всего |
|---|---|---|---|---|---|---|---|
| **A: переписано представление** | 225 | 0 | 0 | 0 | 6 | 1 | 232 |
| **B: весь блок трансформирован** | 719 | 0 | 0 | 0 | 137 | 14 | 870 |
| **C1: один объект удалён** | 27 | 0 | 141 | 0 | 0 | 0 | 168 |
| **C3+B: сдвиг объекта ниже допуска** | 0 | 0 | 1 | 0 | 0 | 10 | 11 |
| **C3+B: блок И объект сдвинуты** | 10 | 205 | 34 | 0 | 0 | 0 | 249 |
| **C3: один объект сдвинут на < 0.5 pt** | 291 | 0 | 0 | 0 | 0 | 0 | 291 |
| **C3: один объект сдвинут на ≥ 0.5 pt** | 82 | 800 | 106 | 0 | 0 | 0 | 988 |
| **D: изменён только текст** | 101 | 0 | 0 | 0 | 0 | 0 | 101 |

### [T3] Флаг BLOCK_TRANSFORMED [CF]

| класс | инстансов | флаг выставлен |
|---|---|---|
| A1 | 58 | 0 |
| A5 | 58 | 0 |
| A6 | 115 | 8 |
| B1 | 171 | 171 |
| B2 | 168 | 167 |
| B3 | 290 | 12 |
| B4 | 116 | 2 |
| B5 | 112 | 112 |
| C1 | 168 | 0 |
| C3_ | 1279 | 12 |
| D1 | 54 | 0 |
| D3 | 47 | 0 |

### [T4] Точность восстановления параметров [CF]

| контрфакт | n | ошибка сдвига, медиана pt | p90 | max | ≤ 0.01 pt | ошибка масштаба, медиана | угол верен |
|---|---|---|---|---|---|---|---|
| B1_translate@0.005 | 57 | 0.0 | 0.0 | 0.72 | 56/57 | 0.0 | 57/57 |
| B1_translate@0.02 | 57 | 0.0 | 0.0 | 39.395 | 56/57 | 0.0 | 57/57 |
| B1_translate@0.1 | 57 | 0.0 | 0.0 | 161.425 | 55/57 | 0.0 | 57/57 |
| B2_scale@0.95 | 57 | 4e-05 | 6e-05 | 20.458 | 55/57 | 0.0 | 57/57 |
| B2_scale@1.05 | 58 | 4e-05 | 6e-05 | 136.588 | 55/58 | 0.0 | 58/58 |
| B2_scale@1.2 | 53 | 5e-05 | 0.00117 | 91.615 | 48/53 | 0.0 | 53/53 |
| B5_rotate_page@270 | 56 | None | None | — | — | 0.0 | 56/56 |
| B5_rotate_page@90 | 56 | None | None | — | — | 0.0 | 56/56 |

### [T5] Кривая чувствительности к δ, абсолютная ось [CF]

| δ, pt | n | recall (назван переносом) | локализовано |
|---|---|---|---|
| 0 – 0.25 | 144 | 0.000 | 0.000 |
| 0.25 – 0.5 | 147 | 0.000 | 0.000 |
| 0.5 – 1 | 141 | 0.731 | 0.858 |
| 1 – 2 | 151 | 0.795 | 0.920 |
| 2 – 4 | 154 | 0.818 | 0.929 |
| 4 – 8 | 160 | 0.812 | 0.925 |
| 8 – 16 | 151 | 0.795 | 0.927 |
| 16 – 32 | 144 | 0.840 | 0.924 |
| 32 – 64 | 52 | 0.923 | 0.942 |
| 64 – ∞ | 35 | 0.914 | 0.943 |

### [T5b] Самопроверка объяснения: δ, нормированная на допуск сопоставления [CF]

допуск `tol = max(0.5, 0.05·S)` в корпусе: медиана 0.5 pt, диапазон 0.5…0.57 pt, выше 0.5 pt у 0.228 инстансов

| δ / tol | n | recall |
|---|---|---|
| 0 – 0.5 | 155 | 0.000 |
| 0.5 – 0.9 | 125 | 0.000 |
| 0.9 – 1.1 | 74 | 0.460 |
| 1.1 – 2 | 78 | 0.885 |
| 2 – 5 | 189 | 0.815 |
| 5 – ∞ | 658 | 0.825 |

### [T6] То же по плотности блока [CF]

| плотность | 0–0.25 pt | 0.25–0.5 pt | 0.5–1 pt | 1–2 pt | 2–4 pt | 4–8 pt | 8–16 pt | 16–32 pt | 32–64 pt | 64–∞ pt |
|---|---|---|---|---|---|---|---|---|---|---|
| <500 | 0.000 (47) | 0.000 (27) | 0.960 (25) | 0.962 (26) | 0.960 (25) | 0.963 (27) | 0.947 (19) | 1.000 (19) | 1.000 (2) | — |
| 500-5k | 0.000 (56) | 0.000 (55) | 0.833 (54) | 0.812 (48) | 0.836 (55) | 0.839 (56) | 0.855 (55) | 0.875 (48) | 1.000 (17) | 1.000 (2) |
| 5k-20k | 0.000 (8) | 0.000 (20) | 0.643 (14) | 0.870 (23) | 0.913 (23) | 0.913 (23) | 0.870 (23) | 0.913 (23) | 1.000 (12) | 1.000 (15) |
| >20k | 0.000 (33) | 0.000 (45) | 0.521 (48) | 0.667 (54) | 0.686 (51) | 0.667 (54) | 0.648 (54) | 0.722 (54) | 0.809 (21) | 0.833 (18) |

### [T7] Кривая по δ как доле диагонали блока, по размеру объекта [CF]

| δ, доля диагонали | tiny (<0.1 % площади) | small (0.1–1 %) | large (>1 %) |
|---|---|---|---|
| 0.0002 (медиана 0.1106 pt) | 0.037 (54) | 0.036 (56) | 0.059 (51) |
| 0.0005 (медиана 0.2765 pt) | 0.222 (54) | 0.250 (56) | 0.255 (51) |
| 0.001 (медиана 0.553 pt) | 0.574 (54) | 0.696 (56) | 0.706 (51) |
| 0.0025 (медиана 1.3826 pt) | 0.741 (54) | 0.821 (56) | 0.863 (51) |
| 0.005 (медиана 2.7651 pt) | 0.741 (54) | 0.839 (56) | 0.843 (51) |
| 0.01 (медиана 5.5301 pt) | 0.759 (54) | 0.821 (56) | 0.837 (49) |
| 0.02 (медиана 11.11095 pt) | 0.741 (54) | 0.818 (55) | 0.857 (49) |
| 0.05 (медиана 27.9046 pt) | 0.755 (53) | 0.891 (55) | 0.898 (49) |

### [T8] M3 — блок трансформирован И объект сдвинут [CF]

| комбинация | n | recall | локализовано | ложных находок вне объекта (медиана / p90 / max) |
|---|---|---|---|---|
| C3+B1|B1_translate0.02 | 103 | 0.816 | 0.971 | 0.0 / 0.0 / 0 |
| C3+B1|B1_translate0.1 | 50 | 0.820 | 0.940 | 0.0 / 0.0 / 0 |
| C3+B2|B2_scale1.05 | 48 | 0.812 | 1.000 | 0.0 / 0.0 / 0 |
| C3+B2|B2_scale1.2 | 49 | 0.837 | 0.918 | 0.0 / 0.0 / 0 |

### [T9] Ложные срабатывания по классам, правило v1 [CF]

| класс | инстансов | с находками | доля |
|---|---|---|---|
| A1 | 58 | 0 | 0.0000 |
| A5 | 58 | 0 | 0.0000 |
| A6 | 115 | 6 | 0.0522 |
| B1 | 171 | 3 | 0.0175 |
| B2 | 168 | 5 | 0.0298 |
| B3 | 290 | 98 | 0.3379 |
| B4 | 116 | 31 | 0.2672 |
| B5 | 112 | 0 | 0.0000 |
| D1 | 54 | 0 | 0.0000 |
| D3 | 47 | 0 | 0.0000 |

всего «молчащих» инстансов 1189, с находками 143 (0.1203)

### [T10] M4b — правило границы v1 против v2 [CF]

носителей 27, сравнений 275, «молчащих» 189, «настоящих» 86; доля сегментов, обрезанных рамкой, медиана 0.00038

| правило | ложных на классах A/B (доля) | истинных найдено (доля) |
|---|---|---|
| v1 (пересечение кадров, без провенанса) | **0.2434** | 0.9070 |
| v2, отступ 0.0 pt | **0.0053** | 0.8605 |
| v2, отступ 0.5 pt | **0.0053** | 0.8605 |
| v2, отступ 1.0 pt | **0.0053** | 0.8605 |
| v2, отступ 2.0 pt | **0.0053** | 0.8488 |
| v2, отступ 4.0 pt | **0.0053** | 0.8256 |

| класс | n | v1 | v2 (отступ 0) | v1: нашёл истину | v2: нашёл истину |
|---|---|---|---|---|---|
| B1 | 27 | 0.037 | 0.037 | — | — |
| B3 | 108 | 0.296 | 0.000 | — | — |
| B4 | 54 | 0.241 | 0.000 | — | — |
| C1 | 26 | 0.769 | 0.769 | 0.7692 | 0.7692 |
| C2 | 10 | 1.000 | 1.000 | 1.0 | 1.0 |
| C3 | 50 | 0.960 | 0.880 | 0.96 | 0.88 |

### [T_PH] Слой объектов не инвариантен к чистому сдвигу блока [CF]

16 реальных блоков; сдвиг задан в долях характерного масштаба S блока, рисунок не тронут — идеальный слой обязан вернуть ТО ЖЕ разбиение. churn = доля длины штриха в объектах, состав которых изменился хотя бы одним сегментом

| сдвиг, доли S | блоков | churn разбиения, медиана | max | блоков без единого изменения | n_obj / n_obj₀, медиана |
|---|---|---|---|---|---|
| 0.0 | 16 | 0.0 | 0.0 | 16/16 | 1.0 |
| 0.001 | 16 | 0.0 | 0.65055 | 13/16 | 1.0 |
| 0.01 | 16 | 0.0303 | 0.65055 | 5/16 | 1.0 |
| 0.1 | 16 | 0.12293 | 0.86542 | 2/16 | 1.0 |
| 0.25 | 16 | 0.15019 | 0.86542 | 2/16 | 1.0124 |
| 0.5 | 16 | 0.12299 | 0.88507 | 1/16 | 1.0 |
| 0.75 | 16 | 0.13656 | 0.37485 | 2/16 | 1.0179 |
| 1.0 | 16 | 0.09457 | 0.72176 | 1/16 | 1.0 |
| 2.0 | 16 | 0.13128 | 0.37485 | 2/16 | 1.0 |
| 3.0 | 16 | 0.17686 | 0.88507 | 2/16 | 1.0087 |

### [T_AVB] Якоря против bbox блока [REAL]

пар 34 (бенчмарк + запасная ось Р↔Р)

| выравнивание | несопоставленная краска A, медиана | p90 | max | якоря лучше | якоря хуже | расхождение оценки сдвига, медиана pt |
|---|---|---|---|---|---|---|
| **якоря-объекты** | **0.05775** | 0.53118 | 0.96776 | — | — | — |
| рамка кропа, совмещены начала (s=1) | 0.9973 | 1.0 | 1.0 | 34 | 0 | 18.425 |
| рамка кропа, подогнана изотропно | 0.99931 | 1.0 | 1.0 | 34 | 0 | 60.5855 |

### [T10b] Ложные «объект сдвинулся» от рамки кропа: v1 против v2 [CF]

| контрфакт | инстансов | инстансов с ложным MOVED, v1 | v2 | всего ложных находок, v1 | v2 |
|---|---|---|---|---|---|
| B1 | 27 | **0** | **0** | 2 | 2 |
| B3 | 108 | **5** | **0** | 119 | 0 |
| B4 | 54 | **0** | **0** | 64 | 0 |

### [T11] Величина глобального преобразования на РЕАЛЬНЫХ парах [REAL]

| набор | пар | выровнено | \|t\|, медиана pt | p90 | max | доля \|t\|>1 pt | доля \|s−1\|>0.01 | поворот ≠ 0 |
|---|---|---|---|---|---|---|---|---|
| benchmark 33 [REAL cross-revision] | 33 | 29 | 0.0001 | 23.33544 | 140.8134 | 0.1379 | 0.0345 | 0 |
| fallback R<->R (pd) [REAL] | 7 | 5 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0 |
| random cross-revision sample [REAL] | 81 | 58 | 0.0 | 0.07573 | 4.128 | 0.0345 | 0.0 | 0 |

### [T12] Бенчмарк: вердикт против разметки [REAL]

* `NO_GRAPHIC_CHANGE->NO_GRAPHIC_CHANGE` — 12
* `GRAPHIC_CHANGE->GRAPHIC_CHANGE` — 9
* `GRAPHIC_CHANGE->NO_GRAPHIC_CHANGE` — 5
* `NO_GRAPHIC_CHANGE->GRAPHIC_CHANGE` — 2

| пара | классы | ожидалось | статус | вердикт | находок | границ | \|t\|, pt |
|---|---|---|---|---|---|---|---|
| AR-441907a2 | object_removed,with_labels | GRAPHIC_CHANGE | ALIGNED | NO_GRAPHIC_CHANGE | 0 | 6 | 0.0 |
| AR-46f62636 | bbox_boundary_artifact,dense_block,with_labels | NO_GRAPHIC_CHANGE | ALIGNED | NO_GRAPHIC_CHANGE | 0 | 5 | 0.0 |
| AR-490254e9 | block_scope_change,with_labels | NOT_COMPARABLE | ALIGNED | GRAPHIC_CHANGE | 4 | 4 | 0.0019 |
| AR-55eda7fb | small_local_change,table_only_change,with_labels | GRAPHIC_CHANGE | ALIGNED | GRAPHIC_CHANGE | 10 | 7 | 0.0001 |
| AR-577a293f | small_local_change,table_only_change,with_labels | GRAPHIC_CHANGE | ALIGNED | GRAPHIC_CHANGE | 3 | 0 | 20.2892 |
| AR-5acaab0e | object_added,dense_block,hatch_noise,with_labels | GRAPHIC_CHANGE | ALIGNED | GRAPHIC_CHANGE | 42 | 2 | 35.5204 |
| AR-a32b30a6 | object_added,raster_graphics,no_labels | GRAPHIC_CHANGE | NO_VECTOR | UNKNOWN | None | None | None |
| AR-b38a7dbc | unchanged_control,dense_block,with_labels | NO_GRAPHIC_CHANGE | ALIGNED | NO_GRAPHIC_CHANGE | 0 | 0 | 0.0 |
| AR-dbfd82b8 | object_removed,with_labels,table_like | GRAPHIC_CHANGE | ALIGNED | NO_GRAPHIC_CHANGE | 0 | 12 | 0.0 |
| EOM-0c86dfde | text_only_change,object_moved_label,dense_block | NO_GRAPHIC_CHANGE | ALIGNED | GRAPHIC_CHANGE | 35 | 1 | 0.0005 |
| EOM-14558cda | object_added,occluding_fill,with_labels | GRAPHIC_CHANGE | ALIGNED | GRAPHIC_CHANGE | 3 | 4 | 0.0 |
| EOM-1db297d2 | uncertain,block_match_failure | UNCERTAIN | ALIGNMENT_UNAVAILABLE | UNKNOWN | None | None | None |
| EOM-3306e907 | block_moved,rotated_page | NO_GRAPHIC_CHANGE | ALIGNED | NO_GRAPHIC_CHANGE | 0 | 0 | 0.0 |
| EOM-36de2ce2 | block_moved,rotated_page,few_labels | NO_GRAPHIC_CHANGE | ALIGNED | NO_GRAPHIC_CHANGE | 0 | 0 | 0.0003 |
| EOM-46355862 | object_added,dense_block,with_labels | GRAPHIC_CHANGE | ALIGNED | GRAPHIC_CHANGE | 37 | 0 | 110.544 |
| EOM-7fef43a3 | object_added,with_labels | GRAPHIC_CHANGE | ALIGNED | NO_GRAPHIC_CHANGE | 0 | 0 | 0.0 |
| EOM-c50e2170 | object_removed,dense_block,with_labels | GRAPHIC_CHANGE | ALIGNED | GRAPHIC_CHANGE | 6 | 2 | 0.0 |
| GP-6bc1c029 | uncertain,dense_block | UNCERTAIN | SKIPPED_TOO_DENSE | None | None | None | None |
| KJ-25717577 | text_only_change,table_only_change,with_labels | NO_GRAPHIC_CHANGE | ALIGNED | NO_GRAPHIC_CHANGE | 0 | 2 | 0.0 |
| KJ-548814a7 | block_moved,few_labels | NO_GRAPHIC_CHANGE | ALIGNED | BLOCK_TRANSFORMED | 0 | 0 | 140.8134 |
| KJ-b5c82f7c | bbox_boundary_artifact,with_labels | NO_GRAPHIC_CHANGE | ALIGNED | NO_GRAPHIC_CHANGE | 0 | 1 | 0.0 |
| OV-2cc2a382 | block_scope_change,raster_graphics,no_labels | NOT_COMPARABLE | NO_VECTOR | UNKNOWN | None | None | None |
| OV-93cc012f | object_added,object_moved,with_labels | GRAPHIC_CHANGE | ALIGNED | GRAPHIC_CHANGE | 5 | 2 | 0.0001 |
| SS-392b7bd3 | bbox_boundary_artifact,with_labels | NO_GRAPHIC_CHANGE | ALIGNED | GRAPHIC_CHANGE | 0 | 1 | 0.0556 |
| SS-6fc75e05 | different_packaging,with_labels,table_like | NO_GRAPHIC_CHANGE | ALIGNED | GRAPHIC_CHANGE | 1 | 2 | 0.0763 |
| SS-76640e11 | text_only_change,with_labels | NO_GRAPHIC_CHANGE | ALIGNED | NO_GRAPHIC_CHANGE | 0 | 2 | 0.0 |
| SS-982f7f30 | block_moved,with_labels,table_like | NO_GRAPHIC_CHANGE | ALIGNED | NO_GRAPHIC_CHANGE | 0 | 3 | 0.0435 |
| SS-a369f492 | object_added,object_moved,raster_graphics,with_labels | GRAPHIC_CHANGE | ALIGNED | NO_GRAPHIC_CHANGE | 0 | 2 | 0.0 |
| SS-c7aa8d26 | object_added,with_labels,sparse_block | GRAPHIC_CHANGE | ALIGNED | NO_GRAPHIC_CHANGE | 0 | 3 | 0.0994 |
| TX-95031dbd | unchanged_control,no_labels,rotated_page | NO_GRAPHIC_CHANGE | ALIGNED | NO_GRAPHIC_CHANGE | 0 | 0 | 0.0 |
| VK-148ffe6c | object_added,object_moved,rotated_page,dense_block | GRAPHIC_CHANGE | ALIGNED | GRAPHIC_CHANGE | 12 | 6 | 0.0007 |
| VK-56115717 | object_removed,with_labels | GRAPHIC_CHANGE | ALIGNED | GRAPHIC_CHANGE | 18 | 9 | 0.6455 |
| VK-b71afe81 | different_packaging,rotated_page,dense_block,garbled_text_layer | NO_GRAPHIC_CHANGE | ALIGNED | NO_GRAPHIC_CHANGE | 0 | 0 | 0.0 |

### [T13] Сопоставимая область (доля краски внутри пересечения кадров) [REAL]

пар 34; медиана A 1.0, B 1.0; min(A,B) < 0.95 у 0.3529, < 0.80 у 0.2059

| пара | ожидалось | доля краски A | доля краски B | классы |
|---|---|---|---|---|
| AR-490254e9 | NOT_COMPARABLE | 1.0000 | 0.1049 | block_scope_change,with_labels |
| SS-c7aa8d26 | GRAPHIC_CHANGE | 0.9352 | 0.4794 | object_added,with_labels,sparse_block |
| VK-56115717 | GRAPHIC_CHANGE | 0.5726 | 1.0000 | object_removed,with_labels |
| AR-441907a2 | GRAPHIC_CHANGE | 0.6650 | 1.0000 | object_removed,with_labels |
| VK-148ffe6c | GRAPHIC_CHANGE | 0.9459 | 0.6833 | object_added,object_moved,rotated_page,dense_block |
| SS-a369f492 | GRAPHIC_CHANGE | 1.0000 | 0.7249 | object_added,object_moved,raster_graphics,with_labels |
| AR-dbfd82b8 | GRAPHIC_CHANGE | 0.7853 | 1.0000 | object_removed,with_labels,table_like |
| SS-392b7bd3 | NO_GRAPHIC_CHANGE | 1.0000 | 0.8274 | bbox_boundary_artifact,with_labels |
| KJ-25717577 | NO_GRAPHIC_CHANGE | 0.8737 | 1.0000 | text_only_change,table_only_change,with_labels |
| SS-982f7f30 | NO_GRAPHIC_CHANGE | 1.0000 | 0.8981 | block_moved,with_labels,table_like |
| SS-6fc75e05 | NO_GRAPHIC_CHANGE | 0.9879 | 0.9240 | different_packaging,with_labels,table_like |
| RR06 | None | 1.0000 | 0.9447 | fallback_R_to_R |
| RR05 | None | 0.9656 | 1.0000 | fallback_R_to_R |
| RR04 | None | 0.9752 | 1.0000 | fallback_R_to_R |

### [T14] M4b на реальном корпусе: «вся невязка на границе» против контроля [REAL]

| группа | пар | выровнено | пар с находками | из них с MOVED | находок, медиана | граничных записей, медиана | краска на границе A, медиана |
|---|---|---|---|---|---|---|---|
| border_only | 51 | 40 | 0.300 | 0.000 | 0.0 | 2.0 | 0.1582 |
| interior | 30 | 18 | 0.389 | 0.056 | 0.0 | 1.5 | 0.103 |

### [T15] M5 — когда выравнивание невозможно

[CF] отказов 25 из 3074 (0.0081); по контрфактам: [['C3+B2@small@0.02@B2_scale1.2', 6], ['B2_scale@1.2', 6], ['B5_rotate_page@90', 2], ['B5_rotate_page@270', 2], ['C3+B1@large@0.0025@B1_translate0.1', 1], ['A6_round_0.25', 1]]
[CF] неоднозначных (второй консенсус): 1

[REAL] статусы по источникам:

| источник | ALIGNED | ALIGNMENT_UNAVAILABLE | NO_VECTOR | SKIPPED_TOO_DENSE | ERROR | всего |
|---|---|---|---|---|---|---|
| benchmark | 29 | 1 | 2 | 1 | 0 | 33 |
| fallback_RR | 5 | 0 | 0 | 0 | 2 | 7 |
| border_sample | 58 | 10 | 0 | 13 | 0 | 81 |

причины отказа [REAL]: [['low_inlier_ratio', 4], ['too_few_anchors', 4], ['no_consensus', 3]]

