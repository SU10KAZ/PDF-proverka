# FMC — Failure-Mode Corpus. Probe findings

Prefix: `fmc`. Track-B (Opus) independent audit. Research only; nothing outside
`experiments/stage_comparison_vector_architecture_opus/` was written.
All commands run from `/home/coder/projects/PDF-proverka`.

## Headline table

| claim | evidence (measured number / file) | confidence | how it could be falsified |
|---|---|---|---|
| **F1.** On a class-balanced failure-mode corpus of 21 real pairs, v0.1's status accuracy is **6/21 = 28.6 %**, *below* the best constant baseline **8/21 = 38.1 %** ("always STRUCTURE_CHANGED"). Track A reports 8/10 on its own benchmark. | `artifacts/fmc_score.json → status_accuracy 0.2857`, `best_constant_baseline.accuracy 0.381`; human class mix 8 / 6 / 4 / 3 (STRUCTURE_CHANGED / SAME_VALUES / NEAR_IDENTICAL / IDENTICAL) | high | Dispute individual human labels; 8 label flips would be needed to reach parity with the baseline. |
| **F2.** Page-order shift is the norm, not an edge case: **1620 of 3096 matched page pairs (52.3 %)** land on a different page index in the new version. Index-based pairing is wrong more often than right. | `probes/fmc_mine_candidates.py` + `artifacts/fmc_candidates.json`; count reproduced in the command block below | high | Show that the word-set-Jaccard matcher mis-pairs; but 896 left-only and 725 right-only pages already prove sheets are inserted/removed wholesale. |
| **F3.** **Two crop windows over literally the same PDF page** — left `[0.100,0.250,0.420,0.800]`, right `[0.160,0.290,0.450,0.830]` (origin moved +0.06/+0.04, width 0.320→0.290, height 0.550→0.540) — produce `STRUCTURE_CHANGED` (geometry 0.475, text 0.264, topology 0.930). Block-bbox normalization turns a shifted, slightly smaller window into a scale change. | pair `fmc_crop_mismatch_same_sheet`, both sides = ЭМ-К3 v002 p21 (same file, same page), `artifacts/fmc_v01_results.json` | high | Re-run with equal bboxes → IDENTICAL; the failure is specific to non-identical block extents, which is the production case. |
| **F4.** A **raster-only block hides a real engineering change and is reported `IDENTICAL`**: the fan curve block (OV ОВ2-К1) changed fan model `VO-PatAIR-Kp-5-6/9-5,5-2 → …-9-3-2-V1` and duty point `Pv 795→757 Па, Ps 633→692 Па`, but the crop holds 2 vector primitives / 0 text spans, and `exact_vector_signature_equal = true`. Pixel diff inside the same bbox is **2.56 %** (5263 px). | pair `fmc_ov_page_shift_geometry`; `probes/fmc_raster_diff.py`; measured `drawings_in_bbox = 2`, `images_in_bbox = 1` | high | Show the vector layer does carry the curve — `page.get_drawings()` returns 2 paths in that rect on both sides. |
| **F5.** **CAD text drawn as paths is invisible to the whole text layer.** On GP ГП2 p7 `page.search_for("Тротуар") == []`, `search_for("131,56") == []`, `search_for("Р4.3") == []` although all three are legible in the render; fact recall for that pair = **0.00**. Corpus-wide there are **14 pages with 0 fonts / 0 text and no raster cover**, incl. 11 of the 15 pages of EOM ЭМ2-ПА v001 (page 5 = 7124 paths, 146 189 items, 92 135 Bézier ops). | `artifacts/fmc_outlined_pages.json`, pair `fmc_gp_section_hatch_dims`, pair `fmc_eom_text_as_paths` | high | Show a Unicode text layer exists on those pages; `page.get_fonts()` returns `[]`. |
| **F6.** **v0.1 truncates text spans at the block border**, destroying exactly the strings the expert needs. `4х(1х120)+1х70` arrives as `20)+1х70`; `Количество` as `чество`; `+4,450` as `+4`; `границы объектов природного комплекса` as `ного комплекса`. **130 of 6650 spans (1.9 %) across the 42 corpus blocks, in 24 of 42 blocks; worst block 28 / 37 = 76 %.** | `probes/fmc_text_clipping.py` → `artifacts/fmc_text_clipping.json`; cause: `extractor._extract_text` line 418 `page.get_text("dict", clip=…)` | high | Pass an unclipped `get_text` and filter by bbox instead; that is the fix, not a falsification. |
| **F7.** **Real value changes are reported as `NEAR_IDENTICAL`.** Room-schedule areas `17,68→17,37`, `2,67→2,65`, total `497,57→497,24` give text similarity 0.995 (threshold 0.92) → `NEAR_IDENTICAL`. Adding six position numbers 81–86 to a ВК specification gives 0.934 → `NEAR_IDENTICAL`. The facts *are* in `differences`, but the status contradicts them. | pairs `fmc_eom_room_schedule_values`, `fmc_vk_spec_positions` in `artifacts/fmc_v01_results.json` | high | Lower the threshold — which then breaks the reflow cases (F8) that need it high. The two requirements are in direct conflict. |
| **F8.** **Pure layout moves are reported as `STRUCTURE_CHANGED`.** Byte-identical page text, table row spacing changed → geometry 0.841 → `STRUCTURE_CHANGED`. The same table translated by ~0.09 of sheet height → geometry 0.405 → `STRUCTURE_CHANGED`. | pairs `fmc_kj_spec_table_reflow`, `fmc_kj_steel_table_shift`; `text_similarity = 1.000` on both | high | Add affine/translation alignment — but Track A explicitly excludes warping (`comparator.caveats`), so this is a design consequence, not a bug. |
| **F9.** **Fact recall (are the load-bearing tokens of the Russian sentence physically present in the machine output?) is 0.72 mean over the 13 pairs that have facts; 3 pairs score 0.00.** Even at recall 1.00 the payload is an unordered add/remove multiset — nothing links `4х(1х120)+1х70` to `4х(1х185)+1х95` as one value change. | `artifacts/fmc_score.json → mean_fact_recall 0.7179`, `pairs_with_zero_fact_recall 3`; see `fmc_eom_cable_table_values` differences | high | Show a rule that pairs the two strings from the emitted payload without an object layer; positions differ by more than the comparator's matching radius because the bboxes are offset. |
| **F10.** **Raster repacking is pervasive and always lands in `INSUFFICIENT_VECTOR_DATA`.** 1099 of 3096 matched page pairs change their image count; OV ОВ1.1-К2 p71→p73 goes from **274 image tiles to 1 image** with identical content. 8 of 42 corpus blocks are `VECTOR_DATA_INSUFFICIENT`, 18 of 42 are `LIMITED_CAPPED`. | `artifacts/fmc_batch_diff.json`; `artifacts/fmc_descriptions/*` quality census in the command block | high | Show those blocks carry usable vector geometry; `primitive_count = 0` on both sides for the raster pairs. |
| **F11.** **O3 (inverted anchor confidence) reproduces on independent data.** Dense block `fmc_eom_tray_plan_geometry` (25 629 segments): 92 `high`, 0 `candidate`. Sparse blocks: `fmc_kj_steel_table_shift` (74 segments) 0 `high` / 28 `none`, `fmc_eom_cable_table_values` (319 segments) 15 `high` / 28 `candidate`. | left-side descriptions in `artifacts/fmc_descriptions/`; census command below | high | Would need a dense block where anchors are mostly `candidate`; none of the 21 shows that. |
| **F12.** **`hatch_like_structures` saturates its cap of 30 in 14 of 42 blocks (33 %)**, including tables and a drawing-list sheet, reproducing O6 on new data. | census command below over `artifacts/fmc_descriptions/*.json` | high | Raise the cap and show the extra entries are informative; the entries at the cap are already table grid lines. |
| **F13.** **v0.1 is fast on realistic block sizes.** 42 blocks extracted in 0.01–4.5 s each; the whole 21-pair corpus runs in well under a minute of extraction. Cost is not the limiting factor — representation is. | `artifacts/fmc_v01_results.json → *_seconds`; max 4.511 s (`fmc_gp_section_hatch_dims` left, 12 575 primitives) | high | Larger blocks (full A0 sheets) will be slower; the capped blocks already hit `storage_cap = 20000`. |

## What this corpus is

21 pairs, mined from the real corpus rather than hand-picked for comfort:

- 98 documents under `projects_v2` have ≥2 version PDFs; 210 distinct PDFs, 112 version steps, **7 byte-identical** (those are excluded — this is exactly Track A's O1 defect).
- Cheap per-page descriptors for all 210 PDFs → pages matched across versions by word-set Jaccard → **3096 matched page pairs** raster-diffed at 900 px → font / ToUnicode / image profiling → text-orientation deltas.
- Disciplines covered: **EOM, KJ, KM, OV, SS, VK, GP, TX, AR** (Track A used SS, AR, VK, EOM only).
- Human class mix: 8 `STRUCTURE_CHANGED`, 6 `STRUCTURE_SAME_VALUES_CHANGED`, 4 `NEAR_IDENTICAL`, 3 `IDENTICAL` — no single class exceeds 38 %, versus Track A's 6/10 `NEAR_IDENTICAL`.
- **No pair compares a PDF with itself except one deliberate control** (`fmc_crop_mismatch_same_sheet`), which exists precisely to measure crop sensitivity in isolation and is labelled `IDENTICAL`.

### Failure modes covered (target list → case)

| target failure mode | case |
|---|---|
| outlined fonts (text drawn as paths) | `fmc_eom_text_as_paths` (0 spans vs 1796), `fmc_gp_section_hatch_dims` (labels absent from text layer) |
| broken / undecodable text | `fmc_km_broken_text_swap` (U+FFFD / PUA code points on the page) |
| visually same but geometry different | `fmc_ov_raster_retile` (274 tiles → 1 image, pixel diff only in the stamp corner) |
| geometry same but meaning different | `fmc_eom_room_schedule_values` (geometry 1.000, three areas changed) |
| dimension-only change | `fmc_gp_section_hatch_dims` (0,12 / 0,12 added, Р4.3→Р4.2) |
| dense hatch | `fmc_ar_hatch_sections` (32 675 line ops, ~4 700 at 135°) |
| table | `fmc_kj_spec_table_reflow`, `fmc_kj_steel_table_shift`, `fmc_eom_drawing_list_rows`, `fmc_eom_cable_table_values`, `fmc_ss_a4_to_a3_reissue` |
| one object as many primitives | `fmc_eom_text_as_paths` (one paragraph = 92 135 Bézier ops) |
| repeated symbols with a count change | `fmc_eom_drawing_list_rows` (4 rows added), `fmc_ss_a4_to_a3_reissue` (V14 → V21) |
| crop mismatch (same content, different extents) | `fmc_crop_mismatch_same_sheet`, `fmc_eom_cable_table_values` |
| raster-only block | `fmc_tx_raster_scan`, `fmc_ov_page_shift_geometry`, `fmc_eom_qr_stamp_only` |
| rotated / mirrored | `fmc_eom_rotated_labels` (263 spans at +90° → 49 at −90°) |
| 1→N split | `fmc_ov_block_split_widened` (1 image block → 3, sheet 1684→2526 pt) |
| page-order shift | `fmc_ov_page_shift_geometry` (p186→p134), `fmc_km_broken_text_swap` (sheets 7/8 swapped), `fmc_ss_a4_to_a3_reissue` (p31→p44) |
| scale / format change | `fmc_ss_a4_to_a3_reissue` (A4→A3), `fmc_eom_drawing_list_rows` (A4x3→A4x4), `fmc_ov_block_split_widened` |
| legend change | `fmc_gp_section_hatch_dims` (legend content), `fmc_ar_hatch_sections` (У2 / У4 / Ш1 added) |
| nested symbols | `fmc_ar_hatch_sections` (Фрагмент 1: shaft inside insulation inside masonry) |
| non-engineering mark added | `fmc_eom_qr_stamp_only` (QR only) |
| block correspondence invalid | `fmc_eom_layout_reorg_mismatch` (same coordinates, different objects) |
| text reflow, no engineering change | `fmc_eom_notes_reflow` |

## Per-pair result table

Legend: **ok** = v0.1 status equals the human verdict. **fact recall** = share of the load-bearing tokens
of the Russian sentence that appear anywhere in the comparator payload (`differences`,
`text.added/removed`, `text.value_changes`). `—` = the correct answer contains no positive fact.

| # | pair_id | дисциплина | класс отказа | человек (RU) | ожидание | v0.1 | ok | geom | txt | topo | fact recall |
|---|---|---|---|---|---|---|---|---:|---:|---:|---:|
| 1 | `fmc_eom_text_as_paths` | EOM | TEXT_LAYER_ABSENT_ONE_SIDE | Лист «Общие данные (окончание)» тот же; в новой версии текст стал выбираемым (в старой — в кривых), текст переверстан, в п.2 добавлена ссылка на отдельный том 13АВ-РД-ГРЩ2-ПА, в правом нижнем углу добавлен QR-код. | STRUCTURE_SAME_VALUES_CHANGED | STRUCTURE_CHANGED | ✘ | 0.224 | 0.000 | 0.119 | 0.00 |
| 2 | `fmc_kj_spec_table_reflow` | KJ | LAYOUT_ONLY_NO_ENGINEERING_CHANGE | Изменений по существу нет: в спецификации элементов армирования убран разрыв между строками, позиции 12-поз.м., 12-Г-1, 16-П-1, их диаметры, длины, количества и массы прежние. | NEAR_IDENTICAL | STRUCTURE_CHANGED | ✘ | 0.841 | 1.000 | 0.980 | — |
| 3 | `fmc_kj_steel_table_shift` | KJ | PURE_TRANSLATION | Ведомость расхода стали не изменилась (Ø12 3506,33 кг, Ø16 935,90 кг, всего 4442,23 кг) — таблица только смещена по листу. | NEAR_IDENTICAL | STRUCTURE_CHANGED | ✘ | 0.405 | 1.000 | 1.000 | — |
| 4 | `fmc_eom_room_schedule_values` | EOM | VALUES_CHANGED | В экспликации помещений уточнены площади: лестничная клетка 17,68→17,37 м², кладовая 3.К.8 2,67→2,65 м², итог по этажу 497,57→497,24 м². | STRUCTURE_SAME_VALUES_CHANGED | NEAR_IDENTICAL | ✘ | 1.000 | 0.995 | 1.000 | 1.00 |
| 5 | `fmc_eom_tray_plan_geometry` | EOM | GEOMETRY_CHANGED_PLUS_NEW_MARKS | На плане кабельных лотков −1 этажа перестроен участок в осях 3.И–3.Л (изменены контуры венткамеры и короба) и проставлены отметки высоты h=2650 и h=3350. | STRUCTURE_CHANGED | NEAR_IDENTICAL | ✘ | 0.993 | 0.989 | 0.973 | 1.00 |
| 6 | `fmc_eom_notes_reflow` | EOM | TEXT_REFLOW_NO_ENGINEERING_CHANGE | Содержание указаний не изменилось — абзацы переверстаны, исправлена ссылка «ПУЭ, гл. 1.7, п. 1.7.76». | NEAR_IDENTICAL | INSUFFICIENT_VECTOR_DATA | ✘ | 0.000 | 1.000 | 1.000 | — |
| 7 | `fmc_eom_qr_stamp_only` | EOM | ADDED_NON_ENGINEERING_MARK | Инженерных изменений нет: на листе появился QR-код системы документооборота. | NEAR_IDENTICAL | INSUFFICIENT_VECTOR_DATA | ✘ | 1.000 | 1.000 | 1.000 | — |
| 8 | `fmc_ov_raster_retile` | OV | RASTER_REPACK_NO_CHANGE | Инженерных изменений нет: тот же вставленный растровый лист, изменена только его нарезка на изображения внутри PDF. | IDENTICAL | INSUFFICIENT_VECTOR_DATA | ✘ | 0.000 | 1.000 | 1.000 | — |
| 9 | `fmc_tx_raster_scan` | TX | RASTER_ONLY_UNCHANGED | Лист — растровый скан, изменений нет. | IDENTICAL | INSUFFICIENT_VECTOR_DATA | ✘ | 0.000 | 1.000 | 1.000 | — |
| 10 | `fmc_ss_a4_to_a3_reissue` | SS | FORMAT_AND_PAGE_ORDER_CHANGE | Лист «Расчёт сечения кабеля» перевыпущен с A4 на A3: таблица расчёта линий оповещения расширена с 14 до 21 линии (добавлены V15–V21) и все значения пересчитаны (V1: длина 390→230 м, мощность 120→84 Вт, расчётное сечение 0,819→0,338 мм²); таблица перенесена из-под текста вправо; лист переставлен с 31-й на 44-ю страницу. | STRUCTURE_CHANGED | STRUCTURE_CHANGED | ✔ | 0.003 | 0.391 | 0.549 | 1.00 |
| 11 | `fmc_ov_block_split_widened` | OV | ONE_TO_N_BLOCK_SPLIT | Лист плана фреонопроводов перевыпущен на более широком формате (1684→2526 pt) и разбит на три блока; часть участков увеличена с ø15,9х0,89 до ø19,1х0,89, добавлены подписи «Подъём на 3 этаж» / «Подъём на 4 этаж», проставлены привязки (120, 250, 270, 390, 430, 480, 1130, 1820) и к огнезащитному коробу добавлена ссылка «см. раздел АР». | STRUCTURE_CHANGED | STRUCTURE_CHANGED | ✔ | 0.519 | 0.638 | 0.628 | 1.00 |
| 12 | `fmc_eom_drawing_list_rows` | EOM | TABLE_ROWS_ADDED | В ведомость рабочих чертежей добавлены листы 43–46 (компоновка УЭРВ и наполняемость гильз, планы кабельных лотков на −1 этаже, в техпространстве и на кровле); формат листа изменён с A4x3 (630х297) на A4x4 (840х297). | STRUCTURE_CHANGED | STRUCTURE_CHANGED | ✔ | 0.740 | 0.921 | 0.777 | 1.00 |
| 13 | `fmc_eom_layout_reorg_mismatch` | EOM | LAYOUT_REORGANISED_COORD_MATCH_INVALID | Сравнивать нечего: в этих границах листа слева находится таблица потребности кабелей и схемы сигнализации, справа — план расстановки панелей ВРУ-НП6. Требуется сопоставление по объектам, а не по координатам. | STRUCTURE_CHANGED | STRUCTURE_CHANGED | ✔ | 0.109 | 0.165 | 0.095 | — |
| 14 | `fmc_eom_cable_table_values` | EOM | ENGINEERING_VALUES_CHANGED | В потребности кабелей марка питающего кабеля изменена с 4х(1х120)+1х70 на 4х(1х185)+1х95 (объём 140 м прежний); остальные строки и итоги 520 / 35 м не изменились. | STRUCTURE_SAME_VALUES_CHANGED | STRUCTURE_CHANGED | ✘ | 0.226 | 0.478 | 0.171 | 1.00 |
| 15 | `fmc_gp_section_hatch_dims` | GP | DIMENSIONS_AND_HATCH_ADDED_PLUS_LEGEND_SHIFT | На сечении 1–1 лестничного схода тип тротуара изменён с Р4.3 на Р4.2, проставлены высоты ступеней 0,12 м, справа добавлена зона существующего асфальтового покрытия (новая штриховка); условные обозначения генплана сместились и попали в границы блока. | STRUCTURE_SAME_VALUES_CHANGED | STRUCTURE_CHANGED | ✘ | 0.782 | 0.127 | 0.657 | 0.00 |
| 16 | `fmc_ov_page_shift_geometry` | OV | PAGE_ORDER_SHIFT_PLUS_GEOMETRY | Подобран другой вентилятор: VO-PatAIR-Kp-5-6/9-5,5-2 заменён на VO-PatAIR-Kp-5-6/9-3-2-V1; рабочая точка сместилась Pv 795→757 Па, Ps 633→692 Па (кривая характеристики другая). Лист переставлен с 186-й на 134-ю страницу. | STRUCTURE_CHANGED | IDENTICAL | ✘ | 1.000 | 1.000 | 1.000 | 0.00 |
| 17 | `fmc_ar_hatch_sections` | AR | DENSE_HATCH_CHANGED | На плане кровли пристройки К4/К5 исправлена отметка +59,935 → +4,935; уклоны кровли переназначены (1,63 %→2 %, 2,54 %→3 %, 3,72 %→4 %); добавлены ходовые дорожки и две шахты ОВ с привязками; в условные обозначения добавлены утеплители У2 (100 мм), У4 (50 мм) и штукатурка Ш1 (10 мм); размеры фрагмента 1 уточнены (1070→1080, 770→740, 530→500); изменения обведены облаками ревизии. | STRUCTURE_CHANGED | STRUCTURE_CHANGED | ✔ | 0.837 | 0.643 | 0.885 | 0.33 |
| 18 | `fmc_vk_spec_positions` | VK | SMALL_REAL_TEXT_CHANGE | В спецификации проставлены номера позиций 81–86 для тройников 45°/67,3° и заглушек DN50/DN110; сами изделия не изменились. | STRUCTURE_SAME_VALUES_CHANGED | NEAR_IDENTICAL | ✘ | 1.000 | 0.934 | 1.000 | 1.00 |
| 19 | `fmc_km_broken_text_swap` | KM | UNDECODABLE_TEXT_PLUS_PAGE_SWAP | Лист переставлен (был 7-м, стал 8-м); отметка верха конструкций изменена с +55,850 на +52,400, переразмерены траверсы Тр-1/Тр-2/Тр-3 (600→85, 165→55, 35→225, 565→190) и добавлена ссылка «см. л. 2». | STRUCTURE_SAME_VALUES_CHANGED | STRUCTURE_CHANGED | ✘ | 0.479 | 0.667 | 0.794 | 1.00 |
| 20 | `fmc_eom_rotated_labels` | EOM | ORIENTATION_FLIPPED | Схема этажного щита перевыпущена на большем формате и перекомпонована: вертикальные подписи развёрнуты, добавлены сведения по автоматам C25 и УЗО 40А/100 мА. | STRUCTURE_CHANGED | STRUCTURE_CHANGED | ✔ | 0.117 | 0.189 | 0.506 | 1.00 |
| 21 | `fmc_crop_mismatch_same_sheet` | EOM | CROP_MISMATCH_NO_CHANGE | Изменений нет: это один и тот же фрагмент листа, взятый с другими границами блока. | IDENTICAL | STRUCTURE_CHANGED | ✘ | 0.475 | 0.264 | 0.930 | — |
## Where v0.1 succeeds

- **It never lies about missing data.** Every raster-only or text-only block came back
  `VECTOR_DATA_INSUFFICIENT` rather than `IDENTICAL` (`fmc_tx_raster_scan`, `fmc_ov_raster_retile`,
  `fmc_eom_notes_reflow`, `fmc_eom_qr_stamp_only`). That honesty is worth keeping — the one case where
  it *did* say `IDENTICAL` on raster content (`fmc_ov_page_shift_geometry`) is precisely the case where
  two stray background lines were enough to clear the `segments_total >= 3` bar. The quality gate is
  right in principle and its threshold is wrong.
- **Order-independent segment coverage works.** On `fmc_eom_room_schedule_values` the table grid is
  identical and coverage is 1.000 in both directions at tolerance 0.01 — no false geometry alarm from a
  re-exported PDF.
- **The text value-change pairing works when the bboxes agree.** `2,67 → 2,65`, `17,68 → 17,37`,
  `497,57 → 497,24` were emitted as explicit `value_changes` with `position_distance_norm ≤ 0.0005`.
  This is the single component of v0.1 that produces expert-readable output today.
- **Cost is not the problem** (F13): 0.01–4.5 s per block.

## Where v0.1 fails, grouped by root cause

1. **No object layer** — every difference is expressed as segments, primitives and a text multiset.
   `Число примитивов: 20000 → 5`, `Изменены повторяющиеся motifs: 100`, `ветвления 2585 → 2697` appear
   in the user-facing `differences` list on 13 of 21 pairs. None of them can become
   «Добавлены два ответвления» or «Количество аппаратов 12 → 14».
2. **No relation layer** — `4х(1х120)+1х70` and `4х(1х185)+1х95` end up in two disjoint lists
   (`removed`, `added`) because the block origins differ by 0.155 of page width. Without
   "this label belongs to this table row belongs to this cable line" there is no way to
   emit «Номинал 120 → 185».
3. **Block identity is coordinate-based** — `fmc_eom_layout_reorg_mismatch` compares a cable table with
   a panel layout plan because both sit at the same normalized rectangle. 52.3 % of matched pages moved
   index (F2); sheets grow from A4 to A3 (F1 corpus) and blocks split 1→3. Any architecture that pairs
   by page index and bbox will keep producing this.
4. **The status ladder collapses five different questions into one label.** "Did the drawing change?",
   "did a value change?", "did the layout move?", "is the block comparable at all?" and "is there data?"
   are answered by one enum, so the thresholds must satisfy contradictory constraints (F7 vs F8).
5. **The text layer is lossy by construction** — clipped spans (F6), outlined CAD text absent
   entirely (F5), undecodable glyphs (F: KM pages carry U+FFFD / PUA code points).

## Reproduction

```bash
cd /home/coder/projects/PDF-proverka

# 1. cheap per-page descriptors for every document with 2+ versions (210 PDFs)
python -m experiments.stage_comparison_vector_architecture_opus.probes.fmc_scan_corpus

# 2. match pages across versions, classify candidates
python -m experiments.stage_comparison_vector_architecture_opus.probes.fmc_mine_candidates

# 3. font / ToUnicode / image profile (slow, ~3 min)
python -m experiments.stage_comparison_vector_architecture_opus.probes.fmc_page_profile

# 4. raster-diff all 3096 matched page pairs at 900 px (~85 s on 6 procs)
python -m experiments.stage_comparison_vector_architecture_opus.probes.fmc_batch_diff --px 900 --procs 6 --max-pages-per-step 250

# 5. undecodable-text census and text-orientation deltas
python -m experiments.stage_comparison_vector_architecture_opus.probes.fmc_find_broken_text
python -m experiments.stage_comparison_vector_architecture_opus.probes.fmc_find_rotation_hatch --max-pairs 300

# 6. build the 21-pair manifest + crops
python -m experiments.stage_comparison_vector_architecture_opus.probes.fmc_build_pairs

# 7. run Track A v0.1 extractor + comparator over the new corpus
#    (own runner: Track A's run_research.py writes into Track A's artifacts dir, which is read-only here)
python -m experiments.stage_comparison_vector_architecture_opus.probes.fmc_run_v01

# 8. score status + fact recall
python -m experiments.stage_comparison_vector_architecture_opus.probes.fmc_score

# 9. span-truncation measurement
python -m experiments.stage_comparison_vector_architecture_opus.probes.fmc_text_clipping

# one-off inspection of any page pair
python -m experiments.stage_comparison_vector_architecture_opus.probes.fmc_inspect \
  --left <pdf> --li 20 --right <pdf> --ri 21 [--bbox x0 y0 x1 y1] [--drawings] [--png out]
python -m experiments.stage_comparison_vector_architecture_opus.probes.fmc_raster_diff \
  --left <pdf> --li 20 --right <pdf> --ri 21 --px 1500 --png out
```

Corpus-level counts quoted above:

```bash
python - <<'PY'
import json
scan=json.load(open('experiments/stage_comparison_vector_architecture_opus/artifacts/fmc_page_scan.json'))
cand=json.load(open('experiments/stage_comparison_vector_architecture_opus/artifacts/fmc_candidates.json'))
rows=[r for r in json.load(open('experiments/stage_comparison_vector_architecture_opus/artifacts/fmc_batch_diff.json')) if 'error' not in r]
print("docs", len(scan['documents']), "pdfs", len(scan['scans']))
steps=sum(len(d['versions'])-1 for d in scan['documents'])
ident=sum(1 for d in scan['documents'] for a,b in zip(d['versions'],d['versions'][1:]) if a['sha256']==b['sha256'])
print("version steps", steps, "byte-identical", ident)
m=sum(len(s['matched']) for s in cand)
print("matched", m, "unmatched L", sum(len(s['unmatched_left']) for s in cand), "unmatched R", sum(len(s['unmatched_right']) for s in cand))
print("index moved", sum(1 for s in cand for x in s['matched'] if x['index_shift']!=0))
print("visually identical", sum(1 for r in rows if r['changed_frac']==0.0), "of", len(rows))
print("text-identical", sum(1 for r in rows if r['text_identical']),
      "of which visibly different", sum(1 for r in rows if r['text_identical'] and r['changed_frac']>0.001))
print("image-count changed", sum(1 for r in rows if r['n_images'][0]!=r['n_images'][1]))
PY
```

Quality / hatch / anchor census over the 42 corpus blocks:

```bash
python - <<'PY'
import json, glob, collections
q=collections.Counter(); hatch=0; capped=0; anch=collections.Counter()
for f in glob.glob('experiments/stage_comparison_vector_architecture_opus/artifacts/fmc_descriptions/*_left.json')+ \
         glob.glob('experiments/stage_comparison_vector_architecture_opus/artifacts/fmc_descriptions/*_right.json'):
    d=json.load(open(f)); q[d['vector_quality']]+=1
    hatch += len(d['hatch_like_structures'])>=30
    capped += bool(d['geometry']['extraction'].get('storage_capped') or d['topology'].get('segments_capped'))
for f in glob.glob('experiments/stage_comparison_vector_architecture_opus/artifacts/fmc_descriptions/*_left.json'):
    anch.update(a['confidence'] for a in json.load(open(f))['anchors'])
print(dict(q), "capped", capped, "hatch_at_cap", hatch, "anchors", dict(anch))
PY
# blocks 42 quality {'LIMITED_CAPPED': 18, 'GOOD': 13, 'VECTOR_DATA_INSUFFICIENT': 8, 'LIMITED': 3}
# capped 18 hatch_at_cap 14 anchors {'candidate': 342, 'none': 790, 'high': 1534}
```

Outlined-text page census (F5) and GP text-layer proof:

```bash
python - <<'PY'
import fitz
d=fitz.open('projects_v2/objects/214_Alia_ASTERUS/disciplines/GP/documents/13АВ-РД-ГП2/versions/v001/02_work/document.pdf')
p=d[7]
print(p.search_for("Тротуар"), p.search_for("131,56"), p.search_for("Р4.3"))   # -> [] [] []
d2=fitz.open('projects_v2/objects/214_Alia_ASTERUS/disciplines/EOM/documents/13АВ-РД-ЭМ2-ПА V1/versions/v001/02_work/document.pdf')
pg=d2[5]; items=sum(len(x['items']) for x in pg.get_drawings())
print("fonts", pg.get_fonts(), "spans", len(pg.get_text('dict')['blocks']), "draw items", items)  # -> [] 0 146189
PY
```

## Artifacts

| file | what |
|---|---|
| `artifacts/fmc_pairs.json` | **the deliverable** — 21 pairs in Track A's `block_pairs.json` shape, plus `change_class`, `why_hard`, `human_expected_ru` |
| `artifacts/fmc_crops/*.png` | 42 rendered crops, both sides of every pair |
| `artifacts/fmc_v01_results.json` | v0.1 extractor+comparator output per pair (status, similarities, differences, timings, token sizes) |
| `artifacts/fmc_descriptions/*.json(.gz)` | 42 `VectorBlockDescription` v0.1 documents (gzipped — the largest is 22 MB raw) + 21 comparison documents (plain) |
| `artifacts/fmc_score.json` | status accuracy, constant baseline, per-pair fact recall |
| `artifacts/fmc_text_clipping.json` | span-truncation measurement (F6) |
| `artifacts/fmc_page_scan.json.gz` | per-page descriptors, 210 PDFs |
| `artifacts/fmc_candidates.json.gz` | cross-version page matching + per-page deltas, 105 version steps |
| `artifacts/fmc_batch_diff.json.gz` | raster diff of 3096 matched page pairs |
| `artifacts/fmc_page_profile.json.gz` | fonts / ToUnicode / image coverage per page |

Big artifacts are stored gzipped; every probe reads through `probes/fmc_io.py`, which accepts both
`x.json` and `x.json.gz`, so no manual `gunzip` is needed.
| `artifacts/fmc_broken_text.json` | 107 pages carrying U+FFFD or PUA code points |
| `artifacts/fmc_rotation_candidates.json` | 97 matched pages whose text-orientation mix changed |
| `artifacts/fmc_outlined_pages.json` | 14 pages with no text layer and no raster cover |

## Caveats / UNVERIFIED

- Human labels are mine, written after rendering and reading both crops of every pair. They are
  documentation-expert judgements, not a second reviewer's. `fmc_ar_hatch_sections` and
  `fmc_gp_section_hatch_dims` contain more sub-changes than the sentence lists; I recorded only the
  ones I could read with certainty.
- `fact recall` is a *presence* test on the emitted payload. It does not claim a downstream model
  would actually produce the sentence — it is an upper bound.
- Page matching uses word-set Jaccard > 0.35 with an index-proximity tie-break. Pages whose text is
  outlined (F5) cannot be matched this way and fall into `unmatched_*`; the 52.3 % index-shift figure
  is therefore computed only over pages that *do* have a text layer. **UNVERIFIED:** the true
  index-shift rate including text-less pages.
- The pixel-diff threshold (grey delta > 60, 900 px long side) was not tuned; it is used only to
  *find* candidates, never to judge them.
- I did not measure token cost of a Vision alternative on this corpus — out of scope for this probe.
