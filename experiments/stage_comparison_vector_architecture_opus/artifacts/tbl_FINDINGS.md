# Probe `tbl` — TABLE LAYER: is a generic vector table reconstruction needed above the text layer?

Track B (Opus) independent audit. Research only; nothing outside
`experiments/stage_comparison_vector_architecture_opus/` was modified.
All commands below run from the repository root `/home/coder/projects/PDF-proverka`.

## Headline claims

| # | claim | evidence (measured number / file) | confidence | how it could be falsified |
|---|---|---|---|---|
| T1 | Track A's `ss_table_graphic` false positive «Добавлена позиция «1 Монтажная коробка RVi 2BM»» is **entirely a crop-window artifact**, not a document change. The two PDFs' table text is identical line for line. | `tbl_failure_reproduction.json`: `unclipped_identical: true` for all 9 legend lines; `crop_window_delta_pt = {x0:+2.38, y0:−14.48, x1:−15.26, y1:+14.59}`; **14 of 14** differing span strings classified, `unexplained_text_difference: []` | high | Show one legend line whose unclipped text differs between v002 p16 and v003 p16 |
| T2 | The mechanism is two-fold and both parts are pure geometry: the right crop is **15.26 pt narrower**, cutting six words mid-string (`видеокамера→видеокаме`, `витую→вит`, `"Sto→"`, `протяжко→протя`, `под→по`, `н`); the left crop starts **14.48 pt lower**, so table row 1 (span bbox y 77.56–88.73) misses the left clip (y0 = 100.53) but intersects the right one (y0 = 86.05) | `tbl_failure_reproduction.json → symmetric_text_difference`: 10 strings explained by the right edge, 3 (`RVi`, `Монтажная`, `коробка`) by the top edge | high | Recompute the clip rects from `block_pairs.json` and find a differing span whose bbox is interior to both crops |
| T3 | A generic vector table reconstructor (rulings → grid → merged cells → span-to-cell assignment, **no discipline knowledge**) reads real CAD tables essentially exactly: **482 / 486 = 99.18 %** cell accuracy over 9 tables on 6 sheets from 4 documents; **486/486 = 100 %** once the 4 remaining misses (soft hyphenation inside header words, e.g. PDF literally holds `Код обору- дования`) are de-hyphenated | `tbl_eval.json → totals`; ground truth transcribed by reading the crops in `tbl_crops/` (`tbl_ground_truth.json`) | high | Transcribe a different ground-truth sample from the same crops and get a materially lower score |
| T4 | Span fragmentation is fully resolved by the cell: **104 / 108 = 96.3 %** of ground-truth cells that are built from ≥2 PDF spans are joined into exactly the right value (100 % de-hyphenated). The heaviest scored cell joins **15** spans into one value (`Автоматический выключатель ВА-103 3P, 6 кА : C25 -для 1, 2, 3 и 4 Е комн . квартир`); the heaviest cell anywhere in the run joins 34 | `tbl_eval.json → totals.multi_span_join_accuracy = 0.963`, `multi_span_gt = 108` | high | Find a cell where two spans of the same visual value land in different cells |
| T5 | **A table-level diff removes the false positive.** Comparing `ss_table_graphic` as a 10×2 table instead of a span multiset yields verdict **`NO_CHANGE`** with the single sentence «Изменений в таблице нет.» — agreeing with the Vision arm that Track A scored as the better one on this pair | `tbl_diff_ss_table_graphic.md`; `tbl_tables.json → diffs.ss_table_graphic.frame.verdict = "NO_CHANGE"` | high | Produce a cell whose value differs between the two versions |
| T6 | When the block crop, rather than the table's own frame, defines the region, **no complete table exists** and the safe answer is "nothing comparable here" — also not a false positive. The clipped candidate collapses to the 1-column position strip (`802.8–864.7`), and its top edge moves by exactly one row band between the sides (`y 116.2` left vs `y 93.4` right) | `tbl_clipmode_diagnostics.json`; `tbl_tables.json → diffs.ss_table_graphic.clip.verdict = "NO_TABLE_DETECTED"` | high | Detect a 2-column table inside the left crop whose right border is present |
| T7 | The detector **separates the table region from the engineering graphic in the same block**. In `ss_table_graphic` left: 37 of 52 ink spans fall inside the detected table, 15 belong to the mounting detail; of 1598 drawing segments in the block only **11 (0.7 %)** are table rulings | `tbl_separation_and_size.json → ss_table_graphic.left` | high | Show a detected table bbox that overlaps the drawing, or a legend row assigned to the drawing |
| T8 | Tables are **not** a niche concern of one discipline. On 150 randomly drawn corpus pages across 12 disciplines, **77.3 %** carry at least one ruled table, **43.3 %** carry a body table beyond the title block, and **28.3 % of all ink text spans (19 074 / 67 472)** sit inside a table cell (SS 42.4 %, OV 40.0 %, VK 37.5 %, KJ 32.8 %, EOM 29.7 %, AR 18.1 %) | `tbl_coverage.json → summary` | high | Re-draw a different 150-page sample and get a materially different rate |
| T9 | The table layer is **cheap**: median 0.08 s to reconstruct every table on a page (max 6.99 s over 150 pages), and the whole `ss_table_graphic` legend serialises to **409 characters** of Markdown against 2 369 characters of Track A's Level-3 description and 183 196 characters of its JSON | `tbl_coverage.json → median_t_reconstruct_s`; `tbl_separation_and_size.json → table_markdown_chars / track_a_level3_md_chars / track_a_json_chars` | high | Time it on a denser corpus and exceed the stated medians |
| T10 | The row **is** the natural unit for the sentences the expert must read. Real version pairs produce them directly: «Строка «18», колонка 7: 277 → 278», «Строка «19», колонка 7: 208 → 207» (АПЗ.АПС-К3 v001→v002), «Добавлена строка «ОСПД1.1-ВК1.1.1.1»: … 220 | 235 | Лоток СС…» (СОТ-К7 cable journal v002→v003), «Изменений в таблице нет.» (СОТ-К7 specification v002→v003) | `tbl_version_diff.json`; verified against the crop `tbl_crops/kkpa_v002_p15_top.png` (266 / 562 / 836 read from the image match the reconstruction) | high | Show that a reported value pair does not exist in the source PDFs |
| T11 | **Row identity is a separate, unsolved problem, and the table layer alone does not solve it.** Keying rows on the position number gives 44 cell changes for КК-ПА v001→v002; content-based row matching gives 2 rows added, 2 removed and 18 changes of which **12 are pure renumbering** — i.e. **6 genuine quantity changes** (247→266, 3→6, 35→32, 525→562, 6→10, 49→56). On the cable journal the ranking inverts: key alignment gives the right answer (3 rows added, 3 removed) and content alignment mislabels the same edit as 15 value changes | `tbl_version_diff.json → kk_pa_specification` and `sot_k7_cable_journal`, both alignments | high | Find one alignment rule that is right on both sheets without per-sheet tuning |
| T12 | **Bonus (falsifies part of Track A's foundation).** PyMuPDF returns `get_drawings()` *and* `get_text()` in **unrotated** space while `page.rect` and `get_pixmap` are in **displayed** space. `extractor.py` builds its block rect from `page.rect` (lines 946–949) and never rotates, so on a `/Rotate` page it samples the wrong part of the sheet. For `eom_singleline_changed` **left** (rotation 270) the crop-region IoU is **0.4644**, the text Jaccard **0.6316**: Track A read 14 spans that are not in the crop image (`Согласовано`, `ГАП`, `Рябцева`, `Взам. инв. №`, `Гл.спец.ВК`…) and missed 14 that are (`QF1`, `QD1`, `Wh1`, `Шина N`, `ЩМкв1`, `Ip=21,2A`, `Pp=13кВт;`…). Per O2 this is one of only **two** pairs that test change recall at all | `tbl_rotation_check.json`; `experiments/stage_comparison_vector_blocks/extractor.py:946-949` (no rotation term) | high | Show `page.get_drawings()` returning displayed-space coordinates on that page |
| T13 | Rotated sheets are common enough to matter: **250 + 40 + 3 = 293 of 4 959 pages (5.9 %)** were rotated in a 120-PDF random draw, and **33 of 120 documents (27.5 %)** contain at least one rotated page. (An earlier draw over 4 917 pages gave 4.41 % / 30 of 120; the corpus grew from 563 to 571 PDFs between runs, so treat the rate as ≈4–6 %.) | `tbl_rotation_check.json → corpus_rotation_sample` | medium (sample-dependent) | Scan every corpus page instead of a 120-PDF sample |
| T14 | Two-table-in-one-frame is a real failure mode with a generic fix. A specification and the title block sharing the sheet frame merged into one 32×13 region (columns interleaved, matrix accuracy 58/90). Splitting the region at any band ≥4× the median row height that contains **no text** restores 20×9 and 87/90 | `tbl_table_layer.py → _split_on_empty_bands`; before/after runs of `tbl_run_eval` + `tbl_score` (58/90 → 87/90 for `fresh_aps_k3_specification`) | high | Find a real table with an internal empty band ≥4× median that must not be split |

## Answer to the probe question

**Yes — a generic vector table reconstruction is needed, and it belongs above the text layer as a
first-class primitive, not inside a discipline profile.**

Three measured reasons:

1. **It is generic.** The reconstructor in `probes/tbl_table_layer.py` contains no discipline
   knowledge whatsoever: no keyword list, no column-name dictionary, no expected schema. It reads a
   СОТ cable journal, an АПС specification, an АСКУВТ cable journal with a three-level merged
   header, a КК-ПА specification on a `/Rotate 90` page and an EOM legend on a `/Rotate 270` page at
   99.2 % cell accuracy with the same 4 constants (T3).
2. **It is everywhere.** 28.3 % of all ink text on a random 150-page corpus draw is inside a table
   cell, in every discipline sampled (T8). A layer that 28 % of the text needs cannot sit inside one
   profile; each profile would have to re-implement it.
3. **It changes the answer, not just the presentation.** The single Vision-vs-Vector loss Track A
   recorded flips from a false positive to `NO_CHANGE` (T5), and real version pairs turn into exactly
   the sentences the brief demands (T10).

**But the table layer is a *generic object layer for one object class*, and that is the point.** What
made the diff safe was not the text — it was giving the text a *container with identity*: a cell that
knows its row, its column, its neighbours, and whether it was truncated. The moment the container
existed, span fragmentation stopped being expressible as a change, and `250 → 315` became
expressible as one.

The same probe also shows the limit. **Row identity is not derivable from the table geometry**
(T11): position numbers renumber on insert, and content similarity mismatches template rows. Deciding
that «Лоток перфорированный 300х100х3000 / 35344» in v001 and v002 are *the same object* is exactly
an object-identity decision, one level above the grid. My best generic rule is right on one sheet and
wrong on the other, and no tuning of the table layer fixes that.

So this probe leans **B**: the geometry/topology/text backbone is not enough, and the missing piece is
a layer of *objects with identity and relations*. The table is the cheapest, most universal, most
verifiable instance of that layer — and it is already needed, today, for the corpus that exists.

## What this probe says about `VectorBlockDescription v0.1`

- **Add** `tables[]`: `{bbox, rows, cols, cells:[{row, col, rowspan, colspan, rect, text,
  span_count, clipped}], open_sides}`. Every measurement above comes from that structure alone.
- **Add** `open_sides` / per-cell `clipped`. Without it a crop boundary is indistinguishable from a
  design change; with it the comparator can say «не сравнивается (обрезано)» instead of inventing a
  row (T2, T6).
- **Add** a `coordinate_frame` field and rotate drawings and text into displayed space. Track A's
  descriptions of `/Rotate` pages describe a region the crop image does not show (T12).
- **Drop / demote** the raw `texts[]` multiset as a comparison signal wherever a table exists: 71 %
  of the ink spans in `ss_table_graphic` (37/52) are table cell fragments, and it is exactly those
  that produced all ten `differences` lines Track A emitted for that pair.
- **Do not** put table handling in a discipline profile.

## Limitations / not verified

- The reconstructor assumes **axis-aligned rulings after derotation**. Tables drawn at an arbitrary
  angle are UNVERIFIED; none appeared in the sample.
- Curves (`c` items) are ignored as rulings — correct for every sheet seen, UNVERIFIED in general.
- Borderless tables (whitespace-aligned columns with no rulings) are **out of scope by construction**
  and were not measured.
- Ground truth is 486 cells over 9 tables read by eye from raster crops; the comparison key
  normalises whitespace, dash/ellipsis glyphs and case, because transcription cannot resolve those
  reliably. Raw byte equality was **not** measured.
- The `EMPTY_BAND_SPLIT = 4.0` and `MIN_RULING = 14.0` constants were set on this sample; their
  generality is UNVERIFIED.
- Scanned (raster) sheets have no vector rulings; this layer does nothing for them. Not measured.
- The corpus rotation rate is a 120-PDF sample, and the corpus changed size between two runs (T13).

## Files

| path | what |
|---|---|
| `probes/tbl_table_layer.py` | the generic reconstructor (rulings, row-band seeding, merged cells, span→cell, openness, rotation handling) |
| `probes/tbl_table_diff.py` | table-level diff, key and content row alignment, Russian sentences |
| `probes/tbl_reproduce_fp.py` | reproduces the Track A false positive with numbers |
| `probes/tbl_rotation_check.py` | coordinate-frame defect + corpus rotation rate |
| `probes/tbl_run_eval.py` | reconstructs the evaluated blocks, runs both diff modes, renders crops |
| `probes/tbl_score.py` | scores reconstruction against `tbl_ground_truth.json` |
| `probes/tbl_coverage.py` | corpus-wide table coverage and timing |
| `probes/tbl_separation_and_size.py` | table-vs-graphic separation, representation size |
| `probes/tbl_version_diff.py` | real version-pair table diffs, both alignments |
| `probes/tbl_write_diff_md.py` | renders `tbl_diff_ss_table_graphic.md` |
| `probes/tbl_scan_corpus.py` | helper: find table pages in a PDF |
| `artifacts/tbl_failure_reproduction.json` | T1, T2 |
| `artifacts/tbl_ground_truth.json` | 486 hand-read cells |
| `artifacts/tbl_tables.json` | all reconstructions + both diff modes |
| `artifacts/tbl_eval.json` | T3, T4 |
| `artifacts/tbl_diff_ss_table_graphic.md` | T5, the headline diff |
| `artifacts/tbl_clipmode_diagnostics.json` | T6 |
| `artifacts/tbl_separation_and_size.json` | T7, T9 |
| `artifacts/tbl_coverage.json` | T8, T9 |
| `artifacts/tbl_version_diff.json` | T10, T11 |
| `artifacts/tbl_rotation_check.json` | T12, T13 |
| `artifacts/tbl_crops/*.png` | crops used as ground truth |

## Reproduction

```bash
cd /home/coder/projects/PDF-proverka
python -m experiments.stage_comparison_vector_architecture_opus.probes.tbl_reproduce_fp
python -m experiments.stage_comparison_vector_architecture_opus.probes.tbl_rotation_check
python -m experiments.stage_comparison_vector_architecture_opus.probes.tbl_run_eval
python -m experiments.stage_comparison_vector_architecture_opus.probes.tbl_score
python -m experiments.stage_comparison_vector_architecture_opus.probes.tbl_separation_and_size
python -m experiments.stage_comparison_vector_architecture_opus.probes.tbl_version_diff
python -m experiments.stage_comparison_vector_architecture_opus.probes.tbl_coverage 150
python -m experiments.stage_comparison_vector_architecture_opus.probes.tbl_write_diff_md
# single page, ad hoc:
python -m experiments.stage_comparison_vector_architecture_opus.probes.tbl_table_layer \
  "projects_v2/objects/214_Alia_ASTERUS/disciplines/SS/documents/13AB-РД-СОТ-К7 V1/versions/v002/02_work/document.pdf" 15
```

Total wall time for the whole set on this machine: under 6 minutes; the 150-page corpus sweep
dominates it (~2 min). Extraction was never the bottleneck: median `page.get_drawings()` 0.07 s.
