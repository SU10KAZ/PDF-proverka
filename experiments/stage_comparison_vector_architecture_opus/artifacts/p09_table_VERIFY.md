# p09_table — adversarial verification of the `tbl` probe's five headline claims

Verifier: independent agent. Everything below is something I re-ran, recomputed a second way, or read
myself. All commands from repo root `/home/coder/projects/PDF-proverka`.
Nothing outside `experiments/stage_comparison_vector_architecture_opus/` was written.

| # | claim (short) | verdict |
|---|---|---|
| 1 | ss_table_graphic FP is entirely a crop-window artifact | **CONFIRMED** (stronger than claimed) |
| 2 | mechanism = right edge −15.26 pt + left top edge +14.48 pt | **CONFIRMED** (two numeric imprecisions) |
| 3 | generic reconstructor = 482/486 = 99.18 %, 100 % de-hyphenated | **WEAKENED** |
| 4 | 104/108 = 96.3 % multi-span cells joined correctly | **WEAKENED** |
| 5 | table-level diff removes the FP (NO_CHANGE / NO_TABLE_DETECTED) | **WEAKENED** |

---

## Claim 1 — CONFIRMED, and the probe under-states its own case

Re-ran `python -m …probes.tbl_reproduce_fp` → byte-identical output to the artifact
(`crop_window_delta_pt {x0:+2.38, y0:−14.48, x1:−15.26, y1:+14.59}`, `unclipped_identical: true`,
`unexplained_text_difference: []`).

The probe's `unclipped_identical` is a **weak test**: it only compares the 9 lines that contain one of
9 hard-coded keywords and have `x0 > 800`. I replaced it with three stronger, independent tests:

1. **Whole-page text.** `page.get_text("dict")` on v002 p16 and v003 p16: 110 text lines each, the
   sorted multiset of line strings is **equal**, `Counter(left)-Counter(right)` and the reverse are
   both **empty**. The 63 lines with `x0>800` are identical in text *and* in rounded coordinates.
2. **Crop-intersection test** (the probe's own falsification criterion — "find a differing span whose
   bbox is interior to both crops"): spans extracted with `get_text("dict", clip=rect)` for each
   side, kept only if strictly inside the intersection rect `[57.59, 100.53, 999.83, 594.71]`:
   **48 spans on each side, identical in text and bbox (`A == B` → True).** Every differing span
   touches or crosses a crop edge. No unexplained difference exists.
3. **Pixel test.** `md5(page.get_pixmap(matrix=2×).samples)` = `1ce7acb30da13aef8cd677ca09ac98df` on
   **both** v002 p16 and v003 p16 → the sheet is unchanged, not merely its table text.
   (11 of 20 pages of the two PDFs render identically; the PDFs themselves differ,
   sha256 `a3e7451c…` vs `5560a190…`, so this is *not* the O1 "same file on both sides" case.)

Two bookkeeping notes, neither of which touches the substance:

- "**14 of 14** differing span strings" is a *set* difference. Track A's own
  `comparison.differences` emits **16** items (10 added + 6 removed, i.e. a multiset). My multiset
  recomputation reproduces the 16 (`под` and `1` occur twice on one side); I checked both extra
  instances and they are crop-edge truncation pairs (`под`→`по`, row-1 `1` above the left top edge),
  so 16 of 16 are explained, but the headline number does not match the artifact it explains.
- `unclipped_identical` proves identity for 9 keyword lines only; the whole-page result above is the
  claim actually wanted.

## Claim 2 — CONFIRMED, with two loose numbers

Recomputed the crop rects from `block_pairs.json` myself:
left `[55.21, 100.53, 1015.09, 594.71]`, right `[57.59, 86.05, 999.83, 609.30]`.

- Row 1 spans: `1` (831.84, **77.56**, 835.44, **88.73**), `Монтажная`, `коробка`, `RVi`, `2BM` — all
  y 77.56–88.73. `88.73 < 100.53` (outside the left crop) and `88.73 > 86.05` (inside the right one).
  **Exactly as claimed.**
- The six mid-string cuts reproduce: `видеокамера→видеокаме`, `витую→вит`, `"Sto→"`, `протяжко→протя`,
  `под→по`, `н→∅`, all at the right crop's `x1 = 999.83`.

Imprecisions:

- "the right crop is **15.26 pt narrower**" — 15.26 pt is the shift of the *right edge*. The right
  crop is **17.64 pt** narrower in width (959.88 → 942.24), because `x0` also moves +2.38.
- The 10/3/1 attribution is partly arbitrary: `2BM` is counted as right-edge-explained although it is
  a row-1 span above the left crop's top edge (it happens to satisfy both). The probe's classifier
  also uses a loose `startswith` prefix rule; I replaced it with pure bbox geometry (test 2 above) and
  the conclusion survives, so this does not matter for the verdict.

## Claim 3 — WEAKENED

**What holds.** `tbl_run_eval` + `tbl_score` re-ran end to end; `tbl_eval.json` came back
**byte-identical** (`diff` on canonicalised JSON → no output). 482/486, 4 errors, all soft
hyphenation in header cells (3 in `fresh_aps_k3_specification`, 1 in `fresh_kk_pa_specification`).

**The ground truth is real.** I read two crops myself and checked them against the GT vectors:
- `tbl_crops/fresh_kk_pa_specification_0.png` (a `/Rotate 90` page): the 9-column header, the
  `1…9` numbering row, the two section headings and rows 1–6 (`1279`, `136`, `131`, `450`, `178`,
  `1201`, `Для перегородки`, `Огнезащита шпилек короба`) match the GT exactly. The image also shows
  `Единица измере-/ния` literally hyphenated, which is the very error the scorer records — GT is a
  human reading, not a copy of the output.
- `tbl_crops/fresh_askuvt_cable_journal_0.png`: 10 columns, 3-level merged header, rows 4–9 match.

**It is not a trivial result.** PyMuPDF's own `page.find_tables()` on the same five fresh pages
returns shapes (36×19, 45×21, 34×19, 20×9, 34×17) and scores **0/56, 29/90, 28/90, 35/100, 59/90**
against the same GT. The reconstructor is doing real work.

**Why the claim is still weakened.**

1. **Sample concentration.** All 9 tables come from **one object** (`214_Alia_ASTERUS`) and 8 of 9
   from **one discipline** (SS); the 9th is EOM in the same object. The claim's "6 sheets from 4
   documents" is also wrong in the ledger: the artifacts give **9 distinct (pdf, page) pairs,
   7 logical sheets, 5 documents** (СОТ-К7, АПЗ.АПС-К3, АСКУВТ, КК-ПА, ЭМ-К4). "2 of them on
   /Rotate pages" is correct (ЭМ-К4 left rot 270, КК-ПА rot 90).
2. **Truncated denominator.** `tbl_score` scores `n_rows = len(gt["rows"])` only. Detected vs scored
   rows: 17→8, 24→10, 20→10, 20→10, 22→10. In 5 of the 9 tables **less than half the reconstructed
   rows are scored**, and the header — where every observed error lives — is always inside the
   scored window while the body tail never is.
3. **27 % of the denominator is free.** `nonempty_gt = 355` of `gt_cells = 486`: **131 scored cells
   are empty in GT and empty in the prediction.** Non-empty accuracy is 351/355 = 98.87 %.
   40 further cells are the *same* table scored twice (`ss_table_graphic` left and right are
   identical rows over a pixel-identical page).
4. **The 100 % is a filter applied to the evaluation set.** `cell_accuracy_dehyphenated = 1.0` comes
   from `canon(..., dehyphenate=True)`, a `(\w)-\s+(\w)` rewrite added at *scoring* time that exists
   solely to absorb the only 4 errors in the sample. A principled fix belongs in
   `fill_table_text` (which joins wrapped lines with `" ".join`, creating the artefact); reporting
   100 % from the scorer is a post-hoc number on the data it is measured on.
   The other tuned constant, `EMPTY_BAND_SPLIT`, is **not** knife-edge: I swept it and 2.0–6.0 all
   give 482/486, while ≥8.0 (or no split) drops to **453/486 = 93.2 %** because
   `fresh_aps_k3_specification` collapses back to 32×13 / 58 of 90. So the rule matters, its value
   does not.
5. **Held-out counter-measurements (mine).** I ran the same reconstructor on tables it has never
   seen, from other objects/disciplines, and hand-read the GT from the rendered crop the same way:

   | held-out table | scored region | result |
   |---|---|---|
   | `256_Primavera` OV `СТ26_01-14-ОВ1-Г-РД` p12 specification (`artifacts/p09_table_verify_prim_p11.png`) | 10 rows × 9 cols | **80/90 = 88.9 %**, de-hyphenation gains **0** |
   | `314_Sobytie` ITP `ПД-00542664-ИТП.ЭОМ` p10 cable journal, `/Rotate 270` (`artifacts/p09_table_verify_itp_p9.png`) | 8 data rows × 12 visual cols | **80/96 = 83.3 %** |
   | `256_Primavera` OV `СТ26_01-14-ОВ1-Г-РД` p6 ventilation table (15×18) | 7 data rows × 18 cols | 126/126 = 100 % |

   The two failures are **structural, not cosmetic**: on the Primavera specification the header cells
   of columns 5–9 collapse into one cell (`'Ед. Масса изме- Завод-изготовитель Кол. 1 ед., Примечание
   ре- кг ния'`, numbering row `'5 6 7 8 9'`); on the ITP journal the detector returns **16 columns
   for a 12-column table** (title-block rulings interleaved into the sheet-frame region), so every
   value sits one column right of where it belongs. `_split_on_empty_bands` does not fire in either
   case because the empty band contains the title block's text. A third held-out table
   (`13АВ-РД-ВК2-К1` p24, taken from the probe's own coverage sample) puts a stray `12` into the
   `Примечание` header cell.

   The probe's own `Limitations` admits the constants are sample-set; it does not report that a
   held-out sheet loses 11–17 points, nor that the residual errors change class.

**Verdict:** the number 482/486 is real and reproduces exactly, on a real hand-read ground truth,
against a strong baseline. The sentence *"reads real CAD tables at 99.18 % cell accuracy"* is not
supported: it is 99.18 % on 9 hand-picked tables from a single object with a leading-rows-only
denominator, 27 % of which is free; held-out sheets measure 83–89 %.

## Claim 4 — WEAKENED

`totals.multi_span_join_accuracy = 0.963`, `multi_span_gt = 108`, and the 15-span cell
(`eom_singleline_changed_right` r2c1, «Автоматический выключатель ВА-103 3P…») all reproduce exactly;
the span_count histogram over the scored region is `{0:131, 1:247, 2:43, 3:19, 4:17, 5:5, 6:3, 7:2,
8:3, 9:6, 11:1, 12:7, 13:1, 15:1}` → 108 cells with ≥2, max 15. So the arithmetic is right.

What the metric does **not** say:

1. **The denominator is defined by the system under test.** `tbl_score` reads `span_count` from the
   *predicted* cell (`matrix_from_cells(table)`), so "ground-truth cells built from ≥2 PDF spans" is
   really "predicted cells that ended up holding ≥2 spans". A cell that *should* have joined two
   spans but lost one cannot enter the denominator — the metric is structurally blind to its own
   worst failure mode. (Here it happens not to bite: all 4 scored errors are inside the 108.)
2. **"Joining" is containment, not an algorithm.** `fill_table_text` puts a span in a cell if its
   centre is inside the cell rect ±0.5 pt, then sorts by line and concatenates. The 96.3 % therefore
   measures **grid correctness**, not a solution to span fragmentation — which is exactly why the
   held-out ITP journal (Claim 3) mis-joins nothing yet still puts every value in the wrong column.
3. **The average hides the spread.** Per table: 38/38, 18/18, 11/11, 9/9, 9/9, 5/5, 5/5, 7/8 and
   **2/5 = 40 %** (`fresh_aps_k3_specification`). One table carries 38 of the 108. 18 of the 108 are
   the same 9 cells scored twice (identical `ss_table_graphic` sides).
4. "100 % de-hyphenated" is the same scorer-side rewrite as in Claim 3.

## Claim 5 — WEAKENED

**Both verdicts reproduce.** Re-running `tbl_run_eval` prints
`ss_table_graphic [frame] verdict=NO_CHANGE … * Изменений в таблице нет.` and
`ss_table_graphic [clip] verdict=NO_TABLE_DETECTED`. I also recomputed the clip-mode diagnostics from
scratch (no script in `probes/` generates `tbl_clipmode_diagnostics.json`): rulings 86/93 full →
47/70 (left) and 48/69 (right) after clipping, 3 candidates → 1, surviving candidate
`[802.80, 116.16, 864.72, 274.80]` left vs `[802.80, 93.36, 864.72, 274.80]` right — the 1-column
position strip with the top edge one row band apart, exactly as claimed.

**But the causal attribution is wrong, and a trivial baseline wins the same point.**

- In frame mode the reconstructor abandons the block crop and re-anchors on the table's own frame:
  both sides return bbox `[802.80, 14.16, 1176.24, 274.80]`. The two rendered frame crops are
  **byte-identical PNGs** (`md5 9d429f938b95705778e37aee5bafd602` for
  `ss_table_graphic_left_frame_0.png` and `…right_frame_0.png`). The two 10×2 table dicts differ in
  **exactly one boolean** (`cells[r2c0].clipped True→False`); all 20 cell texts are identical.
  `diff_tables` compares whitespace-normalised strings, so `NO_CHANGE` is forced by construction.
- Since page 16 of v002 and v003 renders **pixel-identical** and its text is identical line for line
  (Claim 1), the same "false positive removed" result is produced by: comparing the two pages' text;
  md5-ing the rendered table region; or any comparator that is anchored on document coordinates
  instead of the block bbox. **The table structure is sufficient but not necessary** — what removes
  the FP is dropping the crop window, which the probe itself proves is the sole cause (T1/T2).
- The win is measured on a pair where **nothing changed at all**, i.e. one of the 6 NEAR_IDENTICAL
  pairs from orchestrator finding O2. It demonstrates zero recall on real engineering change; a
  constant "always answer NO_CHANGE" baseline scores identically on this pair. (Real-change evidence
  exists elsewhere in the probe — T10/T11 — but not in this claim.)
- The clip-mode half is factually correct but operationally empty: on the exact block Track A got
  wrong, the table layer produces **no table**, so the FP is avoided only if something else suppresses
  the span-multiset comparison. Note also that in frame mode `open_sides` is computed with
  `region=None`, so the `TABLE_CLIPPED_NOT_COMPARABLE` gate — the probe's proposed safety net — can
  never fire in the very mode that yields `NO_CHANGE`.

## Traps checked

| trap | result |
|---|---|
| claim resting on 1–2 blocks | **yes** — claims 1, 2, 5 rest entirely on `ss_table_graphic`; claim 5's "win" is 1 pair with zero document change |
| ground truth the probe assigned itself | **no** — I re-read two crops; GT is a genuine human reading (its 4 disagreements are exactly the hyphenation the PDF really contains). Caveat: GT *shape* (rows/cols, merged-cell convention) follows the reconstructor's segmentation |
| precision/recall without denominator | **yes** — 486 includes 131 free empty cells and 40 duplicate cells; only the leading 8–10 of 17–24 rows are scored; the multi-span denominator is prediction-defined |
| filter tuned on the evaluation data | **partly** — `dehyphenate` (the 100 % figure) is scorer-side and post-hoc; `EMPTY_BAND_SPLIT` is load-bearing (93.2 % without it) but stable over 2.0–6.0 |
| win only on the pairs where something changed | **inverted** — the win is only on a pair where *nothing* changed |
| contaminated by O1/O2 | **claim 5 yes** — the pair is not O1 (different files) but the *page* is pixel-identical, so it carries no change signal at all |

## Reproduction

```bash
cd /home/coder/projects/PDF-proverka
python -m experiments.stage_comparison_vector_architecture_opus.probes.tbl_reproduce_fp
python -m experiments.stage_comparison_vector_architecture_opus.probes.tbl_run_eval
python -m experiments.stage_comparison_vector_architecture_opus.probes.tbl_score
# independent checks in this note were run as inline python heredocs:
#  - whole-page text multiset equality + strictly-interior span equality (48/48)
#  - md5 of page pixmaps (v002 p16 vs v003 p16)
#  - EMPTY_BAND_SPLIT sweep {2,3,3.5,4,5,6,8,inf}
#  - pymupdf page.find_tables() scored against tbl_ground_truth.json
#  - held-out reconstruction + hand-read GT for the three tables listed above
```

Artifacts I added (verification evidence only, prefix `p09_table_verify_`):
`p09_table_verify_prim_p5.png`, `p09_table_verify_prim_p11.png`, `p09_table_verify_itp_p9.png`,
`p09_table_verify_merge_0.png`, `p09_table_verify_merge_1.png`.
