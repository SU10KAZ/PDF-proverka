# `hatchnoise` — can foreground engineering geometry be separated from hatch/background
# WITHOUT knowing the discipline?

Track B (Opus) independent audit. Probe prefix `hatchnoise`. Research only; nothing outside
`experiments/stage_comparison_vector_architecture_opus/` was modified.

**Short answer: no.** Every feature that looks like it should separate hatch from linework either
sits at chance or flips its sign between sheets, and across 560 threshold settings scored on 8
CAD-layered blocks from 5 disciplines, the only setting whose worst-case foreground loss stays
≤1 % removes 0.0 % of the background on 7 of 8 sheets (mean 0.0003). What actually works — "this filled
outline repeated 24 times is a socket, that stroke repeated 24 times is hatch" — is an object
judgement, not a geometry threshold.

---

## Findings table

| # | claim | evidence (measured number / file) | confidence | how it could be falsified |
|---|-------|-----------------------------------|-----------|---------------------------|
| H1 | No discipline-free threshold setting removes background safely across sheets. Of 560 settings scored on 8 layered blocks, the best whose **worst-case** foreground loss ≤1 % removes 0.0003 of background as a mean over blocks (exactly 0.0 on 7 of 8, 0.002 on the eighth). Relaxing to ≤5 % worst-case loss buys mean 0.111, and that is one block (eom_em_k1 0.848) with 0.0 on five others. | `hatchnoise_p4_universal.json`; `hatchnoise_p4_transfer.json` | high | Find a feature set (or a per-sheet auto-calibration) whose worst-case foreground loss stays ≤1 % while removing >30 % of background on all 8 blocks. |
| H2 | Thresholds tuned on one sheet destroy the next sheet. The setting that is optimal on `eom_em_k1` (95.3 % background removed at 1 % foreground loss) eats **44 %** of foreground on `ar_layered_plan`, **48 %** on `ar41_k5_plan`, **51 %** on `ss_askuvt_k1`, **57 %** on `km_nvf_facade`. | transfer matrix in `hatchnoise_p4_transfer.json → transfer` | high | Show the transfer loss is an artefact of my layer→class map by re-labelling by hand and re-running `evaluate()`. |
| H3 | "Hatch = one dominant local angle" is false. `cell_dominant_angle_share` mean AUC **0.525** (min 0.206, below chance on 4 of 8 blocks); `cell_orientation_entropy_inv` 0.528; `angle_is_45deg` 0.503; `cell_segment_density` 0.506; `cell_tiny_frac` 0.507; `enclosed_in_closed_contour` 0.409. | `hatchnoise_p1_features.json → cross_block_summary` | high | Compute the same AUCs at a different neighbourhood size than 1/64 block and show they rise above ~0.75 on every block. |
| H4 | Style features do not merely weaken, they **invert**. `stroke_luminance` AUC ranges 0.160→0.900 (3 of 8 blocks below 0.5); `stroke_width_inv` 0.218→0.840 (4 below); `is_filled_path` 0.212→0.698 (7 below); `is_colored` 0.235→0.493 (8 below). On `ar_layered_plan` the hatch is *darker and thicker* than the linework. | same file, `auc_values` per feature | high | Show the inversions come from mis-set page rotation or a wrong bbox on those blocks. |
| H5 | Only "the path is short / few-segment / its shape repeats" carries signal, and even it flips once: `motif_repetition` mean AUC 0.851 (0.470–0.975), `parent_path_segment_count_inv` 0.848 (0.485–0.952), `parent_path_length_inv` 0.844 (0.480–0.958). | same file | high | Same as H3. |
| H6 | The repetition feature eats the countable engineering objects. On the socket-layout sheet the filter destroyed **6 050 of 9 522 primitives (63.5 %)** and **14 728 of 31 387 segments (46.9 %)** on layer `08_Розетки и выводы` — the sheet's own subject — plus 67 % of `Двери ArchiCAD` and 62 % of `A-AREA-____-IDEN`. | `hatchnoise_p2c_eaten_breakdown.json`; render `hatchnoise/ar_layered_plan/zoomA_07_eaten.png` | high | Re-run with a symbol-aware fingerprint (connected-component level) and show the socket layer survives at >95 %. |
| H7 | On a sheet whose text is outlined into curves, the "repeated motif" rule deletes the **text**, not the hatch. `ov_nodes_hatch` has **1** vector text span for 423 535 segments; rule P2 dropped 283 233 segments (66.9 %) and the render of what it dropped is the sheet's Russian callouts, while the construction hatch survives into the KEEP image. | `hatchnoise/ov_nodes_hatch/04_drop_P2.png` vs `02_keep.png`; `hatchnoise_p2d_motif_identity.json` | high (visual + counts) | Show the P2 drop image contains no legible text at higher render resolution. |
| H8 | Track A's motif fingerprint carries no object identity: hash `70d40d626ad9` is a **single horizontal micro-line** (`length_norm = 6e-05`) that occurs 7 444× in an OV nodes sheet and 14 017× in an unrelated AR plan of another object. `_primitive_pattern` normalises by bbox and rounds to 1 decimal, so all 1-segment strokes of similar aspect collapse to one token. | measured by re-running `extractor._primitive_pattern` over both blocks (command below) | high | Show these are genuinely the same drawn object. |
| H9 | The filter is unstable under sub-pixel crop jitter. On `ar_plan`, whose two sides are the *same PDF bytes* (O1), **475 of 17 869** identically-positioned segments (2.66 %) flip keep/drop; on `ar_wall_sections` 142 of 35 867 (0.40 %). A real change like «добавлены два ответвления» is a handful of segments — the filter's own noise is larger than the signal. | `hatchnoise_p5_stability.json` | high | Show the flips come from segment-key rounding rather than the filter (increase key precision and re-run). |
| H10 | Filtering does fix the caps: `LIMITED_CAPPED → GOOD` on `ar_plan`, `ar_wall_sections`, `vk_nodes`; the 20 000-primitive cap stops firing on 4 of the 5 pairs where it fired, the 8 000-segment topology cap on 3 of 6, the comparator's 12 000-segment cap on 4 of 6. | `hatchnoise_p3_payoff.json` | high | — |
| H11 | …but it changes the comparator's verdict on **2 of 8 pairs**: `ar_layered_plan` STRUCTURE_CHANGED → STRUCTURE_SAME_VALUES_CHANGED (geometry similarity rises 0.959→0.985, i.e. a real geometric change is *masked*), `vk_nodes` NEAR_IDENTICAL → STRUCTURE_CHANGED (0.997→0.944, a change is *invented*). Unchanged pairs stay high but degrade: topology similarity 0.999→0.960 (`ar_plan`), 0.967→0.908 (`ar_wall_sections`). | `hatchnoise_p3_payoff.json` | high | Show the flips are correct against an expert verdict on those two pairs. |
| H12 | A PDF **layer / OCG name** is the one feature that would work — and it is missing on ~80 % of the corpus. 165 of 558 documents carry OCGs; on a mid-page probe of those, 108 pages carry informative layer names, 59 carry none, 2 are degenerate (everything on layer `0`). In a random 40-page sample only **162 788 of 1 179 518 drawings (13.8 %)** had a non-empty layer, and the split is bimodal (86 pages at 100 %, 51 at 0 %). | `hatchnoise_layer_census.json`, `hatchnoise_layer_quality.json` | high | Recover layer names for unlayered PDFs from another PDF structure (content-stream marked content, XObject names). |
| H13 | Where layers exist they are extremely informative: on `13АВ-РД-АР4.2-К4` page 24, **66 587 of 89 302 drawings (74.6 %)** sit on `A-WALL-____-PATT` (wall hatch); across 58 dense layered pages the mean share of drawings on a name-declared hatch layer is 16.5 % (max 78 %). Names seen: `A-WALL-____-PATT`, `!Подоснова`, `!АДМ-Мебель`, `Стены штриховка`. | `hatchnoise_layer_quality.json`, `hatchnoise_layer_census.json` | high | — |
| H14 | **Side finding (Track A defect).** `page.get_drawings()` and `page.get_text()` return coordinates in the *unrotated* page box while `page.rect` is the rotated rect. `extractor.extract_block` builds `block_rect` from `page.rect`, so on a rotated page it clips the wrong window. 4 of the 10 Track A benchmark pairs are on rotated pages (vk_plan/vk_nodes/vk_node_plan at 90°, eom_singleline_changed left at 270°). For `vk_plan` the Track A window catches **1 488** drawings where the de-rotated window contains **103 269** — Track A's "GOOD"-quality vk_plan description covers 3 163 primitives of the block's real **883 686**. Corpus-wide **1 390 of 23 036 pages (6.0 %)** are rotated, in **159 of 570 documents (27.9 %)**. | `hatchnoise_p0_rotation.json` | high | Show `get_drawings` returns rotated coordinates on those files (it does not — the drawings' bbox on vk page 5 is x≤786, y≤2237 inside the 1191×2526 unrotated box). |
| H15 | Segment counts are not a measure of content. The de-rotated `vk_plan` block holds **883 686 primitives / 505 213 segments** for one floor plan, `ov_nodes_hatch` 423 535 segments for one details sheet, and a `km_nvf_facade` page yields 671 875 primitives. 93 % of vk_plan's segments are shorter than 0.0008 of the block. Any "primitive/segment count" difference line shown to a user is packaging noise at this scale. | `hatchnoise_p2_filter.json`, `hatchnoise_p1_features.json` | high | — |
| H16 | Deciding foreground/background per **segment** is unsafe by construction: a flattened arc is 6–24 micro-segments, so a "tiny segment" rule deletes every curve. The first (segment-level) version of the filter removed 469 649 of 505 213 vk_plan segments and the door swings, sanitary fixtures and axis bubbles disappeared from the KEEP render. All reported results use the per-path version. | first-run counts recorded in `hatchnoise_filter.py` docstring; current results in `hatchnoise_p2_filter.json` | high | — |

---

## What was measured, and how to reproduce

All commands run from `/home/coder/projects/PDF-proverka`.

```bash
# corpus-level availability of the layer / OCG signal
python -m experiments.stage_comparison_vector_architecture_opus.probes.hatchnoise_layer_census
python -m experiments.stage_comparison_vector_architecture_opus.probes.hatchnoise_layer_quality

# side finding: page rotation breaks the block window
python -m experiments.stage_comparison_vector_architecture_opus.probes.hatchnoise_p0_rotation

# per-feature separating power against CAD-layer ground truth (8 blocks, 5 disciplines)
python -m experiments.stage_comparison_vector_architecture_opus.probes.hatchnoise_p1_features

# apply the discipline-free filter and render KEEP / DROP / EATEN
python -m experiments.stage_comparison_vector_architecture_opus.probes.hatchnoise_p2_filter_render
python -m experiments.stage_comparison_vector_architecture_opus.probes.hatchnoise_p2b_zoom ar_layered_plan 0.30 0.40 0.46 0.66 zoomA
python -m experiments.stage_comparison_vector_architecture_opus.probes.hatchnoise_p2c_eaten_breakdown
python -m experiments.stage_comparison_vector_architecture_opus.probes.hatchnoise_p2d_motif_identity

# payoff: caps + comparator, with and without the filter
python -m experiments.stage_comparison_vector_architecture_opus.probes.hatchnoise_p3_payoff

# H8: the dominant motif fingerprint is a single micro-line shared across documents
python - <<'EOF'
from experiments.stage_comparison_vector_architecture_opus.probes import hatchnoise_core as C
from experiments.stage_comparison_vector_blocks import extractor as ta
import collections
for block in ('ov_nodes_hatch', 'ar_layered_plan'):
    payload = C.load_primitives(*C.BLOCKS[block]['left'])
    groups = collections.defaultdict(list)
    for primitive in payload['primitives']:
        groups[ta._primitive_pattern(primitive)].append(primitive)
    for h in ('70d40d626ad9', '6974c97ac5ac'):
        first = groups[h][0]
        print(block, h, 'n=', len(groups[h]), first['type'], 'segs=', first['segment_count'],
              'len_norm=', first['length_norm'])
EOF

# is there ANY safe threshold, and does it transfer?
python -m experiments.stage_comparison_vector_architecture_opus.probes.hatchnoise_p4_transfer
python -m experiments.stage_comparison_vector_architecture_opus.probes.hatchnoise_p4b_universal

# stability of the decision under a 0.1 % crop jitter on byte-identical PDFs
python -m experiments.stage_comparison_vector_architecture_opus.probes.hatchnoise_p5_stability
```

`hatchnoise_core.load_primitives` caches uncapped Track-A primitives per (pdf, page, bbox) in
`$HATCHNOISE_CACHE` (default: this session's scratchpad). First extraction of the dense blocks
takes 8–25 s each; `vk_plan` produces a 341 MB cache entry.

### Ground truth

The only discipline-free ground truth available at scale is the **CAD layer name** that PyMuPDF
exposes as `drawing["layer"]`. Layer names are mapped to classes by regex **on the name only**
(`PATT|HATCH|штрих|IZOLAT|ИЗОЛЯ|заливк` → hatch, `мебел|FURN|растен|озелен` → furniture,
`XREF|подоснов` → underlay, everything else → foreground); the full per-block map is in
`hatchnoise_p1_features.json → layer_to_class` so it can be audited. The map was checked visually:
`hatchnoise/ar_layered_plan/05_gt_background.png` renders exactly the wall hatch and the furniture,
`06_gt_foreground.png` the rest.

**Ground-truth caveats.** (a) Hatch drawn on a wall layer (`A-WALL-____-MCUT`) counts as
foreground, so the *foreground-eaten* numbers are an upper bound on real damage — visible in
`zoomA_07_eaten.png`, where some of the blue is genuine wall hatch. (b) `km_nvf_facade` declares
only one hatch layer, so its background class is probably under-labelled. The socket-layer number
(H6) is unaffected by both caveats: `08_Розетки и выводы` is unambiguously the sheet's content.

---

## Per-feature separating power (H3/H4/H5)

AUC = P(feature ranks a background segment above a foreground one). 8 layered blocks:
`ar_layered_plan`, `ar41_k5_plan`, `ar41_k6_plan` (AR), `eom_em_k1` (EOM), `ss_askuvt_k1`,
`ss_soue_k3k6` (SS), `tx_tx2_k4` (TX), `km_nvf_facade` (KM).
`drop@fg99` = fraction of hatch removed by a **per-block-tuned** single threshold that keeps 99 %
of foreground (median over blocks).

| feature | AUC min | AUC max | AUC mean | blocks below chance | drop@fg99 median |
|---|---|---|---|---|---|
| motif_repetition | 0.470 | 0.975 | **0.851** | 1 | 0.000 |
| parent_path_segment_count_inv | 0.485 | 0.952 | 0.848 | 1 | 0.000 |
| parent_path_length_inv | 0.480 | 0.958 | 0.844 | 1 | **0.452** |
| seg_length_norm_inv | 0.478 | 0.836 | 0.618 | 1 | 0.000 |
| stroke_luminance | 0.160 | 0.900 | 0.584 | 3 | 0.000 |
| cell_text_spans_inv (text-density overlap) | 0.438 | 0.648 | 0.562 | 1 | 0.000 |
| cell_orientation_entropy_inv | 0.257 | 0.799 | 0.528 | 5 | 0.000 |
| cell_dominant_angle_share | 0.206 | 0.835 | 0.525 | 4 | 0.000 |
| cell_tiny_frac | 0.343 | 0.711 | 0.507 | 4 | 0.000 |
| cell_segment_density | 0.338 | 0.828 | 0.506 | 5 | 0.000 |
| angle_is_45deg | 0.461 | 0.584 | 0.503 | 4 | 0.000 |
| stroke_width_inv | 0.218 | 0.840 | 0.491 | 4 | 0.000 |
| is_filled_path | 0.212 | 0.698 | 0.423 | 7 | 0.000 |
| is_colored | 0.235 | 0.493 | 0.417 | 8 | 0.000 |
| enclosure inside a closed contour | 0.268 | 0.679 | 0.409 | 7 | 0.000 |
| PDF layer / OC group | — | — | — | — | *is the ground truth; absent on ~80 % of the corpus (H12)* |

Per-block AUC of the three signal-bearing features (block order as above):

```
motif_repetition        0.942 0.470 0.825 0.918 0.865 0.940 0.868 0.975
parent_path_length_inv  0.953 0.480 0.790 0.958 0.852 0.952 0.888 0.881
cell_dominant_angle_sh. 0.353 0.513 0.206 0.835 0.425 0.470 0.649 0.749
stroke_luminance        0.160 0.876 0.464 0.832 0.648 0.293 0.503 0.900
```

## Transfer matrix (H1/H2)

Rows: the sheet whose thresholds were tuned (best background removal at ≤1 % own-sheet foreground
loss). Cells: `background_removed / foreground_eaten` when that same setting is applied unchanged.

```
tuned on         ar_layered  ar41_k5   ar41_k6   eom_em_k1 ss_askuvt ss_soue   tx_tx2    km_nvf
ar_layered_plan  0.79/0.01  0.25/0.16 0.45/0.06 0.47/0.00 0.72/0.25 0.80/0.04 0.19/0.02 0.24/0.10
ar41_k6_plan     0.66/0.06  0.08/0.11 0.27/0.01 0.85/0.00 0.25/0.12 0.69/0.01 0.10/0.06 0.94/0.04
eom_em_k1        0.90/0.44  0.45/0.48 0.61/0.30 0.95/0.01 0.92/0.51 0.97/0.17 0.92/0.17 1.00/0.57
ss_soue_k3k6     0.79/0.09  0.17/0.17 0.36/0.05 0.40/0.00 0.42/0.16 0.81/0.01 0.42/0.07 0.96/0.06
ar41_k5 / ss_askuvt / tx_tx2 / km_nvf: their own best safe setting removes 0.00 of background
```

Universal-threshold search over all 560 settings (`hatchnoise_p4_universal.json`):

| worst-case foreground loss allowed | settings that qualify | best mean background removed | per-block background removed |
|---|---|---|---|
| ≤ 1 % | 2 of 560 | **0.0003** | 0.002, 0, 0, 0, 0, 0, 0, 0 |
| ≤ 5 % | 4 of 560 | 0.111 | 0.005, 0, 0.025, 0.848, 0, 0.010, 0, 0 |
| per-sheet **oracle** (a human picks thresholds per sheet) | — | 0.353 | 0.791, 0, 0.271, 0.953, 0, 0.809, 0, 0 |

Even the oracle removes **exactly zero** background on 4 of the 8 sheets.

## What the fixed filter does per block (H10/H16)

Rules, all per-path, all discipline-free: **P1** short simple stroke in a locally repeated parallel
same-style family, **P2** small path whose shape fingerprint repeats ≥12×, **P3** light-grey stroke
(luminance ≥0.62), **P4** path shorter than 0.0015 of the block.

| block | disc | segments | primitives | vector text spans | segments dropped | P1 | P2 | P3 | P4 |
|---|---|---|---|---|---|---|---|---|---|
| ar_plan | AR | 18 080 | 14 800 | 836 | 63.1 % | 5 949 | 3 882 | 3 399 | 3 401 |
| ss_plan_dense | SS | 84 439 | 1 604 | 522 | 88.3 % | 0 | 108 | 74 529 | 62 |
| ar_wall_sections | AR | 33 427 | 36 027 | 119 | 94.6 % | 28 218 | 20 232 | 1 138 | 22 589 |
| vk_plan | VK | 505 213 | 883 686 | 131 | 99.4 % | 439 806 | 276 288 | 383 320 | 479 321 |
| ar_layered_plan (fresh, layered) | AR | 122 227 | 87 395 | 655 | 71.0 % | 64 300 | 42 251 | 1 476 | 69 090 |
| ov_nodes_hatch (fresh, no text layer) | OV | 423 535 | 128 788 | **1** | 90.6 % | 64 030 | 283 233 | 0 | 372 129 |

Ground-truth scoring on `ar_layered_plan`: precision 0.783, recall 0.884, and
**41.5 % of the CAD-layer foreground eaten** (18 838 of 45 386 segments).

## Payoff and risk on real pairs (H10/H11)

| pair | segments (left) | kept | quality before → after | comparator status before → after | geometry sim | topology sim |
|---|---|---|---|---|---|---|
| ss_plan_dense | 84 439 | 11.7 % | LIMITED_CAPPED → LIMITED_CAPPED | NEAR_IDENTICAL → NEAR_IDENTICAL | 1.000 → 0.999 | 0.996 → 0.992 |
| ar_plan | 18 080 | 36.9 % | LIMITED_CAPPED → **GOOD** | NEAR_IDENTICAL → NEAR_IDENTICAL | 1.000 → 0.986 | 0.999 → 0.960 |
| ar_wall_sections | 33 427 | 5.4 % | LIMITED_CAPPED → **GOOD** | NEAR_IDENTICAL → NEAR_IDENTICAL | 1.000 → 0.992 | 0.967 → 0.908 |
| ss_scheme_text_changed | 710 | 100 % | GOOD → GOOD | SAME_VALUES_CHANGED → SAME_VALUES_CHANGED | 0.866 → 0.866 | 0.733 → 0.733 |
| eom_singleline_changed | 1 663 | 93.9 % | GOOD → GOOD | STRUCTURE_CHANGED → STRUCTURE_CHANGED | 0.495 → 0.497 | 0.541 → 0.448 |
| vk_nodes | 110 226 | 1.4 % | LIMITED_CAPPED → **GOOD** | NEAR_IDENTICAL → **STRUCTURE_CHANGED** | 0.997 → 0.944 | 0.991 → 0.926 |
| ar_layered_plan (fresh) | 122 227 | 29.0 % | LIMITED_CAPPED → LIMITED_CAPPED | STRUCTURE_CHANGED → **SAME_VALUES_CHANGED** | 0.959 → 0.985 | 0.938 → 0.959 |
| ov_nodes_hatch (fresh) | 423 535 | 9.4 % | LIMITED_CAPPED → LIMITED_CAPPED | STRUCTURE_CHANGED → STRUCTURE_CHANGED | 0.033 → 0.121 | 0.279 → 0.063 |

`ov_nodes_hatch` is **not a valid pair**: page indices shift between v001 (28 pages) and v002
(31 pages) so the two sides are different sheets (geometry similarity 0.033). It contributes
single-block measurements only. The `ar_layered_plan` pair is heuristic (same page index, drawing
count 89 302 vs 89 199).

## Visual evidence (the deliverable that proves or kills the idea)

Per block in `artifacts/hatchnoise/<block>/`:
`00_pdf_crop.png` (what a human sees) · `01_all_segments.png` (my re-render, sanity check) ·
`02_keep.png` · `03_drop.png` · `04_drop_P*.png` (per rule) · and, for layered blocks,
`05_gt_background.png` · `06_gt_foreground.png` · `07_false_positives.png`.

Read these four first:

1. `ar_wall_sections/02_keep.png` — **the best case.** 94.6 % dropped; what remains is a clean
   wall section: outlines, slabs, openings, dimension chains, leaders. «Появился новый проём»
   would be safely derivable from this. The brick coursing is gone — which is only correct as long
   as the coursing itself is never the change.
2. `ar_layered_plan/zoomA_07_eaten.png` — **the worst case.** At symbol scale, what the filter
   classed as background is: socket symbols, apartment-number capsules, dashed appliance outlines,
   door swings. These are exactly the objects behind «Количество аппаратов 12 → 14».
3. `ov_nodes_hatch/04_drop_P2.png` vs `ov_nodes_hatch/02_keep.png` — the "repeated motif" rule
   deleted every Russian callout on the sheet (legible in the drop image) and kept the hatch.
4. `vk_plan/02_keep.png` — 0.6 % kept: the axes, the axis bubbles and the VK piping runs survive,
   the whole architectural underlay is gone. Right answer for a piping diff, wrong answer the
   moment the question is about the plan.

## Worst example, stated plainly

`13АВ-РД-АР4.2-К4`, page 24, «План размещения розеток и выводов». The discipline-free filter
removes 97.8 % of the wall-hatch primitives (63 680 of 65 142) — and 63.5 % of the primitives on
`08_Розетки и выводы`, the layer that *is* the sheet. An expert reading the filtered diff would be
told the hatch is stable and would not be told that two thirds of the sockets vanished.

## What this says about `VectorBlockDescription` v0.1

**Keep** (they survive the probe and are load-bearing): `geometry.primitives.normalized.segments`,
`style.stroke/fill/stroke_width` (as *evidence*, not as a classifier), `style.layer` — currently
extracted and then never used, though it is the single strongest discriminator where present —
`texts[].bbox_norm/rotation/font_size`, `vector_quality`, `geometry.extraction` (cap bookkeeping).

**Drop or demote:** `hatch_like_structures` — my P1 rule is a strictly stronger version of the same
idea (family = angle + length + width + colour + local support, rather than angle + length bucket
alone) and it still sits at mean AUC ≈0.5 on the local-geometry features; a capped list of 30
parallel-segment buckets carries less. `repeated_elements` as currently fingerprinted — H8 shows
one token shared verbatim between unrelated documents. `primitive_count` in user-visible
differences (H15).

**Add:** (1) the **path/OCG layer** promoted to a first-class field with an explicit
`layer_signal_available` flag, since it is present on ~20 % of documents and decides the question
outright there; (2) **page-rotation-correct block extraction** (H14) — without it 6 % of pages and
4 of Track A's 10 benchmark pairs describe the wrong region; (3) a **graphical-object layer**: the
unit of the keep/drop decision has to be an object (a symbol instance, a hatch region, a leader),
because every rule I could write at segment or path level is either at chance or eats the objects
the expert counts.

## Gaps / not verified

- Ground truth is layer-name-derived, not expert-annotated. The direction of every finding is
  robust to this (a mis-labelled hatch layer would only make the filter look *better*), but the
  exact percentages carry that label noise.
- No P↔РД pair was tested: the corpus under `projects_v2/objects/` holds РД↔РД version pairs only,
  so "does the filter survive a stage change" is **UNVERIFIED**.
- `hatch_like_structures` and `repeated_elements` were not re-scored directly against the layer
  ground truth (I scored my own stronger reimplementations of the same two ideas). **UNVERIFIED**
  whether Track A's exact implementations score better — they gate on `type != "line"` and
  `length_norm ≥ 0.002`, which excludes most primitives in these blocks.
- Only one filter family was searched (4 rules × 560 threshold combinations). A learned classifier
  over the same features was not tried; H1 constrains hand-set thresholds, not learning.
- `vk_plan` and `km_nvf_facade` were measured at one page each; runtime capped the sweep at 8
  ground-truth blocks.
