# falsify_ — FALSIFICATION PROBE of the v0.1 universal-backbone hypothesis

**Hypothesis under test:** *normalized geometry + generic topology + positioned text + nearest-geometry
anchors + repeated patterns* is a sufficient universal backbone for comparing graphic blocks
between design stages.

**Verdict: the hypothesis does not survive.** It breaks in both directions at once, on real blocks,
and the two failures are not independent — they are the two halves of the same missing layer.
Everything below was measured; every command runs from `/home/coder/projects/PDF-proverka`.

---

## Headline table

| # | claim | evidence (measured number / file) | confidence | falsifiable by |
|---|---|---|---|---|
| F1 | A **real removed wall** between two real versions is reported as `NEAR_IDENTICAL` as soon as the block is bigger than ~×8 the change | `falsify_case_results_real_change.json`: tight `STRUCTURE_CHANGED` geom 0.8498 → ×8 `NEAR_IDENTICAL` 0.9919 → whole page `NEAR_IDENTICAL` 0.9957. Crops `falsify_visual/sot_k7_p8/region1_{left,right}.png` | high | show a block size at which the same v0.1 code reports the wall; or show the change is not real |
| F2 | For that same real change on a whole page the only user-visible line is **`Число примитивов: 814 → 815`** — the count rises while a wall is deleted; `total_segment_count` also rises 11054 → 11073 | `falsify_cases/A_real_wall_removed_wholepage/comparison.json` `differences` | high | find a v0.1 field that decreases and is shown to the user |
| F3 | On 5 real blocks you can erase **every primitive inside a central window of 20 % of the block width (4 % of its area)** and still get `NEAR_IDENTICAL` | `falsify_dilution.json → localized`: `vk_nodes` 333 prims/333 segs → NEAR_IDENTICAL; `ar_wall_sections` 133 prims/136 segs → NEAR_IDENTICAL; `vk_plan` 27 prims → NEAR_IDENTICAL | high | rerun `falsify_dilution.py`; a different erasure geometry that trips the gate earlier |
| F4 | A **left-hand and a right-hand door** (real, same page) have a **byte-identical v0.1 `level_3_structural_topology` signature**, identical counts, `topology similarity 1.000`, and an **empty `differences` list** | `falsify_case_results_ss.json → A_door_swing_mirrored`: `level3_signature_equal: true`, both sides 1 primitive / 90 segments / 3 components / 4 endpoints / 0 branch / 0 closed; crop `falsify_crops/ss_sot_p7/mirror_01.png` | high | show a v0.1 field that separates the two doors |
| F5 | The **same object rotated 180°** gets geometry similarity **0.0000** while its L3 signature stays **identical** — the two layers fail in opposite directions on one object | `falsify_case_results_ss.json → B_door_rotated_180`: geom 0.0, `level3_signature_equal: true`, `differences: []`. A second, 90° case (`falsify_case_results_symbols.json → B_rot90_cable_bundle`) also scores geom 0.0 (its L3 differs only because the crop caught extra context) | high | show v0.1 recovering rotation, or that the twins are different objects (crops attached) |
| F6 | On a real SS plan page **253 of 366 symbol-like components (69 %)** live in a generic-topology descriptor class that contains ≥2 measurably different shapes | `falsify_sym_ss_sot_p7.json`: 119 distinct L3 keys, 25 keys with >1 shape cluster (threshold 0.90) | medium-high | raise the shape-cluster threshold; add a discriminating field to the L3 key |
| F7 | **Swapping two text values** in a real block leaves v0.1 at `NEAR_IDENTICAL` with an **empty `differences` list** on **10 of 10** Track A blocks — because `effective_similarity = max(multiset_f1, stream)` and multiset_f1 = 1.0000 | `falsify_text_permutation.json`: all 10 rows multiset 1.0000, stream 0.9466–0.9992, used 1.0000 | high | drop the `max()`; use positional matching |
| F8 | Two **pixel-identical** pages (0 changed pixels at 110 dpi) of one document, text multiset identical, are reported `STRUCTURE_CHANGED`; L3 says `total_segment_count 8290 → 20513` | `falsify_visual/ak_k6_p14_repack/diff.json` `changed_pixels: 0`; `falsify_case_results_reexport.json → B_reexport_ak_k6_p14_wholepage` geom 0.9955, topo 0.786 | high | render at higher dpi and find a real pixel difference |
| F9 | The same unchanged re-export is `STRUCTURE_CHANGED` for the whole page and `NEAR_IDENTICAL` for its left half — **the verdict depends on the crop, not on the drawing** | `falsify_case_results_reexport.json`: wholepage STRUCTURE_CHANGED, left_half NEAR_IDENTICAL | high | show the two crops contain different content |
| F10 | `extractor._norm_point` scales x and y **independently**, so a **10 % block-crop disagreement** flips an unchanged block to `STRUCTURE_CHANGED` with the content held constant | `extractor.py:104-110`; `falsify_crop_anisotropy.json`: `ss_scheme_left aniso_h ×1.10` segments 710 → 711 (+1), aspect ×0.9209, geom **0.8304**, STRUCTURE_CHANGED; `vk_plan_left aniso_w ×1.10` segments 3180 → 3205, geom 0.9786, STRUCTURE_CHANGED | high | show block detectors agree to better than a few percent between stages |
| F11 | **3 of the 10** Track A benchmark blocks have an `UNDECODABLE` text layer on **both** sides (24–54 % of spans contain control characters); the text score is therefore **excluded from the status decision** and the `differences` list shows mojibake | `falsify_text_layer_quality.json` (vk_nodes L 207/421 = 49.2 %, R 88/360 = 24.4 %); Track A `comparisons/vk_nodes/comparison.json` `reliable:false`, `differences[0] = "Текст/значение \x04\x18 \x15\x15 . \x16\x11 → \x0f-3!-\x0f 1- 4"` | high | show a decoding path that recovers those fonts |
| F12 | **30.24 % of all comparable page pairs in the corpus change page size between versions** (1064 of 3519), and 15 page pairs (4 documents) have a side with **zero** text words but >500 segments (text drawn as outlines) | `falsify_corpus_census.json`; outlined example `13АВ-РД-ЭМ-К3 v002 p5`: 0 words / 202 694 drawing items, crop `falsify_visual/em_k3_p5_outlined/zoom_v002.png` | high | rerun `falsify_scan_corpus.py` |
| F13 | Of 466 page pairs with identical page size **and** identical text multiset, only **72** have geometry Jaccard < 0.99 — real graphics-only revisions are rare and small, exactly the regime where F1/F3 hide them | `falsify_corpus_census.json`: median jaccard 1.00000, `<0.999: 83`, `<0.99: 72`, `<0.95: 66`, `==1.0: 379` | high | rerun the scan with a different quantization |
| F14 | v0.1 geometry similarity is **not monotonic** in the perturbation size (selected-tolerance escalation), so the score cannot be read as a distance | `falsify_crop_anisotropy.json` `vk_plan_left aniso_w`: 0.9879 → 0.9970 → 0.9998 → 1.0000 for growth 0.5 % → 5 % | medium-high | show the selection rule is monotone |

---

## Artifacts

| file | what it holds |
|---|---|
| `falsify_corpus_scan.json` | per-page fingerprints for 3519 page pairs across 98 documents |
| `falsify_corpus_census.json` | the corpus-level counts quoted in F12/F13 |
| `falsify_shortlist.json` | A/B falsification candidates picked out of the scan |
| `falsify_all_cases.json` | all 15 comparator runs in one machine-readable list |
| `falsify_case_results_{real_change,ss,symbols,reexport}.json` | per-family comparator results |
| `falsify_cases/<case_id>/{left,right}.png,comparison.json` | crop + full v0.1 comparison per case |
| `falsify_visual/<tag>/` | raster diff localisation + before/after crops of real revisions |
| `falsify_crops/<page>/` | mined component crops, isolated redraws, mirror and rotation twins |
| `falsify_dilution.json` | scattered- and localized-erasure curves against the real comparator |
| `falsify_crop_anisotropy.json` | crop-perturbation sweep |
| `falsify_text_permutation.json`, `falsify_text_layer_quality.json` | text-layer falsifications |
| `falsify_spread_eom_k4_p10.json`, `falsify_sym_*.json` | same-symbol spread, descriptor collisions |
| `falsify_text_only_changes.json` | the 184 text-only version changes and their token diffs |

## Reproduction

```bash
cd /home/coder/projects/PDF-proverka
# 1. corpus-wide cheap scan (98 documents, 105 version pairs, 3519 page pairs, ~19 min)
python -m experiments.stage_comparison_vector_architecture_opus.probes.falsify_scan_corpus
python -m experiments.stage_comparison_vector_architecture_opus.probes.falsify_shortlist

# 2. dilution against the real Track A comparator
python -m experiments.stage_comparison_vector_architecture_opus.probes.falsify_dilution

# 3. real localized revision, block grown x1/x3/x8/x20/whole page
python -m experiments.stage_comparison_vector_architecture_opus.probes.falsify_visual_diff \
  --left  "projects_v2/objects/214_Alia_ASTERUS/disciplines/SS/documents/13AB-РД-СОТ-К7 V1/versions/v001/02_work/document.pdf" \
  --right "projects_v2/objects/214_Alia_ASTERUS/disciplines/SS/documents/13AB-РД-СОТ-К7 V1/versions/v002/02_work/document.pdf" \
  --page 8 --tag sot_k7_p8 --dpi 110
python -m experiments.stage_comparison_vector_architecture_opus.probes.falsify_harness \
  experiments/stage_comparison_vector_architecture_opus/artifacts/falsify_cases_real_change.json \
  falsify_case_results_real_change.json

# 4. descriptor collisions / mirrors / rotation twins among real components
python -m experiments.stage_comparison_vector_architecture_opus.probes.falsify_symbol_collisions \
  --pdf "projects_v2/objects/214_Alia_ASTERUS/disciplines/SS/documents/13AB-РД-СОТ-К7 V1/versions/v002/02_work/document.pdf" \
  --page 7 --min-seg 6 --max-seg 200 --min-size 5 --max-size 80 --min-cycles 1 \
  --out falsify_sym_ss_sot_p7.json
python -m experiments.stage_comparison_vector_architecture_opus.probes.falsify_render_collisions \
  --json falsify_sym_ss_sot_p7.json --top 6 --mirror-top 3 --outdir falsify_crops/ss_sot_p7
python -m experiments.stage_comparison_vector_architecture_opus.probes.falsify_harness \
  experiments/stage_comparison_vector_architecture_opus/artifacts/falsify_cases_ss.json falsify_case_results_ss.json

# 5. same-symbol geometry stability, text permutation, crop anisotropy
python -m experiments.stage_comparison_vector_architecture_opus.probes.falsify_same_symbol_spread \
  --pdf "projects_v2/objects/214_Alia_ASTERUS/disciplines/EOM/documents/13АВ-РД-ЭМ-К4/versions/v002/02_work/document.pdf" \
  --page 10 --out falsify_spread_eom_k4_p10.json
python -m experiments.stage_comparison_vector_architecture_opus.probes.falsify_text_permutation
python -m experiments.stage_comparison_vector_architecture_opus.probes.falsify_crop_anisotropy
```

---

# ATTACK A — same numbers, different meaning

## A1. A real wall disappears and v0.1 says «почти идентично»

**Real change.** `13AB-РД-СОТ-К7 V1`, sheet on page index 8, `v001 → v002`. Raster diff at 110 dpi:
**395 changed pixels out of 4 682 860 (0.011 %)** in 2 regions. The crops
`falsify_visual/sot_k7_p8/region1_left.png` / `region1_right.png` show a hatched wall segment between
two rooms present in v001 and **absent in v002** — the expert sentence is «убран участок стены / появился
проём».

The same real change, compared with the unmodified Track A extractor + comparator at five block sizes
(`falsify_case_results_real_change.json`):

| block | status | geometry sim | `differences` shown to the expert |
|---|---|---|---|
| change bbox (tight) | STRUCTURE_CHANGED | 0.8498 | `Топология изменилась (similarity=0.778, ветвления 112 → 141)` |
| ×3 | STRUCTURE_CHANGED | 0.9843 | `Число примитивов: 130 → 131` + топология |
| ×8 | **NEAR_IDENTICAL** | 0.9919 | `Число примитивов: 297 → 298` |
| ×20 | **NEAR_IDENTICAL** | 0.9956 | `Число примитивов: 800 → 801`, `Изменены повторяющиеся motifs: 1` |
| whole page | **NEAR_IDENTICAL** | 0.9957 | `Число примитивов: 814 → 815`, `Изменены повторяющиеся motifs: 1` |

Track A's own benchmark blocks are at the "whole plan region" scale (`ss_plan_dense` covers 0.009–0.999
of the page). At that scale this revision is invisible. Worse, the counts move the **wrong way**:
primitives 814 → 815 and segments 11054 → 11073 both *increase* while geometry is *removed*, because
the PDF writer re-packed paths (orchestrator finding O7, now with a concrete engineering victim).

## A2. Dilution is a property of the block, not of the change

`falsify_dilution.py` erases every primitive whose normalized centroid falls in a centred square
window, rebuilds every derived layer with Track A's own `_topology/_summary/_signatures/_size_metrics`,
and runs the real comparator (`falsify_dilution.json → localized`):

| block | last window still called NEAR_IDENTICAL | primitives erased | segments erased |
|---|---|---|---|
| `vk_nodes` | 20 % of width (4 % of area) | 333 | 333 (1.66 %) |
| `vk_plan` | 20 % of width | 27 | 27 (0.85 %) |
| `ar_wall_sections` | 20 % of width | 133 | 136 (0.67 %) |
| `ar_plan` | 10 % of width | 350 | 444 (2.45 %) |
| `ss_plan_dense` | 10 % of width | 7 | 4860 (5.48 %) |

Scattered deletion is even more forgiving: dropping the **400 smallest real primitives** of `ar_plan`
still yields `NEAR_IDENTICAL` (geom 0.9874); `ss_plan_dense` survives 400 primitives / 606 segments
(geom 0.9893). Analytically the budget is `(1 − 0.985)·N` segments — 272 for `ar_plan`,
**1331 for `ss_plan_dense`** (`falsify_dilution.json → analytic`).

The sentence «Количество аппаратов 12 → 14» needs a two-instance delta. Two instances of a 4-segment
motif are 8 segments against a budget of 272–1331. It is not derivable.

## A3. A door that opens the other way is byte-identical at L3

`falsify_sym_ss_sot_p7.json → mirror_pairs[0]` — two real components on one SS plan page, 52 segments
each, `degree_histogram {2: 26}`, `kinds {l: 52}`, `cycles 1`, `l3_key` **identical**; measured shape
similarity **direct 0.1154 / mirrored 1.0000**. Crop: `falsify_crops/ss_sot_p7/mirror_01.png` — a door
leaf with its swing arc, left-hand and right-hand.

Run through the real Track A pipeline as two blocks (`falsify_case_results_ss.json`):

```
A_door_swing_mirrored   STRUCTURE_CHANGED   geom 0.0444  topo 1.000
  level3_signature_equal : true
  left  : 1 primitive, 90 segments, 3 components, 4 endpoints, 0 branch, 0 closed contours
  right : 1 primitive, 90 segments, 3 components, 4 endpoints, 0 branch, 0 closed contours
  differences : []
```

Both v0.1 layers fail. The L3 compact payload — the thing Track A's AI experiment actually hands the
model — cannot tell a left door from a right door. The raw-geometry layer knows they differ but emits
**zero** difference lines, so the expert receives the word `STRUCTURE_CHANGED` and nothing else.
«Дверь открывается в другую сторону» is not derivable from either.

## A4. Generic topology collides on real symbols

`falsify_symbol_collisions.py` mines connected components (0.5 pt node welding), keeps symbol-like ones
(≥6 segments, ≥1 closed loop, 5–80 pt), computes the same tuple `extractor._signatures` hashes into
`level_3_structural_topology` (primitive-type counter, cycles, degree histogram, segment count,
rounded aspect) and clusters each descriptor class by **measured** shape similarity (threshold 0.90):

| page | symbol-like components | distinct L3 keys | keys holding ≥2 different shapes | components in those keys |
|---|---|---|---|---|
| SS plan `13AB-РД-СОТ-К7 v002` p8 | 366 | 119 | 25 | **253 (69 %)** |
| VK nodes `13АВ-РД-ВК.КВ-К4 v001` p9 | 118 | 83 | 11 | 37 (31 %) |
| EOM single-line `13АВ-РД-ЭМ-К4 v002` p11 | 181 | 44 | 2 | 40 (22 %) |

Confirmed visually. `falsify_crops/ss_sot_p7/collision_01_isolated.png` is the strongest: two
25-segment axis callouts (координационная ось) with the bubble at the top and at the bottom — one L3
key, measured direct shape similarity **0.0**. Comparing them as blocks
(`falsify_case_results_ss.json → A_l3collision_ss_25seg`) gives geom 0.0000 and the difference lines
`Число примитивов: 3 → 9` / `ветвления 8 → 18`.

The honest caveat: on the EOM page the tight filter leaves only 2 colliding keys, and part of the SS
collision mass is furniture/underlay linework rather than devices. The claim I can defend is
**"generic topology is not a symbol identity"**, not "every symbol collides".

## A5. Any value permutation is invisible by construction

`comparator._text_diff` returns `effective_similarity = max(multiset_f1, character_stream_similarity)`
and `compare_descriptions` uses only that number. A multiset is permutation-invariant.
`falsify_text_permutation.py` performs the minimal realistic perturbation on the **real** Track A
descriptions — two same-category spans, ≥0.15 apart in y, exchange their strings; geometry untouched;
`_summary/_signatures/_size_metrics` recomputed so the description stays consistent:

```
pair                     cat        multiset   stream    used  status           swapped
ar_plan                  label        1.0000   0.9992  1.0000  NEAR_IDENTICAL   ['П.А', 'П.20']
ss_table_graphic         numeric      1.0000   0.9929  1.0000  NEAR_IDENTICAL   ['8', '2']
vk_nodes                 numeric      1.0000   0.9989  1.0000  NEAR_IDENTICAL   ['100', '2']
vk_plan                  numeric      1.0000   0.9989  1.0000  NEAR_IDENTICAL   ['9', '1']
... 10 of 10 rows identical in shape; every `differences` list is EMPTY
```

The character-stream similarity *did* detect it (0.9466–0.9992) and `max()` throws that away.
«Номинал 250 А переехал на другой ввод» is structurally underivable.

## A6. For a whole discipline the text layer is not readable at all

`falsify_text_layer_quality.json`: all six VK block sides in Track A's benchmark carry control
characters in 24.4–54.3 % of spans (embedded CAD font without a usable ToUnicode map). Track A's own
`_text_diff` classifies them `UNDECODABLE`, sets `reliable=false`, and drops the text score from the
status decision — while still printing the mojibake to the user:

```
vk_nodes      NEAR_IDENTICAL  text sim 0.289  reliable False  added 200 removed 200 value_changes 100
  differences[0] = "Текст/значение \x04\x18 \x15\x15 . \x16\x11 → \x0f-3!-\x0f 1- 4"
```

Left and right decode *differently* (49.2 % vs 24.4 % garbage), so the same physical label produces
different strings in the two versions and is reported as a value change. On these blocks the backbone
has no working value channel at all — and Track A's human validation still scored them CORRECT,
because the *status* happened to be right.

---

# ATTACK B — same meaning, different raw geometry

## B1. Pixel-identical pages, 2.7× the linework, verdict STRUCTURE_CHANGED

`13АВ-РД-АК-К6 (Книга 1)` page index 14, `v001 → v002`. Raster diff at 110 dpi:
**`changed_pixels: 0`** out of 3 292 340 (`falsify_visual/ak_k6_p14_repack/diff.json`). Text multiset
identical. Crops `proof_v001.png` / `proof_v002.png` show the same connection scheme of a heat curtain.

Yet the PDF is re-packaged: drawing items **7309 → 19532**, `l` operators **7099 → 19322**, my
quantized segment Jaccard 0.492. Through the real v0.1 pipeline
(`falsify_case_results_reexport.json`):

```
B_reexport_ak_k6_p14_wholepage  STRUCTURE_CHANGED  geom 0.9955  topo 0.786
  left  : 1164 primitives,  8290 segments, 12 components, 601 branch points
  right : 1170 primitives, 20513 segments, 33 components, 554 branch points
  differences: ['Число примитивов: 1164 → 1170', 'Изменены повторяющиеся motifs: 1',
                'Топология изменилась (similarity=0.786, ветвления 601 → 554)']
```

An expert reading the L3 compact payload would be told the drawing's segment count went from 8290 to
20513 — a 2.5× "growth" of a page that did not change by one pixel. That is not a threshold problem;
the representation encodes the exporter, not the design.

**And the verdict is not stable under cropping**: the *left half* of the very same page pair comes back
`NEAR_IDENTICAL` (geom 0.9909, topo 0.907). Same drawing, same non-change, two opposite answers.

## B2. Rotation destroys the geometry layer and is invisible to the count layer

Real rotation twins found by matching each component's normalized segment set against 90/180/270°
rotations of another (`falsify_sym_*.json → rotation_twins`):

| case | object | segments | measured direct sim | rotated sim | v0.1 block comparison |
|---|---|---|---|---|---|
| `B_rot90_cable_bundle` | 6-conduit bundle section, EOM p11, drawn once 2×3 and once 3×2 | 72 / 72 | 0.6667 | **1.0000** | **geom 0.0000**, STRUCTURE_CHANGED |
| | *caveat:* the left instance carries a red cancellation cross in the surrounding context; the two **matched components** (the 72-segment circle groups themselves) are exact 90° rotations | | | | |
| `B_door_rotated_180` | door leaf + swing arc, SS p8 | 52 / 52 | 0.0 | **1.0000** | **geom 0.0000**, `level3_signature_equal: true`, `differences: []` |

Crops: `falsify_crops/eom_k4_p10/rotation_01.png`, `falsify_cases/B_door_rotated_180/{left,right}.png`.
`extractor.coordinate_system.normalization_removes` lists page position and uniform scale — rotation is
not in the list, and the L3 counters (`primitive_types`, `closed`, `degree_histogram`,
`component_segment_counts`) are all rotation-invariant, so the two layers cancel each other out: the
one that could see the difference says 0.0 and prints nothing, the one that is shown to the model says
"identical".

## B3. The normalization is anisotropic — a 10 % crop disagreement is a false alarm

`extractor._norm_point` (`extractor.py:104-110`) divides x by the block width and y by the block height
**independently**. The schema nevertheless advertises
`normalization_removes: ["page position", "uniform presentation scale"]`.

Content-controlled measurement (`falsify_crop_anisotropy.json`) — the same real block, same page, only
the crop window changes:

| block | perturbation | aspect change | segments | status | geometry sim |
|---|---|---|---|---|---|
| `ss_scheme_left` | height ×1.10 | ×0.9209 | 710 → 711 | **STRUCTURE_CHANGED** | **0.8304** |
| `ss_scheme_left` | isotropic ×1.10 | ×0.9596 | 710 → 714 | STRUCTURE_CHANGED | 0.7724 |
| `ss_scheme_left` | shift 5 % of width | ×1.0 | 710 → 649 | STRUCTURE_CHANGED | 0.8596 |
| `vk_plan_left` | width ×1.10 | ×1.1000 | 3180 → 3205 | STRUCTURE_CHANGED | 0.9786 |
| `vk_plan_left` | shift 5 % of width | ×1.0 | 3180 → 2939 | STRUCTURE_CHANGED | 0.7920 |

The `ss_scheme_left aniso_h ×1.10` row is the clean one: **one extra segment** enters the crop, the
aspect ratio changes by 8 %, and the geometry score collapses from 1.0 to 0.83. A П↔РД block detector
that disagrees by 10 % on where a block ends will therefore report a structural change on every
unchanged block. Track A's benchmark only ever tested 0.1–2 % jitter (orchestrator finding O1).

Corpus context: **1064 of 3519 comparable page pairs (30.24 %) already change page size between two
versions of the same document** (`falsify_corpus_census.json`), so aspect disagreement is the normal
case, not the exception.

## B4. The same symbol is not the same geometry

`falsify_spread_eom_k4_p10.json` — six largest same-descriptor instance groups on a real EOM page,
all pairwise similarities inside each group:

```
segs   inst      min   median     mean      max   share of pairs < 0.985
12     24     1.0000   1.0000   1.0000   1.0000     0.0%
 7     24     0.5714   1.0000   0.8561   1.0000    47.1%      <-- circle, 7-gon approximation
 6     24     1.0000   1.0000   1.0000   1.0000     0.0%
11      7     1.0000   1.0000   1.0000   1.0000     0.0%
15      7     1.0000   1.0000   1.0000   1.0000     0.0%
 7      7     1.0000   1.0000   1.0000   1.0000     0.0%
```

Five of six groups are perfectly stable; one is not, and it is the one that matters — a cable circle
whose polygon approximation starts at a different vertex phase per instance.
`falsify_crops/eom_k4_p10/collision_01.png` and `collision_02_isolated.png` show two visually identical
cable circles; run as blocks they score **geom 0.7788** and **geom 0.3333**, both `STRUCTURE_CHANGED`
(`falsify_case_results_symbols.json`). For the 0.3333 pair `level3_signature_equal` is `true` and
`differences` is empty — a false alarm with no explanation attached.

## B5. Text drawn as outlines removes the value channel entirely

`13АВ-РД-ЭМ-К3 v002` page index 5: **0 text words, 202 694 drawing items**, rendering perfectly legible
Russian (`falsify_visual/em_k3_p5_outlined/zoom_v002.png`); the v001 counterpart page has 1035 words and
3244 items. Corpus-wide this affects **15 of 3519 page pairs (0.43 %) in 4 of 94 documents**
(`falsify_corpus_census.json`). Rare, but on those sheets `texts` is empty → `layer_quality: ABSENT` →
`reliable: false` → every value change is invisible in principle and the whole verdict rests on the
geometry layer that B1–B4 just showed to be unreliable.

## B6. Typographic reflow with identical text

`13АВ-РД-АР1.2-К4 v002 → v003` page 11: text multiset identical, 86 775 changed pixels (1.85 %). The
crops `falsify_visual/ar12k4_p11/region1_{left,right}.png` show the same 11 numbered notes re-set at a
larger font in a wider column. No engineering change; every text bbox moved.

---

# What I searched and did not find

* **No П (проектная документация) vs РД pair exists in `projects_v2`** that I could locate — all 98
  multi-version documents are РД revisions of the same document (`v001→v002→v003`). Every "stage"
  claim here is therefore extrapolated from real *revision* pairs. **UNVERIFIED for true П↔РД.**
* **No clean «Номинал 250 → 315 А» version pair.** I enumerated all 184 page pairs whose text changed
  while the quantized geometry stayed identical and diffed their word multisets
  (`falsify_text_only_changes.json`): 159 have ≤14 differing tokens and they are almost entirely
  title-block revision stamps — `+['Изменение','1']`, `-['2025'] +['2026']`, `-['2'] +['3']`. The
  corpus in `projects_v2` does not contain an easily-findable rating change between versions.
* **Valve-type swap:** not found as a version-pair. The VK legend pages I opened
  (e.g. `13АВ-РД-ВК2-К4 v001` p5) are designation-notation legends, not symbol tables, and VK plan
  components at symbol scale are dominated by dimension leaders, so I could not build a valve-symbol
  ground truth automatically. **UNVERIFIED.**
* **Different hatch tiling of the same fill:** not isolated as a clean real pair. The re-export case B1
  is the closest evidence (2.7× more line operators on a pixel-identical page) but I did not prove the
  extra operators are hatch.
* My `shape_similarity` (midpoint + length + angle within 0.03 / 0.25 rad) is *my* metric, not Track A's
  `_directional_segment_coverage`. Wherever a number matters I re-ran the **real** Track A
  extractor + comparator; the mining statistics (F6) rest on my metric and are marked medium-high.
* The first corpus scan crashed when a `v003` directory appeared under
  `13АВ-РД-ВК.КВ-К4_V1` during the run (live system). The reported scan is the completed re-run;
  98 documents, 105 version pairs, 0 errors.

---

# Does the hypothesis survive? Where exactly does it break?

**No.** The break is not at a threshold — it is at the layer boundary.

v0.1 offers exactly two levels of description and every question falls between them:

* **Raw normalized geometry** is *too specific*. It changes when nothing changed (re-export B1,
  rotation B2, crop aspect B3, curve phase B4) and it is a scalar, so it can only ever say
  "0.9955" — never «то же самое, повёрнуто на 90°».
* **Generic counts / topology / repeated patterns** are *too generic*. They are invariant under
  mirroring and rotation (F4, F5), they collide across unrelated shapes (F6), they are dominated by
  packaging noise (F2: 814 → 815 while a wall is removed), and they dilute linearly with block size
  (F1, F3).

The decisive observation is that **the two layers fail in opposite directions on the same object**.
For one real door: L3 says *identical* to its own mirror image and to its own 180° rotation, while raw
geometry says *0.0000 similar* to both. There is no threshold, no weighting, and no prompt that turns
those two numbers into «дверь развернули» or «то же самое, просто повёрнуто». The information that
would distinguish the three cases — *is this the same object?*, *where is it?*, *how is it oriented?*,
*what is it connected to?* — is destroyed by the time either layer is computed.

That missing information is exactly an **object layer** (a stable identity per drawn thing, with pose:
position, orientation, mirror flag, scale) plus a **relation graph** (which object carries which value,
what is connected to what, what is inside what). Concretely, each falsification above dissolves once
objects exist:

| falsification | what an object+relation layer would supply |
|---|---|
| F1/F3 dilution | change is counted in *objects added/removed*, not in segments, so it does not dilute with block size |
| F2 wrong-direction counts | object deltas are signed and packaging-independent |
| F4/F5 mirror & rotation | pose is an explicit attribute; `mirrored: true` is a readable sentence |
| F6 descriptor collisions | identity comes from matching against a symbol prototype, not from a count tuple |
| F7 value permutation | a value belongs to an object, so moving it between objects is a relation change |
| F8/F9 re-export | objects are recovered from geometry, so re-packaging is absorbed before comparison |
| F10 crop anisotropy | objects are matched by shape and pose, not by block-relative coordinates |
| B4 curve phase | prototype matching is tolerant to flattening phase by construction |

Note what does **not** need to be discipline-specific: pose, identity, containment, connection,
and "which text belongs to which object" are the same for a door, a valve and a circuit breaker.
Only the *naming* («аппарат», «проём», «ответвление») and the value grammar («Ø», «А», «мм») are.
So the layer that is missing is generic, and it is missing **below** the discipline profiles, not
inside them.

The one thing v0.1 does deliver honestly is a **cheap, deterministic same/not-same screen with a very
low false-negative rate on large changes** — useful as a *router*, provided the block crops are the
same (F10) and the exporter did not change (F8). It is not a description an expert can read.
