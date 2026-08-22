# TXGEO — TEXT ↔ GEOMETRY relation types for a v0.2 VectorBlockDescription

Probe prefix `txgeo`. Track B (Opus), independent architectural audit.
Everything below was computed by scripts in `experiments/stage_comparison_vector_architecture_opus/probes/`
on 20 Track A descriptions **plus 5 fresh blocks × 2 versions** extracted from the corpus with
Track A's unmodified `extractor.py`. All commands run from the repository root.

```bash
python -m experiments.stage_comparison_vector_architecture_opus.probes.txgeo_extract_fresh
python -m experiments.stage_comparison_vector_architecture_opus.probes.txgeo_run_corpus --set all --unit-mode line
python -m experiments.stage_comparison_vector_architecture_opus.probes.txgeo_run_corpus --set all --unit-mode span
python -m experiments.stage_comparison_vector_architecture_opus.probes.txgeo_metrics
python -m experiments.stage_comparison_vector_architecture_opus.probes.txgeo_dimension_check
python -m experiments.stage_comparison_vector_architecture_opus.probes.txgeo_confidence
python -m experiments.stage_comparison_vector_architecture_opus.probes.txgeo_rank
python -m experiments.stage_comparison_vector_architecture_opus.probes.txgeo_usefulness
python -m experiments.stage_comparison_vector_architecture_opus.probes.txgeo_render_crops fresh_kj_sections:left ...
```

---

## 1. Claims

| # | claim | evidence (measured number / file) | confidence | falsifiable by |
|---|---|---|---|---|
| T1 | `anchors.confidence` carries **zero** bits. On the 374 dimension spans where correctness is objectively known, v0.1 confidence is `high` for **374/374** and precision is **0.348** in both strata. | `txgeo_confidence.json → stratifications[v0.1_anchor_confidence]`: `high n=374 precision=0.348`; no other bucket exists | high | find a corpus where `high` and `candidate` split the sample and precision differs by >10 pts |
| T2 | The honest confidence is **candidate count in the corridor**, not distance. `ticks_in_reach ≤ 2` → precision **0.677** (n=186); `≥ 3` → **0.024** (n=188). | `txgeo_confidence.txt` §ticks_in_reach | high | show a block class where ≥3 ticks still resolves correctly |
| T3 | A 3-signal gate turns the relation into something publishable: all hits n=374 p=0.348 → `ticks≤2 & centred` n=175 p=**0.720** → `+ single line candidate` n=83 p=**0.867**. The price is 78 % of recall. | `txgeo_confidence.json → gate_coverage` | high | a 4th cheap signal that keeps p≥0.85 at >50 % coverage |
| T4 | **The referent of a dimension number is a derived interval, not a primitive.** Anchoring to the whole segment gives an implied drawing scale wrong by 8.8–39.9 % on 7 of 8 measurable blocks; anchoring to the tick-to-tick interval recovers the exact standard scale (1:100.01, 1:50.02, 1:10.05, 1:49.92, 1:99.97, 1:20.05 — error 0.01–0.52 %). | `txgeo_referent_shape.txt`; `txgeo_dimension_check.json` | high | reproduce the standard scale from whole-segment lengths on any block |
| T5 | Drawing scale is deterministically recoverable from text+geometry alone on 6 of 15 blocks (error ≤0.52 % vs a standard scale) — a self-verification channel v0.1 has no way to express. | `txgeo_dimension_check.txt` col `err%` | high | show the modal ratio is an artefact of my binning (change bin_factor and watch it move) |
| T6 | Even with the interval referent, only **33–73 %** of pure-integer texts get the right value per block; on a 22-span hand check 4 of 8 dimension spans got a **wrong value** while still firing the right relation *type*. Cause is visible in the crop: hatching and section-break zigzags are indistinguishable from dimension ticks. | `txgeo_dimension_check.txt`; `txgeo_usefulness.json` (`WRONG_VALUE: 4`) | high | mask `hatch_like_structures` before tick collection and re-measure |
| T7 | `leader` (выноска) is the single most useful new type: hit **1164/2316 = 50.3 %** of line units, tip referent resolved **99.6 %**, pair stability **387/387 = 1.000**. But only **37.5 %** of hits have exactly one leader chain. | `txgeo_ranking.json → leader`; `txgeo_metrics.json → line.stability.per_type` | high | show the resolved tip is the wrong object on a hand sample |
| T8 | `symbol_cluster` is the cheapest high-quality type: hit 21.4 %, uniqueness given hit **0.925**, pair stability **87/87 = 1.000**. | `txgeo_ranking.txt`; `txgeo_metrics.json` | high | hand-check 20 symbol_cluster referents and find <80 % correct |
| T9 | `grid_cell` and `repeated_label` show uniqueness **1.000 — by construction**, not by evidence. `grid_cell` fires on 75–97 % of units in *plans* (ar_plan 0.90, ss_plan_dense 0.97, fresh_kj_plan_part 0.89) exactly as it does in the real table (fresh_ov_spec_table 0.84). Column/row repetition does not separate them (ar_plan 0.77/0.78 vs table 0.89/0.99). | `txgeo_gridcell_check.txt` | high | find a per-text signal that separates a plan "cell" from a table cell without a block-level table object |
| T10 | Fingerprint-based `repeated_label` cannot support «Количество аппаратов 12 → 14». Pair stability **0.629** (lowest of all types). On `fresh_kj_plan_part`, a visually identical fragment yields 2 motifs only-left and 2 only-right that differ only in the **first decimal** of the rounded bbox (`motif-4x1.0x1.0` vs `motif-4x1.0x1.1`). No pair in the corpus produced a clean "same motif, count N→M" statement. | `txgeo_motif_counts.json` | high | quantise the fingerprint tolerantly and show the same motif key survives both sides |
| T11 | **The text span is the wrong unit.** 6751 spans collapse to 4597 printed lines (−31.9 %); **23.5 %** of multi-span printed lines have spans that disagree with each other about their own relation (247/323 agree). On `ss_simple_node` 0 of 3 multi-span lines agree. | `txgeo_referent_shape.txt` §text unit; `txgeo_metrics.txt` unit counts | high | show a grouping-free formulation with ≥95 % intra-line agreement |
| T12 | The **legend** — the most stereotyped text↔symbol layout in all design documentation, and the block that defines what every hatch on the plan means — is essentially unreachable by proximity relations. `fresh_ar_legend`: safely-bound **3/46 = 6.5 %**, the lowest of all 15 blocks; 12/46 units stay unbound even after adding a layout-band relation. | `txgeo_ranking.txt` per-block; `txgeo_crops/fresh_ar_legend_left.png` | high | a proximity relation that binds ≥80 % of legend rows to their swatch |
| T13 | Relation assignment is **stable across a version pair**: pooled **1640/1685 = 0.973**; excluding the two identical-file AR pairs (orchestrator O1) **815/858 = 0.950**. The one pair with real drawn change (`ss_scheme_text_changed`) drops to **0.415**. | `txgeo_metrics.json → line.stability` | high | more real-change pairs; the 0.415 may be typical rather than exceptional |
| T14 | Geometry alone cannot pick the relation *type*: before a numeric text guard, `dimension_interval` claimed the axis marks `П.Т`/`П.С` (crop-verified circles on axis stems). A one-line text prior fixed it. Relation typing needs a weak text-category prior, i.e. it is not purely geometric. | `probes/txgeo_relations.py` `_DIM_TEXT_RE`; before/after in `txgeo_metrics` (dimension hits 1915 → 1386) | high | remove the guard and show no loss of precision |
| T15 | Hand validation on 22 spans across 4 blocks whose crops were read by eye: **13 RECOVERED, 4 PARTIAL, 4 WRONG_VALUE, 1 NOT_RECOVERED**. Two referent kinds present in the crops have **no type at all** in the brief's candidate list: the elevation mark (отметка `-0,430` on a level arrow) and the drawing title (`1 - 1`, `Перемычки 2 этаж`). | `txgeo_usefulness.txt` | medium (n=22, single annotator) | a second annotator on the same 22 spans |

---

## 2. What was built and measured

Eleven deterministic relation types, all computed in **raw PDF point space** (v0.1's normalized space
divides x and y by different factors, so every angle in it is wrong — this alone makes
`text_along_line`, dimension parallelism and perpendicularity undefined on a non-square block).
The unit of analysis is a **printed line** (spans merged by baseline + gap ≤ 1.2 × text height);
`--unit-mode span` reproduces the v0.1 unit for comparison.

Corpus: 20 Track A descriptions + 10 fresh descriptions
(`artifacts/txgeo_fresh_descriptions/manifest.json`):

| fresh block | discipline | what it is | change between v001/v002 |
|---|---|---|---|
| `fresh_ar_lintels` | AR | «Перемычки 2 этаж» — five section details with dimension chains, arrow leaders, dot leaders | page redrawn (2840 → 2458 drawings) |
| `fresh_ar_legend` | AR | «Условные обозначения» — legend, swatch + description rows | 125 → 129 text spans, rows edited |
| `fresh_kj_sections` | KJ | Разрезы 1-1 / 3-3 опалубки | none visible |
| `fresh_kj_plan_part` | KJ | fragment of a formwork plan, dimension chains + «по N» leaders | none visible |
| `fresh_ov_spec_table` | OV | «Характеристика систем вентиляции» — pure table | 236 → 404 text spans |

Total runtime: extraction of 10 fresh descriptions **2.8 s**; relation pass over all 30
descriptions **≈ 37 s** in line mode (ss_plan_dense, 84 439 segments, is the slowest at 5.4 s).
Nothing hit the 10-minute guard.

---

## 3. Ranked list — which types earn a place in v0.2

Measured on left sides only, 2316 line units (`txgeo_ranking.txt`).
`stability` from `txgeo_metrics.json → line.stability.per_type` (pooled over 15 pairs).

| rank | relation | hit rate | uniqueness given hit | pair stability | verdict |
|---|---|---|---|---|---|
| 1 | `symbol_cluster` (text centred in a small connected component) | 0.214 | **0.925** | 87/87 = 1.000 | **KEEP.** Cheapest, most unambiguous, stable. Carries marks, axis circles, boxed values. |
| 2 | `leader` (полочка + inclined leader, or free chain end at the text) | 0.503 | 0.121 (single chain 0.375) | 387/387 = 1.000 | **KEEP, but store the tip and the resolved object, not "unique/not".** Tip resolution 99.6 %. |
| 3 | `dimension_interval` (tick-to-tick interval on the line the text sits on) | 0.301 | 0.248 | 626/631 = 0.992 | **KEEP with the gate of T3.** It is the only relation that can be *self-verified* (T5). |
| 4 | `contour_caption` (text just outside a closed contour, overlapping one side) | 0.247 | 0.547 | 39/42 = 0.929 | **KEEP.** Needed for «КР», «БГЗ», «ВК» — equipment/room tags; not in the brief's candidate list. |
| 5 | `enclosure_tight` (text inside the smallest closed contour, area ≤ 60 × text area) | 0.253 | 0.244 | 20/20 = 1.000 | **KEEP.** The tight-area filter is what makes it meaningful; raw enclosure just finds the building outline. |
| 6 | `dimension_line_only` (parallel line with terminators, whole-segment referent) | 0.332 | 0.568 | 226/230 = 0.983 | **DEMOTE to evidence.** High uniqueness, but the referent is the wrong object (T4). Useful only as the carrier for `dimension_interval`. |
| 7 | `band_association` (text and a graphic sharing a horizontal band) | 0.673 | 0.061 | 39/40 = 0.975 | **KEEP ONLY inside a detected table/legend object.** Binds 30/46 legend units; unique in 1 % of dense-plan units. |
| 8 | `grid_cell` | 0.829 | 1.000 *by construction* | 166/180 = 0.922 | **DO NOT KEEP as a text relation.** Fires on plans as readily as on tables (T9). Belongs to a block-level table object. |
| 9 | `repeated_label` (stable offset from a repeated motif) | 0.188 | 1.000 *by construction* | 22/35 = **0.629** | **DO NOT KEEP as fingerprinted.** Cannot survive a version pair (T10). |
| 10 | `along_line` | 0.555 | 0.477 | 4/5 = 0.800 | **DROP.** Everything that matters in it is already covered by `dimension_interval` / `leader`; on its own it is "there is a line near the text". |
| 11 | `between_extension_lines` (as a standalone type) | 0.592 | **0.044** | 5/5 | **DROP as a type; keep as the mechanism inside `dimension_interval`.** Standalone it is the least unique relation measured. |
| — | `nearest_geometry` (v0.1) | 0.970 | 0.312 | conf. stability 1.000 | **DROP.** 97 % coverage of nothing; see T1. |

---

## 4. The honest "unbound" case

With **all** eleven types active, only 30 of 2316 line units end up `unbound` (1.3 %) — but that
number is a lie produced by `grid_cell`, `band_association` and `along_line`, which fire on almost
everything. Restricting "bound" to *a unique hit of a type whose corpus uniqueness ≥ 0.5*
(`dimension_line_only`, `symbol_cluster`, `contour_caption`, `repeated_label`, `enclosure_tight`):

* **safely bound 1048 / 2316 = 45.3 %**, honest unbound **54.7 %** (`txgeo_ranking.json → coverage`).

Per block it splits by drawing genre, not by discipline:

| block | safely bound |
|---|---|
| ss_plan_dense | 0.904 |
| ar_wall_sections | 0.780 |
| fresh_kj_plan_part | 0.766 |
| ar_plan | 0.741 |
| fresh_ar_lintels | 0.580 |
| ss_scheme_text_changed | 0.471 |
| fresh_kj_sections | 0.450 |
| ss_simple_node | 0.444 |
| vk_nodes | 0.194 |
| eom_singleline_changed | 0.156 |
| vk_node_plan | 0.098 |
| ss_table_graphic | 0.077 |
| fresh_ar_legend | **0.065** |
| vk_plan | 0.038 |
| fresh_ov_spec_table | **0.000** |

**Recommended fallback contract.** When nothing fires, say so with a reason code and a
*positional* fallback, never a proximity fallback:

```
{"text_id": "...", "relation": "unbound",
 "reason": "no_typed_relation" | "ambiguous_candidates" | "capped_geometry" | "no_geometry_in_reach",
 "positional": {"bbox_norm": [...], "reading_order_line": 12, "column": 3}}
```

A v0.1-style `nearest_geometry` fallback is worse than `unbound`, because it is indistinguishable
from a real binding at the point of use: 97 % of spans get one, 100 % of the objectively checkable
ones are labelled `high`, and 65 % of those are wrong.

---

## 5. What this says about the audit's question

Three measurements point the same way, and none of them is an opinion:

1. **T4 — the referent of the most valuable text in the corpus is not a primitive.** A dimension
   number refers to *the interval between two ticks on a dimension line*. That object does not exist
   in the PDF; it has to be constructed. Anchoring to the primitive that is physically there gives a
   drawing scale wrong by 8.8–39.9 %; constructing the interval gives 1:100.01. A contract whose
   `anchors[].geometry_id` points at a primitive **cannot express the right answer** — not badly, at all.

2. **T9 / T12 — relation quality is decided at block level, not at text level.** The same
   `grid_cell` computation is correct in `fresh_ov_spec_table` and meaningless in `ar_plan`, and no
   per-text signal separates them (0.89/0.99 vs 0.77/0.78 column/row repetition). The same
   `band_association` is correct in the legend and meaningless on a dense plan. The missing thing is
   an object — «этот блок содержит таблицу», «этот блок содержит легенду» — above the text layer.

3. **T6 / T10 — the noise that breaks the relations is itself an object that v0.1 already half-detects
   but never uses.** Hatching produces the false ticks that make 4 of 8 hand-checked dimensions wrong;
   `hatch_like_structures` exists in v0.1 (and saturates its cap — orchestrator O6) but is never used
   to mask geometry. Repeated symbols produce fingerprints that drift by one decimal across a version
   pair; `repeated_elements` exists in v0.1 (orchestrator O5) but has no identity that survives a redraw.

So: better relation types are **necessary and worth building** — `symbol_cluster`, `leader`,
`dimension_interval`, `contour_caption`, `enclosure_tight` are all cheap (37 s for 30 blocks),
stable across versions (0.95–1.00 pooled) and far more informative than the v0.1 anchor. But they top out
at 45 % safely-bound text and 87 % dimension precision at 22 % recall, and the residue is not noise —
it is the absence of a layer that names *what the geometry is* (a hatch, a table, a legend, a symbol
instance with an identity) before asking *what the text points at*.

For the expert's sentence: «Появился новый проём» and «Номинал 250 → 315 А» become reachable via
`leader` + `symbol_cluster` + gated `dimension_interval`. «Количество аппаратов 12 → 14» does **not**
become reachable — T10 shows the counting primitive itself does not survive a version pair.

---

## 6. Gaps and caveats

* `ar_wall_sections`, `vk_nodes`, `vk_node_plan` were extracted by Track A with `storage_capped: true`
  (20 000 of 36 027 / 164 738 / 37 641 primitives). Every relation measured on those three blocks is
  measured on truncated geometry. Verified in the descriptions' `geometry.extraction`.
* The correctness ground truth for the confidence stratification (T2, T3) is the drawing-scale check,
  which is only valid on the 6 blocks whose modal ratio landed on a standard scale. Blocks that mix
  two scales in one frame (`fresh_ar_lintels` mixes a 1:10 section with a wall view at another scale —
  visible in the crop) break the assumption; that block is included and probably depresses its own
  precision figure.
* `fresh_kj_plan_part` (implied 1:137) and `fresh_ov_spec_table` (1:90) did **not** land on a standard
  scale; their dimension numbers are UNVERIFIED.
* The 22-span hand validation has a single annotator (me, from the rendered crops in
  `artifacts/txgeo_crops/`). Verdicts for non-dimension spans are judgement; verdicts for dimension
  spans are arithmetic against the printed value.
* Detector thresholds (1.0 u shelf gap, 6° parallelism, 0.7 u tick distance, 60 × text area for
  `enclosure_tight`) were tuned by looking at **two** blocks (`ss_simple_node`, `fresh_kj_sections`)
  and then applied unchanged to the other 28. They are not swept; a sensitivity study is not done.
* Stability is measured by matching identical text strings between sides. Units whose text changed
  are excluded, so stability is measured on the *unchanged* part of each pair by construction.
