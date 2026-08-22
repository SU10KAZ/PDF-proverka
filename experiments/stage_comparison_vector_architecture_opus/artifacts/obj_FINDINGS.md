# Probe `obj` — GRAPHICAL OBJECT CANDIDATES: a discipline-free grouper, measured

Track B (Opus) independent audit. Research only; nothing outside
`experiments/stage_comparison_vector_architecture_opus/` was modified.

All commands below run from the repository root `/home/coder/projects/PDF-proverka`.

| # | claim | evidence (measured number / file) | confidence | how it could be falsified |
|---|-------|-----------------------------------|-----------|---------------------------|
| OBJ‑1 | **7 of the 20 benchmark blocks describe a different region of the sheet than the PNG a human validated.** Track A clips `get_drawings()`/`get_text()` with a rect built from `page.rect` (rotation applied) while both APIs return *unrotated* coordinates; `get_pixmap(clip=…)` uses the rotated space, so JSON and PNG diverge on every `/Rotate` page. | Text‑multiset Jaccard between Track A's description and a rotation‑corrected extraction: rot=0 pages **1.0000** (ss_scheme L/R, eom right); rot=90/270 pages **0.5426** (eom left), **0.2725/0.2193** (vk_nodes), **0.1980/0.1968** (vk_plan), **0.0836/0.0730** (vk_node_plan). `artifacts/obj_rotation_bug.json`; extractor.py:945‑951 (`block_rect` from `page.rect`) vs extractor.py:1138 (`get_pixmap(clip=…)`). | high | Show that `page.get_drawings()` returns rotated coordinates on these files — it does not: on eom v001 p9 drawings span x∈[0,841.8], y∈[0,2383.9] while `page.rect` is 2383.9×842.0. |
| OBJ‑2 | The one pair carrying the benchmark's whole "STRUCTURE_CHANGED" recall (`eom_singleline_changed`) is **asymmetrically** affected: left is rot=270, right is rot=0, so the two sides were never the same kind of region. Corrected, the left block's text is exactly what the PNG shows (`Шина N`, `ЩМкв1`, `QD1`, `Стояк 1 от ВРУ-К`); Track A's contained title‑block text (`Подп. и дата`, `Согласовано`, `Рябцева`) and `Стояк 2 от ВРУ-К`, none of which is in the PNG. | `artifacts/obj_rotation_bug.json` (jaccard 0.5426); text dumps in the repro below. My displayed‑clip render is **byte‑identical** (sha256 of pixmap samples `5a26f6f82a9499fc`) to `diagnostics/eom_singleline_changed/left.png`. | high | Render the region Track A actually clipped and find it equals the diagnostic PNG. It does not — see `artifacts/obj_zoom/eom_singleline_changed_left_ACTUAL_JSON_REGION.png`. |
| OBJ‑3 | **A large share of CAD geometry paints nothing.** `ss_scheme_text_changed` right carries 120 white‑filled unstroked rectangles (text‑knockout boxes behind edited labels); `vk_plan` right carries 31 839 invisible paths = **44 % of its segments**. | `artifacts/obj_invisible_ink.json`: vk_plan segments 104 586 / 153 521 raw → 84 309 / 85 985 inked. `obj_objects.json` `invisible_paths_dropped`. | high | Show the white fills are visible in the render — they are `type='f'`, `fill=(1.0,1.0,1.0)`, `color=None`, drawn on white paper. |
| OBJ‑4 | **Dropping invisible ink removes 94–100 % of the *apparent* geometry difference** between versions. `ss_scheme_text_changed`: relative segment‑count difference **0.4099 → 0.0000**; `vk_plan`: **0.3188 → 0.0195**. Track A reports `geometry_similarity = 0.8664` for `ss_scheme_text_changed`, i.e. a 13 % geometry change, where the *inked* geometry is bit‑for‑bit the same count on both sides. | `artifacts/obj_invisible_ink.json`; `experiments/stage_comparison_vector_blocks/artifacts/benchmark_results.json` pair `ss_scheme_text_changed`. | high | Find a visible geometric difference in that pair. Both sides yield 691 inked segments and identical object counts (25 symbol / 6 closed‑area / 17 linear / 4 stray). |
| OBJ‑5 | **The naive signal the brief lists first — "spatial proximity clustering with a scale‑adaptive radius" — has no stable object scale.** Cluster count slides monotonically from "one per stroke" to "one per sheet" with no symbol‑scale plateau. | `artifacts/obj_analysis.json → radius_sweep`. ss_scheme: 486 → 429 → 398 → **3** → 1 (a single step from 398 to 3 between r=0.3·S and r=0.6·S). eom: 109 → 44 → 43 → 38 → 26 → 16 → 13 → 3 → 1. ss_simple_node: 18 → 3 → 2 → 1. | high | Exhibit a radius where the count matches a human object count on more than one block. |
| OBJ‑6 | **A generic object layer *is* achievable, but only with three structural rules on top of proximity**: (a) invisible‑ink filter, (b) dashed‑run consolidation (collinear + equal length + regular gaps), (c) a hard symbol‑scale cap that refuses merges beyond `diag_max·S` and separates long linear runs. With them, ss_scheme goes from 92 phantom symbol clusters to **25**, against a human count of 20 device symbols. | `probes/obj_poc.py` (`_is_invisible`, `dash_runs`, `group_objects`); measured 92 → 25 by toggling `dash_merge`. Overlay `artifacts/obj_overlays/ss_scheme_text_changed/left.png`. | high | Remove any one rule and get the same counts. Removing dash merge alone gives 92 symbol candidates, 56 of which are fragments of one dashed floor boundary. |
| OBJ‑7 | **Object counts are far more stable across versions than segments or generic topology.** Across the 9 pairs without a real drawing change: symbol‑candidate count identical in **8/9**, segment count identical in **4/9**, endpoint‑component count identical in **2/9** (worst component drift 1121→1059, −5.5 %; 1204→1314, +9.1 %). | `artifacts/obj_objects.json`, `artifacts/obj_pair_object_diff.json`. | high | Show a pair where object counts drift more than component counts. Only `vk_plan` (439/436) is not exact, and it drifts less than its component count in relative terms. |
| OBJ‑8 | **Object‑level matching yields an exact zero on 5/10 pairs where segment coverage does not.** Object change rate (added+removed / total): ss_scheme **0.0000**, ss_simple_node **0.0000**, ss_table_graphic **0.0000** (segment rate 0.0057), ar_plan **0.0000**, ar_wall_sections **0.0000**, vk_node_plan 0.0081, ss_plan_dense 0.0051. | `artifacts/obj_pair_object_diff.json` (pos tol 0.03 norm, shape L1 ≤ 0.15). | high | Loosen/tighten the shape threshold and see the zeros move — they do not: `artifacts/obj_eps_sensitivity.json` shows ss_plan_dense flat at 0.0051 for eps 0.05…0.6. |
| OBJ‑9 | **The dominant residual failure mode is object‑boundary instability in dense linework, not shape matching.** `vk_plan` shows 28.7 % object churn (126 added / 129 removed of ~445) that is *not* an eps artefact (0.325 at eps=0.05 → 0.204 at eps=0.60) and *not* a positional offset (median Δx −0.0024, p90 0.0031 in normalised units). 66 of the 129 removed objects have a co‑located right‑hand object whose descriptor distance is a median 0.586 — the grouping drew a different boundary. | `artifacts/obj_eps_sensitivity.json`; diagnostic run in the repro section. | high | Show the churn disappears under a positional‑only matcher — it does not; the objects are co‑located but differently bounded. |
| OBJ‑10 | **A decomposition‑insensitive shape descriptor transfers between two different PDF exports.** The same terminal marker is drawn with **30 segments in v001 and 11 in v002** (a Bézier centre dot resampled to 24 chords vs a 5‑gon). Length‑weighted normalised descriptor distance across versions: median **0.0945**, max 0.1133; within‑version max 0.0125 / 0.0954; distance to the nearest *other* object on the far side **1.8891** → **separation ratio 16.7×**. | `artifacts/obj_eom_object_diff.json → Q2_descriptor_transfer`. | high | Find a same‑symbol pair whose cross‑version distance exceeds the nearest‑other distance. |
| OBJ‑11 | **An exact motif hash does not transfer between versions; only 4–8 of ~50 classes appear on both sides.** Hash‑based `repeated_elements` (Track A's approach, O5) is quantised in absolute units and keyed on segment counts, both of which move between exports. | `artifacts/obj_analysis.json → eom_object_class_table` (53 classes, 4 with members on both sides); `obj_eom_object_diff.json → Q3_class_census` recovers the marker class (14 L / 12 R) **only** with the descriptor, not the hash. | high | Show a hash‑matched class across the eom pair. There is none with count ≥ 2 on both sides. |
| OBJ‑12 | **Object‑level comparison does produce «Количество аппаратов 6 → 12».** Label‑anchored recall of drawn devices: left 6/6, right 12/12, one distinct object per tag (no double counting). Terminal markers 14 → 12 (ground truth 14 → 14; the two bus‑adjacent markers on the right merge into the busbar object). | `artifacts/obj_eom_object_diff.json → Q1_device_objects`, `Q1_terminal_markers`, `sentences_ru`. Ground truth in `artifacts/obj_ground_truth.json`. | high | Count the devices on the diagnostic PNGs and get another number. v001 draws QD1/Wh1/QF1 and QDn/Whn/QFn; v002 draws QD1‑4/Wh1‑4/QF1‑4. |
| OBJ‑13 | **The object layer alone cannot produce that sentence — the count is selected by a text anchor.** Pure shape matching on the eom pair matches only 14–16 of 50 objects at any eps, because the QD/Wh/QF groups are bounded differently on the two sides (left QD = 42 segments in one object; right QD splits across objects). | `artifacts/obj_eom_object_diff.json → Q3_object_matching_sweep` (eps 0.05→1.5: matched 7→28 of 50/62). | high | Produce 6→12 from geometry alone on this pair. The class census does it only for the isolated terminal marker (K17: 14 → 12). |
| OBJ‑14 | **Symbol‑scale objects are recovered well; composite‑scale objects (сечение, узел) are a different layer.** On `ar_wall_sections` the grouper returns 181 symbol candidates and 6 closed areas, none of which is a "section". A separate multi‑scale pass recovers the four sections only at r≈6·S: 8 clusters, of which 3 sections are exactly one cluster, «Сечение 6» splits into 2, and 3 clusters are unrelated axis circles. The cluster‑count curve (13 → 8 → 8 → 5 → 1) has a weak plateau at that scale. | `artifacts/obj_sections.json`; renders `artifacts/obj_sections/ar_wall_sections_r{2.5,4.0,6.0,9.0,14.0}.png`, zoom `artifacts/obj_zoom/sec_r6_small.png`, `sec_r9_B.png` (at r=9·S «Сечение 5» and «Сечение 6» merge). | medium | Find a single radius that yields exactly 4 clusters. None of the five tested does. |
| OBJ‑15 | **A dense plan neither collapses nor explodes.** `ss_plan_dense`: 1226 symbol candidates + 138 closed areas + 299 linear objects, identical on both sides, and the overlay shows parking bays as closed areas, wheelchair pictograms, circled axis/room marks and door leaves each as one object. | `artifacts/obj_objects.json`; overlay `artifacts/obj_overlays/ss_plan_dense/left.png`, zoom `artifacts/obj_zoom/ovl_ssdense_left_zoom.png`. | medium (no exhaustive human count on 1364 objects) | Show the overlay boxes do not correspond to drawing entities. |
| OBJ‑16 | On the smallest block an exhaustive human count gives **13 objects; the PoC returns 15** (3 closed areas, 4 symbol candidates, 5 linear, 3 strays). 11 of 13 ground‑truth objects map 1:1; two adjacent arrow symbols merge into one object and one open rectangle is classed as a symbol instead of a closed area. | `artifacts/obj_ground_truth.json` (`ss_simple_node`); overlay `artifacts/obj_overlays/ss_simple_node/left.png`. | high | Re-count the PNG. |

## What the PoC is

`probes/obj_poc.py`, ~520 lines, no discipline vocabulary anywhere. Pipeline:

1. **Rotation‑correct extraction.** Clip is mapped into the page's own space with
   `page.derotation_matrix`, geometry is read there, then mapped forward with
   `page.rotation_matrix`, so segments, text and the rendered crop share one frame.
2. **Invisible‑ink filter** (`_is_invisible`): white fill with no stroke, white stroke with
   no fill, or zero opacity → not an object.
3. **Characteristic scale `S`** = median font size in points (median segment length when a
   block has < 5 text spans). Every threshold below is a multiple of `S`.
4. **Dashed‑run consolidation** (`dash_runs`): short segments bucketed by
   (direction 2°, perpendicular offset 0.15·S), chained along the line when the gap ≤ 2·S,
   accepted when ≥ 4 members with length CV ≤ 0.25 and gap CV ≤ 0.40 → one `linear_object`.
5. **Connected components of the endpoint graph** at tolerance 0.05·S. Per component:
   small (diag ≤ 8·S) → symbol core; large **with a cycle** (`edges ≥ nodes`) →
   `closed_area_object`; large acyclic → split into `linear_object`s (segments > 6·S) plus
   residual small cores.
6. **Core merging** at radius 0.6·S, **refusing any merge whose bbox exceeds 8·S** — this
   cap is what creates a stable object scale (OBJ‑5/OBJ‑6).
7. **Shape descriptor** (`shape_descriptor`): 24 numbers — aspect, length/diagonal,
   6‑bin length‑weighted angle histogram, 4×4 length occupancy grid. Translation‑ and
   scale‑invariant and, crucially, insensitive to how a curve was decomposed.
8. **Label attachment**: nearest text within 1.6·S (2.5·S in the diff scripts).

Classes emitted: `symbol_candidate`, `repeated` flag + motif class, `closed_area_object`,
`linear_object` (incl. `dashed`), `dense_region`, `stray`. `connector`, `annotation` and
`dimension` were **not** implemented — see Gaps.

## Per‑block scoreboard

| block | ground truth (human) | PoC | recall | notes |
|---|---|---|---|---|
| ss_simple_node L=R | 13 objects (4 rect, 4 symbol, 5 linear) | 15 objects (3+4+5+3 stray) | 11/13 = 0.85 | 2 arrows merge; 1 open rect misclassed; 3 spurious strays |
| ss_scheme L / R | 15 cameras, 5 ОСПД | 25 symbol candidates, 6 closed areas (identical both sides) | cameras 15/15 = 1.00 (0 ambiguous), ОСПД 5/5 = 1.00 (5/5 ambiguous — the room rectangle is also adjacent) | object counts identical L↔R; only text changed |
| eom left | 6 devices, 14 markers, 2 rows | 6 device objects, 14 markers | 6/6, 14/14 | — |
| eom right | 12 devices, 14 markers, 4 rows | 12 device objects, 12 markers | 12/12, 12/14 = 0.86 | 2 bus‑adjacent markers absorbed into the busbar |
| ar_wall_sections | 4 wall sections | 181 symbol candidates, 6 closed areas; separate pass → 8 clusters at r=6·S | composite recall 3/4 exact + 1 split | sections are not a symbol‑scale object |
| ss_plan_dense L=R | qualitative | 1226 symbol + 138 closed + 299 linear, identical both sides | — | does not collapse or explode |

`precision` against a *class* ground truth is not meaningful here: the PoC deliberately
emits every graphical object, so cameras are 15 of 31 non‑linear objects. The number that
matters is **`gt_with_multiple_candidates`** (0 for cameras, 5/5 for ОСПД, 12/15 for eom
right devices) — i.e. how often a discipline profile would have to disambiguate.

## The eom object‑level diff, explicitly

`artifacts/obj_eom_object_diff.json`. Sides: v001 p9 (rot 270, 1378 inked segments, 50 objects)
vs v002 p11 (rot 0, 1859 inked segments, 62 objects).

```
Отходящих линий: 2 → 4.
Аппаратов (QD/Wh/QF) на схеме: 6 → 12.
Клеммных маркеров (повторяющийся символ): 14 → 12.      (ground truth 14 → 14)
Объектов-кандидатов всего: 50 → 62; сопоставлено по форме 14, добавлено 48, удалено 36.
```

Shape‑only class census (descriptor eps 0.15, no segment‑count gate), top rows:

| class | diag/S | L | R | Δ | n_seg L | n_seg R | labels |
|---|---|---|---|---|---|---|---|
| K17 | 1.42 | 14 | 12 | −2 | 30 | 11 | Шина PE |
| K58 | 2.93 | 0 | 4 | +4 | — | 15 | 40 |
| K19 | 0.88 | 4 | 4 | 0 | 3 | 3 | — |
| K31 | 3.56 | 0 | 3 | +3 | — | 11 | 1, 2, 3 |
| K55 | 7.92 | 0 | 3 | +3 | — | 11 | C25 |
| K22 | 2.59 | 2 | 0 | −2 | 8 | — | QF1, QFn |
| K24 | 4.22 | 4 | 0 | −2 | 42 | — | QD1, QDn, Wh1, Whn |

Read it: the geometry layer alone says «появилось 4 объекта класса ~2.9·S рядом с текстом
«40», появилось 3 объекта рядом с «C25»» — true and useful, but it does **not** by itself
say that K22/K24 (left) and K58/K31/K55 (right) are the same kind of device drawn
differently. Only the text anchor closes that gap (OBJ‑12/OBJ‑13).

## Object change rate vs segment change rate vs Track A

| pair | objects L/R | matched | +added | −removed | object rate | segment rate | Track A geometry_similarity |
|---|---|---|---|---|---|---|---|
| ss_scheme_text_changed | 31/31 | 31 | 0 | 0 | **0.0000** | 0.0000 (0.4099 unfiltered) | 0.8664 |
| ss_plan_dense | 1364/1364 | 1357 | 7 | 7 | 0.0051 | 0.0017 | 1.0000 |
| ss_simple_node | 7/7 | 7 | 0 | 0 | 0.0000 | 0.0000 | 1.0000 |
| ss_table_graphic | 40/40 | 40 | 0 | 0 | **0.0000** | 0.0057 | 0.9965 |
| ar_plan | 696/696 | 696 | 0 | 0 | 0.0000 | 0.0000 | 0.9999 |
| ar_wall_sections | 187/187 | 187 | 0 | 0 | 0.0000 | 0.0000 | 1.0000 |
| vk_plan | 446/443 | 317 | 126 | 129 | **0.2868** | 0.0195 (0.3188 unfiltered) | 0.9930 |
| vk_nodes | 306/305 | 278 | 27 | 28 | 0.0900 | 0.0163 | 0.9910 |
| vk_node_plan | 247/247 | 245 | 2 | 2 | 0.0081 | 0.0002 | 0.9948 |
| eom_singleline_changed | 50/62 | 14 | 48 | 36 | **0.7500** | 0.2587 | 0.1739 |

Note the two disagreements that matter: on `ss_scheme_text_changed` the object layer is
*exactly* zero where Track A reports 13 % geometry change (Track A is wrong — it is counting
white knockout boxes); on `vk_plan` the object layer reports 29 % churn where the segments
say 2 % (the object layer is wrong — see OBJ‑9).

## Reproduction

```bash
cd /home/coder/projects/PDF-proverka

# whole benchmark through the grouper + overlays (~7 min, dominated by vk_plan/ss_plan_dense)
python experiments/stage_comparison_vector_architecture_opus/probes/obj_run.py

# tolerant motif classes, label-anchored scoring, radius sweep (~5 min)
python experiments/stage_comparison_vector_architecture_opus/probes/obj_analyze.py

# invisible-ink census, with/without filter (~3 min)
python experiments/stage_comparison_vector_architecture_opus/probes/obj_ink.py

# the eom object-level diff and the Russian sentences (~10 s)
python experiments/stage_comparison_vector_architecture_opus/probes/obj_eom_diff.py

# object change rate vs segment change rate, all 10 pairs (~4 min)
python experiments/stage_comparison_vector_architecture_opus/probes/obj_pairdiff.py

# composite-object (wall section) recovery + renders (~6 min)
python experiments/stage_comparison_vector_architecture_opus/probes/obj_sections.py
```

Rotation bug, from scratch:

```bash
python - <<'EOF'
import fitz
p = fitz.open("projects_v2/objects/214_Alia_ASTERUS/disciplines/EOM/documents/13АВ-РД-ЭМ-К4/versions/v001/02_work/document.pdf")[8]
print("page.rect", p.rect, "rotation", p.rotation)
xs = [c for g in p.get_drawings() for c in (g['rect'].x0, g['rect'].x1)]
ys = [c for g in p.get_drawings() for c in (g['rect'].y0, g['rect'].y1)]
print("drawings x", min(xs), max(xs), "y", min(ys), max(ys))   # -> 0..841.8 x 0..2383.9, i.e. UNROTATED
EOF
```

Invisible knockout boxes, from scratch:

```bash
python - <<'EOF'
import fitz, collections
d = fitz.open("projects_v2/objects/214_Alia_ASTERUS/disciplines/SS/documents/13AB-РД-СОТ-К7 V1/versions/v003/02_work/document.pdf")
p = d[5]; r = p.rect
clip = fitz.Rect(0.0296*r.width, 0.0131*r.height, 0.9908*r.width, 0.3784*r.height)
c = collections.Counter()
for g in p.get_drawings():
    if fitz.Rect(g['rect']).intersects(clip) and any(i[0]=='re' for i in g['items']):
        c[(g['type'], str(g['color']), str(g['fill']))] += 1
print(c)   # -> {('f', 'None', '(1.0, 1.0, 1.0)'): 120}
EOF
```

## Verdict on the audit question

**A universal graphical‑object layer is achievable and it is worth building — but it is not
"clustering with a radius", and it is not sufficient on its own.**

Achievable: 8 of 9 unchanged pairs give an *identical* object count; 5 of 10 pairs give a
literal zero object diff; a decomposition‑insensitive shape descriptor separates same‑symbol
from other‑symbol by 16.7×; a dense plan yields 1364 objects that visibly correspond to
drawing entities.

Not sufficient: (1) the layer only exists once three non‑obvious *rules* are added
(ink test, dash reassembly, hard symbol‑scale cap) — the naive proximity signal has no
stable scale at all (OBJ‑5); (2) composite objects (сечение, узел, помещение) live at a
different scale and need a second, weakly‑determined pass (OBJ‑14); (3) object *identity
across stages* fails on exactly the objects that touch other linework, so «Количество
аппаратов 6 → 12» came out of the text anchor, not out of shape matching (OBJ‑13). That
anchor — "this object is the one under the tag `QD3`" — is a **relation**, not a property of
either the geometry or the text. It is the missing layer.

So the shape of the answer is: **normalised geometry + generic topology + positioned text is
the right *substrate*, and an object layer sits on it, but the thing that makes an expert
sentence safe is the object↔text and object↔connector relation graph.** The current
`VectorBlockDescription` v0.1 fields that feed that (segments, texts with position, topology
counts) survive; the ones that pretend to be an object layer already (`repeated_elements`
hashes, `hatch_like_structures`, `primitive_count`, `anchors.confidence`) do not.

## Gaps / not measured

- `connector`, `annotation` and `dimension` classes were **not implemented**; the brief's
  "text label sitting inside or beside the cluster" was implemented only as nearest‑label
  attachment. Dimension detection is UNVERIFIED here.
- No exhaustive human count on `ss_plan_dense` (1364 objects) or `ar_plan` (696) — those two
  are qualitative (overlay inspection) only.
- Stroke‑width / colour coherence was extracted per segment but **not used** as a grouping
  signal; whether it repairs the `vk_plan` boundary churn is UNVERIFIED.
- Enclosure‑by‑contour ("object inside a closed area") is not used for grouping, only for
  classifying the contour itself.
- Only the 10 Track A pairs were used; no П↔РД pair exists in this corpus (orchestrator
  note), so every "cross‑stage" number here is really "cross‑revision".
- The `ar_wall_sections` composite sweep ran on the left side only; the right side is the
  same file (O1) so it adds nothing, but the plateau‑picking heuristic was not tested on any
  other block — treat OBJ‑14 as a single‑block observation.
