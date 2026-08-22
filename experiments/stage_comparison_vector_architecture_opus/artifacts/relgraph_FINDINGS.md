# relgraph — RELATION GRAPH probe (Track B, Opus)

Prefix: `relgraph`. Research only. Nothing outside
`experiments/stage_comparison_vector_architecture_opus/` was modified.

Question assigned: *is a graph of relations more useful than a big list of normalized segments?*

Short answer, measured: **the relation graph is not a better change detector on matched crops,
it is a far better one on mismatched crops, and it still cannot say «Добавлены два ответвления».**
Along the way the probe found a coordinate-space defect that invalidates 4 of the 10 Track-A pairs,
including the only pair with a real geometric change.

---

## Findings table

| # | claim | evidence (measured number / file) | confidence | how it could be falsified |
|---|---|---|---|---|
| R1 | `extractor.extract_block` clips rotated pages in the wrong coordinate space: `block_rect` comes from `page.rect` (rotated) while `page.get_drawings()` / `page.get_text()` return unrotated cropbox coordinates. 7 of 20 benchmark blocks are on `/Rotate 90\|270` pages. | `relgraph_rotation.json`: drawings extent for vk_plan = `[0, 0.5, 1190.4, 2526.0]` vs `page.rect` `2526.0 x 1191.0` (transposed). 13 unrotated blocks agree 100 %, 7 rotated blocks agree 0.0–32.0 %. | high | show that PyMuPDF returns page-space coordinates for these files (re-run the extent printout in `probes/relgraph_rotation.py`) |
| R2 | For those blocks the described region and the named/rendered region are largely or entirely disjoint. `vk_node_plan`: **0 of 217 094** drawing points in common; `vk_plan` 3 632 of 1 767 370 (**0.2 %**); `vk_nodes` 32.0 %; `eom` left 58.4 %. | `relgraph_rotation_pointcheck.json`; visual proof `relgraph_rotation_crops/vk_node_plan_left_{NAMED,EXTRACTED}_region.png` — a floor plan vs a riser-node detail + spec table | high | point containment recomputed with a different rotation convention |
| R3 | Consequence for Track A: 4 of 10 pairs (`vk_plan`, `vk_nodes`, `vk_node_plan`, `eom_singleline_changed`) do not describe the drawing they are named after; `eom` also **mixes rotations** (left `/Rotate 270`, right `/Rotate 0`). With O1 (ar_plan, ar_wall_sections = same file) that leaves **4 usable pairs, all SS, from one document, none containing a geometric design change**. | `relgraph_summary.json → benchmark_pairs_corrupted_by_page_rotation`; O1 | high | show the human validator used the extracted region, not the diagnostic PNG |
| R4 | After a rotation-correct re-extraction the eom verdict moves a lot: segment coverage @0.01 **0.1739 → 0.4951**, i.e. **38.9 %** of the measured dissimilarity `(0.4951−0.1739)/(1−0.1739)` was the bug. The corrected left block now contains the spans the crop shows and the old one lacked: `QD1, Wh1, QF1, ЩМкв1` and `Ip=21,2A` (before: only the `…n` set and 3 of 4 `Ip=` rows). | `relgraph_rotfix.json → eom_singleline_changed`; span check in §6 | high | a different correct rect for the left block |
| R5 | **Crop-noise dominance.** On the rotation-corrected eom block, segment coverage cannot separate the real design change from a 2 % crop jitter of the *same content*: signal 0.4951 @0.01 vs crop-noise floor **0.1091** (margin **−0.386**). The relation multiset at shape-class granularity can: signal 0.3891 vs floor **0.6678** (margin **+0.279**). | `relgraph_final.json → eom_signal_vs_crop_noise` | high | a crop-tolerant matcher (affine alignment) for coverage would change this |
| R6 | On *matched* crops the flat relation multiset is **worse** than v0.1 coverage. Rotation-corrected, geometry-change-only partition: cov@0.01 margin **+0.3713**, relG1 **+0.2108**, relG3 +0.1276, relG0 +0.1333. | `relgraph_final.json → separation` | high | more geometrically-changed pairs |
| R7 | **No scalar separates "changed" from "unchanged"** once text-only changes are included: every metric has a negative margin (cov@0.01 −0.0316, relG1 −0.2890, text −0.6556) because `vk_plan` (NEAR_IDENTICAL) scores lower than `vk_nodes` (changed). | `relgraph_final.json → separation ::any_change` | high | different human labels |
| R8 | **Typing objects by normalized size/aspect destroys crop invariance.** Crop noise floor over {2 %, 5 % jitter, 10 % edge crop}: relG3 (shape\|size\|aspect) 0.046–0.577, worse than coverage; relG1 (shape only) 0.700–0.790. Because normalization is per-block, any quantized normalized dimension flips buckets when the crop moves. | `relgraph_summary.md → crop noise floor` | high | size buckets wider than the crop error |
| R9 | **Higher-order graph structure is unusable over crude clusters.** Weisfeiler-Lehman depth 1 on `ar_plan` (byte-identical PDF, 0.1 % bbox jitter — O1) gives label-multiset jaccard **0.0022**; depth 2 gives **0.0000** on 6 of 10 pairs. One neighbourhood perturbation rewrites labels globally. | `relgraph_wl.json` | high | WL with attribute smoothing / soft matching |
| R10 | **Object segmentation by connected components is frame-sensitive** because the merge tolerance lives in normalized units: on `ss_plan_dense`, identical content re-normalized after a 10 % edge crop gives **3 137 → 2 770 clusters (−11.7 %)**. | `relgraph_crop_invariance.json` (experiment `A_frame_only`) | high | tolerance expressed in page/mm units |
| R11 | **«Добавлены два ответвления» is not derivable from the generic relation graph.** No WL-1 label has multiplicity 2 left / 4 right (0 of 117 left, 180 right labels). "Horizontal long clusters" go 12 → 9, the wrong direction. After the rotation fix the *class-level* group table does show 2 → 4 for **4 of 13 shape classes** — but 9 of 13 rows are class churn, so precision is ≈31 % and the sentence is unsafe. | `relgraph_wl.json → eom.wl1_multiplicity_2_to_4 = []`; `relgraph_eom_groups_corrected.json`; `relgraph_eom_expressiveness.json` | high | an object layer with stable identity across versions |
| R12 | **Shape class is not invariant to PDF encoding.** The eom left PDF writes the bus-terminal circles as 34 `circle` primitives; the right PDF writes the visually identical circles as 147 `curve` primitives. My clusterer therefore reports `round: 16 → 1`, i.e. "16 круглых элементов исчезли" — a pure authoring artefact. | measured shape histograms, left `{'round': 16, ...}` right `{'round': 1, ...}`; extractor primitive types left `circle: 34`, right `circle: 0, curve: 147` (see reproduction §6) | high | a shape fitter that closes bezier chains into circles before classification |
| R13 | **Size: the relation graph is cheap.** 20 blocks, estimated tokens: L0 16 156 271, L1 15 385 253, L2 378 582, L3 43 757, **relation graph (shape class) 24 027**, relation graph + raw text list 40 072. RG1 is **640×** smaller than L1, **15.8×** smaller than L2, **1.8×** smaller than L3. | `relgraph_size.json → totals` | high | different serialization |
| R14 | **Tractable on dense plans, but not readable.** `ss_plan_dense` (84 439 segments): 3 137 clusters, 20 267 relation instances over 2 544 distinct tokens, 2.6 s. `ar_plan`: 1 619 clusters, 17 312 instances, 1 917 tokens, 1.0 s. Top token on ss_plan_dense is `('adjacent','rect\|s-9\|a0','rect\|s0\|a0') × 1 191` — grid/hatch noise (cf. O6). | `relgraph_size.json`, `relgraph_stability.json → *_stats` | high | a relation graph built over *filtered* objects rather than every component |

---

## 1. Stability — which representation separates changed from unchanged?

Changed set as assigned: `ss_scheme_text_changed`, `eom_singleline_changed`, `vk_nodes`.
Note from `human_validation.md`: two of those three are **text-only** changes ("labels changed";
"adds notes and a −0.034 annotation"), so no geometry-derived metric can be expected to flag them.

### 1a. On Track A's data as published (`relgraph_granularity.json`)

| metric | margin (min unchanged − max changed) | max changed | min unchanged |
|---|---:|---:|---|
| v0.1 geometry similarity | **+0.0019** | 0.9910 (vk_nodes) | 0.9930 (vk_plan) |
| v0.1 topology similarity | −0.1190 | 0.9800 | 0.8611 |
| v0.1 text similarity | −0.4924 | 0.9137 | 0.4213 |
| relation multiset G0 (rel types only) | −0.1574 | 0.9426 | 0.7852 |
| relation multiset G1 (rel + shape class) | −0.2168 | 0.9132 | 0.6964 |
| relation multiset G2 (+ size bucket) | −0.3282 | 0.8735 | 0.5453 |
| relation multiset G3 (+ aspect bucket) | −0.3779 | 0.8719 | 0.4940 |
| entity inventory (coarse class) | −0.0035 | 0.9194 | 0.9159 |

The v0.1 geometry number is the only positive margin and it is **+0.0019** — one part in five hundred.
It is not a separation, it is a coincidence: the worst unchanged pair (`vk_plan`, 0.9930) and the best
changed pair (`vk_nodes`, 0.9910) are 0.002 apart, and both of those pairs are rotation-corrupted (R2).

### 1b. On rotation-corrected data (`relgraph_final.json`)

Geometry-change-only partition (`eom_singleline_changed` vs the other nine):

| metric | margin | max changed | min unchanged |
|---|---:|---:|---|
| segment coverage @0.01 | **+0.3713** | 0.4951 | 0.8664 (ss_scheme_text_changed) |
| text multiset | +0.1920 | 0.0661 | 0.2581 (vk_node_plan) |
| relation multiset G1 | +0.2108 | 0.3891 | 0.5999 (vk_plan) |
| relation multiset G0 | +0.1333 | 0.6325 | 0.7658 |
| relation multiset G3 | +0.1276 | 0.1610 | 0.2886 |

**Answer to question 1: on matched crops, v0.1 segment coverage separates by the widest margin
(+0.371 vs +0.211 for the best relation view).** The relation graph does not win here.
It wins in §2.

Any-change partition (all three human-changed pairs): **every** metric is negative
(cov@0.01 −0.0316, relG1 −0.2890, relG3 −0.5833, relG0 −0.1756, text −0.6556). No single
scalar over this benchmark answers "did this block change".

---

## 2. Crop invariance — the decisive measurement

Two experiments per block. **A (frame-only)** keeps every PDF coordinate byte-identical and
re-normalizes against a jittered rect, so any drop is pure normalization-frame sensitivity.
**B (re-extract)** really re-runs `extract_block` with the jittered `bbox_norm`.
Jitter sign pattern `(+1, −1, −1, +1)` on `(x0, y0, x1, y1)` — translation plus non-uniform scale.

Crop-noise floor = min over {jitter 2 %, jitter 5 %, edge-crop 10 %} × {A, B}:

| block | cov@0.005 | cov@0.01 | relG3 | relG1 | relG0 | entity | text |
|---|---:|---:|---:|---:|---:|---:|---:|
| eom_singleline_changed (as published) | 0.1094 | 0.3883 | 0.5763 | 0.7420 | 0.8833 | 0.7785 | 0.8354 |
| ss_plan_dense | 0.5373 | 0.8570 | 0.1158 | **0.7009** | 0.7529 | 0.5657 | 0.8112 |
| ss_scheme_text_changed | 0.3021 | 0.4824 | 0.0459 | **0.7897** | 0.8422 | 0.1342 | 0.9118 |

**Same-block signal-vs-noise on the rotation-corrected eom block** (`relgraph_final.json`) —
this is the number that matters:

| metric | change signal (real pair) | crop-noise floor (identical content) | margin | separable? |
|---|---:|---:|---:|---|
| segment coverage @0.005 | 0.2282 | 0.0313 | −0.1970 | **no** |
| segment coverage @0.01 | 0.4951 | 0.1091 | −0.3859 | **no** |
| relation multiset G3 | 0.1610 | 0.1771 | +0.0161 | marginal |
| relation multiset G1 | 0.3891 | **0.6678** | **+0.2786** | **yes** |
| relation multiset G0 | 0.6325 | 0.8882 | +0.2557 | yes |
| text multiset | 0.0661 | 1.0000 | +0.9339 | yes |

A 2 % bbox jitter of *identical content* already pushes segment coverage below what a genuine
П→РД redesign produces. The relation view at shape-class granularity keeps a +0.28 margin.

**Frame-attributable fraction** `(1 − control) / (1 − real)`, where the control stretches the left
block's frame by the pair's own aspect mismatch and keeps content identical
(`relgraph_frame_control.json`):

| pair | aspect distortion | cov@0.005 | cov@0.01 | rel_G3 | rel_G1 | rel_G0 | entity |
|---|---:|---:|---:|---:|---:|---:|---:|
| eom_singleline_changed | ×1.1371 | 0.918 | 0.763 | 0.866 | **0.264** | 0.503 | 0.824 |
| vk_nodes | ×1.0308 | 0.973 | 0.014 | 0.636 | 0.383 | 0.380 | 0.405 |
| ss_scheme_text_changed | ×0.9931 | 0.000 | 0.000 | 0.033 | 0.012 | 0.014 | 0.000 |

At a 3.1 % aspect mismatch, **97.3 %** of vk_nodes' coverage@0.005 drop is the frame.

---

## 3. Size

Estimated tokens (same rule as `extractor._size_metrics`, `ceil(chars/4)`), summed over all 20 blocks:

| payload | tokens | ratio to relation graph |
|---|---:|---:|
| L0 raw vector | 16 156 271 | ×672 |
| L1 normalized primitives | 15 385 253 | ×640 |
| L2 groups + topology | 378 582 | ×15.8 |
| L3 compact | 43 757 | ×1.8 |
| relation graph, full class (G3) | 309 791 | ×12.9 |
| **relation graph, shape class (G1)** | **24 027** | ×1.0 |
| relation graph + raw text list | 40 072 | ×1.7 |

The usable relation view is cheaper than L3 while carrying explicit structure L3 does not have.
The *full-class* relation graph costs 12.9× more and is the version that fails crop invariance (R8) —
paying more buys worse behaviour.

---

## 4. Expressiveness on `eom_singleline_changed`

Ground truth from the rendered crops: LEFT (v001) draws **two** outgoing branches
(ЩМкв1 … ЩМквn) with a vertical ellipsis standing for the rest; RIGHT (v002) draws **four**
explicit branches ЩМкв1…ЩМкв4, each QD•/Wh•/QF•, and adds ratings 40 А / C25 and two cloud callouts.
Target sentence: «Добавлены два ответвления», «Появились номиналы 40 А и C25».

What the relation-level diff actually says (G1 tokens, `relgraph_eom_expressiveness.json`):

```
 -187  ('adjacent', 'seg', 'seg')            380 -> 193
 +98   ('adjacent', 'poly_m', 'seg')          10 -> 108
 +98   ('labelled_by', 'poly_m', 'txt')        4 -> 102
 +72   ('labelled_by', 'rect', 'txt')         37 -> 109
 -70   ('adjacent', 'round', 'seg')           73 -> 3
 +59   ('contains', 'rect', 'seg')             7 -> 66
 +54   ('text_unanchored', '-', 'txt')         3 -> 57
 +27   ('connected', 'poly_m', 'seg')          9 -> 36
 +23   ('connected', 'poly_m', 'poly_s')       1 -> 24
 -19   ('crosses', 'round', 'seg')            19 -> 0
```

Read as prose this is *«больше подписанных средних полилиний, меньше кружков»*. It is not
«Добавлены два ответвления». Concretely:

* **WL motif counting fails.** 0 of 117 left / 180 right WL-1 labels have multiplicity 2→4.
* **Feeder counting fails.** Long horizontal clusters: 12 (left) → 9 (right) — wrong direction,
  because on the left the branch lines merge into the bus component and on the right they do not.
* **Class-level group counting half-works, after the coordinate fix.** 4 of 13 shape classes go
  2 → 4 (`poly_m|s-4|a0`, `poly_s|s-6|a0`, `poly_s|s-6|a1`, `poly_s|s-6|a2`). The other 9 rows are
  churn — including `round|s-7|a1  8 → 0`, which R12 shows is a PDF encoding artefact.
  A sentence built on this table is right about 4 rows in 13.
* **The text layer answers it immediately**: after the coordinate fix the right block carries
  `QD1..QD4, QF1..QF4, Wh1..Wh4, ЩМкв ×4` and the left carries exactly
  `['QD1','QD1...QDn','QDn','QF1','QF1...QFn','QFn','Wh1','Wh1...Whn','Whn','ЩМкв1','ЩМквn']`
  (measured, §6) — two drawn branches plus the legend row.
  Turning that into «ответвлений 2 → 4» needs to know that QD+Wh+QF is one branch and that the
  suffix `n` means "generic" — i.e. a **discipline profile**, not generic topology.

**Verdict for question 4: «Добавлены два ответвления» is NOT safely derivable from a generic
relation graph.** It becomes derivable from a *typed object* layer (apparatus, feeder, panel)
plus the designation text — which is exactly the object + relation layer under discussion,
but the "object" half has to be discipline-aware, not shape-generic.

---

## 5. Noise on dense plans — is the relation graph tractable?

| block | segments | clusters | relation instances | distinct tokens | build time |
|---|---:|---:|---:|---:|---:|
| ss_plan_dense (left) | 84 439 | 3 137 | 20 267 | 2 544 | 2.6 s |
| ar_plan (left) | 18 080 | 1 619 | 17 312 | 1 917 | 1.0 s |
| vk_nodes (left) | 20 086 | 592 | 3 256 | 733 | not timed separately |
| eom (left) | 3 294 | 388 | 1 328 | 241 | 0.1 s |

**Computationally tractable** (worst case 2.6 s/block, no cap hit). **Not tractable as an object
inventory**: 3 137 "objects" on one plan is not something an expert reads, and the highest-count
relations are grid/hatch pairs (`adjacent rect|s-9|a0 – rect|s0|a0` ×1 191), i.e. the same
saturation O6 reports for `hatch_like_structures`. A relation graph over *unfiltered* components
inherits the plan's linework noise one-for-one.

---

## 6. Reproduction

All commands from `/home/coder/projects/PDF-proverka`.

```bash
# relation graph on one block
python experiments/stage_comparison_vector_architecture_opus/probes/relgraph_core.py \
  experiments/stage_comparison_vector_blocks/artifacts/descriptions/eom_singleline_changed/left/vector_block.json

# 1. stability over the 10 pairs (~17 s)
python experiments/stage_comparison_vector_architecture_opus/probes/relgraph_stability.py
python experiments/stage_comparison_vector_architecture_opus/probes/relgraph_granularity.py

# 2. crop invariance, frame-only + re-extraction (~80 s)
python experiments/stage_comparison_vector_architecture_opus/probes/relgraph_crop.py

# 3. size
python experiments/stage_comparison_vector_architecture_opus/probes/relgraph_size.py

# 4. expressiveness on the eom pair
python experiments/stage_comparison_vector_architecture_opus/probes/relgraph_eom.py
python experiments/stage_comparison_vector_architecture_opus/probes/relgraph_wl.py

# rotation defect (R1/R2) and the frame control (R5)
python experiments/stage_comparison_vector_architecture_opus/probes/relgraph_rotation.py
python experiments/stage_comparison_vector_architecture_opus/probes/relgraph_control.py

# rotation-corrected re-extraction and final margins (~4 min)
python experiments/stage_comparison_vector_architecture_opus/probes/relgraph_rotfix.py
python experiments/stage_comparison_vector_architecture_opus/probes/relgraph_final.py

# derived summary tables
python experiments/stage_comparison_vector_architecture_opus/probes/relgraph_summary.py
```

R12's primitive-type histogram:

```bash
python - <<'PY'
import json, sys, collections
sys.path.insert(0,'.'); sys.path.insert(0,'experiments/stage_comparison_vector_architecture_opus/probes')
from relgraph_rotfix import extract_rotation_correct
pairs={p['pair_id']:p for p in json.load(open('experiments/stage_comparison_vector_blocks/artifacts/block_pairs.json'))['pairs']}
p=pairs['eom_singleline_changed']
for sn in ('left','right'):
    s=p[sn]
    d,_,_=extract_rotation_correct(s['pdf'], int(s['page_index']), s['bbox_norm'], sn)
    print(sn, dict(collections.Counter(pr['type'] for pr in d['geometry']['primitives'])))
PY
```

## 7. UNVERIFIED

* I did not re-run Track A's `run_research` end to end; the v0.1 similarity numbers in §1a are read
  from `experiments/stage_comparison_vector_blocks/artifacts/comparisons/*/comparison.json`.
* The rotation-corrected VK re-extractions hit the extractor's 20 000-primitive storage cap on all
  four sides, so their post-fix similarities (`relgraph_rotfix.json`) are computed on capped samples.
* Ground truth for the eom pair ("2 branches → 4 branches") is my reading of
  `experiments/stage_comparison_vector_blocks/artifacts/diagnostics/eom_singleline_changed/{left,right}.png`,
  not an expert annotation.
* Crop-invariance was measured on 3 blocks (as briefed), not on all 20.
* Whether the *production* Stage Comparison code path has the same rotation defect was not checked;
  the finding is about `experiments/stage_comparison_vector_blocks/extractor.py`.
