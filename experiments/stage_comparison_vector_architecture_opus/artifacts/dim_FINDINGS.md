# dim_* probe — DIMENSIONS as the highest-value change class for П↔РД

Prefix: `dim_`. Everything below is measured. Reproduction commands are inline.
Scripts: `probes/dim_cache.py`, `probes/dim_detect.py`, `probes/dim_run_all.py`,
`probes/dim_eval.py`, `probes/dim_diff.py`, `probes/dim_overlay.py`,
`probes/dim_crop.py`, `probes/dim_rotation_audit.py`, block list `probes/dim_blocks.json`.

## Headline table

| # | claim | evidence (measured number / file) | confidence | how it could be falsified |
|---|-------|-----------------------------------|-----------|----------------------------|
| D1 | A **discipline-free dimension detector is buildable** from vector geometry alone and finds essentially every dimension value on well-formed drawings. | `ar_wall_sections/left`: 61 GT dimension values, **61 bound, 0 missed, 0 false positives** (the 3 balloon "2"s were refused). `vk_nodes/left`: 46 GT dims, **46 bound, 2 axis marks refused**. `dim_ground_truth.json`, `dim_detection_summary.json` | high | show a GT dimension the detector misses on either block, or a bound span that is not a dimension |
| D2 | **Binding a value to the geometry it measures is the hard part, and it is only ~95 % reliable at best.** | share of *bound* values whose measured span corroborates the printed one: `ar_wall_sections` 58/61 = **95.1 %** (median residual **0.04 %**), `kj_slab_top` 138/154 = **89.6 %**, `vk_nodes` 34/46 = 73.9 %, `ar_roof_plan` 87/224 = **38.8 %**. Recall against hand-built GT on `kj_slab_top`: 138/165 = 83.6 %. `dim_residuals.json` | high | re-review the 3 wall-section failures and show they are correct |
| D3 | **The printed value × drawing scale vs the measured span is a near-perfect guard.** Correct bindings and wrong bindings are separated by 2 orders of magnitude. | `ar_wall_sections/left`: 58 bindings with rel-err ≤ 1 % (median 0.04 %), 3 with rel-err **11 %, 43 %, 154 %**. The fitted scale is **17.6374 mm/pt → 1:50.00**, and the sheet's own title reads «Сечение 3-3 ( 1 : 50)». `dim_residuals.json`, `dim_detected_ar_wall_sections__left.json` | high | find a wrong binding whose residual is under 2 %, or a correct binding above it |
| D4 | The scale is **recoverable per block with no discipline knowledge**, and it matches the scale printed on the sheet in every block where the sheet states one. | fitted → title text: ar_wall_sections **1:50.00** vs «1 : 50»; vk_nodes/vk_node_plan **1:20.02** vs «1 : 20»; ar_roof_plan **1:100.02**; kj_slab_top **1:74.99**. `dim_detection_summary.json` | high | a block where the fitted cluster scale disagrees with the printed scale |
| D5 | **The guard has a floor set by drafting sloppiness, and that floor is drawing-dependent, not detector-dependent.** | On `vk_nodes` the drawn geometry does not match the printed numbers: measured from a 700-dpi render of the «980 \| 50 50 100 70» chain, printed **70 measures ≈ 66 mm**, printed **50 measures ≈ 53 and ≈ 45 mm** (`dim_evidence/dim_vk_chain_value_vs_geometry.png`). Hence only 34/46 of vk_nodes bindings fall inside 2 % while 44/46 fall inside 25 %, against 58/61 inside 1 % on the AR sections. | medium-high | re-measure that chain at higher zoom and show the intervals do equal 50/50/100/70 |
| D6 | **Terminators must be assembled into OBJECTS before use.** A raw "segment endpoint near the line" test forges bounds from the arms of a *neighbouring* chain's tick. | Concrete case, `ar_wall_sections` '980': the arm `(2662.98,787.02)-(2666.64,790.74)` belongs to the y=788.88 chain but its endpoint is 0.48 pt from the y=787.50 chain, splitting the true 55.56 pt interval into 31.38 + 24. Merging collinear arms into one slash and requiring the line to cross it at 0.25–0.75 of its length removes it: rel-err ≤ 1 % rises **57 → 58** and the median residual halves **0.0008 → 0.0004**. `dim_residuals.json` rows `ar_wall_sections/*/raw_endpoint` vs `slash_object` | high | show the false bound survives slash assembly, or that the improvement is noise |
| D7 | **Requiring the full dimension object (terminator AND an extension-line foot at the same place) is what saves the dense plan.** | `ar_roof_plan/left` corroborated bindings **54 → 87 (+61 %)** when the extension-line foot is required (`C_scale_arbitration` → `D_require_extension`, raw_endpoint); `kj_slab_top/left` 136 → 138; `ar_wall_sections` unchanged at 58. `dim_residuals.json` | high | run without `--require-extension` and get the same or better numbers |
| D8 | **A LEADER is a different graphical object and must be routed away from the dimension channel.** Values on a shelf-with-oblique-leader (wall thickness 120 / 270 / 190) are not span dimensions, yet a nearest-line binder happily attaches them to a wall or grid line. | `ar_roof_plan/left`: of 137 non-corroborated bindings, **44 (32 %)** are recognised as leader objects by a generic shelf+leader test; the visual overlay `dim_evidence/dim_overlay_ar_roof_plan_left.png` shows them as long red segments across the plan. 900-dpi crop of one '120' confirms a shelf + oblique leader, not a dimension line. `dim_detection_summary.json` field `leader_objects_among_wrong` | high | show those 44 sit between two terminators after all |
| D9 | **A dimension-level diff produces exactly the sentences the expert needs, with correct location.** | `kj_slab_top` (fresh real revision pair, 13АВ-РД-КЖ5.24.2-К2 lst 4, v001 30.03.26 → v002 07.05.26) emits 4 statements, **4/4 verified by rendering both versions of each location at 420–500 dpi** (`dim_evidence/dim_change_*.png`, `dim_evidence/dim_chain_*.png`): «Размер 1750 → 2250», «Размер 200 → 150», «Размерная цепочка: 1500 разбит на 300 + 1000 + 200», «Размерная цепочка: 1400 разбит на 400 + 1000». The 1750→2250 change is marked with a revision cloud and tag "1.1" in v002. `dim_diff_kj_slab_top__absolute.json`, `dim_evidence/` | high | re-render either location and show the values did not change |
| D10 | **The null test passes: identical documents produce zero dimension changes.** | `ar_wall_sections` (byte-identical PDFs per O1): L=58, R=58, **matched 58, changed 0, removed 0, added 0**. `vk_node_plan` 25/25/0/0/0. `dim_diff_*_absolute.json` | high | any spurious change line on those pairs |
| D11 | **Track A's crop-normalised coordinate frame manufactures false change events.** Because the two crops differ in width by 3.9 %, dividing by the bbox breaks location matching. | `vk_nodes`, bbox-normalised frame: **matched 17/35, 17 spurious "removed" + 17 spurious "added" + 1 nonsense «Размер 100 → 100»**. Same data in absolute page points: **35/35 matched, 0 spurious**. `dim_diff_vk_nodes__bbox_norm.json` vs `dim_diff_vk_nodes__absolute.json` | high | show the 17/17 are real changes |
| D12 | **A value change moves the geometry, so span-midpoint matching alone is not enough — you need the extension-line foot as the anchor and the chain as a relation.** | On `kj_slab_top` midpoint matching found **0** of the 4 real changes; adding shared-foot matching found 3 (one of them wrong); adding chain re-tiling (does the left value equal the sum of the right intervals that tile it?) produced the correct 4. `probes/dim_diff.py` passes 1 / 1b / 2 | high | recover «Размер 1750 → 2250» with midpoint matching only |
| D13 | **(side finding, outside the dimension mandate) Track A extracts the wrong region on every `/Rotate 90` page.** `page.rect` is rotation-aware, but `get_drawings()` and `get_text(clip=…)` return mediabox coordinates while `get_pixmap(clip=…)` uses the rotated space. | `dim_rotation_audit.json`: intended-region overlap **vk_node_plan 0.00 %** (both sides), **vk_plan 9.78 % / 9.62 %**, **vk_nodes 21.98 % / 24.77 %**, **eom_singleline_changed left 63.43 %** (rot 270) against right 100 % (rot 0). 8 of 20 Track A descriptions are affected; the human validated PNGs that do not correspond to the JSON. | high | show `page.get_drawings()` returns rotated coordinates on those pages |

## What was measured, exactly

### Corpus

`probes/dim_blocks.json`. Three Track A pairs (`ar_wall_sections`, `vk_nodes`, `vk_node_plan`)
plus **two fresh pairs found by this probe**, both genuine consecutive revisions of the *same*
sheet with different bytes (unlike the AR pairs, cf. O1):

* `ar_roof_plan` — 13АВ-РД-АР3-К6 «Кладочный план кровли», v001 p.5 ↔ v002 p.6 (1:100)
* `kj_slab_top` — 13АВ-РД-КЖ5.24.2-К2 «Схема верхнего армирования», v001 p.8 ↔ v002 p.9 (1:75)

### The detector (no discipline knowledge anywhere)

A DIMENSION is modelled as a graphical **object with relations**:

```
value_text --on--> dimension_line --bounded_by--> (terminator_a, terminator_b)
terminator --coincides_with--> extension_line_foot
measured_span := |t_b - t_a|   (PDF points)
chain := ordered set of dimensions sharing one dimension_line
```

Only geometry is used: segment endpoints/angles, path fill, text span bbox + baseline direction.
No unit tables, no symbol library, no layer names, no discipline profile.

Ablations (all in `dim_residuals.json` / `dim_detection_summary.json`):

| axis | variants |
|------|----------|
| terminator model | `raw_endpoint` (segment-level, Track-A-style proximity) vs `slash_object` (collinear arms merged, line must cross it at 0.25–0.75 of its length) |
| value→interval selection | `A_nearest_line` → `B_side_convention` (ГОСТ 2.307 / ISO 129: value above its own line) → `C_scale_arbitration` (re-pick using the fitted scale) → `D_require_extension` (bound must be a terminator *and* an extension-line foot) |
| diff frame | `bbox_norm` (Track A's normalisation) vs `absolute` (page points + fitted translation) |

### Per-block numbers (best configuration)

| block | config | scale fitted | numeric candidates | bound | corroborated (≤2 %) | median rel-err |
|-------|--------|-------------|--------------------|-------|---------------------|----------------|
| ar_wall_sections L/R | slash_object + D | 1:50.00 | 64 | 61 | **58** | 0.04 % |
| vk_nodes L | raw_endpoint + D | 1:20.02 | 48 | 46 | 34 | 0.38 % |
| vk_nodes L | slash_object + D | 1:20.02 | 48 | 46 | 36 | 0.28 % |
| vk_node_plan L | raw_endpoint + D | 1:20.02 | 39 | 30 | 29 | 0.09 % |
| kj_slab_top L | raw_endpoint + D | 1:74.99 | 183 | 154 | **138** | 0.32 % |
| ar_roof_plan L | raw_endpoint + D | 1:100.02 | 239 | 224 | **87** | 53 % |

The spread 38 %…95 % across drawing types, with the *same unchanged code*, is the central
measurement: a generic geometric backbone gets you a candidate set, not an answer.

## Reproduction

```bash
cd /home/coder/projects/PDF-proverka
S=/tmp/dimprobe            # any scratch dir

# 1. cache flattened geometry + text for every block (rotation-corrected)
python - <<'EOF'
import json, sys; sys.path.insert(0, '.')
from experiments.stage_comparison_vector_architecture_opus.probes.dim_cache import build
B='experiments/stage_comparison_vector_architecture_opus/probes/dim_blocks.json'
for p in json.load(open(B))['pairs']:
    for side in ('left','right'):
        s=p[side]; build(s['pdf'], s['page'], s['bbox_norm'], f"/tmp/dimprobe/cache/{p['pair_id']}__{side}.json")
EOF

# 2. run every ablation over every block (~2 min total)
python -m experiments.stage_comparison_vector_architecture_opus.probes.dim_run_all \
    --cache-dir $S/cache --out-dir $S/out --summary $S/summary.json

# 2b. the D_require_extension variant (not in run_all's grid)
for B in ar_wall_sections vk_nodes vk_node_plan ar_roof_plan kj_slab_top; do
 for SIDE in left right; do
  python -m experiments.stage_comparison_vector_architecture_opus.probes.dim_detect \
    --cache $S/cache/${B}__${SIDE}.json --out $S/out/${B}__${SIDE}__raw_endpoint__D_require_extension.json \
    --variant C_scale_arbitration --terminator-model raw_endpoint --require-extension
 done
done

# 3. residual analysis (D2, D3, D5, D6, D7)
python -m experiments.stage_comparison_vector_architecture_opus.probes.dim_eval \
    --out-dir $S/out --json $S/residuals.json

# 4. dimension-level diff (D9, D10, D11, D12)
python -m experiments.stage_comparison_vector_architecture_opus.probes.dim_diff \
  --left  $S/out/kj_slab_top__left__raw_endpoint__D_require_extension.json \
  --right $S/out/kj_slab_top__right__raw_endpoint__D_require_extension.json \
  --pair kj_slab_top --frame absolute --out $S/diff_kj.json
python -m experiments.stage_comparison_vector_architecture_opus.probes.dim_diff \
  --left  $S/out/vk_nodes__left__slash_object__C_scale_arbitration.json \
  --right $S/out/vk_nodes__right__slash_object__C_scale_arbitration.json \
  --pair vk_nodes --frame bbox_norm --out $S/diff_vk_bbox.json     # 17 spurious removals

# 5. visual verification overlays
python -m experiments.stage_comparison_vector_architecture_opus.probes.dim_overlay \
  --result $S/out/ar_roof_plan__left__raw_endpoint__D_require_extension.json \
  --pair ar_roof_plan --side left --out $S/ov.png --dpi 170

# 6. the /Rotate audit (D13)
python -m experiments.stage_comparison_vector_architecture_opus.probes.dim_rotation_audit
```

## The four sentences the diff actually produced

From `dim_diff_kj_slab_top__absolute.json` (real revision pair, both sides verified by eye at
500 dpi — see `dim_evidence/dim_change_*.png`):

```
Размерная цепочка: 1500 разбит на 300 + 1000 + 200   (лист КЖ5.24.2 л.4, x≈792 y≈301 pt)
Размерная цепочка: 1400 разбит на 400 + 1000          (x≈729 y≈704 pt)
Размер 1750 → 2250                                    (x≈597 y≈699 pt; spans 66.24 → 85.20 pt,
                                                       geometry moved with the value)
Размер 200 → 150                                      (x≈1276 y≈631 pt; spans 7.44 → 5.76 pt)
```

The 1750→2250 edit carries a revision cloud and the tag "1.1" in v002 — an independent
confirmation that the change the probe found is the change the designer made. Next to it the
sheet also reads «шаг 250 (7 шт.)» → «шаг 250 (9 шт.)»; that is «Количество … 7 → 9» and the
dimension layer **cannot** produce it. It needs the object/relation layer (a rebar-position
object carrying a count attribute), which is precisely the layer under audit.

## What this says about the architecture question

1. **The current v0.1 backbone cannot express a dimension at all.** O4 already showed that bare
   numeric spans (36 %) fall out of both L2 projections. This probe adds the harder half: even if
   they were kept, `anchors` (nearest segment, single candidate, confidence inverted per O3) points
   at *a* segment, never at *the pair of extension-line feet*. The measured span — the only thing
   that makes «Размер 2500 → 2700» safe — is not representable in v0.1.
2. **The object layer earns its place with numbers, not arguments.** Terminator-as-object
   (D6) + full dimension object (D7) + leader-as-object (D8) each moved the measured accuracy on
   real sheets. None of the three can be expressed as a property of a segment.
3. **The relation layer earns its place too.** The chain (ordered intervals sharing one dimension
   line) is what turns a misleading «Размер 1400 → 50» into a correct «1400 разбит на 400 + 1000»
   (D12). A flat list of dimension objects would have shipped the wrong sentence.
4. **The scale cross-check is not optional.** It is the only cheap, generic, per-block signal that
   separates a right binding from a wrong one (D3), it recovers the sheet's declared scale (D4),
   and it is what makes a low-precision detector safe to show an expert. But it has a
   drawing-dependent noise floor (D5), so it must be reported as a confidence, never as a filter
   with a single global threshold.
5. **Coordinates must be anchored to the page, not to the crop** (D11), and the extraction must
   respect `/Rotate` (D13). Both are prerequisites for any object layer to be comparable at all.

## Gaps / not measured

* `ar_roof_plan` GT was **capped**: 239 numeric spans, per-span review not completed. The
  certain numbers there are candidates / bound / corroborated / leader-shaped; the exact
  precision and recall are **UNVERIFIED**.
* Arrowhead terminators (filled triangles) are implemented but never exercised — all five blocks
  use 45° tick slashes. **UNVERIFIED** on arrow-terminated drawings (typical of ОВ/ЭОМ details).
* Radial/diameter/angular dimensions and ordinate dimensioning are not modelled.
* The probe never tested a true П (проектная документация) sheet against an РД sheet: no П↔РД
  pair exists in the corpus (consistent with the project memory note
  «Инвентаризация раздела сравнения перед П→РД … данных П↔РД нет»). Every pair used is
  РД revision N ↔ revision N+1, which shares the sheet frame and scale. A real П↔РД pair
  additionally changes scale and sheet layout, and the absolute-coordinate matching used in
  D11/D12 would then need a fitted similarity transform. **UNVERIFIED.**
* Tolerances (`OFFSET_TOL = 12 pt` for chain re-tiling, `MATCH_TOL = 6 pt`) were set from these
  five blocks; they are not validated on a held-out set.
