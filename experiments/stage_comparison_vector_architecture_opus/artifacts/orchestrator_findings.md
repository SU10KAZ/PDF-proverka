# Orchestrator findings O1–O7 — measured, with reproduction

All commands run from the repository root `/home/coder/projects/PDF-proverka`.

## O1 — two benchmark pairs compare a PDF against itself

```bash
python - <<'EOF'
import json, hashlib
d = json.load(open('experiments/stage_comparison_vector_blocks/artifacts/block_pairs.json'))
h = {}
for p in d['pairs']:
    for s in ('left', 'right'):
        path = p[s]['pdf']
        h.setdefault(path, hashlib.sha256(open(path, 'rb').read()).hexdigest()[:16])
    same = h[p['left']['pdf']] == h[p['right']['pdf']]
    print(p['pair_id'], h[p['left']['pdf']], h[p['right']['pdf']], 'SAME_FILE' if same else 'diff')
EOF
```

Result:

```
ss_scheme_text_changed a3e7451c4e4cc964 5560a190b43da3b2 diff
ss_plan_dense          a3e7451c4e4cc964 5560a190b43da3b2 diff
ss_simple_node         a3e7451c4e4cc964 5560a190b43da3b2 diff
ss_table_graphic       a3e7451c4e4cc964 5560a190b43da3b2 diff
ar_plan                3d7242cd5e72b326 3d7242cd5e72b326 SAME_FILE
ar_wall_sections       3d7242cd5e72b326 3d7242cd5e72b326 SAME_FILE
vk_plan                360626f01c1a0ccc 61d95a84b064f4a3 diff
vk_nodes               360626f01c1a0ccc 61d95a84b064f4a3 diff
vk_node_plan           360626f01c1a0ccc 61d95a84b064f4a3 diff
eom_singleline_changed ee122c122e3c33fb 77460c9d1af44d81 diff
```

`256_Primavera_K14_Spartak/.../СТ26_01-14-АР0-АС-1-РД_V1/versions/v001` and `v002` hold the same bytes.
The AR pairs therefore measure sensitivity to a ~0.1 % bbox jitter, not to a document revision.

## O2 — class imbalance

Human verdicts across the 10 pairs (`artifacts/human_validation.md`): 1 IDENTICAL, 6 NEAR_IDENTICAL,
2 STRUCTURE_SAME_VALUES_CHANGED, 1 STRUCTURE_CHANGED. A comparator that always answers
NEAR_IDENTICAL is right on 6 pairs and arguably near-right on the IDENTICAL one, against the real
comparator's 8 correct + 2 partial. Change recall is exercised on `ss_scheme_text_changed` and
`eom_singleline_changed` only.

## O3 — inverted anchor confidence

```bash
python - <<'EOF'
import json, glob, os, collections
for f in sorted(glob.glob('experiments/stage_comparison_vector_blocks/artifacts/descriptions/*/left/vector_block.json')):
    d = json.load(open(f))
    print(os.path.basename(os.path.dirname(os.path.dirname(f))),
          dict(collections.Counter(a['confidence'] for a in d['anchors'])))
EOF
```

```
ar_plan                {'high': 834, 'candidate': 2}
ss_plan_dense          {'high': 522}
vk_nodes               {'high': 415, 'candidate': 4, 'none': 2}
ss_simple_node         {'candidate': 27, 'none': 4}
ss_table_graphic       {'candidate': 50, 'none': 4}
ss_scheme_text_changed {'high': 19, 'candidate': 70, 'none': 11}
```

`extractor._anchors` labels `high` when the nearest segment is closer than 0.012 normalized units.
In a dense CAD plan that is satisfied by background linework for essentially every span, so the
label is anti-correlated with how much the anchor actually tells you.

## O4 — 36 % of text is dropped from both L2 projections

```bash
python - <<'EOF'
import json, glob, collections
tot = collections.Counter()
for f in glob.glob('experiments/stage_comparison_vector_blocks/artifacts/descriptions/*/*/vector_block.json'):
    tot.update(t['category'] for t in json.load(open(f))['texts'])
print(dict(tot))
EOF
# {'label': 2976, 'numeric': 1971, 'engineering_value': 480}
```

`extractor.extract_block` builds `dimensions` from `category == 'engineering_value'` and `labels`
from `category == 'label'`. The `numeric` bucket — 1971 spans, containing `2760`, `250`, `1150`,
`4650`, i.e. the actual dimension values a П↔РД diff cares about — is in neither list.
Conversely `dimensions` on `ar_wall_sections` contains `-3,900`, an elevation mark.

## O5 — repeated-element fingerprints are dominated by rectangles and a resampling constant

```bash
python - <<'EOF'
import json, glob, os
for f in sorted(glob.glob('experiments/stage_comparison_vector_blocks/artifacts/descriptions/*/left/vector_block.json')):
    d = json.load(open(f))
    print(os.path.basename(os.path.dirname(os.path.dirname(f))),
          [(p['count'], p['primitive_type'], p['segment_count']) for p in d['repeated_elements'][:4]])
EOF
```

```
ar_plan       [(40,'filled_polygon',4), (36,'filled_polygon',4), (24,'circle',24), (18,'filled_polygon',4)]
ss_plan_dense [(154,'filled_polygon',4), (35,'path',2), (25,'polyline',24), (19,'path',4)]
vk_nodes      [(10,'filled_polygon',4), (6,'filled_polygon',4), (3,'filled_polygon',4), (2,'filled_polygon',3)]
```

`extractor.CURVE_STEPS`/the ellipse branch resample every detected circle to 24 points, so
`segment_count == 24` for all circles by construction; `_primitive_pattern` rounds local coordinates
to one decimal, so circles of similar aspect share a fingerprint regardless of radius.

## O6 — hatch candidates saturate

`hatch_like_structures` is capped at 30 entries and returns exactly 30 for ar_plan, ar_wall_sections,
ss_plan_dense, vk_node_plan, vk_nodes, vk_plan. `ss_table_graphic` returns 16, where the parallel
segments are the table's own grid lines.

## O7 — primitive count is packaging noise yet reaches the user

`comparator.compare_descriptions` appends `Число примитивов: {left} → {right}` to `differences`
whenever the counts differ, although `_drawing_primitives` deliberately collapses a multi-command
PDF path into a single primitive, making the count a property of the PDF writer.
`ss_scheme_text_changed` left: 39 primitives, 710 segments, 407 connected components.

## O8 — the "undecodable" VK text is not undecodable; it was never decoded

Track A labels the VK text layer `UNDECODABLE`, drops it from status selection on three pairs, and
names broken fonts as a reason to fall back to Vision. All three claims rest on a fixable defect.

**O8a. Half the text already decodes, and the block-level gate throws it away.** In `vk_nodes`
left, 214 of 421 spans contain no control characters at all, and they carry the bare dimension
values (`100`, `320`, `1480`, `430`, …) — 189 of the 214 contain a digit.
`comparator._text_diff.layer_quality` is a *block-level* gate: five control characters anywhere in
the block mark the whole layer `UNDECODABLE`, so the comparator discards those 214 perfectly good
spans because 207 others are broken. Quality must be per span, never per block.

Note the split is not "digits clean, letters broken": 141 of the 207 broken spans also contain a
digit, because compound specifications mix an unmapped character into an otherwise numeric string —
`Ø16×1/2"`, `×3,5`, and DN values with a Cyrillic prefix all break on the multiplication sign or
the Cyrillic letters. That is precisely why per-span quality is not enough on its own and the glyph
canonicalisation of O8c is needed: the most valuable strings in a VK node are exactly the mixed ones.

**O8b. The cause is an incomplete `/ToUnicode`, not a broken font.** The GOSTCommon subset on
page 9 of the VK v001 PDF (xref 294) carries a `/ToUnicode` CMap with `41 beginbfrange` entries
covering codes `01`–`62` for digits, Latin and punctuation only. Cyrillic codes have no entry, so
PyMuPDF returns the raw byte. Reproduce:

```bash
python - <<'PY'
import fitz, re
d = fitz.open('projects_v2/objects/214_Alia_ASTERUS/disciplines/VK/documents/13АВ-РД-ВК.КВ-К4_V1/versions/v001/02_work/document.pdf')
xref = int(re.search(r'/ToUnicode (\d+) 0 R', d.xref_object(294)).group(1))
print(d.xref_stream(xref).decode('latin-1'))
PY
```

**O8c. A deterministic fix exists and works — no OCR, no Vision, no model.** The embedded
TrueType program is extractable (`document.extract_font`, 28 264 bytes) and exposes the raw codes
through the `(1,0)` cmap subtable (`fontTools.getBestCmap()` picks `(3,0)` and resolves none of
them, which is why this looked hopeless). Replacing each character code with a hash of the glyph
outline it draws yields a canonical, document-independent string. **All 99 glyph codes of the v001
subset are present in the v002 subset (99/99; 87 uniquely).**

Measured on the three VK pairs, word-granularity multiset F1 over the *broken* spans only:

| pair | raw text (today) | glyph-canonical | clean-text ceiling in the same block |
|---|---:|---:|---:|
| vk_nodes | 0.000 | **0.456** | 0.466 |
| vk_node_plan | 0.000 | **0.502** | 0.558 |
| vk_plan | 0.000 | **0.588** | 0.791 |

The unreadable half of the text becomes as comparable as the readable half (98 %, 90 %, 74 % of the
in-block ceiling). Reproduce with
`python -m experiments.stage_comparison_vector_architecture_opus.probes.p00_glyph_identity` and
`artifacts/p00_word_level.json`.

**O8d. Span segmentation, not encoding, is the remaining text ceiling.** Even on the perfectly
decodable half, span-level exact multiset F1 is only 0.465 / 0.560 / 0.784 — and word-level scoring
gives the same numbers (0.466 / 0.558 / 0.791). Text must be compared after reconstruction into
words/lines/cells, not as PDF spans. This is the same defect Track A saw as "span splitting" noise
on `ss_table_graphic`; it is general, not a table quirk.

## O9 — the tolerance ladder is paying for a systematic crop offset, not for noise

`comparator.compare_descriptions` raises the matching tolerance until coverage clears 0.985. On
`vk_nodes` coverage jumps 0.095 → 0.365 → 0.713 → 0.991 across 0.10 / 0.25 / 0.50 / 1.00 %.
Random PDF coordinate noise would lift coverage smoothly; a step like that is the signature of a
systematic offset, introduced by normalising two *different* crops onto the same unit square.

Estimating a translation plus per-axis scale directly from the segment midpoints — a four-parameter
fit, no image features, no ORB, no free-form warp — and re-measuring:

| pair | shift (norm) | scale | coverage @0.25 % before | after | @0.50 % before | after |
|---|---|---|---:|---:|---:|---:|
| vk_nodes | (−0.002, 0.000) | (0.98, 0.99) | 0.365 | **0.913** | 0.713 | **0.993** |
| vk_node_plan | (−0.002, 0.000) | (0.99, 1.01) | 0.832 | **0.989** | 0.995 | 0.995 |
| ss_plan_dense | (0.000, 0.006) | (1.00, 1.02) | 0.590 | **0.9997** | 0.924 | 0.9997 |
| ss_table_graphic | (−0.004, 0.002) | (1.02, 0.98) | 0.258 | 0.557 | 0.832 | 0.977 |
| vk_plan | (0.006, −0.004) | (0.99, 1.00) | 0.975 | 0.981 | 0.993 | 0.994 |
| ar_wall_sections | (0.000, 0.000) | (1.00, 1.00) | 0.967 | 0.967 | 1.000 | 1.000 |

`ar_wall_sections` needs no correction because both sides are the same file (O1).

Consequence: the comparator is forced to run at 1 % tolerance — four times coarser than the data
supports — purely to absorb a crop error that a deterministic four-parameter fit removes. Every
real change smaller than 1 % of the block is invisible as a result. Bbox normalisation is not
alignment, and Track A's decision to exclude alignment entirely is what forces the coarse threshold.
Reproduce: `python -m experiments.stage_comparison_vector_architecture_opus.probes.p00b_alignment`.

## O10 — block normalisation is anisotropic, so it silently distorts every angle and shape

`extractor._norm_point` divides x by the block width and y by the block height independently:
`x_norm=(x-x0)/w`, `y_norm=(y-y0)/h`. When two paired blocks have different aspect ratios — which
they always do, because the crops are not pixel-identical — the two sides are placed in *different*
metric spaces. A 45° line is no longer 45°, a circle is no longer a circle, and every shape
signature, angle test and aspect-dependent fingerprint is comparing distorted geometry.

Measured over the 10 benchmark pairs (block size in PDF points, anisotropy = sx/sy):

| pair | left w×h pt | right w×h pt | anisotropy |
|---|---|---|---:|
| eom_singleline_changed | 488.7×770.5 | 562.3×779.5 | **1.137** |
| ss_table_graphic | 959.9×494.2 | 942.2×523.2 | **0.927** |
| vk_nodes | 1031.9×1665.0 | 1071.9×1677.9 | 1.031 |
| vk_node_plan | 1186.8×984.5 | 1203.8×976.2 | 1.023 |
| ss_plan_dense | 3337.4×1849.6 | 3337.4×1810.9 | 1.021 |
| ar_wall_sections | 1515.8×490.1 | 1495.8×488.9 | 0.989 |
| ss_scheme_text_changed | 1616.0×431.3 | 1618.5×434.9 | 0.993 |
| ar_plan | 1427.4×1656.4 | 1431.4×1653.3 | 1.005 |
| vk_plan | 1561.1×999.7 | 1564.7×998.6 | 1.003 |
| ss_simple_node | 524.5×224.2 | 524.5×224.2 | 1.000 |

At an anisotropy of 1.137 a segment that lies at 45° in one side's normalised frame lies at 41.3° in the other's — a 3.7° disagreement, and 2.2° for the 0.927 case.
`comparator._directional_segment_coverage` demands `angle_distance <= max(1.0, tolerance*500)`;
only the coarse 1 % tolerance (5°) hides this. `extractor._primitive_pattern` bakes
`round(width/height, 1)` into every motif fingerprint, so the same symbol fingerprints differently
on the two sides whenever the aspect differs enough to cross a rounding boundary.

O9 and O10 are the same defect seen twice: the per-axis scale correction that lifted coverage from
0.365 to 0.913 on `vk_nodes` is mostly just undoing this distortion. Normalisation must be
**isotropic** (uniform scale + translation), and the residual aspect difference must be reported as
crop-mismatch evidence rather than silently folded into the coordinates.

## O11 — the longest-first cap makes the comparator look at the part of the drawing that cannot change

`extractor.DEFAULT_STORAGE_CAP = 20_000` primitives and `comparator.SEGMENT_COVERAGE_CAP = 12_000`
segments, both keeping the **longest** items. Long segments are frames, borders, buses and walls —
the geometry that is invariant between two versions of the same sheet. Symbols, annotations and
details are short, and they are what changes.

Measured with an independent extractor (isotropic normalisation, no primitive packing), directional
coverage at 0.5 %, comparing the 12 000 longest segments against a random 12 000 of the discarded
remainder:

| pair | segments in block | fraction the comparator sees | coverage of the KEPT longest | coverage of the DISCARDED short |
|---|---:|---:|---:|---:|
| vk_nodes | 138 388 / 140 714 | **8.5 %** | 0.996 | 0.967 |
| ss_plan_dense | 84 439 / 84 298 | **14.2 %** | 1.000 | **0.763** |
| vk_node_plan | 36 946 / 39 139 | 30.7 % | 0.988 | 0.975 |
| ar_wall_sections | 33 427 | 35.9 % | 1.000 | 0.996 |
| ar_plan | 18 089 | 66.3 % | 1.000 | 1.000 |

`ar_plan` and `ar_wall_sections` are same-file pairs (O1), so their 1.000 is the control.

On `ss_plan_dense` the retained long geometry matches perfectly while nearly a quarter of the
discarded short geometry does not. The `NEAR_IDENTICAL` verdict for that pair rests on the 14 % of
the block least able to carry a change. `vk_nodes` shows the same sign at smaller magnitude.
A cap is unavoidable on dense CAD sheets, but sorting by length is the worst possible order:
the sample must be stratified (by region, by object, by stroke width) rather than length-ranked.

Reproduce: `python -m experiments.stage_comparison_vector_architecture_opus.probes.p00c_cap_bias`.

## O12 — stroke granularity is an exporter property, and it moves between versions

Fraction of segments shorter than 0.001 of the block (about 0.5–0.8 pt), measured per side:

| pair | left segments | left tiny | right segments | right tiny |
|---|---:|---:|---:|---:|
| eom_singleline_changed | 3 294 | **75.3 %** | 1 859 | **26.9 %** |
| vk_nodes | 138 388 | 95.0 % | 140 714 | 95.1 % |
| ss_table_graphic | 1 593 | 8.2 % | 1 583 | 8.2 % |
| ss_scheme_text_changed | 710 | 0.0 % | 1 190 | 0.0 % |

In the EOM pair, v001 explodes dashed linework into micro-segments and v002 does not: 2 479 tiny
segments against 501. Any statistic derived from segment counts, median stroke length, or a
proximity-clustering radius scaled to the median is therefore measuring the CAD exporter's dash
handling, not the drawing. Concretely, an object grouper that picks its clustering radius from each
side's own median builds the two sides in metric spaces 4.5× apart (0.00048 vs 0.00215). Any v0.2
contract must derive such scales **jointly for the pair**, or from a scale-free quantity.

## O13 — on rotated pages the research extractor reads a different region than the crop shows

`extract_block` builds `block_rect` from `page.rect`, which for a page with `/Rotate 90|270` is the
**display** frame, and then clips `page.get_drawings()` and `page.get_text(clip=…)`, whose
coordinates PyMuPDF 1.27.2.2 returns in the **unrotated** mediabox frame. The diagnostic PNG is
produced by `page.get_pixmap(clip=block_rect)`, which *does* honour rotation. So the picture the
human validated and the data the comparator consumed are different parts of the sheet.

Page rotation across the benchmark:

| pair | left rotation | right rotation |
|---|---:|---:|
| vk_plan | 90 | 90 |
| vk_nodes | 90 | 90 |
| vk_node_plan | 90 | 90 |
| eom_singleline_changed | **270** | **0** |
| ss_*, ar_* | 0 | 0 |

Seven of the twenty blocks are affected. Visual proof for `vk_node_plan` left
(`bbox_norm = [0.0183, 0.0021, 0.5162, 0.5868]`, page `/Rotate 90`, `page.rect` 2384×1684,
`mediabox` 1684×2384):

```bash
python - <<'PY'
import fitz
p = 'projects_v2/objects/214_Alia_ASTERUS/disciplines/VK/documents/13АВ-РД-ВК.КВ-К4_V1/versions/v001/02_work/document.pdf'
b = [0.018328696489334106, 0.002146989107131958, 0.5161558747235082, 0.5867864828409218]
d = fitz.open(p); pg = d[11]
rect = fitz.Rect(b[0]*pg.rect.width, b[1]*pg.rect.height, b[2]*pg.rect.width, b[3]*pg.rect.height)
pg.get_pixmap(matrix=fitz.Matrix(0.6, 0.6), clip=rect).save('/tmp/rot_display.png')   # what the PNG shows
d2 = fitz.open(p); pg2 = d2[11]; pg2.set_rotation(0)
pg2.get_pixmap(matrix=fitz.Matrix(0.6, 0.6), clip=rect).save('/tmp/rot_data.png')     # what the data describes
PY
```

`rot_display.png` is «План прокладки систем водоснабжения и водоотведения на 2–17 этажах (1:20)» —
two floor plans. `rot_data.png` is a «Спецификация оборудования, изделий и материалов» table plus a
sideways fragment of the axonometric riser. They share almost nothing.

Consequences for the Track A result:

- All three VK pairs describe the spec-table region on both sides. Because both sides are wrong the
  same way, the comparison is still self-consistent — but the human validation was performed on the
  plan crops, so the "8 correct / 2 partial" scoreboard does not measure what it says it measures
  for those pairs.
- `eom_singleline_changed` is worse: left is `/Rotate 270`, right is `/Rotate 0`. The flagship
  `STRUCTURE_CHANGED` pair compares a transposed left region against a correctly framed right one,
  so its geometry similarity of 0.174 is not evidence about the design change.
- Every number computed from those descriptions — including this audit's own O8, O9 and O11 figures
  for the VK blocks — describes the spec-table region. The *mechanisms* those findings establish are
  unaffected (they are about encoding, alignment and capping, not about which drawing was read), but
  the region must be stated.

**This is a Track A regression, not a production defect.** Production block grounding already
derotates correctly — `structural_geometry.py:103`, `electrical_geometry.py:332`,
`water_geometry.py:494` and nine other modules all compute `cr * sp.derotation_matrix` and add
`sp.cropbox_position`. The research extractor re-implemented extraction from scratch and lost that
step. Any v0.2 contract must carry the page rotation and the frame it resolved coordinates in.
