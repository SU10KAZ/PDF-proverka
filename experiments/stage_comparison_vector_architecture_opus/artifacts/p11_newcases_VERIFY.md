# p11_newcases — adversarial verification of the FMC probe's five headline claims

Verifier: independent Track-B adversarial pass. Research only; everything written here lives under
`experiments/stage_comparison_vector_architecture_opus/`. No parallel Codex report was opened.

Probe under test: prefix `fmc` (`artifacts/fmc_FINDINGS.md`, `probes/fmc_*.py`).
New files written by this verification: this file, `artifacts/p11_newcases_verify_crop_controls.json`,
`probes/p11v_crop_controls.py`.

## Verdict table

| # | claim (short) | verdict | one-line reason |
|---|---|---|---|
| 1 | 6/21 = 28.6 % status accuracy, below the 8/21 constant baseline | **WEAKENED** | numbers reproduce exactly, but v0.1's hit set is a strict *subset* of the baseline's: discordance 2–0, exact McNemar p = 0.50, and **one** label flip gives 7–7 parity. Corpus is adversarially selected (all 21 pairs carry a `why_hard`), self-labelled, and drawn from a single object. |
| 2 | 52.3 % of matched pages move index ⇒ index+bbox pairing is wrong more often than right | **WEAKENED** | 1620/3096, 896, 725 reproduce to the unit. The inference does not: one constant per-step offset aligns 67.2 %, the pairing is order-preserving for 88.9 %, and on steps with unchanged page count the shift rate is **3.0 %**. |
| 3 | two crop windows on the same page ⇒ STRUCTURE_CHANGED because bbox normalization turns a shift into a scale change | **REFUTED** (mechanism) | I ran the control the probe named but never ran. Pure translation with size held *exactly equal* reproduces the whole failure (geom 0.4842 vs 0.4751); pure concentric scale of exactly the claimed magnitude costs less than half (0.7205). Driver is content: 78 vs 10 text spans in the two windows. |
| 4 | OV fan-curve block: real change reported IDENTICAL | **CONFIRMED** (one un-reproducible sub-number) | fresh re-run: IDENTICAL, `exact_vector_signature_equal` True, prims 2/2, texts 0/0, segments 3/3. I decoded the embedded chart images myself and read the fan model and duty point on both. n = 1 pair; the "2.56 % / 5263 px" figure is in no artifact and I cannot reproduce it. |
| 5 | CAD text as paths is invisible; 14 corpus pages with 0 fonts / 0 text and no raster cover | **WEAKENED** | GP evidence holds (verified visually + text-layer probe). The census does not: my recount gives **12**, not 14 (two of the listed rows carry 3 and 1 images); 11 of the 12 come from **one** PDF; denominator is 7866 pages = 0.15 %; and the census instrument ("no text layer at all") cannot see the very case the claim leads with — GP p7 has a 10 105-char text layer *and* outlined labels. |

---

## Claim 1 — 6/21 vs the 8/21 constant baseline

### What reproduces

```bash
python3 -c "
import json,collections
res=json.load(open('experiments/stage_comparison_vector_architecture_opus/artifacts/fmc_v01_results.json'))
ok=sum(1 for r in res if r['status']==r['human_expected'])
h=collections.Counter(r['human_expected'] for r in res)
print(ok,'/',len(res),'=',round(ok/len(res),4),'| baseline',h.most_common(1))"
# 6 / 21 = 0.2857 | baseline [('STRUCTURE_CHANGED', 8)]
```

Arithmetic is exact. So is the class mix 8/6/4/3.

### What does not survive

**(a) The gap is 2 pairs and statistically indistinguishable from a tie.**
v0.1's 6 correct answers are `{ar_hatch_sections, eom_drawing_list_rows, eom_layout_reorg_mismatch,
eom_rotated_labels, ov_block_split_widened, ss_a4_to_a3_reissue}` — every one of them is a pair the
human labelled `STRUCTURE_CHANGED`. So the model's hit set is a **strict subset** of the constant
baseline's hit set. Discordant pairs: baseline-right/model-wrong = 2, model-right/baseline-wrong = 0.
Exact two-sided McNemar **p = 0.50**. There is no evidence here that v0.1 is worse than the constant;
there is evidence that it is *never better* on this corpus, which is a different (and weaker) claim.

**(b) One label flip erases the headline — not eight.**
The probe's own falsifiable-by column says "8 label flips would be needed to reach parity with the
baseline". That is arithmetically wrong. Flipping a single label on either discordant pair
(`fmc_eom_tray_plan_geometry` or `fmc_ov_page_shift_geometry`) moves the model to 7 and the baseline
to 7 simultaneously — parity from one flip. Both of those pairs are exactly the ones whose labels are
most contestable (see claim 4: one of them is a block that contains a raster image and 2 stray lines).

**(c) The corpus is adversarially selected against the system under test.**
All **21 of 21** pairs carry a `why_hard` field stating in advance why v0.1 should fail on them
(`fmc_pairs.json`). 28.6 % is therefore a worst-case probe, not an accuracy estimate on the archive.
The comparison "Track A reports 8/10 on its own benchmark" sets an easy, imbalanced set (O1/O2)
against a set built to break the system, and reads as a like-for-like degradation. It is not.

**(d) The baseline number is a property of the probe's own selection.**
"Best constant = 8/21 = 38.1 %" is 38.1 % only because the probe chose to include 8 STRUCTURE_CHANGED
pairs. Pick nine and the baseline is 42.9 %. A constant baseline is informative on a corpus you did not
draw yourself; here both the numerator and the denominator of the comparison are authored.

**(e) Different metrics on the two sides of the comparison.**
Track A's "8/10" is a human `CORRECT / PARTIALLY_CORRECT / WRONG` rating
(`stage_comparison_vector_blocks/artifacts/human_validation.json`, counts 8/2/0), not exact
status-vs-label agreement. FMC scores exact string equality of the status enum. **5 of the 15 FMC
misses have fact recall 1.00** — the value changes *are* in the emitted payload
(`room_schedule_values`, `tray_plan_geometry`, `cable_table_values`, `vk_spec_positions`,
`km_broken_text_swap`) — and would plausibly be `PARTIALLY_CORRECT`, not `WRONG`, under Track A's own
rubric. A further **4 of the 15** are `INSUFFICIENT_VECTOR_DATA` abstentions that the probe's own prose
praises ("it never lies about missing data") while its scorer counts them as errors. Excluding
abstentions: 6/17 vs baseline 8/17 — the same 2-pair gap.

**(f) One object, not "the real archive".**
Mining ran over 210 PDFs / 98 documents, but every one of the 21 pairs comes from
`projects_v2/objects/214_Alia_ASTERUS` (17 documents):

```bash
python3 -c "
import json,collections
d=json.load(open('experiments/stage_comparison_vector_architecture_opus/artifacts/fmc_pairs.json'))
print(collections.Counter(p[s]['pdf'].split('/')[2] for p in d['pairs'] for s in ('left','right')))"
# Counter({'214_Alia_ASTERUS': 42})
```

**(g) The corpus contains the O1 defect it faults Track A for.**
`fmc_crop_mismatch_same_sheet` is the same PDF, same page on both sides. It is declared deliberate — but
it is still counted in the 21 that produce 28.6 %.

**(h) Labels are self-assigned.** Disclosed in `fmc_FINDINGS.md` caveats ("Human labels are mine"),
not in the headline claim. Where I could check a label against pixels I found one to be wrong outright
(claim 3 below).

**Verdict: WEAKENED.** The arithmetic is right; "BELOW the best constant baseline" is a 2-pair,
p = 0.50 difference on a self-selected, self-labelled, single-object, adversarially-built corpus,
scored with a stricter rubric than the number it is contrasted against.

---

## Claim 2 — 52.3 % page-index shift

### What reproduces exactly

```bash
python3 -c "
import gzip,json
d=json.load(gzip.open('experiments/stage_comparison_vector_architecture_opus/artifacts/fmc_candidates.json.gz','rt',encoding='utf-8'))
m=[r for s in d for r in s['matched']]
print('steps',len(d),'matched',len(m),'shifted',sum(1 for r in m if r['index_shift']!=0))
print('L-only',sum(len(s['unmatched_left']) for s in d),'R-only',sum(len(s['unmatched_right']) for s in d))"
# steps 105 matched 3096 shifted 1620   L-only 896 R-only 725
```

1620/3096 = 0.5233. 105 steps over 94 documents. All three numbers land to the unit.

### Robustness checks that the probe passes

- The unmatched counts are **not** an artifact of outlined-text pages (the probe's own caveat):
  only 12 of the 896 left-only and 8 of the 725 right-only pages have `text_len == 0`.
- Large-shift matches are credible, not matcher noise: median word-Jaccard is 0.98–1.00 in the
  `|shift| > 10` bucket, and 15–39 % of matches in the shifted buckets have byte-identical page text.
- The tie-break (`s − 0.0005·|i−j|`) biases *towards* shift = 0, so 52.3 % is if anything conservative.

### Where the claim overreaches

The second sentence — "Any architecture that pairs blocks by page index + bbox is wrong more often than
right" — fails three controls I ran on the probe's own data:

```bash
python3 -c "
import gzip,json,collections,bisect
d=json.load(gzip.open('experiments/stage_comparison_vector_architecture_opus/artifacts/fmc_candidates.json.gz','rt',encoding='utf-8'))
tot=best=lis=0
for s in d:
    m=s['matched']
    if not m: continue
    tot+=len(m)
    best+=collections.Counter(r['index_shift'] for r in m).most_common(1)[0][1]
    seq=[r['j'] for r in sorted(m,key=lambda r:r['i'])]; t=[]
    for x in seq:
        k=bisect.bisect_right(t,x)
        t.append(x) if k==len(t) else t.__setitem__(k,x)
    lis+=len(t)
print('one constant offset per step aligns',best,'/',tot,round(best/tot,4))
print('order-preserving (LIS)',lis,'/',tot,round(lis/tot,4))"
# one constant offset per step aligns 2081 / 3096 0.6722
# order-preserving (LIS)            2751 / 3096 0.8886
```

- **A single constant per-document offset aligns 67.2 %** of matched pages (2081/3096).
- **88.9 % of the pairing is order-preserving**, i.e. an ordinary sequence alignment recovers it.
- **70.9 % of shifts are |Δ| ≤ 1**; 82.1 % are |Δ| ≤ 3.
- Conditioning on stability: among the **28 steps where page count is unchanged**, only
  **19 of 638 matched pages (3.0 %)** move index. The 52.3 % is produced entirely by
  insertion/removal of sheets, which is exactly the case a diff-style alignment handles.
- **Weighting matters.** Micro (per page pair) = 52.3 %; macro (unweighted per version step) = 42.7 %,
  i.e. *below* half. "Wrong more often than right" survives only under micro-weighting, and the top-10
  steps supply 47 % of all shifts.

**Verdict: WEAKENED.** The measurement is solid and reproduces to the unit. The architectural
conclusion attached to it does not: page order moves in a highly structured way that two trivial
mechanisms (constant offset, monotone alignment) recover two-thirds to nine-tenths of.

---

## Claim 3 — crop mismatch on ЭМ-К3 v002 p21

### What reproduces

`fmc_v01_results.json → fmc_crop_mismatch_same_sheet` = STRUCTURE_CHANGED, geometry 0.4751,
text 0.2637, topology 0.9297. A fresh re-extraction through the same `extract_block` /
`compare_descriptions` path gives byte-for-byte the same numbers.

### The control the probe named but did not run

`probes/p11v_crop_controls.py` → `artifacts/p11_newcases_verify_crop_controls.json`:

```
python -m experiments.stage_comparison_vector_architecture_opus.probes.p11v_crop_controls
```

| case | left bbox | right bbox | status | geom | text | topo |
|---|---|---|---|---:|---:|---:|
| a. identical bbox | 0.10,0.25,0.42,0.80 | same | IDENTICAL | 1.000 | 1.000 | 1.000 |
| b. **pure translation, size held exactly equal** (+0.06/+0.04, 0.320×0.550 both sides) | 0.10,0.25,0.42,0.80 | 0.16,0.29,0.48,0.84 | STRUCTURE_CHANGED | **0.4842** | **0.2621** | 0.817 |
| b2. small translation (+0.01/+0.01, equal size) | | 0.11,0.26,0.43,0.81 | STRUCTURE_CHANGED | 0.7213 | 0.2720 | 0.972 |
| c. **pure concentric scale ×0.906** (0.320→0.290 width, centre held) | | 0.115,0.276,0.405,0.774 | STRUCTURE_CHANGED | **0.7205** | 0.2683 | 0.931 |
| c2. concentric scale ×0.99 | | 0.102,0.253,0.418,0.797 | NEAR_IDENTICAL | 0.9893 | 1.000 | 0.981 |
| d. probe's actual pair | | 0.16,0.29,0.45,0.83 | STRUCTURE_CHANGED | 0.4751 | 0.2637 | 0.930 |

Read it: **removing the size change entirely (case b) leaves the failure exactly where it was**
(0.4842 vs 0.4751, 0.2621 vs 0.2637). **Applying only the size change the claim blames (case c) costs
less than half as much** (0.7205), and a 1 % scale is absorbed cleanly (case c2, NEAR_IDENTICAL 0.9893).
The stated mechanism — "block-bbox normalization converts a shifted window into a scale change" —
therefore is not what produces the number.

### What actually produces the number: the two windows hold different content

```bash
python3 -c "
import fitz
d=fitz.open('projects_v2/objects/214_Alia_ASTERUS/disciplines/EOM/documents/13АВ-РД-ЭМ-К3/versions/v002/02_work/document.pdf')
p=d[21]; R=p.rect
L=fitz.Rect(.10*R.width,.25*R.height,.42*R.width,.80*R.height)
Rt=fitz.Rect(.16*R.width,.29*R.height,.45*R.width,.83*R.height)
f=lambda c: sum(len(l['spans']) for b in p.get_text('dict',clip=c)['blocks'] if b['type']==0 for l in b['lines'])
print('text spans  L',f(L),' R',f(Rt),' intersection',f(L&Rt))"
# text spans  L 78  R 10  intersection 6
```

- **Text: 78 spans in the left window, 10 in the right, 6 shared.** 72 of the left window's 78 spans
  are the axis-bubble column (`3.А`, `3.Б`, `3.В`, …) sitting at x ≈ 0.100–0.106 — the strip the right
  window (x from 0.160) cuts off entirely. `text_similarity = 0.264` is the *correct* answer for two
  windows holding different text; no normalization argument is needed to explain it.
- **Vector items: 36 697 (L) / 49 714 (R), 29 188 in the intersection** — 79.5 % of the left window's
  ink and only 58.7 % of the right's. The right window carries 35 % more ink than the left.
- Consequently the ground-truth label — `IDENTICAL`, «изменений нет: это один и тот же фрагмент листа» —
  is **factually wrong**. They are overlapping but materially different fragments.

### Scope

The claim calls non-identical block extents "the production case", but **19 of the 21 corpus pairs use
identical bboxes on both sides**; only `fmc_eom_cable_table_values` and this synthetic control do not,
and both extents were authored by the probe.

**Verdict: REFUTED** on the stated mechanism (and on the ground-truth label). The reported numbers are
real; the causal story attached to them is contradicted by the probe's own named falsification once it
is actually executed.

---

## Claim 4 — OV fan-curve block reported IDENTICAL

### Independently re-derived from scratch

```bash
python3 - <<'PY'
from pathlib import Path
from experiments.stage_comparison_vector_blocks.extractor import extract_block
from experiments.stage_comparison_vector_blocks.comparator import compare_descriptions
b=Path('projects_v2/objects/214_Alia_ASTERUS/disciplines/OV/documents/13АВ-РД-ОВ2-К1 V1/versions')
l=extract_block(b/'v001/02_work/document.pdf',page_index=185,bbox_norm=[0.29,0.12,0.82,0.40],block_id='vl')
r=extract_block(b/'v002/02_work/document.pdf',page_index=133,bbox_norm=[0.29,0.12,0.82,0.40],block_id='vr')
c=compare_descriptions(l,r); print(c['status'], c['exact_vector_signature_equal'], c['differences'])
PY
# IDENTICAL True []
```

prims 2/2, texts 0/0, segments 3/3, quality LIMITED both sides. Every element of the mechanical part
of the claim checks out.

### Ground truth verified against pixels, not taken on trust

The page text layer is byte-identical on both sides (363 chars each) and contains **none** of
`PatAIR`, `795`, `757`, `633`, `692`; `page.search_for` returns `[]` for all five on both pages. So the
probe's Russian sentence could only have come from the raster. I extracted the embedded XObjects and
read them:

- v001 image xref 760, 904×745, page bbox `[140.2, 109.0, 479.5, 385.7]` — inside the block —
  reads **VO-PatAIR-Kp-5-6/9-5,5-2**, **Pv=795 Па**, **Ps=633 Па**, ΔPs=34 Па, L=45072000 м³/с.
- v002 image xref 697, same size and placement — reads **VO-PatAIR-Kp-5-6/9-3-2-У1**,
  **Pv=757 Па**, **Ps=692 Па**, ΔPs=92 Па, L=24264000 м³/с.

Pixel-compared directly, those two images differ on 5.70 % of pixels at grey threshold 60. The claim's
engineering content is therefore real and the failure is real: a genuine change of fan selection and
duty point is reported `IDENTICAL` with `exact_vector_signature_equal = true`.

### Caveats found

- The model suffix is Cyrillic **`У1`**, not `V1` as written in the claim (cosmetic).
- **The "2.56 % (5263 px)" figure is stored in no artifact and I could not reproduce it.** Inside the
  same bbox at 1600 px I measure 9.85 % / 3.71 % / 1.48 % at grey thresholds 25 / 60 / 100; the probe's
  own `fmc_raster_diff.py` at its documented 900 px reports the block region as the largest changed
  region with `px_count = 8763`. The qualitative point stands; that specific number is unverifiable.
- **A pixel diff on this page is not by itself evidence of change.** The other two images on the page
  changed compression (JPEG → PNG) between versions while being pixel-identical: the 898×393 image
  differs on **0** pixels at every threshold, and the 242×68 logo on 3 pixels at threshold 10, 0 at 25.
  Image-bytes-changed and image-format-changed are both false positives here; only the 904×745 chart
  really changed.
- **n = 1 pair.** This is a single block. It is the strongest single piece of evidence in the probe,
  and it is also the pair whose label decides claim 1's headline (see 1(b)).

**Verdict: CONFIRMED** for the substance; the pixel-diff sub-number is UNVERIFIABLE.

---

## Claim 5 — CAD text drawn as paths

### GP part: holds, with one weak instrument

```bash
python3 -c "
import fitz
d=fitz.open('projects_v2/objects/214_Alia_ASTERUS/disciplines/GP/documents/13АВ-РД-ГП2/versions/v001/02_work/document.pdf')
p=d[7]; print(p.search_for('Тротуар'), p.search_for('131,56'), p.search_for('Р4.3'))
t=p.get_text('text'); print(len(t),'chars,',len(p.get_fonts()),'fonts')
print([s in t for s in ('Тротуар','тротуар','Р4.3','Р4.2','0,12','0.12','асфальт')])"
# [] [] []
# 10105 chars, 8 fonts
# [False, False, False, False, False, False, False]
```

I rendered `artifacts/fmc_crops/fmc_gp_section_hatch_dims_left.png` and read it: «Тротуар тип Р4.3»,
`131,56`, `131,44`, `131,32`, `131,20`, `131,08`, «Бортовой камень БР100.20.8» are all plainly legible
and none of them exists in the text layer of either version's page 7. Inside the block bbox
`[0.27,0.55,0.5,0.985]` the only text spans (37 left / 38 right) belong to the general-plan legend that
drifted into the frame — the section itself contributes zero spans. Outlined CAD text confirmed, and
fact recall 0.00 for that pair follows.

**Weak instrument:** `search_for("131,56") == []` is a poor proof, because the same page's text layer
*does* contain `131.56` (with a dot) among 54 `131.xx` survey elevations. Searching a comma form on a
dot-separator sheet returns `[]` regardless of outlining. The conclusion survives on the other two
strings and on the span census; the cited test does not carry it on its own.

### Corpus census: does not support the framing

```bash
python3 -c "
import gzip,json,collections
s=json.load(gzip.open('experiments/stage_comparison_vector_architecture_opus/artifacts/fmc_page_scan.json.gz','rt',encoding='utf-8'))['scans']
tot=z=zn=0; by=collections.Counter()
for pdf,v in s.items():
    for pg in v['pages']:
        tot+=1
        if pg['text_len']==0:
            z+=1
            if pg['n_images']==0: zn+=1; by[pdf]+=1
print('pages',tot,'| 0-text',z,'| 0-text & 0-image',zn); print(by.most_common())"
# pages 7866 | 0-text 77 | 0-text & 0-image 12
# [('.../EOM/documents/13АВ-РД-ЭМ2-ПА V1/versions/v001/...', 11), ('.../EOM/documents/13АВ-РД-ЭМ-К3/versions/v002/...', 1)]
```

- **12, not 14.** Two of the 14 rows in `fmc_outlined_pages.json` violate the stated "no raster cover"
  filter: `AR/13АВ-РД-АР4.1-К5 v002 p2` carries **3** images and `TX/13АВ-РД-ТХ2-К4 v001 p9` carries **1**.
- **Denominator: 7866 pages.** 12/7866 = **0.15 %**. The claim quotes 14 without a denominator.
- **11 of the 12 come from a single PDF** (`ЭМ2-ПА v001`). Verified directly: that PDF has 15 pages and
  pages 4–14 all have 0 chars / 0 fonts / 0 images. Page 5 is exactly 7124 paths, 146 189 items,
  92 135 `c` ops as claimed. So the "corpus-wide" evidence is one document-version.
- **The census measures the wrong thing.** It counts pages with *no text layer at all*. The claim's own
  lead example (GP p7) has a full 10 105-character text layer and outlined labels, so partial outlining
  — the common and more dangerous case — is invisible to this census. The number therefore neither
  establishes nor bounds how common outlined CAD text is.
- **The "92 135 Bézier ops" framing also under-detects.** The twelfth page, `ЭМ-К3 v002 p5`, is a fully
  text-bearing drawing-list sheet (I rendered it: three ведомости plus a stamp, all labels legible) with
  0 text characters and **202 670 `l` ops and 0 `c` ops** — outlined/stroke-font text with no Béziers at all.

**Verdict: WEAKENED.** The phenomenon is real and the GP demonstration is sound; the quantitative
"corpus-wide 14 pages" backing is off by two, mis-filtered, undenominated, concentrated in one file
(0.15 % of pages), and measured with an instrument that cannot see the case the claim leads with.

---

## Cross-cutting notes

1. **Reproducibility.** Every number I could re-derive from a committed script re-derived exactly
   (6/21, 8/21, 1620/3096, 896, 725, prims 2 / texts 0 / IDENTICAL, geom 0.4751 / txt 0.2637,
   7124/146 189/92 135, `search_for` results). Two quoted numbers have **no generator script and no
   stored artifact**: the "2.56 % (5263 px)" pixel diff (claim 4) and the `fmc_outlined_pages.json`
   census (claim 5), which is also the one that miscounts.
2. **Internal inconsistency.** `fmc_FINDINGS.md` row F5 says `fmc_eom_text_as_paths` is "0 spans vs
   1796"; `fmc_v01_results.json` records `left_texts 0 / right_texts 678`. The claim as delivered to me
   uses 678. The 1796 is unsourced.
3. **Contamination by the weak Track A benchmark (O1/O2)** runs in an unexpected direction. FMC does
   not inherit O2's imbalance — it over-corrects. Its own class mix is authored, its labels are
   authored, its pairs are individually justified as hard, and its single reused-PDF pair reproduces
   O1's defect inside the very corpus built to fix it.
4. **What actually stands after this pass.** Claim 4 is the load-bearing finding and it survives intact:
   a real engineering change, carried entirely by a raster tile, is emitted as `IDENTICAL` with an
   "exact vector signature equal" flag — a false-negative that no downstream model can recover from,
   because the payload contains nothing at all. Claims 2 and 5 leave real phenomena (page reordering,
   outlined CAD text) with weaker quantitative backing than stated. Claims 1 and 3 do not support the
   architectural conclusions drawn from them.
