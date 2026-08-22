# p04_relations — adversarial verification (Track B, Opus)

Verifier notes. Target: the 5 headline claims of the `relgraph` / `p04_relations` probe
(`artifacts/relgraph_FINDINGS.md`, `artifacts/relgraph_{rotation,rotation_pointcheck,rotfix,final,granularity,crop_invariance,stability}.json`).
Everything below is something I ran or read myself. Scripts I wrote are saved as
`probes/p04_relations_verify_*.py`. Nothing outside this experiment directory was modified.

| claim | verdict | one-line reason |
|---|---|---|
| 1 rotation defect, 7/20 blocks describe a different region | **CONFIRMED** | reproduced independently by span extraction + a side-by-side render; but the *mechanism sentence* inside the probe's own artifact is wrong |
| 2 invalidates 4/10 pairs; 4 usable SS pairs left | **WEAKENED** | facts confirmed; "invalidates" overstated for the 3 VK pairs (both sides clipped with the *same* wrong rect) and the pairs are recoverable by re-extraction |
| 3 rotation fix moves the STRUCTURE_CHANGED verdict, 0.1739→0.4951 | **WEAKENED** | numbers reproduce exactly, but the **verdict does not move** (STRUCTURE_CHANGED before and after, measured) — the score moves |
| 4 coverage cannot separate the change from a 2 % crop jitter; relation multiset can | **REFUTED** | the "crop-noise floor" is the probe's own frame bug: zero-jitter control already gives cov@0.01 = 0.1479. Correct frame: 2 % jitter → cov@0.01 = **1.0000** |
| 5 relation multiset worse than v0.1 on matched crops; no relation variant separates on published data | **WEAKENED** | direction flips on one leave-one-out (vk_plan, a 2.3 % capped sample); a trivial text-count baseline beats both; second half compares two *different* partitions |

---

## Claim 1 — CONFIRMED (with a defect in the probe's own explanation)

Re-ran `probes/relgraph_rotation.py`. Reproduced: 7 of 20 blocks on `/Rotate 90|270`
(vk_plan L/R, vk_nodes L/R, vk_node_plan L/R, eom left), `eom` mixes 270 (left) / 0 (right).

Independent confirmation that did **not** reuse the probe's code:

* `probes/p04_relations_verify_spans.py` — pulls raw text spans and asks what text sits in
  `block_rect` (raw coords) vs `block_rect` mapped through `page.rotation_matrix`.
  `eom left`: extracted = `['Шина PE','L1','L2','L3','N','PE']`, named = `['Условное','обозначение','Описание','Wh1...Whn','QF1...QFn']`.
  `vk_node_plan left`: extracted = `['Ø25','3/4"',…]`, named = `['4.7','4.9',…]`.
* `probes/p04_relations_verify_render.py` — renders (A) `page.get_pixmap(clip=block_rect)`
  (exactly what `extractor.save_description` does for the diagnostic PNG) and (B) the same
  numeric rect after `page.set_rotation(0)` (= the space `get_drawings()` actually lives in).
  For `vk_node_plan left` A is the floor plan «План прокладки систем водоснабжения и
  водоотведения на 2–17 этажах», B is the **specification table + «Узел ввода» / «Узел
  крепления» details**. I then opened Track A's own
  `experiments/stage_comparison_vector_blocks/artifacts/diagnostics/vk_node_plan/left.png`:
  it is the floor plan. So the human validator scored a floor plan against a description
  built from a spec table. That is the defect, and it is real.
* `probes/p04_relations_verify_points.py` — recomputed the point-containment table from
  scratch. Matches `relgraph_rotation_pointcheck.json` exactly for the VK blocks
  (vk_node_plan 75 033 extracted / 217 094 named / **0 in both**; vk_plan 6 132 / 1 767 370 / 3 632;
  vk_nodes 329 426 / 280 623 / 89 753). My eom number differs slightly (0.573 vs 0.584) because I
  enumerate curve points differently — immaterial.

What does **not** survive:

* The probe's stated mechanism is half wrong. `relgraph_FINDINGS.md` R1 and the docstring of
  `probes/relgraph_rotation.py` both say *"PyMuPDF returns `page.get_text(...)` bboxes in the
  rotated page space but `page.get_drawings()` in the unrotated cropbox space … so the geometry
  layer and the text layer describe different physical regions"*. Measured: on `vk_plan`
  (page.rect 2526×1191) the **text** extent is `[34.3, 19.9, 1189.5, 2525.0]` — transposed, i.e.
  unrotated too. Both layers are in the same space; `extract_block` is internally consistent, it
  just reads the wrong window. The claim as handed to me states this correctly; the artifact
  contradicts itself.
* "13 unrotated blocks agree 100 %" is tautological: for `/Rotate 0` the rotation matrix is the
  identity, so the check is `x == x`. It is not evidence, it is a control that cannot fail.
* Reproducibility gap: neither the script that produced `relgraph_rotation_pointcheck.json` nor the
  one that produced `relgraph_rotation_crops/` is saved under `probes/`, and §6 of the findings
  lists no command for them. Against BRIEF rule 5 that is a hole; I had to rewrite both.

## Claim 2 — WEAKENED

Confirmed by reading `block_pairs.json` and `human_validation.md`:

* the 4 remaining pairs (`ss_scheme_text_changed`, `ss_plan_dense`, `ss_simple_node`,
  `ss_table_graphic`) are all from **one** document, `214_Alia_ASTERUS/SS/13AB-РД-СОТ-К7 V1`,
  v002 ↔ v003, pages 5 / 7 / 14 / 15;
* their human verdicts are STRUCTURE_SAME_VALUES_CHANGED (explicitly *"OSPD/camera/room labels
  changed"*, i.e. text), NEAR_IDENTICAL, IDENTICAL, NEAR_IDENTICAL — no geometric design change;
* O1 (ar_plan / ar_wall_sections byte-identical) re-read in `orchestrator_findings.md`.

Where the claim overreaches:

* **"Invalidates" is too strong for the 3 VK pairs.** Left and right were clipped with
  near-identical wrong rects (Δ of `bbox_norm` ≤ 3.3 % of block size — measured across all 10 pairs),
  so left↔right still compares *corresponding* regions of v001 and v002. What is invalidated is the
  region *label* and the link to the human validation, not the measurement itself.
  `eom` is the exception: 270 vs 0 means the two windows really are shaped differently — that pair
  is genuinely corrupted. `vk_plan` is the other genuine casualty: its published description covers
  6 132 of the 1 767 370 drawing points of the named region (**0.35 %**).
* **The pairs are recoverable, and the probe itself recovers them.** I ran
  `comparator.compare_descriptions` on rotation-corrected re-extractions
  (`probes/p04_relations_verify_status.py`): eom STRUCTURE_CHANGED → STRUCTURE_CHANGED,
  vk_nodes NEAR_IDENTICAL → NEAR_IDENTICAL, vk_node_plan NEAR_IDENTICAL → NEAR_IDENTICAL,
  vk_plan NEAR_IDENTICAL → **STRUCTURE_CHANGED** (now disagreeing with the human).
  So "only 4 usable pairs remain" is a statement about the *published descriptions*, not about the
  corpus. Note the corrected VK extractions keep 20 000 primitives out of up to **883 686**
  (`vk_plan` left, measured with `probes/p04_relations_verify_cap.py`) = 2.3 % — so the vk_plan flip
  may itself be a cap artefact.

## Claim 3 — WEAKENED

Re-ran `probes/relgraph_rotfix.py` end to end. Exact reproduction:
`before cov@0.01 = 0.1739 → after 0.4951`; device-like texts on the corrected left block
`['QD1','QF1','Wh1','ЩМкв1']` vs right `['QD1'..'QD4','QF1'..'QF4','Wh1'..'Wh4','ЩМкв']`.
I checked the old description myself: `descriptions/eom_singleline_changed/left/vector_block.json`
contains only `QD1...QDn`, `QDn`, `ЩМквn` and `Ip=22,9A / 26,1A / 29,4A` — no `QD1`, `Wh1`, `QF1`,
`ЩМкв1`, no `Ip=21,2A`. And I opened `diagnostics/eom_singleline_changed/left.png` myself: all
four devices and `Ip=21,2A` are visibly there. That half of the claim is solid.

Where it fails:

* **"Moves the … verdict" is not what happens.** Measured with the real comparator: status is
  `STRUCTURE_CHANGED` **before and after**. The score moves; no verdict on the benchmark moves in the
  direction the claim implies. The only verdict that does move is `vk_plan`, and it moves *away*
  from the human label.
* n = 1 pair, and the framing "38.9 % of the measured dissimilarity was the bug" cherry-picks one
  metric. The same fix moves `text.effective_similarity` 0.2506 → **0.6977** (measured), a larger
  relative move that the claim does not mention and that would make the pair look *more* similar.
* The other three corrected pairs are all capped (20 000 primitives kept of 37 641 / 164 738 /
  883 686), which the findings admit in §7 but the headline does not.

## Claim 4 — REFUTED

This is the one that does not survive contact.

`relgraph_rotfix.extract_rotation_correct` sets `d["bbox"] = named` (the rect in **rotated page
space**) while the primitives' `raw.segments` stay in **unrotated content space**
(measured: `base["bbox"] = [95.4, 21.1, 584.1, 791.5]`, raw segment extent
`[65.3, 95.4, 802.3, 584.1]`, extraction rect `[50.5, 95.4, 821.0, 584.1]`).
`relgraph_final.py` then builds the "crop-noise floor" as
`renormalize(base, jitter_rect(base["bbox"], f))`, and `renormalize` rebuilds the normalized layer
from the **raw** coordinates. So it normalizes content-space geometry against a transposed rect.

Three measurements (`probes/p04_relations_verify_zerojitter.py`,
`probes/p04_relations_verify_jittersweep.py`):

1. **Zero-jitter control.** `coverage(base, renormalize(base, base["bbox"]))` — no jitter at all —
   gives `cov@0.005 = 0.0902`, `cov@0.01 = **0.1479**`. A self-consistent control must return 1.0.
   The published floor of 0.1091 is this artefact, not crop sensitivity.
2. **The "floor" is flat in the jitter magnitude.** cov@0.01 at jitter 0.2 % / 0.5 % / 1 % / 1.5 % /
   2 % / 3 % / 5 % = 0.147 / 0.149 / 0.157 / 0.149 / **0.136** / 0.121 / 0.109. A crop-noise curve
   cannot start at 0.147 for a 0.2 % perturbation; that is the constant offset of the broken frame.
3. **Redone in the frame `extract_block` actually normalized against** (same block, same content,
   same jitter function): zero → 1.0000; 0.2 % → 1.0000; 0.5 % → 1.0000; 1 % → 1.0000;
   **2 % → cov@0.01 = 1.0000** (cov@0.005 = 0.7832); 5 % → 0.6401; edge-crop 10 % → 0.4835.
   relG1 floor in the same run: 0.9898 / 0.9033 / 0.9703.

So against the change signal 0.4951 the true numbers are: cov@0.01 floor **0.4835** (margin
**−0.0116**, not −0.386) and relG1 floor **0.9033** (margin **+0.5142**, not +0.279).
At the "2 % crop jitter" the claim names, coverage separates the change by **+0.5049**.

The probe's own published-block control agrees with me and contradicts its own prose:
`relgraph_crop_invariance.json`, block `eom_singleline_changed`, `jitter_2%` →
cov@0.01 = **1.0000** (frame-only) / **0.9968** (re-extract). It only degrades at 5 %
(0.3883 / 0.7191) and at the 10 % edge crop (0.4350 / 0.4860). The findings text
«A 2 % bbox jitter of identical content already pushes segment coverage below what a genuine
П→РД redesign produces» is falsified by the artifact it cites.

Selection note: `relgraph_FINDINGS.md` §2 defines the floor as `min over {2 %, 5 %, edge-crop 10 %}`
and drops the `jitter_0.5%` variant that `relgraph_crop.py` computed — the one variant at the
magnitude closest to what the benchmark's own paired blocks actually show (max |Δbbox_norm| per
pair, measured: median 1.1 %, max 3.3 % over the 9 same-page-size pairs; the eom pair is 30 %
because its two sides sit on differently sized pages). At 0.5 % every metric is ≥ 0.998.

What survives: at a **10 %** edge crop, cov@0.01 (0.4835) does fall just below the change signal
(0.4951) while relG1 (0.9703) does not. That is a real but much weaker statement, on 1 block,
1 change pair, and a synthetic perturbation 3× larger than any real pairing error in the corpus.

## Claim 5 — WEAKENED (first half), REFUTED as stated (second half)

**First half** (`+0.3713` cov vs `+0.2108` relG1 on the corrected geometry-change-only partition)
reproduces from `relgraph_final.json`. It does not survive three checks:

* **One pair decides it.** relG1's `min_unchanged` is `vk_plan` (0.5999) — the pair whose corrected
  extraction keeps 20 000 of 883 686 primitives (2.3 %). Drop `vk_plan`: relG1 margin becomes
  **+0.4398**, cov stays **+0.3713** — the claim's direction reverses. cov's margin is set by
  `ss_scheme_text_changed`, an uncapped SS block, so it is not symmetric fragility.
* **Both metrics separate perfectly, so "worse" is a scale statement.** eom is the minimum of all
  10 pairs under *both* metrics; with one positive the ranking AUC is 1.0 for both. Comparing raw
  margins across metrics with different dynamic ranges is not a comparison of discriminative power.
  Under a z-normalised margin cov does stay ahead (z = +8.90 vs +1.89), so the conclusion is not
  dead — but the reported evidence does not establish it.
* **A trivial baseline beats both.** `min(n_texts)/max(n_texts)` — a scalar that needs no geometry,
  no relations and no extraction — gives margin **+0.5186** on the same partition
  (eom 0.2408, min unchanged vk_node_plan 0.7594). `min/max` of cluster counts gives +0.0725,
  of segment counts −0.5268. Any metric ranking on this partition is decided by one positive
  example and is not meaningful.
* Bookkeeping: the column labelled `cov@0.01` is not cov@0.01 for 6 of 10 rows. For the
  non-corrected pairs `relgraph_final.py` reads `comparison.json → geometry.similarity`, which is
  the comparator's *adaptively selected* tolerance (0.0025 ar_plan, 0.005 ar_wall_sections /
  vk_plan / vk_node_plan, 0.001 ss_simple_node). Small numerically here, wrong as a label.

**Second half** («on the published data no relation variant separates at all (G3 −0.378,
G1 −0.217, G0 −0.157) while v0.1 geometry manages only +0.0019») compares two different partitions.
The negative numbers come from `relgraph_granularity.json → separation`, which uses the
**any-change** set {ss_scheme, vk_nodes, eom}; the +0.3713/+0.2108 numbers use the
**geometry-change-only** set {eom}. Recomputed on the *published* data with the geometry-only
partition:

| metric (published, changed = eom only) | margin |
|---|---:|
| v0.1 geometry (selected tol) | +0.6925 |
| relation G1 | **+0.3717** |
| relation G3 | **+0.4089** |
| relation G0 | **+0.1258** |
| v0.1 text | +0.1707 |

Every relation variant separates. The contrast in the claim is produced by the partition switch,
not by the rotation correction.

`+0.0019` is itself an artefact of the comparator's adaptive tolerance: it compares
`vk_plan@0.005 = 0.9930` with `vk_nodes@0.01 = 0.9910`. At a fixed tolerance the same published
any-change margin is **+0.0061** at 0.01 and **+0.0194** at 0.005 (read off
`comparison.json → geometry.tolerance_experiment` for all 10 pairs). The substance — v0.1 geometry
barely separates on the any-change partition — survives; the number does not.

---

## Reproduction

From `/home/coder/projects/PDF-proverka`:

```bash
python experiments/stage_comparison_vector_architecture_opus/probes/relgraph_rotation.py
python experiments/stage_comparison_vector_architecture_opus/probes/relgraph_rotfix.py     # ~6 min
python experiments/stage_comparison_vector_architecture_opus/probes/p04_relations_verify_spans.py
python experiments/stage_comparison_vector_architecture_opus/probes/p04_relations_verify_render.py
python experiments/stage_comparison_vector_architecture_opus/probes/p04_relations_verify_points.py
python experiments/stage_comparison_vector_architecture_opus/probes/p04_relations_verify_zerojitter.py
python experiments/stage_comparison_vector_architecture_opus/probes/p04_relations_verify_jittersweep.py
python experiments/stage_comparison_vector_architecture_opus/probes/p04_relations_verify_cap.py     # ~4 min
python experiments/stage_comparison_vector_architecture_opus/probes/p04_relations_verify_status.py  # ~6 min
```

`p04_relations_verify_render.py` writes its PNGs to the scratchpad, not into the repo; the two
images that matter are reproduced by opening
`experiments/stage_comparison_vector_blocks/artifacts/diagnostics/vk_node_plan/left.png`
(the named region) next to the script's `*_B_unrotated_clip.png` (the extracted region).

## UNVERIFIED / not checked

* Whether the production Stage Comparison path shares the rotation defect (out of scope, as in the
  original findings).
* Whether a rotation-correct, *uncapped* VK re-extraction would keep `vk_plan`'s NEAR_IDENTICAL
  verdict — the extractor's 20 000 cap makes that untestable without changing `extractor.py`,
  which the BRIEF forbids.
* Claims R7–R14 of `relgraph_FINDINGS.md` were outside my 5 assigned claims and were not verified.
