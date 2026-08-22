# ptn — REPEATED PATTERNS: what signature separates 24 real symbols from 24 hatch tiles?

Track B (Opus) probe, prefix `ptn_`. Research only. Every number below was computed by a script in
`experiments/stage_comparison_vector_architecture_opus/probes/`, run from the repository root.
Nothing outside this experiment directory was modified.

## Findings table

| # | claim | evidence (measured number / file) | confidence | falsifiable by |
|---|---|---|---|---|
| P1 | The current `repeated_elements` fingerprints **PDF paths, not symbols**. One path can carry many symbols, and then the count is 1. | `ss_scheme_text_changed/left` `geometry.primitives[0]` has `item_indexes 0..23` and its `normalized.segments` repeat one 6-segment crossed square at x=0.064 / 0.175 / 0.227 / … — 4 ОСПД symbols inside a single primitive. Whole-block `repeated_elements` = **1 group, 15 instances** while the sheet visibly holds **15 cameras + 5 ОСПД**. | high | show `_drawing_primitives` splitting that path into 4 primitives |
| P2 | Rebuilding the motif unit as a **connected component of raw segments** (long "network" segments removed first) recovers eye-exact instance counts. | ss_scheme cameras **15** (eye 15), ОСПД **5** (eye 5); eom ⊠ markers **14** (eye 14, `eye_eom_left_col.png`). `artifacts/ptn/signatures_*.json` | high | eye-recount the diagnostic PNGs |
| P3 | The current fingerprint is **destabilised by block-normalisation + crop jitter**: on a byte-identical PDF it invents 40 differences; the same motifs in raw coordinates invent none. | `ar_plan` (same file, O1): S0 = **27 changed + 6 appeared + 7 disappeared**; S1 (motif, raw coords) = **0/0/0**. `artifacts/ptn_pair_diff.json` | high | show the ar_plan sides differ graphically |
| P4 | The extractor's **20 000-primitive storage cap manufactures false motif-count changes**; uncapped re-extraction removes them. | ar_wall_sections S1 changed **26 → 0**; vk_node_plan **3 → 0**; vk_nodes **19 → 2**. S6 changed 39→0, 10→3, 59→15. `artifacts/ptn_recut_diff.json` | high | re-run `ptn_recut.py` and get a different result |
| P5 | **S2 (local topology / degree profile) adds nothing and sometimes hurts.** | Corpus repeated groups **663 → 656** (−1 %). On eom the 24-instance terminal-circle group splits into **12 + 11**. `artifacts/ptn_signature_summary.json` | high | find a block where S2 raises the count of a real symbol group |
| P6 | **S3 (nearby text) is a near-deterministic confirmer where it applies, but it applies to only 3 of 5 symbol types.** | Offset std ≤ **0.11 motif-diagonals** (ar_plan marking circle 0.00/0.00 with the label inside; ss camera 0.11/0.05 at 1.22 diag above; ОСПД 0.00/0.00 at 0.82 diag right). But **0 of 14** eom ⊠ and **0 of 24** eom terminal circles have any text within 2 diagonals. `artifacts/ptn_text_offset_stability.json` | high | find labels for those two symbol types |
| P7 | **S4 (neighbouring relations) is destructive inside the identity hash.** | ar_plan's 56-instance marking-circle group shatters into **30 / 18 / 7 / …** under S4; corpus repeated groups rise **663 → 719**. `artifacts/ptn_signature_summary.json` | high | show the 56 circles are genuinely different objects |
| P8 | **S5 rotation/mirror normalisation matters on plans and barely on schemes** — so the right answer is discipline-dependent. | **39.9 %** of 8 090 motif instances (left sides, all 10 blocks) sit in shape classes with >1 orientation: ss_plan_dense 51.7 %, ar_plan 44.8 %, ar_wall_sections 31.2 % vs ss_scheme 4.8 %, eom 10.1 %, vk_plan 10.4 %. At the current per-primitive unit ar_plan is **64 %**. `artifacts/ptn_rotation_and_quant.json`, `ptn_rotation_prim_unit.json` | high | recompute with a different motif unit |
| P9 | **Exact hashing is brittle at the quantisation boundary**: a 0.24 pt (2.8 %) CAD export rounding splits one symbol into three motifs. | eom right: 14 physically identical ⊠, measured sizes **8.64×8.64 / 8.64×8.40 / 8.40×8.64** → S1 groups of **8 + 5 + 1**; left (all 8.52×8.52) → one group of 14. | high | show the three sizes are visually different symbols |
| P10 | The tolerance parameter **cannot be tuned safely** — its effect is non-monotone. | ar_plan largest S1 group across q = 0.02/0.035/0.05/0.08/0.12/0.2 → **34 / 53 / 56 / 56 / 56 / 42**; repeated groups → 122/125/116/131/141/137. The ⊠ needs q ≥ 0.08 to fuse (`ptn_q_sweep_crossed_square.json`). | high | find a q that is monotone across all 10 blocks |
| P11 | **Hatch vs symbol IS separable** by geometry+topology alone. | Rule `closure ≥ 0.9 AND straightness > 0.10` on 56 eye-labelled groups (14 SYMBOL / 19 HATCH / 12 RECT / 11 OTHER): recall **0.93**, precision **0.87**, **2 FP of 19** hatch groups. `artifacts/ptn_hatch_vs_symbol.json`, labels in `ptn_visual_labels.json` | medium (labels are one person's eye) | relabel the contact sheets and re-run |
| P12 | **Symbol vs plain rectangle is NOT separable** by anything in the S1–S5 family — and that is the actual O5 pathology. | Same 56 groups, negatives = HATCH+RECT: best precision **0.83 at recall 0.36** (text-inside); the closure rule collapses to precision **0.48 (14 FP)**; `nseg≥6` gives 0.80/0.86 but misses the 4-segment camera symbol. | medium | produce a geometry-only rule above 0.9/0.9 |
| P13 | **On a /Rotate 90 \| 270 page Track A clips the block in DISPLAY space while `get_drawings()`/`get_text()` return UNROTATED space** — the extracted vectors are a different rectangle from the diagnostic PNG. | eom left: display rect `[95.4,21.1,584.1,791.5]`, derotated rect `[50.5,95.4,821.0,584.1]`. Rendering the unrotated rect (`artifacts/ptn/frame_check_blockrect_unrotated.png`) shows *two* Шина PE strips and «Стояк 1» + «Стояк 2» — content absent from `diagnostics/eom_singleline_changed/left.png`. Overlaying the 14 ⊠ motifs on the PNG puts them in blank space (`overlay_eom_singleline_changed_left_S11.png`). | high | show `page.get_drawings()` returns display-space coordinates on a rotated page |
| P14 | **7 of 20 block sides are on rotated pages, and the ONLY pair with a real structural change is the ONLY pair whose two sides have different page rotations.** | vk_plan L+R, vk_nodes L+R, vk_node_plan L+R = /Rotate 90; eom left = /Rotate 270, eom right = 0. | high | re-read `page.rotation` for the 20 sides |
| P15 | After fixing the frame, the label series alone already states the change; before the fix it stated it wrongly. | Corrected eom left device labels = `Wh1, Whn, QF1, QFn, QD1, QDn` (**2 each**); right = `Wh1…Wh4` etc. (**4 each**). Track A's uncorrected left description contains only `Whn/QFn/QDn` (**1 each**) → would say «1 → 4». `ptn_derotate_fix.py` output | high | re-run the derotation fix |
| P16 | **A per-motif count diff CAN produce «Количество аппаратов 2 → 4» — but only with tolerant (non-hash) matching, and it says it six times at 26 % precision.** | Corrected eom, S6 two-pass: **25 clusters**, of which **6 state 2 → 4** (the QD ellipse s=27/26.3 pt, the Wh box s=7/39.3 pt, the QF cross s=5/10.6 pt and three fragments), **2 correctly state no change** (⊠ 14→14), and **17 are false** (encoding noise). Precision of change lines **6/23 = 26 %**. `artifacts/ptn_derot_eom_diff.json`, `artifacts/ptn/sheet_eom_derot_S6.png` | high | show the 17 correspond to real graphic changes |
| P17 | Exact-hash signatures never produce that sentence: on the same corrected pair the only count line they emit is a **false** one. | Corrected eom: S1 shared groups **1**, changed = **[(14, 8)]** for a marker that is 14/14 in both PDFs. S5c: `[(14,8),(2,4),(2,4),(1,2),(2,3)]`. | high | find a q/unit where S1 emits 2→4 without the false 14→8 |
| P18 | **False-positive rate on graphics-unchanged pairs** (8 controls, capped blocks re-extracted uncapped). | current `repeated_elements` fires on **6 of 8**; motif-level exact hash on **2 of 8**; tolerant two-pass on **5 of 8**. `artifacts/ptn_final_table.json` | high | recount from `ptn_pair_diff.json` + `ptn_recut_diff.json` |
| P19 | Greedy tolerant clustering is **order-dependent**; only a deterministic two-pass makes it usable. | ar_plan (byte-identical PDF): greedy leader clustering **50 changed clusters**, two-pass **0**. ss_plan_dense 85 → 2. `ptn_tolerant_match.json` vs `ptn_tolerant_match_twopass.json` | high | re-run with `PTN_TWOPASS=1` |
| P21 | The **literal region test**: a pure 45° wall-hatch band contributes **0** motifs and **0** groups under every signature, because its strokes are isolated single segments; the same band is **1** motif at the current per-primitive unit (the whole band is one PDF path), so it is invisible to `repeated_elements` too. | ar_plan hatch rect `[1046.8, 337.8, 1135.6, 545.1]` pt (`ptn/region_hatch.png`): 59 segments → cc_split 0 motifs, prim 1 motif. Symbol rect `[1180.0, 93.5, 1328.1, 1093.1]` pt (`ptn/region_symbol.png`): 1437 segments → 51 motifs, S1 largest group **6** = the 6 axis bubbles З.Л/З.К/З.И/З.Ж/З.Е/3.5. `artifacts/ptn_region_test.json`, `ptn_region_test_units.json` | high | pick a hatch band whose strokes touch each other |
| P22 | But hatch **does** dominate the repeated-pattern list where it happens to be drawn as touching segments: the single largest 2-segment motif group in ar_plan (41 instances) is one hatch patch inside the «Сечение 7-7» detail. | `ptn/overlay_ar_plan_left_S11.png` (full-page overlay): all 41 red boxes fall inside a ~20×20 pt patch at the top of the section detail. | high | re-render the overlay |
| P20 | In two VK blocks the "repeated patterns" are **not engineering symbols at all**. | vk_plan top 12 motif groups are open polyline fragments 11–20 pt (`sheet_vk_plan_left_S1.png`); the block's text spans are mojibake (`'\x0e1'`, `"- #$%&'"`), so no anchor can be formed. vk_node_plan the same. | high | decode the embedded fonts |

## What the eye counted (ground truth used above)

| block | dominant repeated symbol | eye count L / R | source |
|---|---|---|---|
| `ss_scheme_text_changed` | CCTV camera (triangle + body) | 15 / 15 | `diagnostics/ss_scheme_text_changed/{left,right}.png` — labels ВК2.1.1.1–3, ВК4.1.1.8–13, ВК5.1.1.10–13, ВК6.1.1.9–10 |
| `ss_scheme_text_changed` | ОСПД switch (crossed square) | 5 / 5 | ОСПД5.2, 2.1→1.1, 4.1, 5.1, 6.1 |
| `eom_singleline_changed` | ⊠ phase-tap marker | 14 / 14 | `ptn/eye_eom_{left,right}_col.png` (1 + 3 + 3 + 3 + 3 + 1) |
| `eom_singleline_changed` | terminal-strip circle | 20 / 20 | `ptn/eye_eom_{left,right}_shina{N,PE}.png` (10 + 10 each side) |
| `eom_singleline_changed` | apparatus rows QD / Wh / QF | **2 / 4** | the whole-block PNGs: left QD1 + QDn with an ellipsis, right QD1…QD4 |
| `ar_plan` | marking circle with a number | 63 circle-like motifs of 13–20 pt, split 56 + 6 + 1 by every S1–S5c signature | `ptn/overlay_ar_plan_left_S10.png` shows marked and unmarked instances of the same symbol side by side |

## Signature-by-signature verdict

| signature | what it adds | corpus repeated groups (left sides) | verdict |
|---|---|---|---|
| S0 current (per PDF path, block-normalised) | — | 238 | unusable: counts paths, not symbols; 6/8 false-positive rate; 0 shared groups on the changed pair |
| S1 geometry only (per motif, raw coords) | — | 663 | the right *unit*, the wrong *matcher*: exact, so a 2.8 % size jitter shatters a symbol |
| S2 + degree profile | local topology | 656 | no gain; splits real groups (24 → 12 + 11) |
| S3 + nearest text | label at a stable offset | 609 | excellent as an **attribute** (offset std ≤ 0.11 diag), harmful as part of the **identity** (unlabelled symbols collapse into one "no text" class) |
| S4 + neighbouring relations | what it touches / sits inside | 719 | harmful as identity (56 → 30/18/7); belongs in the relation graph, not the fingerprint |
| S5 / S5c rotation + mirror | D4 / continuous | 639 / 678 | **necessary on plans** (39.9 % of instances affected corpus-wide; ss_plan_dense largest group 140 → 264), near-useless on schemes |
| S6 tolerant prototype (raster + Jaccard, two-pass) | tolerance instead of hashing | 2–235 clusters per pair (both sides clustered together) | the only one that produced a correct engineering count statement (2 → 4) — at 26 % precision, six duplicates per fact |

## Reproduction

```bash
cd /home/coder/projects/PDF-proverka
# S0..S5c on the 20 Track A descriptions (~21 s)
python -m experiments.stage_comparison_vector_architecture_opus.probes.ptn_run_signatures
# per-motif count diff across the 10 pairs
python -m experiments.stage_comparison_vector_architecture_opus.probes.ptn_pair_diff
# rotation prevalence + quantisation sweep (~7 s)
python -m experiments.stage_comparison_vector_architecture_opus.probes.ptn_rotation_and_quant
# tolerant matcher, greedy vs deterministic two-pass
PTN_GRID=16 PTN_JACCARD=0.5 python -m experiments.stage_comparison_vector_architecture_opus.probes.ptn_tolerant_match
PTN_TWOPASS=1 PTN_GRID=16 PTN_JACCARD=0.5 python -m experiments.stage_comparison_vector_architecture_opus.probes.ptn_tolerant_match
# cap contamination (re-extract uncapped, ~2 min)
python -m experiments.stage_comparison_vector_architecture_opus.probes.ptn_recut ar_wall_sections 60000
python -m experiments.stage_comparison_vector_architecture_opus.probes.ptn_recut vk_node_plan 60000
python -m experiments.stage_comparison_vector_architecture_opus.probes.ptn_recut vk_nodes 200000
PTN_GRID=16 PTN_JACCARD=0.5 python -m experiments.stage_comparison_vector_architecture_opus.probes.ptn_recut_diff ar_wall_sections vk_node_plan vk_nodes
# rotated-page frame bug + corrected extraction
python -m experiments.stage_comparison_vector_architecture_opus.probes.ptn_derotate_fix eom_singleline_changed left
python -m experiments.stage_comparison_vector_architecture_opus.probes.ptn_derotate_fix eom_singleline_changed right
# hatch vs symbol on eye labels
python -m experiments.stage_comparison_vector_architecture_opus.probes.ptn_hatch_vs_symbol
# consolidated table
python -m experiments.stage_comparison_vector_architecture_opus.probes.ptn_final_table
# visual aids
python -m experiments.stage_comparison_vector_architecture_opus.probes.ptn_contact_sheet ar_plan left S1 36
python -m experiments.stage_comparison_vector_architecture_opus.probes.ptn_group_features ar_plan left S1 36
python -m experiments.stage_comparison_vector_architecture_opus.probes.ptn_overlay ar_plan left S1 0 700 200 1300 800 2.0
python -m experiments.stage_comparison_vector_architecture_opus.probes.ptn_region_test
```

## What this means for the architecture question

A "repeated pattern" layer built on **fingerprints** answers the wrong question. A fingerprint is an
equivalence relation; it can only say *these N things are byte-equal after normalisation*. Three
things the expert's sentence needs are not equivalence relations:

1. **Tolerance.** Real CAD exports differ by 2–3 % in one edge (P9) and by whole encodings — the same
   circle is 24 sampled points in one PDF and a 5- or 7-point polygon in the other (`sheet_eom_singleline_changed_right_S1.png`).
   Identity must be *nearest-prototype under a shape metric*, not a hash. No quantisation setting rescues
   the hash (P10).
2. **Composition.** One device (QD) is six shape clusters (P16). Counting shapes gives six sentences for
   one fact. Grouping them into one countable object is exactly the missing **graphical object layer**.
3. **Semantics.** The identical 4-segment rectangle is a camera body in SS, a text mask in the other
   revision of the same SS sheet, and a legend cell in the AR sheet (P12). Geometry+topology separate
   hatch from shape (P11) but never symbol from decoration. That separation is what a **discipline profile /
   symbol dictionary** supplies.

Relations (S3 text binding, S4 connectivity) are strong *evidence* about a motif — the label offset is
almost deterministic when it exists (P6) — but poisonous inside the identity key (P7). That is the shape
of the answer: geometry → tolerant object identity → relations as attributes → profile for naming, and
counting done over **objects**, never over segments or fingerprints.

Two engineering defects sit under all of this and must be fixed before any of the layers can be judged:
the rotated-page coordinate-frame mismatch (P13/P14, affecting 7 of 20 block sides and the only
change-bearing pair) and the storage cap (P4).

## Gaps / UNVERIFIED

- The eye labels in `ptn_visual_labels.json` are one person's judgement from rendered thumbnails; P11/P12
  precision/recall inherit that.
- `vk_plan`, `vk_nodes`, `vk_node_plan` are all /Rotate 90 on **both** sides, so their left/right comparison
  is internally consistent but describes a different rectangle than the diagnostic PNG. Their motif numbers
  are therefore **UNVERIFIED against the human-validated image**; only eom left was re-extracted with the
  corrected frame.
- The tolerant matcher was tuned on one pair (grid 16, Jaccard 0.5). Its 5-of-8 false-positive rate may
  improve with per-discipline tuning; that was not measured.
- Only 10 pairs exist, 2 of them the same file (O1); "false-positive rate" here means "pairs of 8", not a
  statistically meaningful rate.
- No П↔РД pair (preliminary vs working documentation) was available in this corpus at all — every pair is
  РД↔РД revision. Whether symbol vocabularies survive a П→РД transition is **UNVERIFIED**.
