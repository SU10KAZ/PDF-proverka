# p06_dims — adversarial verification of the `dim_*` probe

Verifier: independent adversarial pass. Everything below was re-run or re-read; no number is quoted
from the probe without recomputation. Scratch dir used for re-runs:
`/tmp/claude-1001/-home-coder-projects-PDF-proverka/7be66dd6-.../scratchpad` (referred to as `$S`).

Reproduction of the probe itself (done, results identical to the published artifacts):

```bash
python - <<'EOF'          # rebuild caches
import json, sys; sys.path.insert(0,'.')
from experiments.stage_comparison_vector_architecture_opus.probes.dim_cache import build
B='experiments/stage_comparison_vector_architecture_opus/probes/dim_blocks.json'
for p in json.load(open(B))['pairs']:
    for s in ('left','right'):
        x=p[s]; build(x['pdf'], x['page'], x['bbox_norm'], f"$S/cache/{p['pair_id']}__{s}.json")
EOF
for B in ar_wall_sections vk_nodes vk_node_plan ar_roof_plan kj_slab_top; do for SIDE in left right; do
  python -m experiments.stage_comparison_vector_architecture_opus.probes.dim_detect \
   --cache $S/cache/${B}__${SIDE}.json --out $S/out/${B}__${SIDE}__raw_endpoint__D_require_extension.json \
   --variant C_scale_arbitration --terminator-model raw_endpoint --require-extension; done; done
```

Output reproduces the headline table exactly: ar_wall 64/61/58, vk_nodes 48/46/34,
vk_node_plan 39/30/29, ar_roof 239/224/87, kj 183/154/138, scales 1:50.00 / 1:20.02 / 1:100.02 / 1:74.99.
**The probe's numbers are reproducible. The claims built on top of them are not all supported.**

| claim | verdict |
|---|---|
| 1 — detector finds every dimension, zero FP | **REFUTED** as generalised; a regex baseline scores the same on the 2 cited blocks, and the same code makes ≥74 FPs on the 3rd |
| 2 — binding accuracy 38–95 % "correct" | **WEAKENED** — the numbers reproduce, but "correct" = self-consistency with a scale fitted from the same bindings; only the 95 % endpoint is hand-checked, the 38 % endpoint is UNVERIFIED in the probe's own GT file |
| 3 — value×scale is a near-perfect guard | **REFUTED** — the residual is the *selection objective*; the guard's own falsification test is met inside the probe's artifacts (15 leader callouts pass at ≤2 % on ar_roof); false-accept 3–4.5 % over 27 k impostor pairs; 2 of 4 "printed scales" do not exist on the sheet |
| 4 — terminators/dimensions/leaders must be OBJECTS | **WEAKENED** — the terminator-object leg is refuted (net harmful on 3 of 5 blocks, and the showcase case is fixed without it); the extension-foot leg reproduces; the leader test misfires on real dimensions |
| 5 — diff yields exactly the expert sentences, nothing on identical docs | **REFUTED** — the run emits 13 sentences on kj, ≥4 of them demonstrably false (rendered at 400–600 dpi); on the *other* real revision pair it emits 29 sentences and zero of the claimed kind; the null test is the O1 byte-identical pair |

---

## CLAIM 1 — "finds essentially every dimension value … zero false positives" → REFUTED

**1a. The recall denominator is the detector's own candidate set.** `dim_ground_truth.json` states
`GT_dimension_values = 61` for ar_wall_sections and `numeric_candidates_seen_by_detector = 64`;
61 = 64 − 3 balloons. Same for vk: 46 = 48 − 2. Anything that does not match
`^\d{1,6}$` / `^\d{1,4}[.,]\d{1,3}$` (`dim_detect.classify_text`) can never be missed, because it
never enters GT. Recall 1.00 is definitional, not measured.

**1b. The "zero false positives" denominator is five spans.** Re-ran the detector: the refusals are
exactly `[('2','t47'),('2','t73'),('2','t80')]` on ar_wall and `[('4.9','t3'),('4.9','t149')]` on vk.
I rendered t47 at 600 dpi (`$S/balloon_t47.png`) — it is indeed a balloon in a circle, so the probe
did look at crops. But the whole precision claim rests on 3 + 2 negatives.

**1c. A trivial baseline scores the same.** Baseline = "every bare-integer span is a dimension value":

| block | baseline recall | baseline precision | detector | gain |
|---|---|---|---|---|
| vk_nodes/left | 46/46 | 46/46 = 100 % | 46/46, 100 % | **0 spans** (the 2 refusals are decimals a bare-int regex never emits) |
| ar_wall_sections/left | 61/61 | 61/64 = 95.3 % | 61/61, 100 % | **3 spans** |

The entire measured benefit of the geometric machinery over `^\d+$` is three balloon numbers on one
block. (`python - <<… classify_text over $S/cache/*__left.json`)

**1d. The same code has ≥74 false positives on the very next block.** ar_roof_plan/left:
239 candidates, **224 bound**, while the probe's own GT file estimates `~150` span dimensions →
precision ≤ 67 %. Its own leader test flags **59 of the 224 bound values as leader objects**, and
**15 of those pass the ≤2 % scale guard** (`dim_detected_ar_roof_plan__left.json`). I rendered one
of them at 600 dpi (`$S/roof120a.png`): the `120` sits on a shelf with an oblique leader pointing at
a wall — a wall-thickness callout, not a span dimension.

**1e. "well-formed" is defined post hoc.** ar_roof_plan is a normal vector CAD sheet. Also, on
vk_nodes — one of the two blocks the claim rests on — **62 of 186 text spans (33 %) contain control
characters** (broken CAD font encoding, e.g. `'\x113\x11\x134\x12\x11*\x18(05'`). Whatever was
mangled is invisible to both the detector and the GT.

What survives: on two hand-checked blocks the detector refused exactly the 5 non-dimension numerics
it should have. That is a true statement about 107 spans, not a property of the detector.

## CLAIM 2 — "binding … ranges 38 %–95 % correct with the same unchanged code" → WEAKENED

* The four numbers **reproduce exactly** under one config (`raw_endpoint` + `--require-extension`):
  58/61 = 95.1 %, 138/154 = 89.6 %, 34/46 = 73.9 %, 87/224 = 38.8 %. The qualitative point — the same
  generic code spans a 2.5× accuracy range across drawing types — is the probe's most valuable
  finding and it holds.
* **"Correct" is not measured.** The metric is `|span·S − value|/value ≤ 2 %` where `S` is the median
  of `value/span` **over those same bindings** (`dim_detect.fit_scale`). It is a self-consistency
  rate. Independent verification exists only for ar_wall_sections (61 spans hand-classified);
  kj's GT is "candidate set minus enumerated non-dimensions" plus a glance at an overlay; the probe's
  own GT file marks ar_roof — the 38.8 % endpoint — **"UNVERIFIED as an exact GT count"**.
* On ar_roof, 15 of the 87 "corroborated" are leader callouts (1d), so the low endpoint overstates
  true binding accuracy: 72/224 = 32 % if callouts are not dimensions.
* Minor: the *published* artifacts are not one config — `dim_detected_ar_wall_sections__*.json` was
  produced with `slash_object` and no `require_extension`, all others with `raw_endpoint`+D.

## CLAIM 3 — "near-perfect guard, two orders of magnitude separation, reproduces the printed scale" → REFUTED

**3a. The residual is the selection objective.** In `variant=C_scale_arbitration` (used for every
published run) the binding is *re-picked* as `argmin |span·S − value|` and accepted iff ≤ 2 %
(`dim_detect.detect`, lines ~452-470). Then `scale_ok = rel_err ≤ 0.02` is reported as the guard, and
`binding_correct_within_1pct` in `dim_ground_truth.json` uses the same quantity as the definition of
"correct". The separation is manufactured by the selector and then used as its own referee.

**3b. The clean bimodality exists on one block only** — ar_wall_sections (worst good residual 0.0038,
next 0.1131). Recomputed cumulative histograms (`dim_residuals.json`, raw_endpoint + D):

| block | ≤1 % | ≤2 % | ≤5 % | ≤25 % | bound | middle band (1–25 %) |
|---|---|---|---|---|---|---|
| ar_wall_sections | 58 | 58 | 58 | 59 | 61 | 1 |
| kj_slab_top | 117 | 138 | 138 | 139 | 154 | **22** |
| vk_nodes | 29 | 34 | 41 | 44 | 46 | **15** |
| ar_roof_plan | 63 | 87 | 91 | 102 | 224 | **39** |

Moving the threshold 1 %→2 % relabels 21 kj bindings with no independent evidence for either label.

**3c. The probe's own falsification criterion is met by its own artifacts.** D3 says it would be
falsified by "a wrong binding whose residual is under 2 %". `dim_detected_ar_roof_plan__left.json`
contains 15 (six `120` at rel-err 1.2 %, `270` at 0.37 %, `1350` at 0.26 %), all flagged
`leader_object` by the probe's own test; I confirmed one visually.

**3d. The guard checks length, not location.** Impostor test over all (value_i, span_j), i≠j, inside
each block's corroborated set:

| block | impostor pairs | accepted at 2 % | avg. other spans that also "corroborate" a value |
|---|---|---|---|
| ar_wall_sections | 3 306 | 96 (2.9 %) | 1.7 |
| vk_nodes | 1 122 | 50 (4.5 %) | 1.5 |
| kj_slab_top | 18 906 | 607 (3.2 %) | **4.4** |
| ar_roof_plan | 7 482 | 338 (4.5 %) | **3.9** |

On kj every corroborated value would be equally "corroborated" by 4.4 other spans elsewhere on the
sheet. The guard rejects gross mis-binding; it cannot certify that a value is attached to *its own*
geometry — which is exactly what the claim asserts.

**3e. The guard fabricates change sentences.** Because it is applied independently to both versions,
an asymmetric rejection becomes a deletion. Measured on the kj/ar_roof diffs: «Размер 4300 удалён»
(right-side binding exists, rel-err 3.4 % → dropped), «Размер 840 удалён» (6.3 % → dropped),
ar_roof 3350 (9.0 %), 8060 (9.5 %), 190 ×2. 2 of 4 kj removals and 4 of 21 ar_roof removals are
guard artefacts.

**3f. Two of the four "printed scales" do not exist.** ar_wall's title text `Сечение 3-3 ( 1 : 50)`
is in the block (verified in the cache) and vk's `( 1 : … 20)` survives the broken encoding. But a
scan of **all 18 pages** of the KJ PDF and **all 10 pages** of the AR roof PDF for `1\s*:\s*\d+`
returns only `1:1898` (a metadata artefact) — neither sheet prints a scale anywhere. 1:74.99 and
1:100.02 are corroborated by nothing outside the fit itself.

## CLAIM 4 — "terminators/dimensions/leaders must be assembled as OBJECTS" → WEAKENED (leg 1 refuted)

**4a. The `980` case is real but does not support the conclusion.** Re-ran both models on
ar_wall_sections/left: `raw_endpoint` binds `980` to span 31.38 pt (rel-err 43.5 %),
`slash_object` to 55.56 pt (0.01 %). **But `raw_endpoint` + `--require-extension` also gives 55.56 pt,
rel-err 0.0.** The forged bound is removed by the orthogonal rule; terminator-object assembly is not
required for it. (The arm coordinates quoted in D6, `(2662.98,787.02)-(2666.64,790.74)`, are not a
segment in the cache; the nearest real segment is `[2664.84, 788.88, 2662.98, 787.02]` — the quote is
the merged object's extent, not raw geometry.)

**4b. "57 → 58" is a delta of one binding, and it vanishes at the config used everywhere else.**
raw+C = 57, slash+C = 58, **raw+D = 58, slash+D = 58**.

**4c. Terminator-object assembly is net harmful on the corpus** (`dim_residuals.json`, D variant,
corroborated / bound):

| block | raw_endpoint+D | slash_object+D |
|---|---|---|
| ar_wall_sections | 58/61 | 58/61 |
| vk_nodes | 34/46 | 36/46 |
| vk_node_plan | 29/30 | 25/31 |
| kj_slab_top | **138/154** | 114/134 |
| ar_roof_plan | **87/224** | 46/193 |

The probe's own published artifacts use `raw_endpoint` for 4 of 5 blocks — i.e. the object layer this
claim says is mandatory is switched **off** in the runs that produce claims 2, 3 and 5.

**4d. The extension-foot leg reproduces** (ar_roof C→D: 54→87 corroborated, bound 225→224). Cost not
stated in the claim: vk_node_plan bound 36→30 (17 % of values refused) for +1 corroborated. And
"corroborated" is again the self-consistency metric of 3a.

**4e. The leader test has unmeasured precision and demonstrable false positives.** It flags `180` on
vk_nodes/left (rel-err 0.19 %) — I rendered it at 500 dpi (`$S/vkleader_180_553.png`): a genuine
vertical dimension between two 45° tick terminators. It also flags a `1750` and a `500` on kj that
pass the guard. So "explains 44 of 137 wrong bindings" is an attribution by an unvalidated detector,
and it covers 32 % of the wrong bindings in any case.

## CLAIM 5 — "exactly the expert sentences on a real pair, nothing on identical documents" → REFUTED

**5a. The kj run emits 13 sentences, not 4**: 2 chain + 2 value_changed + **4 removed + 5 added**
(`dim_diff_kj_slab_top__absolute.json`, re-run reproduces it). Only 4 were verified by the probe.

**5b. At least 4 of the 13 are false.** Verified by rendering both versions of each location at
400–600 dpi (files in `$S/chk_*.png`, `$S/chk2_*.png`):

| sentence | reality |
|---|---|
| «Размер 4300 удалён» | the 4300 dimension is present in v002 at the same place (text span 0.2 pt away); only hatching was added nearby — **FALSE** |
| «Размер 840 удалён» | 840 with its ticks is present in both versions — **FALSE** |
| «Размер 1000 удалён» + «Добавлен размер 1000» | one dimension displaced 6.5 pt, i.e. just outside `MATCH_TOL = 6.0` → reported as delete+insert — **2 misleading sentences** |

Verified-true rate on that pair: 4 of 13 = 31 %. «Размер 1750 → 2250» *is* real — I rendered it and
see the revision cloud and tag 1.1 in v002 (`$S/chk3_chg1750_right.png`).

**5c. The win exists on one pair only.** ar_roof_plan is the probe's other genuine consecutive
revision. Its diff (`dim_diff_ar_roof_plan__absolute.json`) contains **0 value_changed, 0 chain
sentences, 21 removed + 8 added**. Of those 29, ≥6 are demonstrably false — the same number is
present within 25 pt on the other side (3350 at 0.2 pt, 8060 at 2.1 pt, 190 at 9.7/12.8 pt, 8110 at
11.4 pt, 150 at 17.9 pt).

**5d. Recall is unmeasured and structurally capped.** The diff consumes only `scale_ok` bindings
(`only_corroborated_bindings: true`): 138 of 154 on kj, **87 of 224 (39 %) on ar_roof**. Any change in
the invisible remainder cannot be reported. A trivial bag-of-numbers diff of the text layer — which
Track A's comparator already computes — recovers the same value-level story on kj
(only-v001 `{1500, 1400, 1750}`, only-v002 `{2250, 150, 1000, 300, 50×3, 400, 840, 500}`) and
includes an extra `500` in v002 that the dimension diff never reports.

**5e. The null test is the O1 self-comparison.** `ar_wall_sections` left/right are page 13 of the
byte-identical v001/v002 AR PDFs (`dim_blocks.json` + O1); 58/58 matched is a no-op. The informative
part of the null test is vk_nodes / vk_node_plan (different bytes): 35/35 and 25/25 with the guard on,
and with `--all-bindings` 46/46 + 2 added, 30/30 + 2 added. That part survives, on near-identical
revisions only.

## What still stands after this pass

1. A discipline-free detector can enumerate and bind bare-number dimension candidates on real vector
   sheets, and **binding quality varies 2.5× across drawing types with identical code** (claim 2's
   qualitative core). That is the finding worth carrying into the architecture decision.
2. `value ≈ span × fitted_scale` is a useful **confidence signal** — it recovers 1:50 and 1:20 where
   the sheet prints them — but it is a length-consistency check with a 3–4.5 % false-accept rate, not
   a correctness guard, and using it as a filter on two versions independently **creates** false
   «удалён» sentences.
3. Requiring an extension-line foot in addition to a terminator is a real, reproducible improvement.
   Assembling terminators into slash objects is not — it loses more than it gains on 3 of 5 blocks.
4. A dimension-level diff can produce «Размер 1750 → 2250» at the right place on a real revision, and
   a chain relation is needed to avoid shipping «1400 → 50». On present evidence it ships roughly two
   wrong sentences for every right one, and on half the real pairs tested it produces none of the
   sentence types it is sold on.
