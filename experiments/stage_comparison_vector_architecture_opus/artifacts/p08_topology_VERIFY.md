# p08_topology (probe prefix `tcf`) — adversarial verification

Verifier: independent Track B checker. Everything below was re-run or recomputed from scratch on
2026-08-23 from `/home/coder/projects/PDF-proverka`. No probe artifact was trusted on its face.

## 0. Foundation check — does the instrumented fork actually reproduce Track A?

```bash
python - <<'PY'
import json, pathlib
from experiments.stage_comparison_vector_architecture_opus.probes import tcf_topo
ROOT = pathlib.Path("experiments/stage_comparison_vector_blocks/artifacts/descriptions")
for pd in sorted(ROOT.iterdir()):
    for side in ("left","right"):
        d = json.loads((pd/side/"vector_block.json").read_text())
        print(pd.name, side, tcf_topo.selftest(d))
PY
```

Result: `{}` on **all 20** blocks (probe only claimed 10). The fork is faithful; every number
downstream is comparable to shipped Track A values. **No objection to the instrument itself.**

I also re-derived the comparator gates from source rather than from the probe's prose:
`comparator.py:400-401` — `NEAR_IDENTICAL` requires `geometry ≥ 0.985 AND topology ≥ 0.85 AND
(text unreliable OR text ≥ 0.92)`. Confirmed.

---

## CLAIM 1 — topology similarity is a tolerance artefact (0.43–0.77 self-similarity, 10/10 blocks)

**Status: CONFIRMED as a number, WEAKENED as a diagnosis.**

Recomputed independently from `tcf_p1_tolerance.json → raw` by re-calling
`comparator._topology_diff` myself (not reading `summary`): every `min_self_similarity`
reproduces to 4 decimals (vk_node_plan 0.4300 … ss_simple_node 0.7723). The 0.85 gate is real.

What the probe did not say, and what I measured:

| what | value |
|---|---|
| tolerance at which the minimum occurs | **0.01 on 9 of 10 blocks** — the extreme end of a 20× sweep, 4× the shipped 0.0025 |
| self-sim at one 2× step (0.0025 → 0.005) | 0.6206–0.9778; **8/10** below the gate |
| self-sim at one 2.5× step (0.0025 → 0.001) | 0.6409–0.9575; **7/10** below the gate |
| physical size of tol 0.01 | ar_plan **14.3 pt**, ss_plan_dense **33.4 pt** of merge radius |
| tolerance actually used in production | hard constant **0.0025 on all 20 descriptions** (checked every `topology.tolerance_norm`) |

So the headline "10 of 10" needs the 4× extreme; at a step a practitioner might really argue about
it is 7–8 of 10. That is still damning, so the claim survives.

The **diagnosis** does not survive intact. `tolerance_norm` is identical (0.0025) on both sides of
every pair, so the comparator never compares two graphs built at different tolerances. This
measures *hyperparameter sensitivity of the metric*, i.e. that its units are arbitrary — not that
the comparator's shipped verdicts are being driven by tolerance. Calling it "the comparator's
topology similarity is mostly a tolerance artefact" over-reads a sensitivity analysis as an error
source. Also "10 of 10 benchmark blocks" is 10 of the **20** blocks — only left sides were swept.
Not contaminated by O1 (this is a within-block measurement).

## CLAIM 2 — nine counts are not properties of the drawing; 282×/259×/10×/7.7×, non-monotonic

**Status: WEAKENED.**

Recomputed all per-key max/min ratios over the 5 tolerances, left sides, from `raw`:

| key | worst block ratio | median ratio | blocks with ratio > 1.5 |
|---|---|---|---|
| connected_components | **282.75** (ar_wall_sections 3393→12) | 21.12 | 10/10 |
| endpoints | **259.00** (ar_wall_sections 8288→**32**) | 27.47 | 10/10 |
| node_count | 10.06 (vk_node_plan) | 5.04 | 9/10 |
| branch_points | 7.74 (vk_node_plan) | 3.64 | 9/10 |
| t_junctions | 6.88 | 2.03 | 8/10 |
| x_crossings_unconnected | 9.25 | **1.04** | **3/10** |
| closed_contours | **1.00** | 1.00 | **0/10** |
| nested_contours | **1.00** | 1.00 | **0/10** |

Three problems with the claim as written:

1. **The headline says "the nine counts"; three of the nine do not move.** `closed_contours` and
   `nested_contours` are *exactly* tolerance-invariant (ratio 1.000 on all 10 blocks), and
   `x_crossings_unconnected` has median ratio 1.04 with only 3/10 blocks above 1.5. Six of nine
   move hard; the sentence generalises over nine. (That two of them are stable for a *different*
   bad reason — PDF packaging, O5/T14 — is a separate argument this evidence does not make.)
2. **`endpoints 259× (8288→135)` is internally inconsistent.** 8288→135 is 61×; the 259× figure is
   8288→**32** (tolerance 0.01). The probe's own tier table repeats the same conflation.
3. **Non-monotonicity is generalised from one block on the cited key.** I tested monotonicity per
   key per block: `connected_components` is non-monotonic on **1/10** blocks (only
   ss_scheme_text_changed), and its full series is 432, 429, 407, 71, 138 — monotone for the first
   four points; the claim quotes the last three. `t_junctions` (7/10 non-monotonic) would have been
   the honest example; the probe picked the weak one.

The core — six of nine counts move by 1.5×–283× under settings a human would call the same
extraction — is confirmed. The stated scope is not.

## CLAIM 3 — signal far below noise: +20 branches +20 circles moves ar_plan by 0.0136, 34.7× less than noise, still NEAR_IDENTICAL

**Status: numbers CONFIRMED end-to-end, "signal below noise" REFUTED as stated.**

I did not read the probe's JSON for this. I rebuilt the injected description and ran the **real
`comparator.compare_descriptions`** (the probe only called two private sub-functions):

```
ar_plan   k=20 → status NEAR_IDENTICAL, geometry 0.997333 (tol 0.001), topology 0.986403
ss_plan_dense k=20 → status NEAR_IDENTICAL, geometry 0.995708, topology 0.982681
ss_simple_node k=20 → status STRUCTURE_CHANGED, geometry 0.566667, topology 0.279225
```

Matches the probe to 6 decimals. Both gates pass, status is NEAR_IDENTICAL. Confirmed.

Two corrections:

**(a) The comparator is not silent.** The full call emits
`differences = ["Число примитивов: 14800 → 14840", "Топология изменилась (similarity=0.986,
ветвления 4879 → 4889)"]` (the `< 0.99` topology branch at `comparator.py:431` fires). The status
is uninformative and the difference lines are unreadable for an expert — that is the real finding —
but "it would be reported NEAR_IDENTICAL" implies invisibility, and two diff lines do fire.

**(b) The noise it is compared against is the wrong noise.** 0.4717 is a tolerance sweep the
production path never performs (tolerance is a hard constant on both sides, §CLAIM 1). The noise the
comparator actually faces is the between-version spread on pairs a human labelled no-change:

| pair (human = NEAR_IDENTICAL/IDENTICAL) | measured topology noise `1-sim` |
|---|---|
| ar_plan (O1: byte-identical PDFs) | **0.0013** |
| ss_plan_dense (genuinely two different PDFs) | **0.0039** |
| vk_plan | 0.0296 |
| ar_wall_sections (O1) | 0.0331 |
| vk_node_plan | 0.1069 |
| ss_table_graphic | 0.1389 |

On the very blocks tested the injection signal is **above** the same-block noise, not below it:
ar_plan 0.0136 vs 0.0013 = **10.4× above**; ss_plan_dense 0.0173 vs 0.0039 = **4.4× above**. What
actually fails is the *threshold*: the 0.85 gate leaves 0.15 of headroom that a 0.0136 drop cannot
cross, and the between-block noise spread (0.0013…0.1389, a factor of 107) makes any single global
threshold impossible. That is a calibration/normalisation failure, not an SNR failure, and the
claim's own framing ("34.7× less than the tolerance artefact") is an apples-to-oranges ratio.

**(c) "540 segments" is inflated by ~9×.** I checked how many injected segments survive the caps:
**60 of 540** (only the 3-segment branches; all 480 device-circle segments are shorter than the
longest-first cut-off and are dropped by the 8000 topology cap *and* by the 12000 segment-coverage
cap — visible as `right_coverage = 0.995` exactly = 1 − 60/12000). So the metric was never shown
540 segments. This makes the underlying complaint worse, not better, but the claim's headline
number describes an input that never reached either gate.

Also note each injected "branch" is 3 **collinear** segments (constant angle) — a straight stub, not
a branching polyline; "20 real branches" is the probe's own synthesis, not a real revision.

## CLAIM 4 — adding geometry deletes geometry: segments_used stays 8000, branch_points 4636→4632 downwards

**Status: WEAKENED (the cited direction is a single-seed coin flip).**

Reproduced exactly with the probe's seed: ss_plan_dense k=20 → `segments_used [8000, 8000]`,
`branch_points [4636, 4632]`. My independent full-comparator run gives the same 4636 → 4632.

But I re-ran the injection with four other seeds (same k=20, same block):

| seed | Δ branch_points | Δ node_count | Δ endpoints | injected segs surviving cap |
|---|---|---|---|---|
| 20260843 (probe's) | **−4** | +41 | +26 | 60/540 |
| 1 | **+11** | +34 | +16 | 60/540 |
| 2 | **+14** | +33 | +20 | 60/540 |
| 3 | **+6** | +32 | +19 | 60/540 |
| 4 | **+14** | +28 | +10 | 60/540 |

Four of five seeds move `branch_points` **up**. The headline "moves downwards" is the one draw the
probe happened to make. The robust statement is the magnitude, not the sign: 20 injected branches
move `branch_points` by −4…+14, i.e. by an amount **unrelated to the 20 attachments** — which is the
point that actually matters and which the probe could have made without the sign.

A genuinely robust instance of the effect does exist and the probe did not use it: on **ar_plan**,
`endpoints` moves down on all five seeds (−7, −10, −10, −11, −15), and the stored file already shows
`node_count 6003 → 6002` at k=1 and `→ 6001` at k=2.

`segments_used` "at exactly 8000" is definitionally true (84 439 raw segments against a hard cap of
8000) and carries no information; it is the cap holding, not a measurement.

## CLAIM 5 — x_crossings_unconnected: 90.0–99.8 % same component, 34.9–62.2 % T-glued, 4/15 visually meaningful

**Status: WEAKENED. Two of the three sub-claims have silently trimmed ranges; the third is
self-assigned ground truth on N=15.**

I recomputed all crossing statistics from scratch with `tcf_topo.topology(..., keep_crossings=True)`
on all 10 left blocks. Per-block numbers match the stored JSON exactly. **The stated ranges do not
match the data:**

| statistic | claim's range | range I measured over **all 10** blocks | blocks the claim's range excludes |
|---|---|---|---|
| share_same_component | 90.0–99.8 % | **54.55 – 99.90 %** | ss_scheme_text_changed **0.5455**, ss_simple_node **0.6667** |
| share_joined_by_t_junction | 34.9–62.2 % | **0.00 – 62.16 %** | ss_simple_node **0.0000** |

The excluded blocks are exactly the two sparse ones — a simple engineering node and a scheme — i.e.
the cases where an expert would actually care whether two lines cross. There, a third to a half of
the recorded crossings *are* between different components, and on ss_simple_node the T-junction pass
glued none of them. Reporting the range as "90.0–99.8 %" makes an 8-block statement look like a
10-block one.

Two things that do hold up:

* "**at that exact place**" is fair: of the crossings flagged `joined_by_t_junction`, the crossing
  point itself lies within the node tolerance of an endpoint in 87–100 % of cases per block
  (ar_plan 2776/3108, eom 199/201, ss_plan_dense 1824/1910, vk_plan 303/360). The internal
  contradiction — the extractor labelling as "without a confirmed junction" a pair it glued one pass
  earlier — is real and is the strongest part of this claim.
* The dense-block numbers (0.90–0.999 same component) reproduce.

Two things that do not:

* **The semantic premise is aimed at a meaning the extractor never asserts.** The extractor's own
  human-facing rendering (`extractor.py:1082`) reads «X-пересечений без подтверждённого junction» —
  *crossings without a confirmed junction* — not "crossings in different components". Two segments
  can be in one component through a path elsewhere on the sheet and still cross without connecting.
  "Does not measure unconnected crossings" attacks the abbreviated field name, not the definition.
* **The visual sub-claim is the probe grading its own homework.** `tcf_p3_visual_verdicts.json`
  records `"judge": "Claude Opus 5 (Track B probe agent)"` — the same agent, no domain expert, and
  the probe's own FINDINGS rate it `medium`; the claim as handed to me is rated `high`. N = 15,
  1–2 crossings per block, drawn round-robin, from a population of 15 605 crossings. 4/15 = 27 %
  with a 95 % interval of roughly 8–55 %.

I opened `tcf_x_montage_wide.png` and labelled the 15 crops myself. I agree with 12 of 15. I would
move **#5** (dash-dot axis crossing a wall line) and **#7** (leader line over table rules) *out* of
TRUE_UNCONNECTED_INFORMATIVE — the probe's own BACKGROUND_NOISE definition names "axis grid"
explicitly — and **#9** (leader for «ø110» attaching to the pipe it labels) *into* the meaningful
group, since a label-to-object attachment is precisely the relation an object layer wants. Net
effect is a wash (my count: 3/15). So the *number* is reproducible by a second reader; the
*inference* rests on 15 samples.

**One sample is contaminated by a different bug.** The rhetorically strongest example — «a hand
signature in the title block» (#10, vk_plan) — exists in the pool only because vk_plan's extraction
window has IoU **0.0514** with the region it is supposed to describe and falls partly outside the
unrotated page (the probe's own T16). Same for #9 (vk_nodes, IoU 0.1235) and #8 (vk_node_plan, IoU
0.0). Three of fifteen samples come from regions the extractor should never have described; two of
them are counted as BACKGROUND_NOISE. Those crossings indict the rotated-frame bug, not the
crossing metric.

*(Side observation, outside my remit: crop #8 from vk_node_plan clearly shows a 3-D pipe fitting,
not the «equipment specification table» that T16 says that block actually describes. The crossing
renderer maps normalized → `block_rect_used` → `rotation_matrix`, which is self-consistent with the
extraction, so the crops are correctly located; but that picture and T16's description of the same
block do not agree, and someone verifying T16 should look at it.)*

---

## Reproduction

```bash
cd /home/coder/projects/PDF-proverka
# fork fidelity, all 20 blocks
python -c "import json,pathlib;from experiments.stage_comparison_vector_architecture_opus.probes import tcf_topo;R=pathlib.Path('experiments/stage_comparison_vector_blocks/artifacts/descriptions');[print(p.name,s,tcf_topo.selftest(json.loads((p/s/'vector_block.json').read_text()))) for p in sorted(R.iterdir()) for s in ('left','right')]"
# claim 1/2: recompute self-similarity + per-key ratios from tcf_p1_tolerance.json raw
# claim 3: rebuild injected description, run comparator.compare_descriptions
# claim 4: rerun probes.tcf_p8_injection.inject with seeds 1..4
# claim 5: tcf_topo.topology(..., keep_crossings=True) on the 10 left blocks
```
(Full snippets are in the verifier transcript; each is <25 lines and uses only Track A + `tcf_topo`.)

## Bottom line

The instrument is sound and every headline number reproduces. What does not survive is the framing:
claim 1 measures hyperparameter sensitivity of a metric whose hyperparameter is a fixed constant on
both sides; claim 2 says "nine counts" where three are exactly stable and quotes a 259× ratio next
to a 61× pair of numbers; claim 3's "signal below noise" inverts on the actual production noise
floor (signal is 4–10× *above* same-block noise; the failure is threshold headroom and a 107×
between-block noise spread); claim 4's direction is 1 of 5 seeds; claim 5's two headline percentage
ranges quietly drop the 2 sparse blocks that contradict them, and its visual tally is the probing
agent labelling its own crops, 15 of them, 3 from the wrong region of the sheet.

The load-bearing conclusion the probe wanted — *a single global ε over raw segments cannot support
«Добавлены два ответвления»* — is still standing after all of this. It is supported by: 6 of 9
counts moving 1.5–283× under a 2× tolerance step, a 107× between-block spread in the no-change noise
floor against a single global 0.85 gate, 480 of 540 injected segments never reaching either gate
because of the caps, and counts that move by −4…+14 when 20 branches are added. It is *not*
supported by "signal is below noise", which is the one sentence in this probe I would strike.
