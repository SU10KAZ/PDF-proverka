# p01_signal — adversarial verification

Verifier notes. Everything below was re-run or re-read by me; scripts are in
`experiments/stage_comparison_vector_architecture_opus/probes/verify/` (v1…v9).
Nothing outside this experiment directory was written.

| # | claim (short) | verdict |
|---|---|---|
| 1 | 63.8 % of the contract is never read by the comparator | **CONFIRMED** |
| 2 | style (27.0 %) + raw (22.8 %) are write-only; style ~247x redundant | **WEAKENED** |
| 3 | five top-level fields inert in the comparator, 1,304,264 B (1.09 %) | **CONFIRMED** (tautological evidence) |
| 4 | anchors uninformative in principle (20.9/61.9/173.5/185.8; 94.3/76.5/68.3/51.0 %) | **WEAKENED** |
| 5 | signature levels carry zero information; coarse alternatives beat them | **REFUTED** (in its load-bearing halves) |

---

## C1 — CONFIRMED, and stronger than the probe showed

`probes/verify/v1_minimal_recheck.py` — I re-derived the minimal key set myself from reading
`comparator.py`, and compared the **entire** comparison dict (`json.dumps(sort_keys=True)`), not the
7 keys probe 8 checks (it never compared `normalized_signature_equal`, `structural_signature_equal`,
`geometry.tolerance_experiment`, `primitive_matching_experiment`, `topology.counts`).

```
10/10 pairs FULL_DICT_IDENTICAL=True
corpus compact: 119,671,126 -> 43,299,345   reduction 63.818 %
per-pair reduction: min 60.9  median 64.4  max 80.2  (unweighted mean 66.2)
```

`probes/verify/v2_key_trace.py` — instrumented the two input descriptions with dicts that log every
`__getitem__`/`get`, ran all four reachable status branches (IDENTICAL / SSVC / STRUCTURE_CHANGED /
NEAR_IDENTICAL). The observed read set is **exactly** the probe's minimal set: 41 key paths, none
outside it, none of the kept keys unused. The fifth branch (`INSUFFICIENT_VECTOR_DATA`) reads only
`vector_quality`, which is kept. So the 0/10 result is not a benchmark result at all — it is a code
fact and cannot be contaminated by O1/O2.

Caveats that do not change the verdict but change what it means:

* 63.8 % is a **byte** share on a corpus where 5 of 20 blocks sit exactly on
  `DEFAULT_STORAGE_CAP = 20_000` primitives (75 % of all primitives, 75 % of style bytes). By leaf
  key path the unread share is far higher (~95 %, `v8_share_framing.py`), so the byte framing is
  conservative, not inflated.
* "Unread by the comparator" is not "deletable". `extractor._canonical_primitive` hashes
  `primitive.raw` and `primitive.style` into `level_1`/`level_2`, and `_signatures` hashes
  `topology.components` + `degree_histogram` into `level_3` — all three hashes **are** read by the
  comparator. The 63.8 % is the raw material the read 36.2 % is computed from.

## C2 — WEAKENED: the byte numbers are exact, "write-only" is false

Reproduced to the byte (`v3_style_raw.py`): style 32,341,142 B = 27.025 %, raw 27,299,148 B =
22.812 %, sum 49.837 %; style values 30,899,153 B vs palette 125,208 B = 246.8x; 160,221 primitives
/ 660 distinct styles = 242.8x. The ablation rows also reproduce (`v9_ablation_spot.py`).

What does not survive:

1. **Both keys are read — by the extractor, into hashes the comparator reads.**
   `extractor._canonical_primitive` (lines 791–807) reads `primitive["style"]["stroke_width"]`,
   `stroke`, `fill`, `dashes` **and** `primitive["raw"]`, and feeds them into
   `structural_signature.level_1_exact_vector` and `level_2_normalized_geometry`. `_summary` reads
   style for `stroke_paths`/`filled_paths`; `_size_metrics` reads raw and style.
2. **Style is not information-free on this corpus.** `v4_style_is_read.py`: dropping style from the
   canonical token collapses 129,798 distinct normalized tokens to 120,632 — style separates 9,166
   tokens, on 12 of 20 blocks (vk_node_plan 2,215; vk_nodes 2,031; ar_plan 186 …).
3. **Blanking style can change a verdict.** Same script, test 2: on `ss_simple_node` a style-only
   change (solid → dashed, `stroke_width` 0.75 — a real engineering distinction) gives
   `NEAR_IDENTICAL` while style is in the signature and `IDENTICAL` once style is dropped from it.
   So "blanking either changes nothing anywhere" is false as stated.
4. The ablation cannot see any of this **by construction**: it blanks the stored copy *after* the
   extractor already hashed it. It measures "the comparator does not re-read the stored copy", which
   is a much weaker statement than "write-only".
5. "~247x redundant" is palette-only. A palette scheme still needs one index per primitive:
   30,899,153 / (125,208 + 429,773) = **55.7x** (`v8_share_framing.py`). The probe's own .md says
   "before indices"; the claim as handed to me drops that qualifier.
6. "Half the entire contract" is byte-weighted: per block, style is 0.70–32.35 % and raw
   8.50–45.99 %; style+raw is ~50 % on the big blocks and 10 % on `ss_simple_node`.

One attack that **failed** and should be recorded as supporting the probe: I suspected the palette
factor was an artefact of the longest-first cap (O11) sampling style-homogeneous frame linework.
Distinct styles per length quartile of the retained primitives are flat or falling
(vk_nodes 63/62/61/70, ar_wall_sections 15/7/3/3), so truncation does not manufacture the redundancy.

## C3 — CONFIRMED as stated; the evidence is a tautology

`v3_style_raw.py`: anchors 687,850 + labels 265,784 + size_metrics 186,368 + dimensions 94,117 +
hatch_like_structures 70,145 = **1,304,264 B = 1.090 %** — exact. `v9_ablation_spot.py` reproduces
the "no change" rows.

But the key trace (v2) shows none of the five is referenced anywhere in `compare_descriptions`, so
"0/10 status, 0/10 differences, 0/10 scores" was guaranteed before the ablation ran; it carries no
information beyond `grep`. Two framing caveats:

* `size_metrics.compact_payload` (96.46 % of that field) is the **only** payload Track A ever sent
  to a model (`run_ai_experiment.py:67-68`), and it embeds `hatch_like_structures` as
  `hatch_candidates`. Two of the five "inert" fields are the model-facing contract.
* `dimensions[].geometry_id` is copied from `anchors`, so the two are inert together by construction.

## C4 — WEAKENED: two of the four quoted numbers are artefacts of a 300-span cap

`probes/signoise_07_anchor_information.py` sets `TEXT_SAMPLE = 300` and takes
`description["texts"][:300]` — the **first** 300 spans in PDF order, not a random sample — for
exactly the three blocks the claim quotes with >300 spans (ar_plan 836, ss_plan_dense 522,
vk_nodes 421). The .md's "texts" column prints the full count, not the sampled one.

`probes/verify/v5_anchor_recheck.py`, all spans, no cap:

| block | claim: mean within 0.035 | all spans | claim: % ambiguous | all spans | mean within **0.012** |
|---|---:|---:|---:|---:|---:|
| ss_plan_dense | 20.9 | **27.45** | 94.3 % | **73.0 %** | 6.46 |
| vk_node_plan | 61.9 | 61.87 | 76.5 % | 76.5 % | 4.91 |
| ar_plan | 173.5 | **196.32** | 68.3 % | **73.0 %** | 29.01 |
| vk_nodes | 185.8 | **138.99** | 51.0 % | **56.8 %** | 7.03 |

* `ss_plan_dense` 94.3 % → 73.0 % (−21 pp) and `vk_nodes` 185.8 → 139.0 (−25 %) do not survive.
  The first-300 subsample distorted in both directions.
* The claim attacks the `high` label, which `extractor._anchors` assigns at **0.012**, using counts
  measured at the 0.035 search radius. Restricted to `high` spans and the 0.012 radius the numbers
  are 6.46 / 5.02 / 29.08 / 7.13 — 3–20x smaller than the ones quoted.
* A tie is only harmful if the tied primitives are different things. Max pairwise centroid distance
  inside each tie set: on vk_node_plan and vk_nodes **94.6 % / 92.5 %** of tie sets fit inside 0.02
  (median spread 0.012) — i.e. the tied candidates are neighbours, plausibly parts of one symbol,
  and choosing between them may be semantically free. On ar_plan / ss_plan_dense only 49 % / 44 %
  do, which is where the claim is strongest.
* "Uninformative **in principle**" is untested: no ground truth, no crop was ever opened, and
  correctness of the chosen anchor was never scored. The probe's own falsification note concedes
  this. A tie count is a proxy for ambiguity, not a measurement of wrongness.

Direction survives (dense CAD sheets give many near-equidistant candidates, and `high` is assigned
where the neighbourhood is densest — orchestrator O3). The quoted magnitudes and the "in principle"
do not.

## C5 — REFUTED in its load-bearing halves

**(a) "levels carry zero information about each other; only two triples exist" — REFUTED.**
The 190/190 agreement reproduces, but 189/190 pairings are `(F,F,F)` and 1 is `(T,T,T)`: the target
has no variance, and a constant "both False" predictor scores the identical 190/190
(`v7_signature_triple.py`). The design is a nesting (l1 = raw+style, l2 = normalized+style,
l3 = counters only), so intermediate triples are predicted, not excluded. I built one with the
probe's own 0.5 % shift perturbation — the same drawing moved on the sheet:

```
ss_table_graphic        (l1,l2,l3) equal = (False, False, True)   l3_payload identical = True
ss_simple_node          (l1,l2,l3) equal = (False, False, True)   l3_payload identical = True
ss_scheme_text_changed  (l1,l2,l3) equal = (False, False, True)   l3_payload identical = True
```

3 of 3. "Only two observed triples" is a property of a 20-block corpus with no
near-duplicate-but-not-identical blocks, not of the signature.

**(b) "exact key recovers 1 of 10 (recall 0.10)" — CONFIRMED.** Denominator stated (10 pairs),
verified from `signoise_03_block_features.json`: the single all-equal pairing is `ss_simple_node`.
(Fair to note the levels were designed as identity hashes, not retrieval keys.)

**(c) "my 6-number bucket hash reaches 0.70 with 0 cross-pair collisions" — number true, win false.**
`v6_signature_baselines.py`, same quantisation, same corpus:

```
seg+txt+cmp+ang+lab+asp (the probe's design)  recall 0.70  cross-collisions 0
seg+txt+cmp  (three log-counts, trivial)      recall 0.80  cross-collisions 0   <-- better
seg+txt                                       recall 0.80  cross-collisions 8
```

The three extra hand-picked features (dominant angle bin, label share, aspect ratio) **cost** 0.10
recall. And the quantisation constant is not a tuned optimum, it is a coin flip at n=10:
q = 0.5/1.0/1.5/2.0/3.0/4.0/6.0/8.0 → recall 0.70/**0.80**/0.50/0.70/0.70/0.50/0.60/0.60.
2 of the 7 pairs it recovers (`ar_plan`, `ar_wall_sections`) are the byte-identical same-file pairs
of orchestrator O1.

**(d) "34-dim descriptor rank-1 for 19/20 (20/20 without the grid)" — WEAKENED to near-zero.**

```
34-dim full                      top1 19/20   real-change queries 3/4   same-file queries 4/4
no_grid (the claim's best)       top1 20/20   real-change queries 4/4   same-file queries 4/4
counts_only (3 dims)             top1 18/20   real-change queries 2/4   same-file queries 4/4
log_segments ONLY (1 dim)        top1 17/20   real-change queries 1/4   same-file queries 4/4
```

A single number gets 17/20. Every descriptor, including the 1-dim one, gets the O1 same-file pairs
for free. The entire margin of the 34-dim design over trivial baselines sits on one pair
(`eom_singleline_changed`), and the "20/20" figure is the **best of 7 ablations scored on the same
20 items**, with z-scores fitted on those same 20 — selection on the test set, on top of the n=20
caveat the probe itself flags.

## What I did not check

* I did not re-run `signoise_02_ablation` in full (10–15 min); the rows C2/C3 cite are entailed by
  the key trace and I spot-reproduced 6 ablations x 4 pairs (`v9_ablation_spot.py`).
* I did not open any block crop, so I did not test anchor correctness either — C4's "in principle"
  stays unresolved in both directions.
* Claims 1 and 3 are code facts about `comparator.py` at commit 1619fc3f; they say nothing about a
  future consumer, and the probe says so too.

## Reproduce

```bash
cd /home/coder/projects/PDF-proverka
python experiments/stage_comparison_vector_architecture_opus/probes/verify/v1_minimal_recheck.py    # ~3 min
python experiments/stage_comparison_vector_architecture_opus/probes/verify/v2_key_trace.py          # ~15 s
python experiments/stage_comparison_vector_architecture_opus/probes/verify/v3_style_raw.py          # ~2 min
python experiments/stage_comparison_vector_architecture_opus/probes/verify/v4_style_is_read.py      # ~2 min
python experiments/stage_comparison_vector_architecture_opus/probes/verify/v5_anchor_recheck.py     # ~2 min
python experiments/stage_comparison_vector_architecture_opus/probes/verify/v6_signature_baselines.py # ~1 s
python experiments/stage_comparison_vector_architecture_opus/probes/verify/v7_signature_triple.py   # ~20 s
python experiments/stage_comparison_vector_architecture_opus/probes/verify/v8_share_framing.py      # ~2 min
python experiments/stage_comparison_vector_architecture_opus/probes/verify/v9_ablation_spot.py      # ~20 s
```
