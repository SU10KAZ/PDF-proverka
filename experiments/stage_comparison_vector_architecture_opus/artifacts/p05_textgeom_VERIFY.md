# p05_textgeom (`txgeo`) — adversarial verification

Verifier: independent agent, instructed to refute. Everything below was re-run or re-read;
nothing is quoted from the probe's own summary without recomputation.

Reproduction of every number in this note:

```bash
python -m experiments.stage_comparison_vector_architecture_opus.probes.txgeo_confidence        # probe's own, reproduces exactly
python -m experiments.stage_comparison_vector_architecture_opus.probes.txgeo_dimension_check   # probe's own, reproduces exactly
python -m experiments.stage_comparison_vector_architecture_opus.probes.p05_textgeom_verify     # this verification (V1..V7)
```

| # | claim | verdict |
|---|---|---|
| 1 | `anchors.confidence` carries literally zero bits (high 374/374, p=0.348, no second stratum) | **WEAKENED** |
| 2 | The honest confidence is candidate count, **not distance** (ticks≤2 → 0.677 / ≥3 → 0.024) | **WEAKENED** |
| 3 | 3-signal gate 0.348 → 0.720 → 0.867 where v0.1 has a flat line, cost 78 % of recall | **WEAKENED** |
| 4 | The referent is a DERIVED INTERVAL, not a primitive (whole segment wrong 8.8–39.9 % on 7 of 8 blocks) | **REFUTED** |
| 5 | grid_cell quality is decided at block level, not text level; uniqueness 1.000 by construction | **WEAKENED** (the "by construction" half is CONFIRMED) |

---

## Reproducibility first

`txgeo_confidence.py` and `txgeo_dimension_check.py` re-run and reproduce their artifacts
**byte-for-byte** (`samples=374 overall precision=0.348`, and the whole dimension table). Good.

But three artifacts cited by the claims have **no generating script anywhere in `probes/`**:

```
artifacts/txgeo_referent_shape.txt      <- claim 4's headline table
artifacts/txgeo_gridcell_check.{txt,json} <- claim 5's whole evidence base
artifacts/txgeo_motif_counts.{txt,json}
```

`grep -o 'ART / "[^"]*"' probes/txgeo_*.py` lists only `txgeo_{confidence,dimension_check,metrics,ranking,usefulness}.json`.
The two tables that carry claims 4 and 5 are therefore **unreproducible as shipped**, and the
FINDINGS.md command list does not mention them either. I had to reconstruct both. I reproduced the
`gridHit / share` column of the gridcell table exactly (all 15 rows). I could **not** reproduce the
`colsRepeat / rowsRepeat` columns under any definition I tried, and I could not reproduce the
`lineOnly` column of the referent-shape table at all — see claim 4.

---

## Claim 1 — "confidence carries literally zero bits" → **WEAKENED**

**What reproduces.** `stratifications[v0.1_anchor_confidence] = high n=374 correct=130 p=0.348`, single bucket. Yes.

**Three things the claim does not survive.**

**(a) The number comes from a re-implementation, not from v0.1.** `probes/txgeo_relations.py::rel_nearest_geometry`
computes its own `v01_confidence` as `d0 <= 0.012 * max(bbox_w, bbox_h)` in **raw pt space**, while
Track A's `extractor.py:713` uses `nearest[0] <= 0.012` in **normalized** space (x and y divided by
*different* factors — a point the probe itself makes in its §2). These are different metrics. Reading
v0.1's actual `anchors[].confidence` for the *same 374 units* (V1) gives **371 `high` / 3 `candidate`** —
so a second bucket does exist; it is merely useless (n=3).

**(b) The sample was selected to make this true.** The 374 come from exactly the 6 blocks hardcoded
in `txgeo_confidence.py::TRUSTED`, chosen because their modal ratio landed near a standard scale —
i.e. dense drawing blocks. Orchestrator finding **O3 already states** that `high` saturates in dense
blocks and `candidate` dominates sparse ones. Over the full 15-block corpus (V1) v0.1's confidence
*does* split:

```
ss_simple_node      0 high /  27 candidate      ss_table_graphic     0 /  50
fresh_ov_spec_table 42     / 182                ss_scheme_text_changed 19 / 70
eom_singleline       25    /  33                fresh_ar_legend       3 /  20
```

"There is no second stratum" is a property of the chosen subset, not of the field. And the claim is a
re-derivation of O3, which the BRIEF lists under "do not re-derive".

**(c) "Literally zero bits" is false for the payload v0.1 actually stores.** v0.1 records
`distance_norm` alongside the label. Stratifying the *same 374 units* on v0.1's own `distance_norm` (V2):

```
Q1(<=0.0010) n=75 p=0.280   Q2(<=0.0023) n=75 p=0.187   Q3(<=0.0028) n=76 p=0.276
Q4(<=0.0033) n=85 p=0.494   Q5(> 0.0033) n=63 p=0.508
```

A 2.7× spread, and a single cut `dn >= 0.003` gives **n=103, p=0.592** against a 0.348 base. v0.1's
anchor layer carries real bits; what is degenerate is the **0.012 threshold**, which sits far above
every distance actually observed. That is a materially different — and more actionable — finding than
"zero bits".

## Claim 2 — "candidate count, not distance" → **WEAKENED**

**The candidate-count half holds and is not an artefact.** `ticks_in_reach ≤ 2 → 0.677 (n=186)`
reproduces, and V3 shows it splits *within every block* that has both strata (ar_plan 0.654 vs 0.014,
ss_plan_dense 1.000 vs 0.000, ar_wall_sections 0.667 vs 0.074), so it is a per-text signal, not a
block-identity proxy. V5 shows the ticks≥3 group has no coherent mode of its own (ar_plan: 11 of 143
in the modal bin, implying 1:164), so labelling it noise is fair.

**The "not distance" half is false.** See V2 above: distance carries substantial signal on the same
sample. The defensible statement is "candidate count is a *better* signal than distance", not "distance
is not a signal". (The signal is *inverted* — far ⇒ correct — which is O3's point again.)

**The number is wrong.** `≥3` aggregate is **4/188 = 0.021** (V3), not 0.024. 0.024 is the `3-4`
bucket alone (3/124); the claim pastes that bucket's precision onto the pooled `n=188`.

**ticks≤2 does not mean correct.** The probe's *own* hand-check file `txgeo_usefulness.txt` records
three `WRONG_VALUE` dimension cases at `ticks=2` in `fresh_kj_sections`
(`2650` measured 275.2 mm, err 89.6 %; `1370` err 81.2 %; `400` err 12.2 %). Consistently, my per-block
gate table gives `fresh_kj_sections` gate precision **0.333 (n=6)**.

**Ground-truth caveat.** "Correct" = ratio within 2 % of a per-block modal scale, and V5 shows that
modal scale *is* the ticks≤2 group's own mode (ar_plan ticks≤2 mode 35.282 = the hardcoded
TRUSTED 35.283). So 0.677 is partly the internal concentration of the group that defined the label.
Mitigation, in the probe's favour: that mode lands on 1:100.01 / 1:50.01 / 1:10.05 / 1:49.92 /
1:99.97 / 1:20.05, which is independent corroboration, and it is stable under `bin_factor`
1.01→1.10 (V6, T5's own falsification test — passes on these 6 blocks).

## Claim 3 — "a real precision/recall curve where v0.1 has a flat line" → **WEAKENED**

Numbers reproduce (374 → 175 @ 0.720 → 83 @ 0.867). Four problems.

**(a) 83 % of the sample is two blocks of one PDF file.** V3:

```
ar_plan          n=279  74.6 %   gate1 n=127 p=0.701   gate2 n=61 p=0.820
ar_wall_sections n= 33   8.8 %   gate1 n=  6 p=0.667   gate2 n= 2 p=1.000
ss_plan_dense    n= 26   7.0 %   gate1 n= 19 p=1.000   gate2 n=19 p=1.000
vk_nodes         n= 20   5.3 %   gate1 n= 12 p=0.583   gate2 n= 0
fresh_ar_lintels n= 10   2.7 %   gate1 n=  5 p=1.000   gate2 n= 0
fresh_kj_sections n= 6   1.6 %   gate1 n=  6 p=0.333   gate2 n= 1 p=1.000
```

`ar_plan` + `ar_wall_sections` are pages 8 and 13 of the **same** AR PDF — the file that orchestrator
**O1** flags as byte-identical on both sides. (O1 does not contaminate this particular measurement,
which uses left sides only, but the concentration stands.) **80 of the 83 gated units come from two
blocks**; two of the six blocks contribute zero gated units. The 0.867 headline is `ar_plan` (0.82)
plus `ss_plan_dense` (1.00).

**(b) The gate is tuned and evaluated on the same 374.** `ticks≤2`, `centred`, `candidates==1` were
read off the stratification table in `txgeo_confidence.json` and then scored on it. No held-out block,
no split. Per-block gate1 precision ranges 0.333 → 1.000.

**(c) The recall denominator is the hits, not the texts.** "78 % of recall" = 1 − 83/374, where 374 is
*units that already got a dimension_interval hit*. Against pure-integer dimension texts in the same
6 blocks (**513**, V4) the gate keeps 83 → **16.2 %**; across all 15 blocks (**600**) → **13.8 %**.
Of the 83, 72 are correct, so the verified-value yield is **72/513 = 14.0 %** (6 blocks) or
**12.0 %** (15 blocks) — not 22 %.

**(d) "v0.1 has a flat line" is the wrong comparator.** At matched coverage, a plain threshold on
v0.1's own stored `distance_norm` (V2) gives **p=0.451 at n=175** and **p=0.590 at n=83** against the
0.348 base. That is a real curve, not a flat line. The probe's 3-signal gate is genuinely better
(0.720 / 0.867) — the *relative* claim survives, the *absolute* framing does not.

## Claim 4 — "the referent is a derived interval, not a primitive" → **REFUTED**

**(a) The table compares two different estimators.** `txgeo_referent_shape.txt`'s own header says
`median mm/pt`, while its `interval` column is numerically identical to `modal_mm_per_pt` in
`txgeo_dimension_check.json` (ar_plan 35.283, ar_wall_sections 17.6444, ss_plan_dense 35.2685 …) —
a **mode**, produced by `modal_scale()`. Median vs mode is exactly the difference between "a chain of
mixed-length segments" and "the peak of that chain". No script exists to check what the `lineOnly`
column really did; I could not reproduce it as median, mode, or mean for most blocks
(probe vk_plan 0.212 vs my median 6.285; probe vk_nodes 2.427 vs 6.611; probe ar_wall_sections 5.841
vs 14.547).

**(b) With one estimator for both referents, the headline reverses on the biggest blocks.** V6, using
the probe's *own* `modal_scale(bin_factor=1.02)` on the same pure-integer texts:

| block | whole-segment 1:X (err) | tick-interval 1:X (err) | claim's whole-segment err |
|---|---|---|---|
| ar_plan | **1:99.99 (0.01 %)** | 1:100.01 (0.01 %) | 8.84 % |
| ss_plan_dense | **1:99.77 (0.23 %)** | 1:99.97 (0.03 %) | 0.04 % |
| vk_nodes | **1:19.85 (0.76 %)** | 1:20.05 (0.25 %) | 37.59 % |
| vk_node_plan | **1:9.68 (3.19 %)** | 1:1.73 (13.74 %) | 22.84 % |
| fresh_kj_sections | 1:47.40 (5.20 %) | 1:49.92 (0.15 %) | 11.42 % |
| fresh_kj_plan_part | **1:44.41 (11.17 %)** | 1:136.99 (36.99 %) | 11.61 % |

T4's stated falsification test is *"reproduce the standard scale from whole-segment lengths on any
block"*. It reproduces on **three**: ar_plan 0.01 %, ss_plan_dense 0.23 %, vk_nodes 0.76 %.

**(c) At the per-text level the primitive referent is more accurate, pooled.** V6, restricted to texts
that carry **both** relations, scored against the block's true standard scale (a target taken from the
*interval*'s own output, so if anything biased toward the interval):

```
block               n both   whole-segment   tick-interval
ar_plan               254    114 (0.449)      82 (0.323)
ss_plan_dense          24     22 (0.917)      17 (0.708)
vk_nodes               10      4 (0.400)       3 (0.300)
ar_wall_sections       30      2 (0.067)       5 (0.167)
fresh_ar_lintels       10      0 (0.000)       5 (0.500)
fresh_kj_sections       6      1 (0.167)       2 (0.333)
POOLED                334    143 (0.428)     114 (0.341)
```

The interval wins only on the three blocks with n = 30, 10, 6.

**(d) The mechanism explains it.** `rel_along_line_and_dimension` already requires terminators at
**both** ends of the segment, so in these CAD PDFs the "primitive" *is* the interval: on ar_plan the
interval equals the whole segment exactly in 25.2 % of cases and the interval/segment histogram peaks
at 1.0 (78 of 254); on ss_plan_dense 58.3 % and 17 of 24. In the remaining cases the tick collector
(`reach = 40u`, any segment crossing at >20°) **shortens** an already-correct segment on hatching and
extension lines — which is the probe's own T6 diagnosis, pointed at the wrong layer.

**(e) Selective reporting.** The claim lists 6 blocks where the interval "recovers the exact standard
scale" and omits that it **fails on 6 of the 12 measurable blocks** (fresh_ar_legend 15.78 %,
fresh_kj_plan_part 36.99 %, fresh_ov_spec_table 9.63 %, ss_table_graphic 28.68 %, vk_node_plan 13.74 %,
vk_plan 10.92 %). Those 6 successes are then hardcoded as `TRUSTED` and become the entire evidence
base for claims 1–3 — the selection and the evaluation share one dataset.

**(f) Miscount.** The shipped table has **9** blocks with a `lineOnly` value, 8 of them ≥ 8.84 %.
"7 of 8 measurable blocks" matches neither.

**What does survive:** the interval estimator is better *in aggregate across blocks* (6/12 vs 3/9
blocks landing within 2 % of a standard scale), and drawing-scale self-verification is a real channel
that v0.1 cannot express. The architectural conclusion "the referent must be a derived interval object"
is not supported by these measurements.

## Claim 5 — "quality is decided at block level, not text level" → **WEAKENED**

**Confirmed outright: uniqueness 1.000 is by construction.** `probes/txgeo_relations.py::rel_grid_cell`
returns `{"candidates": 1, "unique": True}` unconditionally whenever it hits. Nothing else is possible.

**Hit shares reproduce exactly** — I recomputed all 15 rows from the relations JSON and matched
(ar_plan 644/0.90, ss_plan_dense 233/0.97, fresh_ov_spec_table 192/0.84, …).

**"The one real table" is wrong — there are two, and grid_cell misses the other one.**
`ss_table_graphic` is a numbered parts table (its texts: «Цилиндрическая IP-видеокамера»,
«Разъём RJ-45 (8P8C)», «Труба гофрированная ПА», «Кабель UTP кат. 5Е», rows numbered 2…9; Track A's
`human_validation.json` describes it as "the camera mounting detail and **visible table rows**").
`grid_cell` fires on **6 of 26 = 0.23** of its units — the second-lowest share in the whole corpus,
below every plan. So grid_cell does not fire "on tables"; it fires on dense orthogonal line work, and
it happens to miss half the tables. That is a *different* defect from the one claimed, and it makes
the "plans look exactly like the real table" framing rest on a single block (`fresh_ov_spec_table`).

**Col/row repetition: direction survives, numbers do not reproduce.** No script generates
`txgeo_gridcell_check.*`. My recomputation from `text_alignment` (V7) gives ar_plan 0.79/0.68 vs table
0.83/0.79 — even *less* separation than the published 0.77/0.78 vs 0.89/0.99, so the claim's direction
holds. But I could not land on 0.89/0.99 for the table under any denominator I tried
(all units / grid-hit units only / threshold ≥2 or ≥3), so the published figures are unverifiable.

**A per-text signal that does separate exists.** Derived from the grid_cell referent alone, no
block-level table object: *is this cell's `(left_x, right_x)` pair shared by ≥3 other units* — i.e. is
it a real column? (V7)

```
fresh_ov_spec_table 0.85   ss_table_graphic 1.00   vk_plan 0.82   ss_scheme_text_changed 0.62
ar_plan 0.53   eom_singleline 0.34   ss_plan_dense 0.29   vk_node_plan 0.24   vk_nodes 0.20
fresh_ar_lintels 0.12   ar_wall_sections 0.07   fresh_kj_plan_part 0.00   fresh_kj_sections 0.00
```

Both real tables sit at the top; four of the five plan blocks the claim cites sit far below.
`vk_plan` at 0.82 is a genuine false positive, so it is not a clean separator — but "no per-text
signal separates a plan cell from a table cell" (the claim's own falsification criterion) is **not
established**; a two-line feature on the same data already gets most of the way.

---

## Contamination check against O1/O2

The precision/scale analyses use **left sides only**, so the identical-file defect O1 does not
directly inflate them. But `ar_plan` and `ar_wall_sections` — pages 8 and 13 of the O1 PDF — supply
83.4 % of the 374-unit sample, and `ar_plan` alone supplies 74.6 %. Claim 3's pair-independent numbers
are, in practice, one drawing. Claim 4's whole disagreement with my recomputation lives on that same
drawing.

## What I would still call solid in this probe

* `txgeo_confidence.py` / `txgeo_dimension_check.py` are honest, deterministic and reproduce exactly.
* Drawing scale really is recoverable from text + geometry with no labels, and the recovered value
  really does land on 1:100 / 1:50 / 1:20 / 1:10 and is stable under `bin_factor` 1.01→1.10 (T5).
  That self-verification channel is a genuine contribution.
* `ticks_in_reach` really is a per-text confidence signal that splits inside blocks.
* The automatic label agrees with the probe's five hand-checked `fresh_kj_sections` dimension cases,
  which is a small (n=5) but real validation of the ground truth.

## What a rebuild should change

1. Re-run the referent comparison with **one** estimator and publish the script.
2. Report recall against pure-integer dimension texts (513 / 600), not against relation hits.
3. Hold out at least `ar_plan` when scoring the gate; the corpus is one drawing wide.
4. Compare against v0.1's `distance_norm`, not against v0.1's thresholded `confidence` label.
5. Ship generating scripts for `txgeo_referent_shape.txt` and `txgeo_gridcell_check.*`.
