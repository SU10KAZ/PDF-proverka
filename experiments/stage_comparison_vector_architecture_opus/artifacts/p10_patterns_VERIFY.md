# p10_patterns (prefix `ptn_`) — adversarial verification

Verifier: independent agent, 2026-08-23. Everything below was re-run or recomputed from the repository
root `/home/coder/projects/PDF-proverka`. The probe's own artifacts were backed up before any re-run
(`ptn_run_signatures` / `ptn_pair_diff` / `ptn_recut` overwrite in place); re-runs reproduced the stored
files byte-for-byte on the numbers checked.

| # | claim (headline) | verdict |
|---|---|---|
| 1 | `repeated_elements` fingerprints PDF paths, not symbols | **CONFIRMED** |
| 2 | motif unit on RAW coords eliminates crop-jitter noise (40 → 0) | **WEAKENED** (attribution refuted by a trivial baseline; counterexample in the probe's own data) |
| 3 | S2 / S4 inside the identity key make instance counting worse | **WEAKENED** (the S2 example argues the opposite once physical sizes are read; "worse" never measured against ground truth) |
| 4 | rotation normalisation necessary on plans, useless on schemes → per-discipline | **WEAKENED** (metric dominated by ≤4-segment fragments and by S5c false merges; zero measured payoff on the very blocks called "plans") |
| 5 | exact hashing cannot survive CAD jitter; tolerance non-tunable because non-monotone | **WEAKENED** (jitter case real and reproduced; "any kind"/"cannot be tuned" both contradicted by measurements, including a q that fixes the cited failure at no cost) |

---

## CLAIM 1 — path-level fingerprinting — CONFIRMED

**What I ran.**

Read `ss_scheme_text_changed/left/vector_block.json` → `geometry.primitives[0]`: `type=path`,
`segment_count=24`, `item_indexes=[0..23]`. Printed all 24 raw segments: they are four disjoint
6-segment crossed squares at start-x `(154.56, 177.36) (333.12, 355.92) (418.32, 440.88)
(1401.60, 1424.16)`, all `y ∈ [345.12, 367.68]`, each 22.8 × 22.56 pt. Exactly as claimed.

I did **not** take the "5 ОСПД" ground truth from the probe. I rebuilt connected components over all
710 raw segments of the block with a union-find on rounded endpoints: exactly **5** components are
6-segment crossed squares of 22.6–22.8 pt — 4 inside primitive 0, the 5th inside primitive 20.

`repeated_elements` for that side = **1 group / 15 instances** (recomputed from the file). The 15 are
primitives 23–37, fifteen separate 8-segment paths of 11.3 × 28.3 pt. **Zero** of the 5 ОСПД appear in
`repeated_elements`, because the 4 that share a path hash once (count 1 < the `len(members) < 2` filter)
and the 5th is embedded in an unrelated 7-segment path.

**Visual check (the probe's ground truth is not self-assigned).** I rendered the two regions at 6× from
`…/13AB-РД-СОТ-К7 V1/versions/v002/02_work/document.pdf` page 5:
`Rect(140,330,200,382)` shows the crossed square labelled **ОСПД**; `Rect(145,185,240,232)` shows two
triangle-on-box symbols labelled **ВК2.1.1.1 / ВК2.1.1.2** — CCTV cameras. Both ground-truth labels hold.

**Code check.** `experiments/stage_comparison_vector_blocks/extractor.py:737 _repeated_elements` iterates
over `primitives`, one `_primitive_pattern` per primitive; `_drawing_primitives` (line 246) carries an
explicit comment *"Keep a PDF path as one primitive even when its line commands are disjoint."*
The mechanism is by design, not an accident of this block.

**Adversarial addition the probe did not report — prevalence.** I measured, for all 20 sides, how many
primitives contain more than one connected component:

```
ss_scheme/left 33/39 (84.6%)   ss_table_graphic/left 8/11 (72.7%)   ss_plan_dense/left 791/1604 (49.3%)
ss_simple_node 2/2 (100%)      eom/right 3/164 (1.8%)
ar_plan 0/14800   ar_wall_sections 0/20000   vk_plan 0/3163   vk_nodes 0/20000   vk_node_plan 0/20000
CORPUS 1669 / 160221 primitives = 1.04 %
```

So the defect is real and demonstrated, but in this corpus it is an **SS-producer phenomenon**; the AR,
VK and EOM PDFs never pack disjoint symbols into one path. That is a scoping caveat, not a refutation —
the claim says "can", and it does.

---

## CLAIM 2 — "motif unit on RAW coordinates eliminates the noise" — WEAKENED

**Reproduced.** `ptn_run_signatures` + `ptn_pair_diff` re-run: every cell of `ptn_pair_diff.json`
reproduced exactly. `ar_plan` S0 = 27 changed + 6 appeared + 7 disappeared = **40**;
S1/S2/S3/S4/S5/S5c = **0/0/0** each. The numbers are real.

### Problem A — a trivial baseline reproduces the whole "win" with the unit unchanged

The claim bundles two independent changes (unit: path → connected component; coordinate space:
block-normalised → raw) and credits the unit. I isolated the coordinate space: I kept
`_primitive_pattern` **verbatim** — one fingerprint per PDF path, same aspect rounding, same
`round(...,1)`, same membership filter — and only fed it `raw.bbox` / `raw.segments` instead of
`normalized.bbox` / `normalized.segments`:

| pair (byte-identical PDFs, O1) | S0 as shipped (block-normalised) | **S0-RAW baseline (unit unchanged)** | S1 motif unit |
|---|---|---|---|
| ar_plan | 32 changed + 4 + 4 = **40** | **0** | 0 |
| ar_wall_sections | 1 changed + 2 + 2 = **5** | **0** | **38** (26 changed + 2 + 10) |

The motif unit contributes **nothing** to the result. The phantom differences come from the
*anisotropic* block normalisation `(x−x0)/W, (y−y0)/H` with W,H jittering by ~0.1 %, which shifts every
per-primitive local coordinate across the `round(...,1)` boundary. Swapping the coordinate space is a
one-line fix that does not require a motif/object layer at all.

### Problem B — the second byte-identical pair goes the other way at the shipped configuration

`ar_wall_sections` is the *other* O1 same-file pair. At the shipped `storage_cap=20000` the current
fingerprint invents **5** differences and the motif fingerprint invents **38**. "The motif fingerprint
invents none" is therefore false on 1 of the 2 byte-identical pairs as measured. It becomes true only
after a *separate* change (uncapping) that is not part of the claim: I re-ran
`ptn_recut ar_wall_sections 60000` (36027 primitives both sides) and recomputed independently —
uncapped S1 = 269 repeated keys, **0 changed / 0 appeared / 0 disappeared**, confirming the probe's
`ptn_recut_diff.json`. But I also computed uncapped S0 myself (`_primitive_pattern` without the
top-100 truncation): 1 changed + 2 appeared + 2 disappeared = **5**, i.e. the current layer is
*unaffected* by the cap there while the motif layer needed the cap removed. Note S0-RAW is 0 **even at
the shipped cap** — it beats the motif unit on this pair.

### Problem C — the test has near-zero discriminating power (contamination by O1)

Both test pairs are the same file (`sha256 3d7242cd…`). Every raw coordinate is identical on both
sides; the only difference is which primitives fall inside a 0.1 %-jittered crop. Any fingerprint
computed in raw coordinates is therefore **tautologically** stable here — motif unit, path unit or
anything else. The experiment cannot distinguish the hypothesis it is offered as evidence for.

Sample size for the headline: **1 block**. `n = 2` byte-identical pairs exist in the entire corpus.

The probe's own `ptn_FINDINGS.md` P3 is worded more carefully ("destabilised by block-normalisation +
crop jitter"). The headline claim's causal attribution to the motif unit does not survive.

---

## CLAIM 3 — S2 / S4 inside the identity key make counting worse — WEAKENED (S2 leg refuted)

**Reproduced.** Corpus repeated groups over the 10 left sides, recomputed from
`ptn_signature_summary.json`: S0 238, S1 **663**, S2 **656**, S3 609, S4 **719**, S5 639, S5c 678. The
cited 663 → 719 and 663 → 656 are correct.

### The corpus statistic is a tautology, not a measurement

I verified that S2 and S4 are **strict refinements** of S1: across all 10 left sides, the number of S2
keys mapping to more than one S1 key is **0**, and likewise for S4. Both signatures embed `geom_core`
verbatim and append a field. Adding any field to an identity hash can only split groups, never merge
them. "Groups go up" is arithmetic; the empirical question is whether the splits are *correct*, and no
correctness measurement was made against any ground truth.

Scale of the effect, which the claim does not state: S2 splits **35 of 663** repeated S1 groups (5 %),
S4 splits **289 of 663**.

### The S2 example argues the opposite of the claim

`eom_singleline_changed/left`, S1 group `bcb451b62544`, n = 24, nseg = 24. The claim calls it "the
24-instance terminal-circle group". I printed the physical sizes of its 24 members:

```
2.76 × 2.76 pt : 12 instances   centres in two vertical columns  x = 451.3 and x = 527.0
5.64 × 5.64 pt : 12 instances   centres in two horizontal rows   y = 485.6 and y = 695.8
```

They are **two different circle families**, at different sizes (ratio 2.04) and in different parts of
the sheet. S1 merged them only because `geom_core` divides by each motif's own bbox and is therefore
scale-blind. The S2 partition is `12 + 11 + 1`, and the cross-tabulation is exact:

```
S2=6a972ea6  width 5.64 pt  n=12
S2=aaa94517  width 2.76 pt  n=11
S2=684f44e9  width 2.76 pt  n=1
```

S2 splits **precisely along the physical size boundary**. It is not destroying a real group; it is
partially repairing a false merge that S1 created. (The mechanism: 2.76 pt circles resampled to 24
points have 0.36 pt point spacing, below the 0.6 pt node-snap tolerance in `build_motifs`, so their
degree profile collapses to `((4,12),)` while the 5.64 pt circles keep `((2,24),)`.)

The probe's own eye ground truth in `ptn_FINDINGS.md` says the eom terminal-strip circles are **20**
(10 + 10 per side) — neither 24 nor 12 — and this is `eom left`, the side the probe itself declares
mis-framed (P13, `/Rotate 270`). The example rests on a block the probe says is extracted from the
wrong rectangle.

### The S4 example survives

I recomputed: `ar_plan/left` largest S1 group = **56**, all nseg = 24, sizes 11.34 × 11.34 (53) and
11.34 × 11.28 (3). S4 partitions it **30 / 18 / 7 / 1** (the claim omits the /1), and the partition is
exactly `relation_context` = `(0,True) 30 / (1,True) 18 / (2,True) 7 / (3,True) 1` — i.e. split purely by
how many long segments touch the circle. Those 56 are physically the same circle, so for *counting one
symbol type* the split is a loss. This leg holds — on one group, in one block.

**Net:** one of the two headline examples inverts on inspection, the corpus number is a mathematical
identity, and no ground truth was used to define "worse".

---

## CLAIM 4 — rotation matters on plans, not on schemes → per-discipline — WEAKENED

**Reproduced exactly.** I re-implemented the probe's metric (an S5c class "has >1 orientation" iff it
contains ≥2 distinct S1 keys) and got **3229 / 8090 = 0.3991**, matching the claim's 39.9 %. Per-block
S5c shares match the artifact (ss_plan_dense .517, ar_plan .448, ar_wall_sections .312, ss_scheme .048,
eom .101). The arithmetic is sound.

### Problem A — the metric is dominated by degenerate fragments

I stratified the same metric by motif complexity:

| block | motifs | affected | share | motifs nseg ≥ 6 | affected | **share (nseg ≥ 6)** |
|---|---|---|---|---|---|---|
| ar_plan | 1184 | 530 | .448 | 356 | 23 | **.065** |
| ar_wall_sections | 1597 | 498 | .312 | 76 | 0 | **.000** |
| vk_node_plan | 77 | 28 | .364 | 24 | 0 | **.000** |
| vk_plan | 193 | 20 | .104 | 69 | 0 | **.000** |
| ss_plan_dense | 3879 | 2005 | .517 | 1262 | 498 | .395 |
| eom | 79 | 8 | .101 | 61 | 2 | .033 |
| **corpus** | 8090 | 3229 | **.399** | 2061 | 527 | **.256** |

Corpus-wide, **79.7 %** of the "rotation-affected" instances are motifs with ≤ 4 segments (nseg
histogram of affected: 2→1343, 3→428, 4→367). ar_wall_sections: 339 of 369 affected are 2-segment
stubs. The headline plan number for `ar_plan` collapses from 44.8 % to **6.5 %** once ≤5-segment
fragments are excluded — *below* the scheme numbers it is contrasted with.

### Problem B — 39 % of "rotations" are S5c false merges, not rotated copies

`geom_core_rot` takes the lexicographic minimum over 3 candidate axes × 4 quarter-turns × 2 mirrors of a
q = 0.05 quantised key. For low-segment motifs that is a very lossy hash. I measured how many affected
instances sit in an S5c class whose members' bbox diagonals differ by more than 15 % — i.e. shapes that
cannot be rotations of one another: **1264 of 3229 = 39.1 %** corpus-wide, and **66 %** for ar_plan
specifically. Those instances are counted as evidence for rotation normalisation while actually being
evidence against S5c's own precision.

### Problem C — "necessary" is never demonstrated; on plans the payoff is zero

Rotation normalisation is defended by a prevalence statistic, never by an outcome. From
`ptn_pair_diff.json`, total emitted diff lines (changed + appeared + disappeared):

```
ar_plan          S1 0    S5 0    S5c 0     <- already perfect; rotation cannot help
ss_plan_dense    S1 0    S5 0    S5c 0     <- already perfect; rotation cannot help
ar_wall_sections S1 38   S5 36   S5c 35
vk_plan          S1 12   S5 12   S5c 12
vk_node_plan     S1 6    S5 4    S5c 5
vk_nodes         S1 35   S5 33   S5c 34
ss_scheme        S1 15   S5 11   S5c 11
eom (only real structural change) S1 24  S5 27  S5c 26   <- rotation makes it WORSE
```

The two blocks that carry the "plans need rotation" argument (`ar_plan` 44.8 %, `ss_plan_dense` 51.7 %)
are exactly the two where the un-rotated signature already emits **zero** differences. On the only pair
with a real structural change, rotation raises `changed` from 1 to 8 (and the probe's own P17 says the
S1 line there is false anyway). There is no block in the corpus where rotation normalisation is shown
to buy a correct statement.

### Problem D — the "per-discipline" split is not what the data shows

Within drawings that are plans: `vk_plan` **10.4 %**, `vk_node_plan` 36.4 %, `ss_plan_dense` 51.7 %,
`ar_plan` 44.8 %. `ar_wall_sections`, cited in support of "plans", is a set of wall **sections**.
`ss_simple_node` — a scheme fragment — scores **100 %** (6 motifs) and is silently absent from the
claim. The gradient tracks linework density (2-segment fragment count), not discipline. And 2 of the 3
"plan" blocks are the O1 byte-identical pairs, where no version change exists to detect.

Finally, N = 10 blocks, left sides only, and one block (`ss_plan_dense`, 3879 motifs) supplies 48 % of
the instance denominator and 62 % of the affected instances.

---

## CLAIM 5 — exact hashing can't survive jitter; tolerance non-tunable — WEAKENED

### The jitter case is real and reproduced (with a correction)

I recomputed the ⊠ markers from the descriptions directly:

```
eom left  : 14 crossed squares, ALL 8.52 × 8.52 pt  -> S1 q=0.05 -> ONE group of 14
eom right : 14 crossed squares, sizes  8.40×8.40 (7), 8.40×8.64 (5), 8.64×8.64 (1), 8.64×8.40 (1)
                                          -> S1 q=0.05 -> groups 8 + 5 + 1
                                          -> S1 q=0.08 -> ONE group of 14
```

The 8 + 5 + 1 split and the q ≥ 0.08 fusion are confirmed, and `ptn_q_sweep_crossed_square.json`
matches. **Correction:** the claim states the three sizes as "8.64×8.64 / 8.64×8.40 / 8.40×8.64". There
are **four** distinct sizes and the dominant class is **8.40 × 8.40** (7 of 14), not 8.64 × 8.64 (1 of
14). The mechanism is the aspect-ratio non-squareness (0.24 pt = 2.9 %) crossing a quantisation cell,
not the absolute size.

### "Exact hashing of ANY kind cannot survive real CAD export jitter" — contradicted by the same corpus

The universal is falsified by the probe's own measurements:

- Same symbol, other side: `eom left`'s 14 ⊠ exact-hash into **one** group of 14.
- `ss_plan_dense` left/right are **different files** (`a3e7451c…` vs `5560a190…`) with genuinely
  different geometry — I hashed the sorted raw segment multiset per side: 88752 vs 88609 segments,
  different digests. Exact S1 hashing over 3879 motifs still yields **307 shared groups, 0 changed,
  0 appeared, 0 disappeared**.
- `ss_table_graphic` and `ss_simple_node` (also different files): 0 invented differences.
- `ar_plan`'s 56-circle group survives exact hashing although 3 of its 56 members are 11.34 × 11.28
  rather than 11.34 × 11.34.

Exact hashing survived a real re-export on 4 of 7 control pairs, including the largest block in the
corpus. It failed on one symbol on one side of one block. The claim as stated over-generalises from
n = 1.

### "The tolerance parameter cannot be tuned safely because its effect is non-monotone"

The non-monotonicity is confirmed **as a statistic**: `ar_plan` largest S1 group over
q = 0.02/0.035/0.05/0.08/0.12/0.2 = **34/53/56/56/56/42**, exactly as cited (5 of the 10 blocks are
completely flat across the whole range, which the claim does not mention).

But largest-group size is a diagnostic, not the quantity being tuned. I swept q over **all 10 pairs**
and measured the actual output — the number of diff lines emitted (changed + appeared + disappeared):

```
q                    0.02  0.035  0.05  0.08  0.12  0.2
all 10 pairs          129    134   130   127   130   137
7 control pairs        54     60    56    55    54    57
```

Over a **10× range of q** the task-level output moves by ≤ 8 %. And `q = 0.08` — the value that fuses
the ⊠ the claim is built on — is simultaneously the **best** total (127) and keeps `ar_plan`'s largest
group at 56 and `ar_plan`/`ss_plan_dense`/`ss_table_graphic`/`ss_simple_node` at 0 invented differences.
The probe's own data contains a q that satisfies both of its cited constraints at no cost, which is the
opposite of "cannot be tuned safely".

(Script: I rebuilt the motif bundles once per side and re-hashed at each q; output cached at
`scratchpad/vfy_qsweep.json`.)

---

## Cross-cutting observations

1. **Denominators.** Claims 2 and 3 rest on 1 block each; claim 5's jitter case on 1 symbol on 1 side.
   Claim 4's corpus figure is 48 % one block. The corpus is 10 pairs, 2 of them the same file (O1).
2. **O1 contamination.** Claim 2 is measured *only* on O1 pairs, where raw-coordinate stability is
   tautological. Claim 4's two strongest "plan" blocks are the same two O1 pairs.
3. **Metric validity.** "Number of repeated groups" (claim 3) and "share of instances in
   multi-orientation classes" (claim 4) are both unanchored: neither has a ground-truth object count
   attached, and both move in a direction that is a mechanical consequence of the signature's
   construction (refinement in one case, a lossy canonicalising hash in the other).
4. **What survives strongly.** Claim 1 — verified three ways (raw geometry, connected components,
   rendered crops) and grounded in an explicit design comment in `extractor.py`.
5. **What the probe got right but mis-attributed.** The 40 phantom differences on `ar_plan` are real
   and important. Their cause is the anisotropic block normalisation, and the cheapest fix keeps the
   current unit. This is a genuine finding about the layer, not an argument for an object layer.

## Reproduction of this verification

```bash
cd /home/coder/projects/PDF-proverka
python -m experiments.stage_comparison_vector_architecture_opus.probes.ptn_run_signatures
python -m experiments.stage_comparison_vector_architecture_opus.probes.ptn_pair_diff
python -m experiments.stage_comparison_vector_architecture_opus.probes.ptn_recut ar_wall_sections 60000
# the ad-hoc checks (S0-RAW baseline, multi-cc census, S2/S4 refinement, nseg stratification,
# size-mixed S5c classes, full q sweep, raw-geometry digests) were run as inline python heredocs;
# each is a few lines over ptn_motifs.build_motifs / extractor._primitive_pattern and is quoted above.
```
