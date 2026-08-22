# signoise — SIGNAL vs NOISE inside VectorBlockDescription v0.1

Track B (Opus) independent audit. Prefix `signoise`. All numbers below were produced by scripts in
`experiments/stage_comparison_vector_architecture_opus/probes/`, run from the repository root.
Corpus = the 20 Track A descriptions and the 10 Track A pairs (commit 1619fc3f), unmodified.

## Claims

| # | claim | evidence (measured number / file) | confidence | how it could be falsified |
|---|---|---|---|---|
| S1 | **63.8 % of the contract is never read by the comparator.** A payload keeping only the keys `compare_descriptions` touches reproduces all 10 statuses, all scores and every emitted difference line byte-identically. | 119,671,126 B → 43,299,373 B, mismatching pairs **0/10** — `signoise_08_minimal_contract.md` | high | Show one pair where the stripped payload changes status, a score or a `differences` line. |
| S2 | **Half the entire contract is two write-only primitive sub-keys.** `primitive.style` = 27.025 % and `primitive.raw` = 22.812 % of corpus bytes; blanking either changes 0/10 statuses, 0/10 difference lists, 0/10 scores. | `signoise_01_field_cost.md` (32,341,142 B / 27,299,148 B); `signoise_02_ablation.md` rows `primitive_style_blank`, `primitive_raw_blank` | high | A consumer that reads `style`/`raw`; note `raw` *is* needed to recompute `level_1_exact_vector`, which is stored anyway. |
| S3 | **`style` is ~247x redundant.** 160,221 primitives carry 660 distinct style dicts (summed per block): 30,899,153 B of style values collapse to 125,208 B of per-block palette (byte factor 246.8x; count factor 242.8x). | `signoise_09_style_palette.md` | high | Find a block whose distinct-style count approaches its primitive count at scale (max observed ratio on a block with >100 primitives: 115/20,000). |
| S4 | **Five whole top-level fields are inert in the current comparator.** Blanking `anchors`, `hatch_like_structures`, `dimensions`, `labels`, `size_metrics` changes **0/10** statuses, **0/10** difference lists and **0/10** scores. Together 1,304,264 B (1.09 %). | `signoise_02_ablation.md`; byte totals `signoise_01_field_cost.md` | high | Any pair where one of them moves any output. (They may still matter to a *future* consumer — this measures the comparator that exists.) |
| S5 | **`anchors` is uninformative in principle, not only unused.** In the blocks where 98–100 % of anchors are labelled `high`, the mean number of primitives inside the 0.035 anchor radius is 20.9 (ss_plan_dense), 61.9 (vk_node_plan), 173.5 (ar_plan), 185.8 (vk_nodes); 94.3 % / 76.5 % / 68.3 % / 51.0 % of spans have ≥2 DISTINCT primitives tied within 0.002 of the winner. | `signoise_07_anchor_information.md` | high | Show that the chosen nearest primitive is the semantically correct carrier more often than the tie count suggests, on labelled ground truth. |
| S6 | **The three signature levels carry no information about each other.** Over all C(20,2)=190 cross pairings, level_2 equality and level_3 equality agree **190/190**; the triple (l1,l2,l3) takes exactly two values: (F,F,F) x189 and (T,T,T) x1. | `signoise_03_redundancy.md` | high | A corpus where a coarse level_3 collides while level_1 does not. |
| S7 | **The signature cannot drive candidate search.** As an exact key it recovers **1 of 10** true П↔РД counterparts (recall 0.10) at every level. A 6-number coarse bucket hash of my own design reaches **0.70** recall with **0** cross-pair collisions; a 34-dim z-scored coarse descriptor puts the true counterpart at rank 1 for **19/20** blocks (20/20 without the spatial grid). | `signoise_04_signature.md` | medium (n=20; z-scores fitted on the same 20 blocks, no held-out corpus) | Run both keys on ≥200 blocks with known counterparts; if the coarse descriptor's top-1 collapses, the advantage was small-corpus luck. |
| S8 | **`topology` never binds as a verdict gate.** Forcing topology similarity to 1.0: 0/10 status changes. Copying the right-hand counts from the left: 0/10. Only the impossible 0.0 moves anything (7/10). Observed baseline similarities 0.610–1.000 against a 0.85 gate. | `signoise_02_ablation.md` rows `topology_forced_1.0`, `topology_equalized`, `topology_forced_0.0`; baselines in `signoise_02_ablation.json` | high | A pair whose verdict actually turns on the 0.85 topology threshold. |
| S9 | **The single correct `STRUCTURE_SAME_VALUES_CHANGED` verdict is produced by a PDF-packaging heuristic.** Blanking `geometry.extraction.source_item_counts` (the `l`/`re` operator counts) turns `ss_scheme_text_changed` into `STRUCTURE_CHANGED`. Its geometry (0.866) and topology (0.733) similarities are both below the alternative branch's thresholds. | `signoise_02_ablation.md` row `extraction_item_counts_blank`; scores in `signoise_02_ablation.json` | high | Show the alternative branch also fires; or show `l`/`re` counts are semantically stable across CAD exporters. |
| S10 | **Deleting *all* text changes 1 of 10 verdicts.** The layer that carries «Номинал 250 → 315 А» is almost irrelevant to the status decision as wired. | `signoise_02_ablation.md` row `texts_blank` (status changed: `ss_scheme_text_changed` only) | high | A benchmark where text drives the verdict on more pairs. |
| S11 | **Positioned text alone does not yield value sentences.** Of 326 `value_changes` records, only **15 (4.6 %)** are number→number; **184 (56.4 %)** contain control characters; the loosest upper bound (a digit on both sides, no control chars) is **94/326 = 28.8 %**. Real emitted examples: `'Согласовано' → 'этажного'`, `'L3' → ','`, `'\x04\x18 \x15\x15 . \x16\x11' → '\x0f-3!-\x0f 1- 4'`. | `signoise_06_output_semantics.md` | high | Show the position-nearest pairing is semantically right more often under a stricter matcher; the 4.6 % is a property of the *current* matcher. |
| S12 | **60 of 93 emitted `Текст/значение` lines come from pairs the comparator itself distrusts.** All three VK pairs have `left/right_layer_quality = UNDECODABLE`, are excluded from the status decision — and still print 93–100 value changes each to the user (capped at 20 lines per pair). | `signoise_06_output_semantics.md`, verified against `comparisons/vk_nodes/comparison.json` `text.reliable=false` | high | Show the emitted lines are filtered by `reliable` somewhere downstream. |
| S13 | **A block compared with ITSELF under a 0.5 % crop shift stops being IDENTICAL and fabricates value changes.** Same PDF, same block, crop shifted 0.5 %: `ss_table_graphic` emits **7** lines including `Текст/значение "Sto → "Stop`, `протяжко → протяжкой`; at 2 % shift it becomes `STRUCTURE_SAME_VALUES_CHANGED`, at 2 % scale `STRUCTURE_CHANGED`. 3 of 4 self-pairs lose `IDENTICAL` at 0.5 %. | `signoise_05_perturbation.md` (mode `self`) | high | Show a crop-invariant text-clipping rule; the artefact is bbox clipping of text spans. |
| S14 | **A 0.5 % crop shift destroys one of the two correct real-change verdicts.** `ss_scheme_text_changed` (human ground truth `STRUCTURE_SAME_VALUES_CHANGED`) flips to `STRUCTURE_CHANGED` at 0.5 % shift and stays wrong at 2 % shift and 2 % scale. At 2 % shift, 2 of 4 tested cross-pairs change status. | `signoise_05_perturbation.md` (mode `cross`) | high | Show that production block bboxes between П and РД agree to better than 0.5 % of block size. |
| S15 | **`repeated_elements` and `primitive_count` never change a verdict but always change what the expert reads.** 0/10 status changes; 8/10 and 4/10 difference-list changes respectively. On `ar_plan` — a byte-identical PDF on both sides (orchestrator O1) — the two emitted lines are `Число примитивов: 14800 → 14799` and `Изменены повторяющиеся motifs: 40`. | `signoise_02_ablation.md`; `signoise_06_output_semantics.md` | high | Show a case where a motif-count delta corresponds to a real design change. |
| S16 | **The non-text quarter of the output is exactly the forbidden register.** 124 difference lines total: 93 value statements, 8 `motifs`, 7 `Топология изменилась`, 6 added, 6 removed, 4 `Число примитивов`. Every non-text line is a count of anonymous primitives. | `signoise_06_output_semantics.md` | high | Rephrase a motif/primitive delta as an object sentence without adding an object layer. |
| S17 | **The level-3 payload that Track A actually sent to the model is 73.9 % text and 17.4 % opaque hashes.** Composition of `size_metrics.compact_payload` over 20 blocks (179,060 B): `texts` 73.85 %, `signatures` 9.79 %, `patterns` 7.58 % (sha1 `pattern_*` ids), `topology` 4.75 %, `summary` 2.76 %, `hatch_candidates` 1.08 %, `quality` 0.26 %. | `signoise_09_style_palette.md`; the payload choice is `run_ai_experiment.py:67-68` | high | Show a model can use a sha256/sha1 token. |

## KEEP / DROP / DEMOTE-TO-CACHE, field by field

`DEMOTE-TO-CACHE` = keep it on disk next to the block for rendering/debugging, never in the
comparison contract and never in a model payload.

| top-level field | corpus bytes | share | ablation: status / differences / scores changed | verdict |
|---|---:|---:|---:|---|
| `geometry.primitives[].normalized` | 24,215,069 | 20.235 % | (the only shape carrier; probe 8 keeps it) | **KEEP** |
| `geometry.primitives[].style` | 32,341,142 | 27.025 % | 0/10, 0/10, 0/10 | **DEMOTE-TO-CACHE** as a per-block palette + index (242.8x, S3). Line weight/dash is real engineering semantics, but not 32 MB of it. |
| `geometry.primitives[].raw` | 27,299,148 | 22.812 % | 0/10, 0/10, 0/10 | **DROP.** Only `level_1_exact_vector` needs it, and that hash is already stored. Note it also makes L1 page-position-dependent (S6/S7). |
| `geometry.primitives[].source_kinds` + `item_indexes` + `drawing_index` + `length` | 14,111,504 | 11.792 % | 0/10, 0/10, 0/10 (probe 2 for the first two, probe 8 for all four) | **DEMOTE-TO-CACHE** (re-render provenance only). |
| `geometry.primitives[]` scalars `id/type/closed/segment_count/length_norm/angle_degrees` | 18,312,337 | 15.302 % | read by `_primitive_feature` | **KEEP** (but see S15: `id` only reaches the user as an anonymous token). |
| `geometry.extraction` | 324,748 | 0.271 % | `source_item_counts`: 1/10 status | **KEEP the two counters, FLAG them.** They are PDF-writer artefacts that currently decide the only correct `STRUCTURE_SAME_VALUES_CHANGED` (S9). |
| `texts` | 1,311,348 | 1.096 % | blank: 1/10 status, 6/10 differences | **KEEP `text` + `x_norm` + `y_norm`; DROP `id`/`bbox`/`bbox_norm`/`font`/`font_size`/`rotation` (898,207 B = 69.1 % of the group, probe 8 output identical); DEMOTE `category`** (flattening it: 0/10 status). |
| `anchors` | 687,850 | 0.575 % | 0/10, 0/10, 0/10 | **DROP.** Inert (S4) and uninformative (S5, 20.9–185.8 equally eligible primitives). Replace with an object-membership edge, not a proximity guess. |
| `labels` | 265,784 | 0.222 % | 0/10, 0/10, 0/10 | **DROP.** A regex filter over `texts` stored a second time. |
| `dimensions` | 94,117 | 0.079 % | 0/10, 0/10, 0/10 | **DROP.** `len(dimensions) == primitive_summary.engineering_values` for **20/20** blocks (identical values, `signoise_03_redundancy.md`), and orchestrator O4 shows it misses 36 % of numeric spans. |
| `topology` | 225,406 | 0.188 % | forced-1.0: 0/10; equalized: 0/10; forced-0.0: 7/10 | **DEMOTE.** Keep the 9 counters as a cheap screen; **DROP `components`** (212,297 B = 94.29 % of the group, never read) — and note the counters describe ≤8,000 segments, i.e. 9.5 % of `ss_plan_dense` linework. |
| `repeated_elements` | 170,113 | 0.142 % | 0/10 status, 8/10 differences | **DEMOTE.** Its only effect is a noise line (S15); `instances` = 64.89 % of the group is never read. Orchestrator O5 shows the motifs are rectangles and a resampling constant. |
| `hatch_like_structures` | 70,145 | 0.059 % | 0/10, 0/10, 0/10 | **DROP** (with O6: saturates its cap of 30 in 6/10 blocks, and on `ss_table_graphic` the "hatch" is a table grid). |
| `size_metrics` | 186,368 | 0.156 % | 0/10, 0/10, 0/10 | **DROP the metrics** (self-measurement, recomputable in one line); **KEEP `compact_payload` as a separate artifact** — it is the only thing ever sent to a model (S17). |
| `structural_signature.level_1_exact_vector` | 1,800 | 0.002 % | 1/10 status | **KEEP** as an equality short-circuit only. |
| `structural_signature.level_2` + `level_3` + `level_3_payload` | 15,414 | 0.013 % | 0/10 status, 0/10 differences | **DROP.** Zero information beyond level_1 (S6) and recall 0.10 as a retrieval key (S7). Replace with a tolerant coarse descriptor. |
| `primitive_summary` | 5,120 | 0.004 % | `primitive_count` equalized: 0/10 status, 4/10 differences | **DEMOTE.** All 9 counters are derivable; `primitive_count` only produces the O7 noise line. |
| `vector_quality` | 580 | 0.0005 % | not exercised — no block in the corpus is `VECTOR_DATA_INSUFFICIENT` (12/20 are `LIMITED_CAPPED`, 8/20 `GOOD`) | **KEEP**, marked `UNVERIFIED` on this corpus. |
| `ambiguities` + `coordinate_system` + `quality_notes` + `source.excluded_sources` + `source.source_layers` | 21,110 | 0.018 % | 0/10, 0/10, 0/10 | **DROP from the per-block record.** Constant prose repeated 20 times; it belongs in the schema doc. |
| `block_id`, `page`, `page_index`, `bbox`, `bbox_norm_on_page`, `polygon_norm_on_page`, `source.pdf`, `source.pdf_sha256`, `source.page_width/height`, `schema_version` | 10,851 | 0.009 % | identity | **KEEP.** |

## What this says about the orientation question

The measurements point the same way from three independent directions.

1. **The contract is mostly write-only** (S1: 63.8 %), and the part that is read is read for the
   wrong purpose: the largest single field (`style`, 27 %) is unused, while the field that decides
   the only correct "values changed" verdict is a count of PDF path operators (S9).
2. **Nothing in the contract binds a value to a thing.** `anchors` is the only construct that tries,
   and where it is most confident there are 20–186 equally eligible primitives (S5). Consequently
   the value sentences the comparator emits are 4.6 % number→number and 56.4 % control-character
   garbage (S11), and 60 of 93 of them come from a text layer the comparator itself marked
   UNDECODABLE (S12). «Номинал 250 → 315 А» is not derivable from this representation *safely*;
   what is derivable is «Согласовано → этажного».
3. **Identity is not stable enough to compare anything.** The same block of the same PDF, cropped
   0.5 % differently, stops being IDENTICAL and produces fabricated value changes (S13); a 0.5 %
   shift destroys one of the two correct real-change verdicts (S14); the exact signature recovers
   1 of 10 true counterparts (S7). A backbone that cannot recognise a block across a crop jitter
   cannot recognise it across a П→РД redraw.

None of these are fixed by adding more geometry fields or by tuning thresholds. They are fixed by
introducing a layer that (a) groups primitives into *objects* with a tolerant identity, (b) attaches
text to an object by membership rather than by distance, and (c) states change as a relation between
objects. My coarse-descriptor result (S7: 0.70 exact-bucket recall, 19/20 rank-1) shows the object
layer's *retrieval* half is cheap. So this probe **leans B**.

Two honest counterweights: the corpus is 20 blocks / 10 pairs, 2 of them the same file (O1) and only
2 exercising real change (O2), so every "0/10" means "never on this benchmark", not "never"; and a
field that is dead weight for *this* comparator (e.g. `style`, `hatch_like_structures`) could be a
primary input for an object detector that does not exist yet. What the numbers do establish
unconditionally is that the current field set is not paying for itself and that its verdicts are not
robust to sub-percent crop noise.

## Reproduction

```bash
cd /home/coder/projects/PDF-proverka
python -m experiments.stage_comparison_vector_architecture_opus.probes.signoise_01_field_cost           # ~11 s
python -m experiments.stage_comparison_vector_architecture_opus.probes.signoise_02_ablation             # ~10-15 min
python -m experiments.stage_comparison_vector_architecture_opus.probes.signoise_03_redundancy           # ~25 s
python -m experiments.stage_comparison_vector_architecture_opus.probes.signoise_04_signature            # ~1 s (needs 03)
python -m experiments.stage_comparison_vector_architecture_opus.probes.signoise_05_perturbation         # ~40 s (re-extracts 40 crops)
python -m experiments.stage_comparison_vector_architecture_opus.probes.signoise_06_output_semantics     # ~1 s (needs 01)
python -m experiments.stage_comparison_vector_architecture_opus.probes.signoise_07_anchor_information   # ~2 min
python -m experiments.stage_comparison_vector_architecture_opus.probes.signoise_08_minimal_contract     # ~2 min
python -m experiments.stage_comparison_vector_architecture_opus.probes.signoise_09_style_palette        # ~20 s
```

## Gaps and UNVERIFIED

- `vector_quality == "VECTOR_DATA_INSUFFICIENT"` never occurs in the corpus, so the
  `INSUFFICIENT_VECTOR_DATA` branch of `compare_descriptions` is **UNVERIFIED**.
- Ablation measures discriminative power **inside the Track A comparator only**. A field that never
  moves this comparator's output could still be the right raw material for a different consumer.
- The nearest-neighbour retrieval in S7 z-scores its features over the same 20 blocks it queries;
  there is no held-out corpus. Chance top-1 on 19 candidates is 5.3 %, so 19/20 is far above chance,
  but the number should be re-measured on hundreds of blocks before it is trusted as a design input.
- Perturbation was run on 4 pairs (8 configurations x 5 crops = 40 extractions), chosen because they
  are cheap to re-extract; the six `LIMITED_CAPPED` dense blocks were **not** perturbed (a single
  ar_wall_sections comparison is ~5 s and each extraction tens of seconds). Stability on dense
  capped blocks is **UNVERIFIED**.
- I did not measure extraction wall-clock cost per field, only byte cost.
- `signoise_02_ablation` neutralises whole field groups. Interactions between two ablated groups
  were not explored.
