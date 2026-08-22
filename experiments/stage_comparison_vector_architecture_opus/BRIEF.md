# BRIEF — Track B (Opus) independent architectural audit of VectorBlockDescription

Read this before doing anything. It is the shared context for every probe agent.

## Ground rules (hard)

1. **Research only.** Never modify anything under `backend/`, `frontend/`, `norms/`, `projects_v2/`,
   `scripts/`, or `experiments/stage_comparison_vector_blocks/`. Read them freely; write nothing.
2. Write **only** inside `experiments/stage_comparison_vector_architecture_opus/`.
3. **Never read** anything under `experiments/stage_comparison_vector_blocks_v02_codex/`.
   That directory is the parallel Codex track's output and is strictly off limits — do not open,
   grep, list, or import from it. Also never read any other report produced by that track. If you find a file that
   looks like another agent's *final* architectural report on this same question
   (e.g. `*CODEX*REPORT*`, a second `*VECTOR*ARCHITECTURE*REPORT*` outside this directory),
   stop and do not open it. We need an independent second opinion.
   `experiments/stage_comparison_vector_blocks/` (Track A, commit 1619fc3f) **is** allowed and required reading.
4. Do not implement production Stage Comparison. Proof-of-concept scripts only.
5. Every claim must be backed by a number you actually computed or a file you actually read.
   Write the reproduction command into your artifact. If you could not verify something, say
   `UNVERIFIED` explicitly. Do not estimate numbers that you could have measured.
6. Do not `git add`/`git commit`. The orchestrator commits.

## What already exists (Track A, commit 1619fc3f)

`experiments/stage_comparison_vector_blocks/`

- `extractor.py` (1172 lines) — builds `VectorBlockDescription` v0.1 from `page.get_drawings()` +
  vector text spans, clipped to a normalized bbox. Layers: identity/source, geometry primitives
  (raw + block-normalized segments, style), texts (span + bbox + rotation + font + category),
  anchors (nearest-geometry candidate), topology (nodes/edges/endpoints/T-junctions/unconnected
  X-crossings/components/closed+nested contours/degree histogram), repeated_elements
  (path-shape fingerprints), hatch_like_structures (parallel-segment clusters), dimensions/labels
  (regex projections of texts), structural_signature (3 levels), size_metrics (L0..L3 + compact payload),
  vector_quality ∈ {GOOD, LIMITED, LIMITED_CAPPED, VECTOR_DATA_INSUFFICIENT}.
- `comparator.py` (566 lines) — order-independent directional segment coverage at 4 tolerances,
  primitive matching (diagnostic), text multiset + character-stream diff, topology count diff,
  repeated-pattern diff, encoding-rewrite heuristic; emits one of 5 statuses.
- `artifacts/block_pairs.json` — 10 manually paired real blocks (SS, AR, VK, EOM).
- `artifacts/descriptions/<pair>/<side>/vector_block.{json,md}` — 20 real descriptions.
- `artifacts/comparisons/<pair>/comparison.{json,md}` + `overlay.svg`.
- `artifacts/diagnostics/<pair>/{left,right}.png` — raster crops for human checking.
- `artifacts/human_validation.{json,md}` — 8 CORRECT, 2 PARTIALLY_CORRECT, 0 WRONG.
- `artifacts/benchmark_results.{json,md}` — statuses + per-pair geometry/text/topology scores.
- `artifacts/ai_experiment/` — Vision (10 PNG crops) vs Vector (L3 + diff) on 5 pairs with the same
  `gpt-5.6-sol`; Vision 81/100, Vector 79/100; vector call cost **70,631 tokens** vs Vision 38,069.
- `VECTOR_BLOCK_RESEARCH_REPORT.md` — Track A conclusion: **B (hybrid Vector + Vision)**.

Reproduce Track A: `python -m experiments.stage_comparison_vector_blocks.run_research`
(`--reuse-descriptions` to skip re-extraction). Extraction is ~6.2 s/block, dense pages tens of seconds.

## Findings already established by the orchestrator (do not re-derive; you may falsify)

These are measured, not guessed. Reproduction commands are in `artifacts/orchestrator_findings.md`.

- **O1. 2 of 10 benchmark pairs compare a PDF against itself.** `ar_plan` and `ar_wall_sections`
  use v001 and v002 AR PDFs that are byte-identical (`sha256 3d7242cd5e72b326…` on both sides).
  The only left/right difference is a ~0.1 % bbox jitter. They test crop tolerance, not version change.
- **O2. The benchmark is class-imbalanced to near-uselessness.** Human verdicts: 1 IDENTICAL,
  6 NEAR_IDENTICAL, 2 STRUCTURE_SAME_VALUES_CHANGED, 1 STRUCTURE_CHANGED. A constant
  "always answer NEAR_IDENTICAL" baseline scores ≈6–7 of 10 against the comparator's 8. Recall on
  real engineering change is measured on **two** pairs.
- **O3. `anchors.confidence` is inverted.** In dense blocks every text is within 0.012 of *some*
  segment, so `high` is assigned exactly where the anchor is least informative
  (ar_plan 834 `high` / 2 `candidate`; ss_plan_dense 522/522 `high`), while sparse blocks where the
  nearest neighbour is actually unique get `candidate` (ss_simple_node 27 `candidate`, 0 `high`).
- **O4. Bare dimension numbers are dropped from both L2 projections.** `category` is
  `engineering_value` (regex hit) / `numeric` (bare number) / `label`. `dimensions` keeps only
  `engineering_value`, `labels` keeps only `label`. **1971 of 5427 spans (36 %) across the 20 blocks
  are `numeric`** — e.g. `2760`, `250`, `1150`, `4650` — and appear in neither list. `dimensions`
  meanwhile contains elevation marks like `-3,900` mixed with Ø/DN values; it is a regex bucket,
  not a dimension list.
- **O5. `repeated_elements` mostly fingerprints filled rectangles and resampling artefacts.**
  Top motifs are `filled_polygon` with 4 segments (ss_plan_dense: 154×) and `circle` with exactly
  24 segments — 24 is the extractor's own circle resampling constant, so every circle in the corpus
  collapses onto the same shape token modulo aspect rounding.
- **O6. `hatch_like_structures` saturates its cap of 30 in 6 of 10 blocks**, including
  `ss_table_graphic`, where the "hatch" is the table grid.
- **O7. `primitive_count` is PDF-packaging noise but still drives a user-visible difference line.**
  `comparator.differences` emits `Число примитивов: X → Y`, while the Track A report itself argues
  primitive packing must not be a signal. `ss_scheme_text_changed` has 39 primitives but 407
  connected components, because one PDF path holds many disjoint line commands.

## The question you are helping answer

Is `normalized geometry + generic topology + positioned text + anchors + repeated patterns` the right
universal backbone for П↔РД graphic comparison — or is a layer of **graphical objects + relation
graph** required between raw geometry and discipline profiles?

The end user is a Russian design-documentation expert. He must read output like
«Добавлены два ответвления», «Количество аппаратов 12 → 14», «Номинал 250 → 315 А»,
«Появился новый проём» — never «добавлено 37 line segments». Judge every representation by whether
that sentence can be derived from it *safely*.

## Corpus

Real documents live under `projects_v2/objects/<OBJECT>/disciplines/<DISC>/documents/<DOC>/versions/<vNNN>/02_work/document.pdf`.
Objects: `213_Mosfilmovskaya_31A_KingSons`, `214_Alia_ASTERUS`, `256_Primavera_K14_Spartak`,
`272_Sadovnicheskaya_76_Balchug_Esteyt`, `314_Sobytie_6_1_Donstroy`.
Block indexes (crops + `index.json`) may exist under a version's `_output/`; check before assuming.
PyMuPDF 1.27.2.2 is available as `fitz`.

## Output convention

Write to `experiments/stage_comparison_vector_architecture_opus/`:

- scripts → `probes/<your_prefix>_*.py`
- data/results → `artifacts/<your_prefix>_*.{json,md}`
- your findings summary → `artifacts/<your_prefix>_FINDINGS.md`

Your `*_FINDINGS.md` must open with a table: `claim | evidence (number/file) | confidence | falsifiable-by`.
Keep every prefix unique to you so parallel agents never collide.

## Late note for the Vision-verification branch (added by the orchestrator mid-run)

The smoke test exposed a confound that anyone scoring this branch must handle explicitly.

On case `vv009` — a **clean, unmutated** description — the verifier returned `PARTIAL`, flagging
claim C6 ("one repeating shape") because the crop plainly shows 15 camera symbols, 5 crossed-square
ОСПД symbols and 3 PoE label boxes. That is not a false alarm. It is the verifier **correctly
catching a real defect of v0.1** (finding O5 / ptn P1: `repeated_elements` fingerprints PDF paths,
not symbols, and reports one group of 15 where the sheet holds three distinct repeating symbols).

Therefore:

- **A "clean" control is not a correct description.** It is an unmutated v0.1 description, and v0.1
  descriptions are provably wrong in specific, catalogued ways (`artifacts/failure_modes.md`).
  A naive false-alarm rate computed against these controls measures the wrong thing.
- Score every PARTIAL/FAILED on a clean control into three buckets, by reading what the verifier
  actually said: **(a) genuine v0.1 defect correctly caught**, **(b) fact-sheet wording problem**
  (the claim was ambiguous, not wrong), **(c) real false alarm** (the verifier contradicts what the
  crop shows). Only (c) is a false alarm. Report all three counts with examples.
- If bucket (a) turns out to be large, that is itself a headline result: it means the Vision
  verifier is a usable *detector of extractor defects*, which is a stronger claim than "it validates
  descriptions".
