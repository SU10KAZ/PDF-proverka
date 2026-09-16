# Pair B semantic decomposer V1

Isolated source-only implementation for the user-requested experiment. The
initial preflight reached the explicit **STOP if broad bundles > 12** rule; a
later authorized continuation completed and froze decomposition. Comparison has
not been run.

The fixed grouping policy is one existing broad retrieval seed per context,
expanded to every indexed region on the seed's referenced pages. Seed headings
have no authority to declare a final engineering subject or a change claim.
No source truth, saved model responses, baseline verdicts or known finding IDs
are used. Modalities are never required to match across document sides.

Run once, from the repository root:

```sh
python -m experiments.pair_b_semantic_decomposer_v1.preflight
```

Output: `/home/coder/auditmanager/corpus-audits/20260914_project_change_272/pair_b_semantic_decomposer_v1/`.
Existing output is immutable; the command refuses to overwrite it. The source
guard admits only DEV pair 8. After admission, an explicit file allowlist rejects
other corpus reads, baseline writes and network/subprocess calls. V4 frozen code
hashes are checked without loading Pair A source evidence or executing inference.

The inventory exposes empty contexts and uncovered source pages. Raw PDF page
locators and verified raster references are availability, not model delivery.
At a pre-inference STOP, later result freezes, XLSX, ProjectChanges and evaluation
files are deliberately absent. Their absence is recorded in the final report.

## Frozen candidate condensation V2

After the decomposition result was frozen, its 2,086 candidates were condensed
without model calls or source-truth access:

```sh
python -m unittest experiments.pair_b_semantic_decomposer_v1.test_candidate_condensation_v2
python -m experiments.pair_b_semantic_decomposer_v1.candidate_condensation_v2
```

The second command is run-once and refuses to overwrite its output at
`candidate_condensation_v2/` below the research output directory. It validates
the frozen inputs, preserves every original candidate, writes exclusive primary
dispositions plus overlapping diagnostic queues, audits comparison package
readiness, and stops without comparison when the ready count exceeds 50.

## Semantic Consolidator V1

This isolated substage reads only the 154 frozen ready rows. It uses one fresh
`gpt-6-astra xhigh` PASS A call per broad context and one PASS B call over the
frozen PASS A groups. Deterministic audits enforce exact coverage and evidence
unions, missing-raster blockers, lineage, and the no-new-values boundary. It
stops before local comparison and never opens truth, validation, or holdout.

```sh
python -m unittest experiments.pair_b_semantic_decomposer_v1.test_semantic_consolidator_v1
python -m experiments.pair_b_semantic_decomposer_v1.semantic_consolidator_v1 prepare
python -m experiments.pair_b_semantic_decomposer_v1.semantic_consolidator_v1 pass-a
python -m experiments.pair_b_semantic_decomposer_v1.semantic_consolidator_v1 freeze-a
python -m experiments.pair_b_semantic_decomposer_v1.semantic_consolidator_v1 pass-b
python -m experiments.pair_b_semantic_decomposer_v1.semantic_consolidator_v1 finalize
```

## Consolidated local comparison V1

The frozen 80-group result is compared without further consolidation. Six
groups whose required graphic raster is missing remain explicit no-call rows;
the other 74 use isolated `gpt-6-astra` / `xhigh` Codex contexts. The inference
freeze is created only after the implementation is committed. The result freeze
must exist before any source-truth evaluation is opened.

```sh
python -m experiments.pair_b_semantic_decomposer_v1.consolidated_local_comparison_v1 prepare
python -m experiments.pair_b_semantic_decomposer_v1.consolidated_local_comparison_v1 freeze
python -m experiments.pair_b_semantic_decomposer_v1.consolidated_local_comparison_v1 run
python -m experiments.pair_b_semantic_decomposer_v1.consolidated_local_comparison_v1 downstream
```

V2 is a clean restart after V1 correctly stopped on shortened atomic IDs. It
changes only their model-facing representation to deterministic group-local
aliases and expands those aliases back to exact frozen IDs before the unchanged
post-inference path.

```sh
python -m unittest experiments.pair_b_semantic_decomposer_v1.test_consolidated_local_comparison_v2
python -m experiments.pair_b_semantic_decomposer_v1.consolidated_local_comparison_v2 prepare
python -m experiments.pair_b_semantic_decomposer_v1.consolidated_local_comparison_v2 freeze
python -m experiments.pair_b_semantic_decomposer_v1.consolidated_local_comparison_v2 run
python -m experiments.pair_b_semantic_decomposer_v1.consolidated_local_comparison_v2 downstream
```
