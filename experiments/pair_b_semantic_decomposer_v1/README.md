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
