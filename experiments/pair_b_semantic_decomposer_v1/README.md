# Pair B semantic decomposer V1

Isolated source-only preparation for the user-requested experiment. This version
reached the explicit **STOP if broad bundles > 12** rule before inference.
It does not implement or claim a completed decomposition/comparison run.

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
