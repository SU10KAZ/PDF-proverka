# ProjectChange V3.7: coverage and source quality

V3.7 keeps the Mapper, ordinary Miner and Dedupe contracts unchanged. It adds
four generic layers around them:

1. `project_change_v3_coverage.json` separates page classification from actual
   content processing. Every OLD/NEW page is exactly one of mapped/unmatched
   and independently one of pending/partial/analyzed/excluded/failed.
2. `project_change_v3_supplemental_results.json` stores bounded analysis of
   meaningful unmatched pages. Opposite-page candidates are selected locally
   from source text. A missing counterpart creates a question, not a claim of
   physical addition or removal.
3. `project_change_v3_quality.json` records source binding for every claim,
   exact identities for hints, non-destructive cross-region resolution
   candidates and parameter verification work items. Model confidence never
   means independently verified.
4. `project_change_v3_verification_results.json` stores independent reads of
   parameters whose declared value is absent from structured source data or
   whose authoritative route is graphical. Corrections and conflicts remain
   visible review items; the original generated card is preserved for audit.

All artifacts are run-scoped, hashed into the immutable run manifest and
bound to the same source PDF hashes. Historical runs remain readable; missing
V3.7 artifacts mean “not evaluated by this layer”, never “complete”.

The implementation is independent of object ID, discipline, sheet number,
equipment name and unit. The same contracts apply to TEXT, TABLE and GRAPHIC
evidence.

## Parallel review and workload limits (3.16.0)

Both review stages run independent provider calls concurrently, with a separate
per-stage limit of 1..10 (default 10):

- `PROJECT_COMPARISON_V3_UNMATCHED_CONCURRENCY`
- `PROJECT_COMPARISON_V3_SOURCE_VERIFICATION_CONCURRENCY`

A free slot immediately takes the next batch; the scheduler does not wait for a
whole wave. Each item owns a provider copy and the run/model/cancel context.
Validation, progress and artifact writes run on the coordinator thread. Results
are merged in planned batch order, regardless of completion order. Dedupe waits
for supplemental review, and source verification waits for Dedupe. Separate
pairs in the batch queue still run sequentially; this is not a global provider
limit across independently launched comparisons.

Concurrency and the amount of work are independent settings. Both stages now
plan **all eligible batches by default**, rather than only the first four:

- `PROJECT_COMPARISON_V3_UNMATCHED_REVIEW=0|1` (default 1)
- `PROJECT_COMPARISON_V3_UNMATCHED_MAX_BATCHES=all|<nonnegative integer>`
- `PROJECT_COMPARISON_V3_SOURCE_VERIFICATION=0|1` (default 1)
- `PROJECT_COMPARISON_V3_SOURCE_VERIFICATION_MAX_BATCHES=all|<nonnegative integer>`

The max-batches default is `all` (stored as JSON null). Explicit numeric caps
continue to apply: `4` limits work to four batches, and `0` schedules no calls.
Each supplemental batch still contains up to eight primary unmatched pages;
each verification batch still contains up to eight parameters. For example,
69 verification work items require nine batches, with no unplanned remainder
under the default. More complete coverage can consume more tokens than the old
four-batch cap. Pages without an eligible counterpart candidate and failed or
explicitly capped work remain pending; scheduling all work is not a guarantee
that the model can resolve every question.

Supplemental calls receive a frozen list of existing subjects from the completed
ordinary Miner. They do not see each other's newly generated cards, even when
concurrency is set to 1. All accepted answers are merged in batch order and go
through the existing global Dedupe step. This input policy is recorded as
`unmatched_existing_subjects_policy=frozen_after_mining` in provenance.

Each received answer (accepted or rejected, including a provider failure's
available response) is saved immediately as an internal run artifact under
`review_attempts/<run_id>/<stage>/<batch_id>.json`. It records input and prompt
hashes, the transport receipt, validation status and raw response. References
and file hashes appear in provenance `review_attempts`. These artifacts do not
automatically resume a run and are not published as comparison results.

Provider/schema failures remain batch failures: other scheduled review work can
complete and the unfinished work remains explicit in the published review.
Cancellation, failure to persist an answer, or failure to persist progress stops
new admissions. Already submitted answers are drained and saved before the run
fails; subsequent stages do not start and no result is published for that run.

The stages use the same provider/model/reasoning configuration as the run.
Development and concurrency tests use synthetic PDFs and local fake providers;
no external inference is needed.

## UI behavior

Stage 3 displays a warning when pages remain pending, unmatched content was not
reviewed, source verification found corrections/conflicts, or parameter checks
remain unfinished. A verification correction becomes an unresolved conflict
on the affected ProjectChange. Human Mapping and prelinks are not gates for
starting analysis.

## Meaning of statuses

- `MAPPED` says where a page was classified; it does not say its content is
  correct or complete.
- `ANALYZED` says an accepted stage explicitly handled the page.
- `BILATERAL_SOURCE_BOUND` says evidence addresses exist on OLD and NEW; it is
  not an accuracy verdict.
- `CONFIRMED` in source verification is an independent repeat read of the
  selected parameter. It does not replace engineer approval.
- `CORRECTED`, `CONFLICT`, `UNREADABLE`, `FAILED` and `PENDING` always keep the
  run in review and remain visible.
