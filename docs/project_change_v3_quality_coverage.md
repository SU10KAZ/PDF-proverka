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

## Limits and configuration

Supplemental review is enabled by default and bounded to four batches of up
to eight primary unmatched pages. It can be controlled with:

- `PROJECT_COMPARISON_V3_UNMATCHED_REVIEW=0|1`
- `PROJECT_COMPARISON_V3_UNMATCHED_MAX_BATCHES=0..12`

Independent source verification is enabled by default and bounded to four
batches of up to eight parameter work items:

- `PROJECT_COMPARISON_V3_SOURCE_VERIFICATION=0|1`
- `PROJECT_COMPARISON_V3_SOURCE_VERIFICATION_MAX_BATCHES=0..12`

Unprocessed work remains explicit and makes the run `REVIEW`. A failed
supplemental or verification batch does not erase accepted ordinary Miner
results. The failure and unfinished work remain in the run artifacts.

The two new stages use the same provider/model/reasoning configuration as the
run. Model mixing is rejected. Development and contract tests use
`FakeProvider` and make no external model calls.

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
