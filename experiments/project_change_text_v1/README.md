# TEXT → ProjectChange V1 (offline)

The product object is one engineering event with source evidence and internal
fact deltas. This experiment does not import into production, add feature flags,
modify Table V3 / Sheet Matcher, or implement a graphic comparator.

Pre-implementation `approaches` compares entity-first, local event-first and a
guarded hybrid on explicitly constructed grouping truth. The chosen hybrid uses
existing LineLedger / TextSection paragraph materialization, conservative TEXT
purity quarantine, bounded sentence templates, lightweight entity references,
replacement hierarchy and same-event/state deduplication. No SectionRelation is
required. A stable mark or adjacent identical local content can corroborate scope.

Evidence carries version, page, block, line hashes and immutable source receipts.
Unknown old scope, competing states and ambiguous entities stay REVIEW. Unmatched
old text does not establish equipment removal. No model/provider is called.

```sh
python -m unittest experiments.project_change_text_v1.test_engine
python -m experiments.project_change_text_v1.approaches
python -m experiments.project_change_text_v1.run freeze
python -m experiments.project_change_text_v1.run run --name run1
python -m experiments.project_change_text_v1.run run --name run2
python -m experiments.project_change_text_v1.run replay
```

The existing project dependency `jsonschema` is required. This audit installed it
only into the output directory's `deps`; add that directory and this repository to
`PYTHONPATH` when using the bare system Python.

Output root:
`/home/coder/auditmanager/corpus-audits/20260913_project_change_text_v1/`.

The JSON Schema admits future TEXT/TABLE/GRAPHIC evidence and conflict records.
The current producer validates TEXT-only evidence and emits no conflicts.
Reports distinguish constructed DEV metrics, frozen corpus predictions and the
subsequent audit. Unannotated corpus quality is not reported as zero errors.

## Final research result

Three architecture iterations, then stopped. Verdict **B**: grouping works on the
tested evidence, but TEXT alignment/purity abstention still limits coverage.
Full readiness is **BLOCKED**, not claimed as validated. The final corpus has
78 candidates (6 PROVEN, 72 REVIEW). Eight detected paired fact differences form
seven paired events; the remaining 71 candidates have no established old scope.
All six PROVEN events were checked against local OLD/NEW source rasters by the
coding agent, not an independent blind reviewer. Three repeated REVIEW quote
groups still need identity review. Twenty-five constructed tests pass and all
70 deterministic run artifacts replay identically.

The final source audit and engineer-facing report are reproducible with
`python -m experiments.project_change_text_v1.audit` after the two frozen runs
and the recorded `quality_audit/AGENT_ASSESSMENT.json` exist. A prepared 48-case
packet contains no candidate decisions; its key is kept separately. No external
annotation request was sent. Earlier candidate runs and failures remain archived.
