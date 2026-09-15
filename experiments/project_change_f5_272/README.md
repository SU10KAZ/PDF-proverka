# F5: offline document-to-package integration

## F5 REPAIR V2

The repair writes a **new** `fresh_dev_sample_f5_pipeline_v2` directory. The
original 53 frozen packages and all other V1 artifacts remain byte-identical.

```sh
python -m experiments.project_change_f5_272.repair_v2 snapshot
python -m experiments.project_change_f5_272.repair_v2 build
python -m experiments.project_change_f5_272.repair_v2 audit
```

Run the tests below plus `project_change_contracts_v3_272/test_allocation.py`,
`project_change_contracts_272/test_witnesses.py` and
`project_change_f2_binding_v4_272/test_binding.py`. Save their JUnit receipt to
`TEST_RESULTS.xml` and a `TEST_RECEIPT.json` with status, counts and code hashes
in the V2 directory before the post-freeze audit.

V2 removes metadata-only source IDs from delivered evidence and V4 manifests.
Their original requirements and budget omissions remain visible. It resolves
coverage by requirement and region, and records actual TEXT/TABLE/GRAPHIC
payload availability separately from semantic completeness. A graphic needs a
raster; a table heading alone does not deliver table rows.

The correspondence graph retains its original broad context candidates and all
edges. Mixed-confidence components additionally expose their exact-scope STRONG
subcomponents, with a link to the parent context. These packages overlap; they
are retrieval scopes, not independent engineering changes. UNKNOWN, disjoint
scopes and source-form mismatch do not become STRONG. The 80-package gate and
F1/F2/F4/V4 contract implementations are unchanged.

`F5_REPAIR_V2_REPORT.md` is the new report. `STRUCTURAL_AUDIT.json` verifies
every package seal, requirement/region link, V4 binding and delivered raster
after freezing, and compares the old artifact tree against its initial hashes.
`ROUTE_DELIVERY_AUDIT.json` records every requested route and actual payload.
Structural PASS does not imply COMPLETE or semantic truth: unverified boundaries
and frozen quarantines remain explicit blockers.

## Original F5 build

Run from the repository root:

```sh
python -m pytest -q experiments/project_change_f5_272/test_pipeline.py
python -m experiments.project_change_f5_272.run
```

The executable admits **only** frozen DEV pairs 2 and 8 through the existing
whole-cipher split/source guard. It never invokes an inference client. A process
audit hook rejects network connections, subprocesses and corpus reads outside
the four admitted source documents and the new output directory. Source-only
metadata and source hashes are verified by the existing guard before that hook.

## Components

- `document_inventory`: PDF native text/geometry plus the semantic foundation
  ledger and source OCR. Every physical page has an entry. Embargo, history and
  front matter are explicit exclusions. History classification is source-only;
  historical audits/model outputs are never inputs.
- `subject_discovery`: document-local engineering function and named scope
  groups. Marks stay within groups. The vocabulary is generic and is not learned
  from the selected pairs or expected changes. Unrecognized regions are retained.
- `subject_correspondence`: functional/scope candidate graph. Components support
  1:1, 1:N, N:1 and N:M. Unknown scope is POSSIBLE; disjoint explicit scopes stay
  unresolved. Connected groups do not certify a semantic transformation.
- `requirement_builder`: automatic F1 requirements for every candidate region,
  OLD context, NEW confirmation, and notes. An unlocated counterpart uses the
  contract's page-1 sentinel with an empty candidate-page list and no evidence.
- `end_to_end_package_builder`: existing F1 v3 allocation/coverage, F4 raster
  validator, F2 typed state contract and V4 binding resolver. Eight images and
  28,000 text characters per package; omissions remain visible.
- `contract_adapters`: the only glue. No F1/F2/F4/V4 source or semantics change.
  The existing delivery function deduplicates page images; the adapter preserves
  all per-region types and locator links. The V4 resolver receives an explicit
  discovery-reference manifest, tagged as **not a model response**. Only subject
  identity, scope and context are linked. State values and conditions are unknown;
  there are no value or change-observation claims. No semantic normalizer or
  ProjectChange admission is run before inference.

## Completeness and reproducibility

The initial retriever does not certify semantic section/table/diagram boundaries.
It deliberately passes `full_page=False` to F1 when that boundary is unknown,
even when a whole-page raster exists. This means PARTIAL evidence is expected.
COMPLETE is never inferred from page presence. F4 acceptance certifies mechanical
image/locator delivery only; witness semantic validity stays false.

`MODEL_READY_PACKAGE` means a serializable model input with explicit evidence
gaps. It is distinct from the success status `READY_FOR_FRESH_INFERENCE`.
The latter also requires the full-content and delivery gates. Existing embargo
and source-history quarantines can therefore prevent a successful full-document
acceptance without preventing a useful package from being produced.

All subject, candidate and package IDs use canonical hashes. Generated raster
paths are relative to the artifact root; original source receipts remain absolute
and hash-pinned. The run repeats extraction, discovery and package construction,
checks identical output, then seals `PACKAGES_FREEZE.json`. A changed existing
artifact is rejected. A structural audit must not mutate frozen packages.

The 80-package gate runs before rendering/building packages. It never drops
candidates, reselects pairs or changes discovery granularity based on answers.
Default artifacts: `/home/coder/auditmanager/corpus-audits/20260914_project_change_272/fresh_dev_sample_f5_pipeline/`.
