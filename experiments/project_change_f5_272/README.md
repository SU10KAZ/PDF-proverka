# F5: offline document-to-package integration

## F5 REPAIR V3: evidence boundary certificates

```sh
python -m experiments.project_change_f5_272.repair_v3 snapshot
python -m experiments.project_change_f5_272.repair_v3 source
python -m pytest -q experiments/project_change_f5_272/test_boundary.py \
  experiments/project_change_f5_272/test_pipeline.py \
  experiments/project_change_contracts_v3_272/test_allocation.py \
  experiments/project_change_contracts_272/test_witnesses.py \
  experiments/project_change_f2_binding_v4_272/test_binding.py
python -m experiments.project_change_f5_272.repair_v3 certify
python -m experiments.project_change_f5_272.repair_v3 audit
```

Default output: `fresh_dev_sample_f5_pipeline_v3/` under the established corpus
artifact root. Save the test JUnit XML as `TEST_RESULTS.xml` and a
`TEST_RECEIPT.json` with `status`, `passed`, `code` (the current `code_hashes()`),
`junit_sha256`, counts and test-file list before the audit. `certify` freezes packages; `audit`
only reads those packages. Every CLI action forbids network/model requests and
subprocesses. Source and certificate stages use the existing four-document DEV
allowlist. Previous V1/V2 artifact trees are read-only.

The `_source_rebuild/` directory contains two deterministic passes of the
unchanged V2 pipeline. The audit compares all 79 rebuilt pipeline packages and
both correspondence graphs against V2 exactly. The new top-level packages add
`boundary_certificates` and `boundary_completeness`; their nested F1 receipt,
evidence packet, F2/V4 state skeleton and F4 witnesses remain unchanged.
`evidence_artifact_root: "_source_rebuild"` identifies the root against which
nested raster paths must be resolved. Consumers must use the explicit V3
boundary coverage for V3 package completeness, while retaining the original F1
receipt for its original meaning.

`boundary.py` evaluates source-grounded TEXT/TABLE/GRAPHIC part proofs,
continuation, note relevance, graphic connectivity and mandatory requirements.
Only `BOUNDED_COMPLETE` with delivered, usable, correctly typed evidence can
satisfy a mandatory requirement. F1's existing allocation supplies mandatory
flags; optional evidence never becomes mandatory simply because it is missing.
Dependencies discovered inside mandatory evidence still block when relevant or
unresolved, including notes stored under optional F1 requirements.

`boundary_sources.py` grounds numbered text headings in both source OCR and
native bold PDF text, closes sections at a peer/ancestor heading, and verifies
all required native text spans against actual delivered quotes. It retains all
subject-bearing sections on the requirement page and checks intervening pages.
Native/OCR discrepancies and unsupported heading forms remain explicit gaps.
A raster alone does not prove legible text. Tables retain relevant explicit row
groups, repeated headers, broken rows, trailing notes and next-page checks;
unknown native cell correspondence prevents certification. Graphic analysis
records native paths crossing the proposed boundary and located mark labels;
without equipment-node associations it stays `UNKNOWN_BOUNDARY`, even when a
whole page is delivered. These limitations must be reported as pipeline defects,
not as objective absence of boundaries in the sources.

V2's TABLE **34/40 counts packages**, not requirements. V3 audits all **356 TABLE
requirements** and lists the six packages with no table payload. Delivery-budget
omissions are reported separately from source-policy blocks.

Artifacts include the requested route audits, certificates, mandatory package
coverage, complete-package audit, partial reason index, hash/leakage/test
receipts and `F5_V3_REPORT.md`. `ZERO_COMPLETE_DIAGNOSIS.json` gives a blocker
trace for every incomplete package if no package passes. A structural PASS with
zero complete packages is `F5_STILL_NEEDS_REPAIR`. Stop after the report; there
is no inference or deployment step.

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

## F5 V4: native subject and typed continuation refinement

V4 is an opt-in offline iteration. It retains the exact 79 tracking questions,
including the 12 in the boundary diagnostic manifest. These addresses are not
assertions of system identity. Native support is checked before correspondence
is re-evaluated; marks cannot determine identity. Unresolved questions receive
two minimal identity requests. No manual merging or subject splitting is used.

`subject_v4` retains canonical function, role, location, source labels and pages,
with unknown system/subsystem/consumer bindings explicit. `SUBJECT_PROVEN`
requires native binding of function, system, location and role; the initial
extractor conservatively emits POSSIBLE/UNRESOLVED. Ciphers in native title blocks
are checked against the admitted document. A mismatch retains its provenance but
the delivery loader suppresses all page payload.

`continuation_v4` separates text, table and graphic decisions and note relevance.
`requirements_v4` closes source dependencies before constructing F1 requirements.
`allocation_v4` retains the eight-raster / 28,000-character delivery mechanics,
but gives a required dependency its parent's priority and mandatory status.
Boundary decisions precede the unchanged F1 coverage evaluator, F4 raster
validator, F2 constructor and V4 reference resolver. Table cell mapping and
graphic topology checks are unchanged; UNKNOWN is not a completeness waiver.

Run each phase in a separate process:

```sh
python -m experiments.project_change_f5_272.repair_v4 snapshot
python -m experiments.project_change_f5_272.repair_v4 diagnostic-build
python -m experiments.project_change_f5_272.repair_v4 diagnostic-audit
python -m experiments.project_change_f5_272.repair_v4 all-build
# Write TEST_RECEIPT.json from the local pytest result before the final audit.
python -m experiments.project_change_f5_272.repair_v4 audit
```

The builder uses only admitted source files and a manifest projection containing
tracking IDs. `audit_v4` is imported only by post-freeze audit actions. It reads
the previous 12 diagnoses, never truth for the other 67. Algorithm hashes are
frozen with the diagnostic packages and checked before the all-package run.
Each delivery/certificate build runs twice from one fresh V4 source inventory,
whose hash-pinned cache is shared across these phases; the 12 must
also reproduce exactly in the 79-package run. Historical artifact trees are
read-only and their complete file hashes are compared with the initial snapshot.

Artifacts live in `fresh_dev_sample_f5_pipeline_v4/` alongside earlier runs.
Read `F5_V4_REPORT.md` for the measured acceptance result. Removing malformed
full-system requirements leaves an identity deferral, not an established system.
The final 12-case classification includes a post-freeze native-source review:
`DIAGNOSTIC_12_AUTOMATED_SCREEN.json` retains the initial mechanical screening;
`SOURCE_POST_REBUILD_REVIEW.json` records the stricter adjudication of lost
in-scope delivery, including gains/losses within the same package. This audit
does not modify the frozen builder, its package hashes, or the 79-package run.
`STRONG_DOWNGRADE_SOURCE_REVIEW.json` separately checks all three observed
STRONG downgrades against source rasters. Their graphical subjects are visible;
the native-literal filter did not justify those downgrades. This iteration fails
the STRONG preservation gate as well as the FALSE_PARTIAL and regression gates.
Tests passing does not satisfy the diagnostic gate if FALSE_PARTIAL does not
decrease. The CLI contains no inference client and rejects network/subprocess
operations, including after a successful rebuild.
