# ProjectChange versioned run storage V1

The authoritative store for a live run is
`comparison/sessions/<session>/pairs/<pair>/production/runs/<run_id>/`.
It contains the existing `project_change_v3_*.json` artifacts, immutable state,
`run_manifest.json`, `unresolved_hints.json`, the `project_change_v3/` workspace
(source packaging, provenance, attempts, checkpoints, dedupe), and `human_mapping/`.
Existing artifact filenames and semantic payloads are unchanged.

`production/current_run.json` is the sole default selection. The pair-level
`state.json` tracks the most recent attempt for progress/cancellation only.
It is NOT the selected result. A running or failed attempt cannot hide the
previous completed result. Catalog `is_current` derives from the pointer;
model, confidence and quality do not participate in selection.

The engine creates a directory exclusively for each new UUID. An existing ID,
even a failed one, cannot be reused. Engine writes use a scoped context reset
in a finally block; writes to terminal artifacts through the store are rejected.
Finalization persists result, mapping and HM first, validates identities and
required source hashes, fsyncs artifacts, writes and verifies the terminal
manifest, then atomically replaces and fsyncs the pointer. A pointer-write
failure leaves the old default; a completed unpublished run can be recovered
with `run_storage.select_current(session, pair, run)` after hash validation.
No public endpoint for arbitrary pointer changes is added in V1.

The existing V3 `REVIEW` status with reason `v3_completed` and a published HM
map means completed with unresolved hints. It is accepted into an immutable
`COMPLETED_FROZEN` manifest without modifying its state/result content.
`REVIEW` due to failed HM publication is not accepted. FAILED, CANCELLED,
PARTIAL, RUNNING and other incomplete attempts never change the pointer.

Reads without a run ID resolve current, or legacy pair-scoped files when no
pointer exists. A present but invalid pointer never silently falls back.
Explicit run reads never fall back to current. Supported APIs:

- `/api/stage-comparison/sessions/{session}/pairs/{pair}/runs/{run}/project-changes`
- Object ProjectChange feed with `session_id`, `pair_id`, `run_id` query fields.
- Evidence crop URLs carry the same explicit scope.
- HM page/API URLs accept `session_id` and `run_id`; default HM resolves current
  once per request, and the page pins that identity for subsequent requests.

Human reviews and BlockLink history append under the run's `human_mapping/`.
Its immutable input map is separate from editable review history. Identical
region/block IDs in different runs never share history. Isolated smoke writes
use `human_mapping_smoke/` under the selected run.

Sealed snapshots retain their content-derived `pcv3snap_*` result identities.
Catalog open routes pass `result_id` and pair. HM accepts that result ID,
validates the catalog's object/pair binding and uses the approved sealed map.
Existing snapshot-backed review storage stays under object/pair for V1; its
single approved source-identical fixture is the snapshot identity contract.
Future live runs use their own directories and cannot consume these decisions.
Sealed source files are never modified.

Legacy adoption verifies result/state/HM run and session ownership, copies
all V3 artifacts and the complete workspace/HM assets/history, verifies every
source and destination SHA256, and writes a migration receipt before publishing.
No legacy files are deleted or rewritten. Legacy content remains for immediate
rollback, but after the pointer exists it is no longer authoritative. If
ownership cannot be proven, adoption fails closed. The next runtime run also
adopts a completed legacy result before creating a new generation.

Rollback: switch to the previous immutable release and restart the backend.
Its legacy paths remain intact. New reviews written after migration remain in
the versioned HM history; an old backend will not display them. Preserve those
files and restore the versioned release to regain access; do not copy them into
another result. No schema migration, model configuration or provenance guard
changes are required.

Verification uses synthetic FakeProvider fixtures only. No inference, new real
comparison, Human Mapping anchors, validation or final holdout is involved.
