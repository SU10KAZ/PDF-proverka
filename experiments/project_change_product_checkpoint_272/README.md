# ProjectChange product checkpoint — research only

Standalone static preview of frozen Pair A V4 and, if preflight passes, a fresh
Pair B run. Production files, routes and all inference components stay unchanged.
Artifacts and release live outside the checkout:
`/home/coder/auditmanager/corpus-audits/20260914_project_change_272/projectchange_product_checkpoint_pair_a_b/`.

Preview: `http://127.0.0.1:8774`. On another machine forward port 8774 over SSH.

```sh
python -m experiments.project_change_product_checkpoint_272.snapshot
python -m experiments.project_change_product_checkpoint_272.serve --directory /home/coder/auditmanager/corpus-audits/20260914_project_change_272/projectchange_product_checkpoint_pair_a_b/release/web --port 8774
node experiments/project_change_product_checkpoint_272/smoke.cjs A
python -m experiments.project_change_product_checkpoint_272.check_components
```

`pair_b preflight` requires Pair A browser PASS and admits only DEV pair 8 using
the existing access guard. The exact V6 inventory and V7 overlays are reused.
The Pair A live model input function, serializer, provider and prompt are imported
unchanged. All post-inference V4 code hashes and runtime configuration are checked.
Every structural/serialization rejection stops the entire run; no package is
silently omitted, truncated or regenerated. The freeze records the exact rejected
input sizes even when canonical request bytes cannot legally be produced.

Commit all checkpoint code before preflight. After the first call do not change
code, inputs or admission. `pair_b run` refuses failed preflight, drift, retries
and automatic resumption. On any error STOP and report; never repair mid-run.
OpenRouter and Claude are not invoked. NumericConflictGuard and the V2 local OCR
extractor are reused; only their output location is rebound. Grouping is unchanged.

The UI projects frozen system output only. It never consumes source truth.
Accepted rows use authoritative ProjectChange groups; REVIEW and NOT_CHANGE have
separate tabs. Diagnostic details are off by default. OLD/NEW evidence types are
independent; images are verified against successful invocation receipts and saved
image bytes. PDF page fragments refer to each side's hash-verified PDF.

The server binds to loopback and exposes only files in the explicit web snapshot,
never the corpus root or reports. It must be restarted if a new Pair B snapshot
adds assets. A failed preflight is displayed as unavailable, never as zero changes.
The component suite uses V4's existing corrected raw-response count assertion from
`final_audit.py`; the superseded 156-versus-52 assertion is preserved unchanged.

Capacity follow-up (explicit user authorization): `capacity.py` measures all 18
eligible requests without inference. Its receipt distinguishes the 272,000-token
runtime window (95% effective) from API limits and from the local character guard.
Tokenizer counts are estimates; the admission audit doubles text tokens, counts
full original image patches, reserves 128,000 output/reasoning tokens, and retains
more than 15,000 further tokens. Backend body hard maximum is unpublished, so the
body check also requires B's full estimated envelope to be smaller than image
bytes alone in an already successful identical-runtime Pair A request.

After the audit passes, the only serializer change is the local text guard:
60,000 -> 83,000. Canonical payload bytes remain identical for every existing
request; all 18 B payloads must match the independent audit reconstruction.
`capacity_run preflight` creates a new immutable `pair_b_capacity_check/run_01`
generation, preserving the failed checkpoint. `capacity_run run` performs the
single-use run through the original orchestrator; downstream and UI projection
are unchanged. No automatic retry or repair is permitted after the first call.
