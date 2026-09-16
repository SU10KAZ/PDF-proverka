Offline Pair A post-inference repair V2, object 272, АР1,
pair key ad0a31a342a666082f2ef66a, OLD stage_1 → NEW stage_2.

Artifact directory:
/home/coder/auditmanager/corpus-audits/20260914_project_change_272/fresh_dev_pair_a_post_inference_repair_v2/

The 18-row RAW_VERDICT_BLOCKER_LEDGER.json and INPUT_FREEZE.json were written
before repair implementation. Only saved raw responses, saved packages and V1
normalized results informed applicability. Live V2 and Repair V1 remain unchanged.

The repair adapter calls the unchanged V1 normalization/binding, then applies
claim applicability and NumericConflictGuard, recomputes F2 and uses the existing
V4 admission and existing resolve_ownership grouping. It is an opt-in experiment;
no production caller or F1/F4/F5 code was changed.

Primary evidence retains V1 locator/identity/scope/provenance validation.
Rejected auxiliary references never become bound proof. OTHER normalization
retains the original role and maps only explicit response semantics.
Numeric conflicts keep printed, derived and claimed values separately.

Tesseract 5.5.0 was unpacked under /tmp/pair_a_v2_ocr (not system-installed).
Local OCR inspected only already delivered raster crops at saved witness
locators. Table OCR and isolated-digit checks must agree. Numeric extraction
probes remain in local_ocr/ for traceability. One accepted extracted fact is
78.61, versus saved row sum and model claim 78.60. No new semantic/provider
calls occurred. The numeric guard has explicit row/total syntax coverage,
not arbitrary numeric prose certification.

85 synthetic/regression tests passed, then one 52-response replay was frozen.
Seven NO_CALL_MISSING packages stayed REVIEW. Frozen source audit was opened
only after REPLAY_RESULT_FREEZE.json. No source truth, admission rule, or replay
result was changed after audit. Actual accepted graphic references were also
checked against the saved invocation image input hashes.

Result: 9 ACCEPT / 50 REVIEW / 0 NOT_CHANGE; 9 ProjectChanges.
Frozen audit: 9 correct ACCEPT, 0 strict false ACCEPT. Three correct V1 ACCEPTs
preserved. One independent group fully accepted, five at least partially
covered, ten missed; four post-output supplements kept separate.

Overall status: PAIR_A_POST_INFERENCE_REPAIR_V2_COMPLETED_REPAIR_REQUIRED.
All ten necessary safety gates pass, but V2 introduces false applicability
blockers for two former NOT_CHANGE cases. Direct multi-value/layered states
can fall through to a basis/witness requirement; per-reference raster coverage
is too strict for mixed primary-reference sets. These defects are explicitly
reported in POST_REPLAY_REGRESSION_AUDIT.json and FINAL_REPORT.md.
Do not represent this iteration as PASS or ready for F5 miss repair.
The requested stop rule was observed: no post-audit tuning or second replay.

Synthetic checks (safe independently of the frozen replay):

    python -m unittest experiments.project_change_post_inference_repair_v2_272.test_repair experiments.project_change_post_inference_repair_272.test_repair experiments.project_change_f2_binding_v4_272.test_binding experiments.project_change_contracts_272.test_comparability experiments.project_change_contracts_272.test_states

The completed replay and evaluation are immutable and reject reruns.
A future repair must use a new iteration and preserve prior exposure.
