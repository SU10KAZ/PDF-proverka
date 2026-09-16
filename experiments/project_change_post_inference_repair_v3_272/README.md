# Pair A post-inference repair V3

Offline applicability cleanup for object 272, logical v002, Pair A АР1
`ad0a31a342a666082f2ef66a`. No providers, new evidence, OCR or model calls.
The four remaining raw ACCEPTs were recorded in an immutable ledger before
code changes. One replay followed 113 passing local tests, then the frozen
source audit was opened for evaluation only.

Artifacts:
`/home/coder/auditmanager/corpus-audits/20260914_project_change_272/fresh_dev_pair_a_post_inference_repair_v3/`

Result: **POST_INFERENCE_STILL_NEEDS_REPAIR**.

- 11 correct ACCEPTs (all 9 V2 ACCEPTs preserved), 0 false ACCEPT.
- 2 scoped NOT_CHANGE restored, 0 false NOT_CHANGE in the frozen audit.
- 46 REVIEW including 7 NO_CALL_MISSING; 11 existing-resolver ProjectChanges.
- Remaining raw ACCEPT/REVIEW: numeric conflict 78.60/78.61 and mixed unknown basis.
- Independent REAL: 2/15 fully accepted, 5/15 at least partly covered, 10/15 missed.
- 3/4 post-output supplements accepted, outside the denominator 15.

Witness requirements now carry explicit reasons. Fully bound graphic primary
proof need not have a second witness. Explicit component arithmetic can satisfy
its own supporting requirement. Auxiliary rejected counter references never
become accepted bindings. Negative sufficiency has its own local scope and
claim-dependent graphic/basis requirements. V1 binding, V2 numeric safety and
existing V4 admission/grouping are reused.

Remaining applicability defect: a graphic negative with mixed primary raster
and additional text citations still inherits V2's per-reference raster
requirement. The staircase no-change remains REVIEW. This fails success gate 8;
no READY_FOR_F5_MISS_REPAIR is claimed. Scope binding and ambiguous OTHER
limitations on two further negatives remain unchanged.

The replay's initial `no_new_binding` check compared in-memory tuples against
JSON lists. `STRUCTURAL_AUDIT_FINAL.json` compares persisted arrays and confirms
identical V2/V3 bindings. Initial audit, frozen replay and frozen code are
preserved. `POST_INFERENCE_REGRESSION_AUDIT_FINAL.json` completes the preliminary
regression audit after truth evaluation; consult these final addenda and
`FINAL_REPORT.md`, not the preliminary audit in isolation.

Entry points (the completed replay is immutable and refuses to run again):

```text
python -m unittest experiments.project_change_post_inference_repair_v3_272.test_repair
python -m experiments.project_change_post_inference_repair_v3_272.replay
python -m experiments.project_change_post_inference_repair_v3_272.evaluation
```

Replay prohibits network/process calls and seals truth before result freeze.
Evaluation never invokes repair or changes any admission result. All prior
artifact manifests, raw hashes, bindings, code/result freeze and actual graphic
invocation image hashes were verified. Pair B, validation, final holdout and
production were not run or changed. No F5 or grouping repair was attempted.
