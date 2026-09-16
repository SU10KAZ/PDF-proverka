# Pair A post-inference repair V4

**PAIR_A_POST_INFERENCE_REPAIR_V4_COMPLETED_PASS — READY_FOR_F5_MISS_REPAIR**

Object 272, logical v002, АР1 `ad0a31a342a666082f2ef66a`. One mechanical replay
of 52 saved responses, zero model/provider/OCR calls. No Pair B, reserves or
production changes. Post-inference admission is frozen for the next stage.

Artifacts:
`/home/coder/auditmanager/corpus-audits/20260914_project_change_272/fresh_dev_pair_a_post_inference_repair_v4/`

- 11 correct ACCEPT preserved; 0 false ACCEPT.
- 3 correct scoped NOT_CHANGE (previously 2); 0 false NOT_CHANGE.
- Numeric 78.60/78.61 and mixed unknown calculation basis remain REVIEW.
- 45 REVIEW including 7 NO_CALL_MISSING; 11 unchanged ProjectChanges.
- Independent REAL: 2/15 full, 5/15 at least partial, 10/15 missed.
- 146 distinct tests PASS after the reporting correction described below.
- Witness, negative contract, graphic applicability, role-scoped modality,
  structural and raw integrity checks PASS. Grouping quality NOT_EVALUABLE;
  existing resolver output equals V3 exactly.

`modality.py` projects existing V1 bindings into EvidenceRoleRequirement records.
Geometric primary requires validated OLD and NEW raster locators. Nonvisual
state citations with an explicit validated identity/scope binding can instead
be supporting citations. Unclassified state citations remain primary, and
graphic intent is retained if delivery is missing or invalid. A citation alone
cannot prove geometry. The existing F4 raster validator is unchanged.

The isolated applicability/repair adapters retain V3 logic; only graphic
requirement propagation and role/witness modality records change. V1 binding,
V2 numeric safety, V3 calculation rules, F2, admission and grouping are reused.
All saved states, bindings and provenance equal V3. General synthetic tests
preceded inspection of the staircase regression; its case ID appears only in
the post-replay regression report, never in admission rules.

The initial reporting assertion counted all 156 raw-format files as responses:
52 JSON answers, plus 52 TXT and 52 JSONL representations. All hashes matched,
but the count assertion failed. The initial failure, code and artifacts remain
immutable. `final_audit.py` corrects only that saved-output assertion; no second
replay, verdict change or rule tuning occurred. Authoritative final artifacts:

- `FINAL_REPORT_VERIFIED.md` (supersedes preliminary `FINAL_REPORT.md`).
- `SUCCESS_GATE_FINAL.json` and `RESULT_RECEIPT_FINAL.json`.
- `POST_INFERENCE_FINAL_REGRESSION_VERIFIED.json`.
- `TEST_RECEIPT_FINAL.json`: 138 pre-replay + 8 saved-output checks.
- `FINAL_MANIFEST_VERIFIED.json`, including all retained original artifacts.
- `PAIR_A_REPAIRED_V4_SYSTEM_OUTPUT.xlsx`: 59 output rows, ACCEPT/NOT_CHANGE detail.

Synthetic tests (safe to rerun without corpus admission):

```text
python -m unittest experiments.project_change_post_inference_repair_v4_272.test_repair
```

Replay and evaluation entry points are single-use and refuse to overwrite
their completed artifacts. The frozen `test_saved.py` retains the original
count error as historical evidence; its corrected hash/count assertion is
`final_audit.RawResponseIntegrityTest`. Input/network/process guards were active
during replay; source truth was used only for post-freeze evaluation. Prior DEV
exposure is acknowledged. All live V2 and V1/V2/V3 repairs were preserved.
