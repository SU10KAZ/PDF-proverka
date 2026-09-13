# TEXT OLD scope recovery V1

Offline recovery of OLD context for the HIGH-priority cohort of frozen TEXT
ProjectChange reviews. The baseline schema, constructor and grouping code are
protected by hashes; this experiment does not edit production entry points.

Artifacts:
`/home/coder/auditmanager/corpus-audits/20260913_text_old_scope_recovery_v1`.

Final result: **PARTIAL**, PROVEN/REVIEW **6/72 → 10/66**. Of 37 HIGH reviews,
six resolve: four new events, one narrative evidence attachment and one unchanged
statement. The implementing agent audited all four accepted new events against
PDF raster; no false event was found. This is not independent blind validation.

The raw experiment inherited two correction-table cells mislabeled TEXT. One
had contributed to a raw promotion; both are vetoed by the source audit. The
same pump replacement has an independent narrative witness and remains proven.
The two original candidates remain REVIEW with explicit source-purity reasons.
No accepted PROVEN event uses their evidence. Source classification needs a
separate repair before production readiness can be claimed.

The candidate used two major iterations. Actual AI usage was 45 bounded calls,
with 29 response replays in iteration 2. “Local AI” means local evidence context
sent to the configured remote provider, not on-device inference.

Reproduce the audited delivery without provider traffic:

```sh
export PYTHONPATH=/home/coder/auditmanager/corpus-audits/20260913_project_change_text_v1/deps:$PWD
python -m unittest experiments.project_change_text_v1.test_engine experiments.text_old_scope_recovery_v1.test_recovery
python -m experiments.text_old_scope_recovery_v1.run verify
python -m experiments.text_old_scope_recovery_v1.integrate --name audited
python -m experiments.text_old_scope_recovery_v1.report
```

The pinned baseline dependencies are required. `report` verifies candidate
hashes, revalidates saved responses and checks deterministic integration before
packaging audit outcomes. Provider proposals and original raw outputs remain
unchanged. Audit annotations are external artifacts in `quality_audit/`, with
delivery hashes; they are not model-training labels or human truth.

`prepare`, `freeze` and `local_ai` are research-run commands, not closeout steps.
Do not overwrite a frozen run or launch paid calls to reproduce reports.

Read `reports/OLD_SCOPE_SCORECARD.md`, `reports/QUALITY_AUDIT.md` and
`reports/PROJECT_TEXT_CHANGES_RECOVERED.json` for the final results. The frozen
candidate manifest preserves pre-audit route assumptions; the delivery manifest
records the final artifact hashes and the discovered purity failures. No push,
deployment or production configuration change belongs to this experiment.
