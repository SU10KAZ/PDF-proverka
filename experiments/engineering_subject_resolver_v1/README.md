# Engineering Subject Resolver V1 (offline)

Shared, evidence-bearing TEXT/TABLE identity experiment. Observation IDs are
version-local; relation certificates establish cross-version correspondence.
Models, scalar values and row ordinals are not identity. The existing
ProjectChange contracts/consumers and Table V3 producer are imported read-only.

Artifacts are under
`/home/coder/auditmanager/corpus-audits/20260913_engineering_subject_resolver_v1/`.
Read `CHECKPOINT.md`, `FALSE_IDENTITY_AUDIT.md` and `SCORECARD.json` there.
Three architectures were evaluated in three frozen development iterations.
Verdict B: partial, not ready for unattended production operation. The final
source audit veto is part of the reviewed delivery; raw provider decisions
remain available and the rejected scope certificate is counted explicitly.

Network-free replay:

```sh
export PYTHONPATH=/home/coder/auditmanager/corpus-audits/20260913_project_change_text_v1/deps:$PWD
python -m unittest experiments.engineering_subject_resolver_v1.test_resolver experiments.engineering_subject_resolver_v1.test_bridge experiments.project_change_text_v1.test_engine experiments.text_old_scope_recovery_v1.test_recovery experiments.table_project_change_v1.test_engine experiments.table_project_change_v1.test_source
python -m experiments.engineering_subject_resolver_v1.evaluate --final
```

`source` creates a new frozen input inventory and refuses to overwrite one.
`local_ai` is a paid research command, not needed for replay. It sends bounded
identity-only excerpts to the existing configured provider with the repository
paid-API guard and content-addressed cache. Credentials are not logged.

`AUDIT_ANCHORS.json` contains traceable implementing-agent assessments, not
human truth or ID-specific resolver rules. `evaluate` preserves raw proposals,
applies general collision checks and those audit vetoes, verifies source
receipts/cells/schema, and replays the frozen consumers. No graphic producer,
route fusion, production flags, deployment or push is included.
