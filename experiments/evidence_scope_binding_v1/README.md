# Evidence Scope Binding V1 (offline DEV experiment)

Source fragments receive a provenance-bearing scope DAG before the unchanged
EngineeringSubject resolver. Source purity, ownership, identity and state
comparison have separate outcomes. No production integration or model calls.

Artifacts:
`/home/coder/auditmanager/corpus-audits/20260914_evidence_scope_binding_v1/`.
Start with `reports/CHECKPOINT.md` and `FINAL_SUMMARY.json` there.

Three approaches were evaluated on 45 TEXT and 41 TABLE natural DEV fragments.
The certified hierarchy yields 23 and 31 PROVEN bindings, all source-audited
correct; 32 fragments abstain. Old inspected validation is explicitly DEV.
One additional TABLE identity is unlocked (known wrong-apartment regression).
One raw consumer event fails the source audit of typed property/event
completeness; accepted complete ProjectChanges remain zero. Verdict PARTIAL.

The post-freeze independent ownership slice is registered, not evaluated:
six documents from two previously unseen Sobytie project IDs, one project
family. A third unrelated complete project and paired versions are unavailable.

Network-free verification from the repository root:

```sh
export PYTHONPATH=/home/coder/auditmanager/corpus-audits/20260913_project_change_text_v1/deps:$PWD
python -m unittest experiments.evidence_scope_binding_v1.test_scope
python -m experiments.evidence_scope_binding_v1.run replay
python -m experiments.evidence_scope_binding_v1.run downstream
```

Replay requires the pinned local source/audit artifacts. `prepare` refuses to
overwrite the selected cohort. `refresh` preserves earlier DEV evidence and
refuses after candidate freeze. `reports` seals receipts, writes the delivery,
then registers the holdout using metadata and byte hashes without reading its
semantic contents. The label-generator artifact is an implementing-agent DEV
assessment, never a modification of human truth.

`core.py` is the generic ownership/gate contract; `source.py` adapts exact
V3 rows and PDF/Markdown structure. It does not reconstruct every merged
header or implicit room/system reference. Unsupported geometry, OCR IDs,
parent chains and continuation remain REVIEW. The optional bounded ownership
package helper has no AI provider or promotion path.
