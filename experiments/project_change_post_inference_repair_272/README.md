Pair A post-inference repair V1 is an offline experiment for object 272,
АР1, pair index 2, key `ad0a31a342a666082f2ef66a`.

`normalize(..., subject_contract=contract(candidate, packet))` opts into the
new contract. The F5 discovery caller and legacy binding callers keep their
existing behavior. Candidate IDs remain transport provenance; discovery IDs
and witnessed physical scope identify subjects. Source/model display labels
do not establish cross-version equivalence. Explicit role references are checked
against side, version, requirement, region, source receipt and witness scope.

Claim applicability is restricted to architectural areas, static dimensions,
room function, elevations and layout. A changed room function is retained in
the OLD/NEW state and stops blocking identity only when the room and physical
location are mapped. Existing sufficiency and materiality remain in force:
an OTHER state can still require review.

Artifacts live in
`/home/coder/auditmanager/corpus-audits/20260914_project_change_272/fresh_dev_pair_a_post_inference_repair_v1/`.
The immutable `REPLAY_RESULT_FREEZE.json` precedes reading the frozen V2 source
audit. `evaluation.py` only evaluates the persisted outputs against that audit;
it neither creates truth nor changes the repair. The one completed replay must
not be rerun or overwritten. No providers are imported; network/process audit
hooks block external calls. V2 files are hash checked and read-only in replay.

Validation: 48 local tests passed. The 52 saved responses yielded 4 ACCEPT,
46 REVIEW and 2 NOT_CHANGE; 7 NO_CALL_MISSING packages bring REVIEW to 53.
The unchanged grouping produced 4 singleton ProjectChanges.
Source audit: 3 clean ACCEPT, 1 real change with an inaccurate numeric total
(strict false ACCEPT; no invented event), 1/15 independent groups partially
covered. Recommendation: `DOWNSTREAM_STILL_NEEDS_REPAIR`.

The Excel contains the engineering system output before audit correction;
its evidence page columns list successfully bound pages. See the final report
and audit comparison for the numeric defect and remaining blockers. Pair B,
validation, final holdout and production were untouched.

Synthetic tests can be run independently of any frozen corpus:

```sh
python -m unittest experiments.project_change_post_inference_repair_272.test_repair experiments.project_change_f2_binding_v4_272.test_binding experiments.project_change_contracts_272.test_comparability experiments.project_change_contracts_272.test_states
```
