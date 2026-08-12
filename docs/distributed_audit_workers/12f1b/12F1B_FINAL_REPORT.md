# 12F.1B final report

## Verdict

- `12F.1B = BLOCKED_AT_RENEWED_SOURCE_FREEZE`
- `BASELINE FREEZE = BLOCKED`
- `READY_FOR_OPERATOR_RESTART = NO`
- `PRODUCTION_CHANGED_BY_12F1B = NO`
- `PRODUCTION_RESTARTED_BY_12F1B = NO`
- `PROVIDER_INFERENCE = 0/0/0`

## Completed captured artifacts

The production checkout was initially stable across three samples over 69
seconds. Snapshot
`12f1b-stable-disk-20260812T192938+0300-137b02af` classified all 100 changed
paths, with 79 approved release-scope paths and zero unknowns. Commit
`8cf5c6738b37d27c54dfa63fd1b7b2e186078a34` exactly froze those approved
paths. Its captured stable-disk equivalence is PASS; historical running-PID
equivalence remains `NOT_FULLY_REPRODUCIBLE`.

All 11 production-only commits are REQUIRED and inherited. The original four
post-start router/store/semantic-diff/test paths were individually reviewed as
PRESERVE, with 14 semantic tests passing. Operator changes in the captured
snapshot were preserved; generated runtime, local config, secrets, DB, cache
and logs were excluded.

Candidate `965337f11382b8bf0bac0ed81c560eff24a83cdb` integrates the captured
production baseline with 12A–12E. It implements the approved viewer/operator/
admin model, with `andrey=admin`, the other five subjects `viewer`, and no
candidate operator subject. Human drain/resume is present, role-gated,
durable and audit-logged. Machine identity remains separate.

The production state target is
`/var/lib/auditmanager/distributed_workers`; no live directory was created.
The reusable bootstrap-secret fallback is removed. The stable polling and
rollback endpoint is `https://auditmanager.app`.

Both release directories are immutable and release-specific. Isolated
baseline boot, candidate boot and candidate-to-baseline rollback passed. The
external test DB remained byte-identical across rollback. All isolated
listeners were stopped.

## Final hard-gate failure

The final read-only check found that current disk no longer equals the captured
snapshot: two captured release-scope files changed and five new source/test/
script files appeared. Most importantly, the release-scope digest changed
during the bounded observation itself because
`backend/app/services/stage_comparison/semantic_diff_v6a2.py` was rewritten at
`20:53:13+03:00`.

The operator instruction is explicit: if SOURCE_CODE continues changing,
stop and mark the baseline freeze blocked. Therefore the two immutable
releases remain useful tested historical artifacts but are not approved
restart targets for the current disk.

Exact blockers:

1. stop all parallel production source writers and prove a new bounded
   quiescent interval;
2. capture a new complete forensic snapshot;
3. classify the seven later release-scope deltas;
4. rebuild/review baseline and integrated candidate from that snapshot;
5. run delta-focused and required regression/rollback tests;
6. obtain separate explicit production restart authorization.

Production integrity remained intact: backend PID `1968811` is active on
`127.0.0.1:8081`; production cloudflared PID `1263127` still targets that
backend; polling Agent PID `1575036` and Executor PID `1384880` remain active
on `.31`. No production service, DB, environment, firewall, proxy or worker
configuration was modified.
