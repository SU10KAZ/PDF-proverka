# 12F.1D production Worker re-enrollment — final report

## Verdict

- **12F.1D WORKER ENROLLMENT: BLOCKED**
- **PHYSICAL POLLING AUTH: FAIL / NOT ATTEMPTED IN 12F.1D**
- **WORKER IDENTITY: BLOCKED (locally preserved, not Center-enrolled)**
- **WORKER TRANSPORT: POLLING**
- **12F RESUME: BLOCKED**

The stop occurred at the explicit section 4 gate, before production mutation.
The exact candidate's canonical registration cannot preserve
`wrk_19c87718`: its request has no existing Worker-ID field, its repository
always generates a new `wrk_*`, and its one-time enrollment authorization is
scoped only to the instance. Manually inserting the old ID or copying its old
token would violate the runbook and the security model.

## Final state

1. Production Center PID `2214574`, exact candidate
   `4767d0bf83fcb99ee69267d94324495b92954b41`, active, health 200,
   `NRestarts=0`. Backend restart: **NO**.
2. Worker ID before/after: `wrk_19c87718` / `wrk_19c87718` locally; it remains
   absent from the new Center registry. Instance before/after:
   `inst_boot_e129036dddf5c59049080ddd15624e72` / same.
3. Enrollment method: **NOT EXECUTED**. One-time token TTL/scope: none issued.
   Raw enrollment token stored Center: **NO**. Runtime credential rotated:
   **NO**. Raw runtime credential read or stored in evidence: **NO**.
4. Runtime credential reference remains protected under the
   `auditworker_11l` `0750` home. The implementation writes secrets atomically
   as `0600`; live metadata below that boundary was intentionally not readable
   by the unprivileged admin SSH user, and no credential content was requested.
5. Polling URL before/after: old trycloudflare URL / same. It was not changed
   because no valid production credential could be installed atomically.
6. Polling Agent PID `2048874 -> 2048874`, active. Executor PID
   `1384880 -> 1384880`, untouched. Transport remains locally `POLLING`.
7. Authenticated heartbeat: **NOT PROVEN**. Center workers/tokens remain `0/0`;
   target Worker, ownership, Event/ACK, pending-result and pending-cancel state
   are not Center-visible.
8. Last Phase B idle evidence remains attempts/processes/unwritten
   EventOutbox/pending-cancel `0/0/0/0`. It is not promoted to a fresh
   Center-visible 12F.1D proof.
9. Scheduler: **DISABLED**. Unexpected jobs/offers/actions: `0/0/0`.
10. Provider auth/config changed: **NO**. Fresh provider probe was not reached.
    Inference Claude/Codex/OpenRouter: **0/0/0**.
11. Required post-heartbeat observation: not started (`0/600 s`).
12. Gateway `:8443`: not started. UFW, nginx, Caddy, cloudflared and other
    infrastructure: unchanged. Real audit/canary: none.
13. Isolated token checks newly completed: `2 passed` (TTL/expiry/replay/wrong
    instance and stdin-not-argv). A larger selected HTTP-fixture group was
    terminated by environment capacity during fixture entry and is not counted
    as a pass; it touched no production state.
14. `12F_RESUME_ALLOWED = NO`.

## Exact blocker and required authority

Choose one path explicitly:

- implement, regression-test, immutably review and deploy a typed
  identity-preserving re-enrollment authorization bound to both the exact
  Worker ID and instance ID; or
- authorize the current canonical one-time enrollment to assign a new
  generated Worker ID and atomically replace the local identity/credential.

Until that choice is made, changing the Worker ID would be an unauthorized
identity migration. Push: **NO**. Merge: **NO**.
