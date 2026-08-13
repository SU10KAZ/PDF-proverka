# 12F.1 Phase B final report

**12F.1 PHASE B = BLOCKED.** The immutable production Center candidate is
healthy and remains active; the physical polling Worker is not connected
because its preserved production identity/token is absent from the newly
initialized Center registry. The authorization for this phase allowed only a
URL change, not auth/identity migration or re-enrollment.

1. Previous backend: PID `1968811`, historical mutable checkout runtime.
2. New backend: PID `2214574`, exact candidate commit
   `4767d0bf83fcb99ee69267d94324495b92954b41`.
3. Candidate release manifest: `fc865d51…79f379f`; rollback baseline commit:
   `46bcd527065aff074ca5fe730e3594e982d080d2`.
4. Backup verified: **YES**, restrictive `0700/0600`, including a consistent
   SQLite backup. Rollback has no `/tmp` dependency.
5. Persistent root: `/var/lib/auditmanager/distributed_workers` (`0700`,
   `coder:coder`); `workers.db` is `0600`, schema 12, WAL, integrity `ok`.
6. Main production audit data/config: unchanged. The only new database is the
   canonical empty distributed registry; isolated 12D/12E state was not
   imported.
7. Supervision: user `auditmanager-backend.service` is installed, enabled and
   active; it runs only the immutable release. Double-launch protection:
   **PASS**. New watchdog SHA: `0dae6c46…fff330`; its old version is backed up.
8. Crontab: only the AuditManager `@reboot` command changed from mutable
   `scripts/server/start_server_deploy.sh` to
   `/home/coder/auditmanager/bin/boot-backend.sh`; unrelated entries changed:
   **NO**.
9. Restart: graceful `SIGTERM`, actual listener downtime **2.091 s**. Local
   and public health, 15 safe API smokes, core DB access and distributed routes:
   **PASS**. Rollback executed: **NO**.
10. Scheduler: **DISABLED**; remote execution and automatic provider dispatch:
    **OFF**; unexpected jobs/offers/actions: `0/0/0`.
11. Worker endpoint probe: old unresolved trycloudflare URL ->
    `https://auditmanager.app` -> exact old URL restored after authentication
    failure. Polling Agent `1575036 -> 2047981 -> 2048874`; final Agent active.
    Executor remained PID `1384880` and was untouched.
12. Physical polling network/protocol reachability: **PASS**. Fresh
    authenticated heartbeat and Center-visible ownership: **FAIL** (`401`,
    Center workers/tokens `0/0`). Local transport remains **POLLING**;
    active attempts/processes/EventOutbox pending/cancel are `0/0/0/0`.
13. Gateway listeners `:8443/:9443`: **ABSENT/ABSENT**. No Gateway production
    cutover occurred.
14. Production cloudflared: PID `1263127`, target `127.0.0.1:8081`, unchanged.
    nginx/Caddy/UFW were not modified; no firewall rule was added or widened.
15. Provider inference: Claude/Codex/OpenRouter = **0/0/0**. Real audit: none.
16. Candidate observation: at least **711 s (11m51s)**, stable, service
    restarts `0`, warning-or-higher journal entries `0`.
17. Production unexplained changes: `0`.
18. `12F_RESUME_ALLOWED = NO`.

Exact blocker: securely migrate the preserved production polling identity and
token into the new Center, or explicitly authorize one-time re-enrollment/token
rotation. Until that separately reviewed action is authorized and the full
Center-visible idle/ownership/Event/ACK gate passes, 12F canary must not resume.

Push: **NO**. Merge: **NO**.
