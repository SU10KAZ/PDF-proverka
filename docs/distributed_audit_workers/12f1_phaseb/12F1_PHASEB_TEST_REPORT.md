# 12F.1 Phase B test report

## Result

The exact candidate passed deployment, health, core API, distributed-router,
canonical database and safe-scheduler acceptance. Phase B as a whole is
**BLOCKED**, because the physical production polling Worker cannot authenticate
to the new empty Center registry under the narrowly authorized URL-only repair.

## Immutable deploy and supervision

- Candidate commit: `4767d0bf83fcb99ee69267d94324495b92954b41`.
- Rollback baseline: `46bcd527065aff074ca5fe730e3594e982d080d2`.
- Both releases are durable and sealed `0555`; neither production startup nor
  rollback depends on `/tmp`.
- The new enabled user unit runs the candidate through
  `/home/coder/auditmanager/current`, with `UMask=0077`,
  `NoNewPrivileges=yes`, `ProtectSystem=strict` and `ProtectHome=read-only`.
- The old mutable-checkout `@reboot` line was the only changed cron entry.
  The watchdog no longer launches a backend and therefore cannot race systemd.

## Restart and acceptance

Historical PID `1968811` received graceful `SIGTERM`. The old listener was
gone at `09:58:25.475149538+03:00`; candidate PID `2214574` was healthy at
`09:58:27.565699942+03:00`, an actual listener outage of **2.091 seconds**.
Rollback was not triggered.

Safe production checks passed:

- `/api/info`, projects, objects, users, audit live state, batch state,
  model/stages, distributed status/identity/registry/jobs/targets/provider
  overview and admin-action listing returned HTTP 200 (15 endpoint smokes);
- project/object/user counts remained 67/5/6;
- no running audit, active batch, distributed job, offer, certificate,
  pending command or distributed admin action appeared;
- distributed state is schema 12, WAL, `integrity_check=ok`, with 27
  application tables and no imported 12D/12E identities;
- distributed subsystem is enabled while remote execution and automatic
  provider dispatch remain disabled;
- public `https://auditmanager.app/api/info` reports the exact candidate base
  directory.

The pre-deploy regression evidence remains the exact-reviewed 12F.1C suite:
`1527 passed / 57 skipped`, direct semantic group `8 passed`, guard group
`5 passed`, polling/routes `181 passed`, distributed critical `122 passed`,
and reliability `90 passed`. These groups overlap and are not summed.

## Physical polling compatibility probe

Worker `.31` was idle (`active_attempts=0`, active processes `0`, unwritten
EventOutbox rows `0`, pending cancel `0`). The only config field changed was
the Center URL. Agent PID `1575036` became probe PID `2047981`; Executor PID
`1384880` was neither signalled nor restarted.

With `https://auditmanager.app`, the Worker established a direct TLS
connection `.31:34400 -> .128:443`. Center observed registration, reconcile,
heartbeat, jobs and commands requests from `176.12.77.31`, so DNS, TLS,
routing and polling protocol reachability passed. Every request returned 401:
the canonical production DB had `workers=0` and `tokens=0`, while the Worker
retained identity `wrk_19c87718`.

The current authorization permits a URL-only repair and explicitly excludes
auth/identity changes. It therefore does not authorize copying an old token,
importing test state or re-enrolling/rotating the Worker. To avoid leaving the
Agent in a tight 401 loop, the exact backup was restored; final Agent PID is
`2048874`, active, and the final config hash equals the before hash. The old
unresolvable endpoint is consequently the restored final value. This is an
auth-provisioning gate, not a candidate network or polling-protocol defect.

## Observation and invariants

The candidate stayed healthy for at least **711 seconds** (11 minutes 51
seconds) through the final snapshot, beyond the required ten-minute window, with
systemd restart count zero, healthy core/distributed DBs, no scheduled work
and no rollback trigger. Production checkout hash, existing audit data,
production `.env`, nginx, Caddy, cloudflared PID/target and firewall were not
changed. Listeners `:8443` and `:9443` remain absent. Provider inference and
real audits were `0/0/0` and none, respectively.

`12F_RESUME_ALLOWED=false`: authenticated heartbeat, Center-visible idle
state, ownership, Event/ACK, pending-result and pending-cancel gates are not
satisfied. Local Worker facts are not substituted for the required Center
visibility.

The final authenticated recheck covered 13 core/distributed GET endpoints,
all HTTP 200. The same distributed status endpoint without a portal session
returned 401, confirming that the portal authorization boundary remained
enforced.
