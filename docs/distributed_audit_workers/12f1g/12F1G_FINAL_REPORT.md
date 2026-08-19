# 12F.1G production re-enrollment patch deploy — final report

## Verdicts

- **12F.1G PATCH DEPLOY: PASS**
- **PRODUCTION CENTER: PATCH_ACTIVE**
- **WORKER REENROLLMENT: PASS**
- **WORKER IDENTITY: PRESERVED**
- **AUTHENTICATED POLLING: PASS**
- **WORKER TRANSPORT: POLLING**
- **12F RESUME: ALLOWED**

The exact immutable identity-preserving patch is active in production. The
physical Worker `.31` is a full record in the new production registry with
its historical Worker ID and instance ID unchanged, a newly issued runtime
credential, the stable `https://auditmanager.app` polling endpoint, and no
production workload or provider inference.

## Final requested facts

1. **12F.1G verdict:** PASS.
2. **Previous production commit:** `e2b98c3b5b290f97257ddcb914e71a65289c26bb`.
3. **New production commit:** `e6015d33bf4fa6b8986a21fa4b9e33c10ec3139f`.
4. **Previous backend PID:** `2281536`.
5. **New backend PID:** `2379476`.
6. **Restart downtime:** measured upper bound `2374 ms` from controlled restart
   start to accepted health.
7. **Patch bundle/hash verified:** YES; bundle SHA-256
   `1a94337d444ea977d73e1be164f6e2d77e10beeff472905e242e9b706726e7f6`,
   tree `00e75c545eb314ff0bac82990b9ad5f11050e476`, release manifest SHA-256
   `9c66b398f7cee0323c47d1e0e9d0a403aef60492713095b3b4a03ad81491580a`.
8. **Backup verified:** YES; restricted backup
   `/home/coder/auditmanager/backups/12f1g-20260813T213817+0300`, schema 12,
   integrity `ok`, DB SHA-256
   `99d4051f9f38094b86a839e5f89e242444e63e073af3036ee36fce1b34def033`.
9. **workers.db schema before:** 12.
10. **workers.db schema after:** 13.
11. **DB integrity:** `ok`; WAL; mode `0600`; foreign-key check clean.
12. **Scheduler state:** DISABLED.
13. **Unexpected jobs/offers/actions:** `0/0/0`.
14. **Worker ID before:** `wrk_19c87718`.
15. **Worker ID after:** `wrk_19c87718`.
16. **Instance ID before/after:**
    `inst_boot_e129036dddf5c59049080ddd15624e72` / same.
17. **Identity preserved:** YES.
18. **Re-enrollment authorization created:** YES, exactly one.
19. **Authorization TTL:** 300 seconds.
20. **Authorization consumed:** YES; final state `CONSUMED`.
21. **New runtime credential issued:** YES.
22. **Old runtime credential imported:** NO.
23. **Raw enrollment secret stored Center-side:** NO; hash-only authorization
    representation.
24. **Raw runtime credential stored Center-side:** NO; `worker_tokens` contains
    only `token_sha256` plus safe metadata.
25. **Worker credential permissions:** `0600`, owner UID `1002`, protected
    Worker boundary.
26. **Worker URL before:** historical trycloudflare endpoint.
27. **Worker URL after:** `https://auditmanager.app`; no old endpoint fallback.
28. **Polling Agent old PID:** `2048874`.
29. **Polling Agent new PID:** `2212836`, active, `NRestarts=0`.
30. **Executor PID before/after:** `1384880/1384880`, active and untouched.
31. **Authenticated heartbeat:** PASS; post-restart Worker API aggregate had
    1,314 requests with 1,267 HTTP 200, 47 HTTP 204, and zero HTTP 401.
32. **Center worker count:** 1.
33. **Center sees `wrk_19c87718`:** YES, online/approved/idle.
34. **Transport:** POLLING.
35. **Ownership:** POLLING.
36. **Active attempts:** 0.
37. **EventOutbox/unacked:** pending `0`; canonical read-only diagnostic
    `last_event_ack=22`.
38. **Pending result:** 0.
39. **Pending cancel:** 0.
40. **Provider auth preserved:** YES; Claude/Codex/OpenRouter are reported
    installed and logged in/configured in the exact Agent environment.
41. **Claude inference:** 0.
42. **Codex inference:** 0.
43. **OpenRouter inference:** 0.
44. **Observation duration:** `603.878 s`, 11 samples.
45. **Backend stable:** YES; PID `2379476`, `NRestarts=0`, HTTP 200 in every
    sample.
46. **Worker heartbeat stable:** YES; online and monotonically fresh in every
    sample, no restart loop or auth failure.
47. **Gateway :8443 started:** NO; listener absent.
48. **UFW changed:** NO; IPv4/IPv6 hashes match preflight. The existing rule is
    still source-scoped to `176.12.77.31 -> TCP/8443`; there is no global 8443
    allow and no 9443 rule.
49. **cloudflared changed:** NO; pre-existing PID `1263127`, start time
    2026-08-11 19:52:01 MSK, target `127.0.0.1:8081`.
50. **nginx/Caddy changed:** NO; nginx PID `57095`, `NRestarts=0`; no Caddy
    unit is installed or active.
51. **Rollback executed:** NO; immediate and deep rollback releases plus the
    new backup remain physically available.
52. **Production unexplained changes:** 0. A pre-existing unrelated listener
    on `:8099` started on 2026-08-08 and was neither created nor touched by
    12F.1G.
53. **12F_RESUME_ALLOWED:** YES.
54. **Exact blockers if NO:** none.
55. **Evidence commit:** recorded after final evidence validation; see final
    handoff for the resulting commit hash.
56. **Push/Merge:** NO/NO.
57. **Recommendation:** YES, resume 12F FIRST PRODUCTION CANARY under its own
    already-defined safety gates; 12F.1G itself stops here and does not start
    the canary.

## Integrity and scope notes

- Production source was not modified; final source-writer count is zero.
- The production Gateway was not started, `.31` was not switched to gRPC, the
  scheduler stayed disabled, and no real audit/project was created.
- cloudflared, nginx, Caddy, UFW and the production Executor were not changed.
- The metadata-only provider checks created no inference grants and made no
  model calls. Claude/Codex/OpenRouter runtime inference remains `0/0/0`.
- Exact-patch regression evidence remains the 12F.1F immutable run: 272 passed,
  0 failed. 12F.1G additionally completed live Center/API acceptance and 11/11
  production observation samples.

Push: **NO**. Merge: **NO**.
