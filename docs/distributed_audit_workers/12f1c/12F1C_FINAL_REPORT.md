# 12F.1C final production freeze and integrated candidate report

1. **12F.1C verdict:** PASS; final artifacts are ready for a separately
   authorized operator restart. No restart/deploy was performed.
2. **Parallel Claude confirmed as production-root coding agent:** YES, PID
   `1630609`, cwd/Git root production, branch `feature/block-vector-graphs`.
3. **Every drifted file attributed to that PID:** NO. Session identity and cwd
   are strong class-level evidence, not per-file audit evidence.
4. **Other source writers:** none. Other Claude PID cwd values are outside the
   repository; the remaining PID 1630609 is a dormant extension daemon with
   no open repository files and unchanged write counter.
5. **Production source writers final count:** `0`.
6. **20-minute quiescence:** PASS, 1200.106 seconds, 81 samples.
7. **Changed/new/removed during window:** `0/0/0`.
8. **Authoritative snapshot:**
   `12f1c-authoritative-stable-disk-20260813T080518+0300-af0ccd9a`.
9. **Deltas vs old snapshot:** `7`.
10. **Delta classifications:** seven `PRESERVE`; unknown `0`.
11. **feature/block-vector-graphs disposition:** coherent completed
    deterministic stage-comparison feature slice preserved after regression;
    not debug/generated state and not distributed-worker runtime.
12. **semantic_diff_v6a2:** exact authoritative SHA-256 `09f8e808…382fc`,
    preserved; deterministic, fail-closed parity, no provider call.
13. **Final baseline commit:**
    `46bcd527065aff074ca5fe730e3594e982d080d2`.
14. **Final baseline release:**
    `/tmp/12f1c-releases/baseline-46bcd527`, immutable.
15. **Stable disk equivalence:** PASS, 39/39 selected runtime paths; all 66
    old exclusions retain explicit non-release classifications.
16. **Unexplained source diffs:** `0`.
17. **Final candidate commit:**
    `4767d0bf83fcb99ee69267d94324495b92954b41`.
18. **Final candidate release:**
    `/tmp/12f1c-releases/candidate-4767d0bf`, immutable.
19. **Provisional commits:** baseline `8cf5c673…` and candidate `965337f1…`
    remain historical comparison artifacts, not deploy targets.
20. **Production core tests:** PASS, 1687 passed / 14 skipped / 11 explicitly
    documented deselections; no real-project benchmark was run.
21. **Stage-comparison/block-vector tests:** PASS, 1527 passed / 57 skipped;
    semantic v6a2 direct group 8 passed.
22. **Distributed tests:** PASS: critical 122, polling/routes 181,
    11K/11L/12A/12E reliability 90. Groups overlap and are not summed.
23. **Polling endpoint:** `https://auditmanager.app`.
24. **Role mapping:** `andrey=admin`; `igor`, `alexey`, `filipp`, `marina`,
    `alexandra=viewer`; operator set empty.
25. **Persistent state:** `/var/lib/auditmanager/distributed_workers`, external
    to releases.
26. **Reusable bootstrap fallback:** REMOVED; one-time TTL,
    session/instance-scoped hashed-at-rest tokens only.
27. **Rollback dry run:** PASS from sealed candidate to sealed baseline;
    schema 12 DB SHA unchanged and both temporary listeners stopped.
28. **Baseline immutable review:** PASS, detached tree `06fac1ef…`, no tracked
    runtime secrets/state and no writable regular paths after seal.
29. **Candidate immutable review:** PASS, detached tree `c3c51e60…`; only
    credential-like match is an intentional invalid `sk-or-v1-TRAP` sentinel.
30. **Provider inference:** Claude/Codex/OpenRouter = `0/0/0`.
31. **Production source modified by 12F.1C:** NO. Final 1477-path hash is still
    `af0ccd9a…704a`.
32. **Production backend restarted/signalled:** NO. PID `1968811`, original
    start time, listener `127.0.0.1:8081`, HTTP 200. Production cloudflared,
    polling Agent PID `1575036` and Executor PID `1384880` are unchanged.
33. **READY_FOR_OPERATOR_RESTART:** YES.
34. **Blockers:** none inside 12F.1C. `operator_restart_authorization=false`
    is expected, so execution stops here without deployment or restart.

The production-root mutation preflight guard is included in the final
baseline. Its tests pass, and a mutation intent from the production root exits
`2`; production read access and the running backend are unaffected.

Push: NO. Merge: NO. Production cutover/restart: NOT DONE.
