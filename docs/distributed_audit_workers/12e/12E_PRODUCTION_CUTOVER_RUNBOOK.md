# Draft production cutover runbook — do not execute in 12E

Scope: one future `.31` Worker and one canary job, only after every 12E gate is
green and an operator separately authorizes 12F.

1. Verify the reviewed Gateway release and direct source-scoped `:8443` path.
2. Verify Gateway certificate, Worker certificate expiry, data-plane HTTPS and
   provider readiness without calling providers.
3. Verify the production Worker has zero active attempts, empty/consistent
   EventOutbox and no pending cancel/result.
4. Mark the Worker draining; prevent new work while rechecking ownership.
5. Stop the **production polling Agent only under a separate 12F approval**;
   confirm its Executor has no active attempt.
6. Change known Worker config/ownership to explicit `grpc_stream` and start the
   production Agent.
7. Verify mTLS, CenterHello, fresh heartbeat, capabilities and `doctor` state.
8. Submit exactly one explicitly authorized canary job and watch it through
   validated ResultAck.
9. Keep polling disabled while the canary is observed. Broader scheduling
   requires a separate acceptance.

12E does not perform step 5 or later.
