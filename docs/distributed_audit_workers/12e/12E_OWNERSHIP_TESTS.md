# Transport ownership and manual rollback contract

Verdict: `PASS` for C33/C34; manual rollback `READY`.

An active `grpc_stream` ownership record fences polling leases, and polling
ownership prevents Gateway offers. Twelve consecutive gRPC connection failures
produced 12 gRPC attempts, 11 reconnects and zero polling calls. There is no
automatic fallback.

For the first canary rollback is deliberately zero-active-only:

1. Drain the Worker and stop new gRPC offers.
2. Reconcile outbox, result and cancel state; require zero active attempts.
3. Stop the gRPC Agent and confirm ownership released.
4. Restore the known polling configuration and start only the polling Agent.
5. Verify polling heartbeat/ownership before allowing a lease.

12E did not perform this production transition. Final production remains
polling with Agent PID 1575036 and Executor PID 1384880 active.
