# Transport ownership and manual rollback contract

The local ownership regression proves that an active gRPC connection prevents a
polling lease for the same Worker. gRPC is explicit; a broken stream retries
gRPC and must never start polling automatically.

For the first canary, rollback is permitted only with zero active attempts:

1. Mark the Worker draining and stop new gRPC offers.
2. Reconcile EventOutbox, result and cancel state; require zero active jobs.
3. Stop the gRPC Agent cleanly and confirm gRPC ownership is released.
4. Restore the known polling configuration and start only the polling Agent.
5. Verify polling heartbeat and ownership before allowing a new lease.

This is a documented future production procedure, not an action performed by
12E.
