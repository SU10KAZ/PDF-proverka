# 12F known issues

## Blocking: production Center is not 12F-capable in the running backend

The live `127.0.0.1:8081` process runs commit `9168c393...`, not the verified
12E/12F line. `DISTRIBUTED_WORKERS_ENABLED` is absent and defaults to false;
the loaded production `main.py` does not register the distributed-worker
router; the configured/default distributed data directory and `workers.db`
are absent.

Impact: Center-side idle, worker identity, ownership, pending result/cancel,
lease, EventOutbox ACK cursor, scheduler fencing and production job state
cannot be established. A production canary would therefore violate the 12F
hard gates.

Required operator decision: authorize a separately planned exact production
backend deployment/restart that preserves `127.0.0.1:8081` data and service
integrity, or provide an already supported hot-deploy mechanism that activates
the exact 12F Center release without stopping/restarting the process. The
current task explicitly forbids stopping or intentionally restarting `:8081`.

## Blocking: current polling control is unhealthy

The production polling Agent process is active, but `worker_state.json`
reports `center_unreachable` with DNS resolution failure for its pre-existing
Quick Tunnel dispatcher hostname. Center reconciliation cannot be performed.

This session did not repair or replace that URL because 12F requires direct
gRPC+mTLS only after every live hard gate is true and forbids fixing a failed
gate on the way.
