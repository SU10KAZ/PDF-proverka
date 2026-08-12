# 12E chaos harness

The harness is process-scoped and test-only.

`tests/chaos_harness_12e.py` starts the actual
`python -m backend.app.agent_gateway` module in a clean subprocess on a random
`127.0.0.1` port.  It verifies that the supplied settings resolve to the
pytest-isolated Center DB before start, waits for an observed listener, and
can perform either SIGKILL or a bounded graceful stop.  It never defaults to
production settings and cannot bind a public address or port 8443.

`tests/test_process_chaos_12e.py` uses observable states rather than a timing
guess: local queue reaches `running`, process death is observed, replacement
Gateway listener is ready, persisted connection epoch rises, executor identity
is unchanged, and terminal result/retention/events converge.

`tools/physical_12e_harness.py` supplies the physical half: it provisions only
an explicit safe test root, isolated Center DB/PKI/identity, direct secure
Gateway, isolated HTTPS data plane, synthetic jobs and deterministic evidence.
Its sequential runner waits for durable `completed + result_acknowledged_at`
rather than sleeping and records event uniqueness, offer count and retention.

The physical topology used `.31` only through its isolated coder-owned runtime
and `.128` only through `/tmp/12e-center-20260812-1245/runtime`. It had no route
to production `127.0.0.1:8081`, production DB, Cloudflared or SSH forwarding.
All isolated processes/listeners are stopped and the temporary :9443 rule is
removed.
