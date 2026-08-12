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

The harness has no route to production `127.0.0.1:8081`, production DB,
Cloudflared, SSH forwarding or a physical Worker.  The later physical phase
uses a separately provisioned 12E identity/config and evidence capture; it
does not reuse this loopback fixture.
