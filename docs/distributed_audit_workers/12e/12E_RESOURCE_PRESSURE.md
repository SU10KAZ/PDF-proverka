# Resource pressure

Verdict: `PASS` for C37/C38 without loading the shared hosts.

C37 uses simulated swap/disk telemetry: unsafe pressure reduces advertised
free slots to zero, preserves existing data and blocks new work. C38 simulates
capability ready→unavailable; new incompatible work is refused while an active
fake job is not arbitrarily cancelled. The exactly-once usage/action-ledger
regression proves a transport retry cannot create a second logical grant.

The lifecycle subprocess regression deliberately hides optional `psutil` so
the host's live swap does not prevent its process-ownership precondition; the
production safety threshold remains unchanged and is tested separately.
Claude/Codex/OpenRouter inference remained `0/0/0`.
