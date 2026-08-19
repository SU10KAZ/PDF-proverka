# Agent / Executor failure evidence

Verdict: `PASS` for C07–C10.

- C07 physical graceful restart: isolated Agent stopped while its fake Executor
  continued; reconnect used a higher epoch and completed the same attempt.
- C08 physical hard Agent death: the isolated Executor survived, the restarted
  Agent reconciled it, and no duplicate process appeared.
- C09 deterministic pre-dispatch crash recovery launches the durable accepted
  attempt exactly once; a late repeated offer cannot recreate it.
- C10 real process regression kills the Agent after Executor launch and proves
  the audit child remains live, its outbox grows, and restart creates no second
  Executor. The regression is isolated from live host swap policy so it tests
  process ownership rather than host capacity.

Historical isolated PID `1692566` was already absent at final inspection and
was not signalled again. Production Agent `1575036` and Executor `1384880`
were never signalled and remained active after cleanup.
