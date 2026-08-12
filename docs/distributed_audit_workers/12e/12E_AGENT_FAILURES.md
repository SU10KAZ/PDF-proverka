# Agent / Executor failure evidence

The established real-Agent test `test_agent_restart_adopts_live_executor_higher_epoch_no_duplicate`
proves graceful Agent shutdown/restart while the Executor keeps its PID. It
checks a higher connection epoch, adoption of the same process record and a
terminal validated result.

Remaining gates are intentionally not inferred from it:

- C08: SIGKILL of an isolated Agent while its Executor runs;
- C09: crash after durable accept and before launch;
- C10: crash immediately after launch and before the next Agent persistence.

They must use a separate isolated Agent process; production polling Agent and
Executor are out of scope and must never be signalled.
