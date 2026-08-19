# Data-plane failure evidence

Verdict: `PASS` for C14–C17 and transfer authorization under retries.

- C14 interrupts a source response after exactly 1 MiB. The durable `.part`
  file is retained, the next request uses `Range: bytes=1048576-`, and only the
  final verified archive may reach Executor launch.
- C15 rejects source hash mismatch before execution.
- C16 interrupts result upload after chunk 0. Recovery reuses the same upload
  identity, skips the accepted chunk, uploads the missing chunk and never
  reruns Executor (`489d2448`, `bd5ee589`).
- C17 corrupt result/package tests reject validation and issue no ResultAck.
- Transfer authorization remains bound to worker, job, attempt and direction;
  cross-worker replay is denied.

Physical jobs used direct isolated HTTPS `176.12.77.128:9443`, never production
`:8081`. The temporary source-scoped `.31→9443` UFW rule and listener are gone.
