# Multi-slot failure evidence

Verdict: `PASS` for C30–C32.

C30 ran two physical fake Executors through one stream, removed the Gateway,
and verified both continued and reconciled without a third process. C31
cancelled A while B completed unaffected. C32 uses 20 simultaneous contenders
for one remaining slot under `BEGIN IMMEDIATE`; exactly one claim succeeds.

Physical concurrency never exceeded two isolated slots and did not change the
production Worker's concurrency or processes.
