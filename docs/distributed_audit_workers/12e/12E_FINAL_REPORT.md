# 12E final report — immutable review candidate

Current candidate result: all implementation, physical, cleanup and evidence
gates pass; the exact-commit immutable adversarial review is the sole remaining
gate before the final `12E RELIABILITY` and `12F ENTRY` verdicts are sealed.

Base: `1ff907211333682c5fcd7db914c8b9d306f1d30a` (verified final 12D tip).
Critical scenarios: `41/41 PASS`. Relevant tests: `660 passed, 1 skipped`.
Job loss/duplicate Executor/premature retention/unexplained orphan: `0/0/0/0`.
EventOutbox, source/result resume, ResultAck, cancel, certificate failures,
multi-slot, backpressure and ownership: `PASS`.

Physical direct path was `.31→.128:8443` real grpcio+mTLS. Isolated HTTPS used
`:9443`. No tunnel, SSH forwarding, reverse proxy, overlay VPN, production DB,
production `:8081`, real project or inference was part of the topology. The
15-minute soak and 10/10 sequential jobs pass.

Cleanup: isolated Agent/Executor/Gateway/issuer/data plane stopped; :8443 and
:9443 listeners absent; temporary `.31→9443` UFW rule removed; agreed
source-scoped `.31→8443` rule retained. Production polling Agent 1575036,
Executor 1384880, backend :8081 and pre-existing cloudflared 1263127 are active.

Production cutover: `NOT_DONE`. Push: `NO`. Merge: `NO`.
