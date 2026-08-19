# 12E final report

## Verdicts

- `12E RELIABILITY = PASS`
- `JOB LOSS = 0`
- `DUPLICATE EXECUTOR = 0`
- `EVENTOUTBOX RECOVERY = PASS`
- `DATA PLANE RESUME = PASS`
- `RESULT ACK / RETENTION = PASS`
- `CANCEL RECOVERY = PASS`
- `CERTIFICATE FAILURE RECOVERY = PASS`
- `MULTI-SLOT = PASS`
- `BACKPRESSURE = PASS`
- `TRANSPORT OWNERSHIP = PASS`
- `MANUAL ROLLBACK = READY` (zero-active-only for first canary)
- `PRODUCTION CUTOVER = NOT_DONE`
- `12F ENTRY = ALLOWED`

Base is `1ff907211333682c5fcd7db914c8b9d306f1d30a`, the verified final
12D tip. All 41 mandatory scenarios pass. Relevant non-overlapping regression
groups total `660 passed, 1 skipped`; immutable review reran an overlapping
high-risk subset with `129 passed`.

## Physical and consistency evidence

The isolated Worker `.31` connected directly to Center `.128:8443` using real
grpcio, HTTP/2 and mTLS. C01/C02 Gateway graceful/SIGKILL, C07/C08 Agent
graceful/hard death, C25 rotation, C26 issuer outage, C27 active revocation,
C29 DB contention, C30 two-slot stream loss and C31 cancel-one all recovered.
Claim/offer/event/result ACK crash seams, source/result interruption, corrupt
packages, temporary validation failure, cancel replay, DB outage, slot race,
ownership, backpressure and duplicates pass deterministic regressions.

Actual soak was `900.104402 s` (15 minutes, not claimed as 30): 31/31 samples
on each side stayed connected/ready at epoch 137, with no reconnect or queue
growth. Ten sequential physical synthetic jobs completed in 20.470642 s:
10 unique attempts, 10 validated ResultAck, zero duplicate offer/Executor and
zero stuck jobs.

Final Center SQLite: `integrity_check=ok`; 24 completed+validated+acknowledged,
2 expected cancelled, nonterminal 0, duplicate event sequence 0, multiple
offer 0. Worker: 24 finished, 1 cancelled, live process rows 0, pending command
0, partial transfer 0, and 3314/3314 event-journal rows durable. Unexplained
orphans and unacknowledged outbox rows are zero.

## Isolation, cleanup and production integrity

No production DB, production `:8081`, real project, Claude/Codex/OpenRouter,
Cloudflare URL, tunnel, SSH forwarding, reverse proxy or overlay VPN was part
of 12E. `TUNNEL USED BY 12E = NO`; provider inference is `0/0/0`.

Isolated Agent 1724350, Executor 1716999, Gateway 1855872, issuer 1826339 and
HTTPS data plane 1798004 are stopped. Listeners :8443/:9443 are absent. The
temporary source-scoped `.31→9443` UFW rule is removed; persisted UFW hashes
match BEFORE and the agreed `.31→8443` source-scoped rule remains. Temporary
C27 CSR/cert transfer copies were removed after evidence capture.

Production polling Agent 1575036 and Executor 1384880 remain active and were
never signalled. Production backend `127.0.0.1:8081` is active and HTTP 200.
Its PID changed independently during the long observation window and is now
1851904; 12E never signalled, restarted, configured or used it. Command, bind
and production data roots are intact. Pre-existing production cloudflared PID
1263127 remains present, unchanged, targets `127.0.0.1:8081`, and
`USED BY 12E = NO`. nginx/Caddy and unrelated firewall rules are unchanged.

Immutable adversarial review of exact detached candidate
`e3052e2aa50faa1dd4aa701a5c17d693aded2047` passed all ten lenses. Cutover and
rollback runbooks plus the read-only Worker doctor surface are ready. `ALLOWED`
authorizes only a separately approved 12F live preflight; it does not itself
authorize production cutover.

Push: `NO`. Merge: `NO`.
