# 12D test report

## Chronology

The first physical attempt is not rewritten: `.31 -> Center TCP/8443` timed
out because Center UFW omitted 8443 (`PORT_BLOCKED`). The operator then added
an allow rule scoped only to source `176.12.77.31`. The second attempt below
is a new physical run on the same 12D branch.

## Regression suite after the isolation fix

The physical run exposed one real defect: HTTPS package traffic was hard-bound
to `dispatcher_url`, so an isolated data-plane origin could not be selected
without using production. Commits `a1003f45` and `1913d92f` add an optional
typed HTTPS base URL and private CA bundle. Both defaults preserve the previous
production behavior; there is no Proto or Gateway change and no `verify=false`.

The original six-file suite plus three new regressions ran at `1913d92f`:

```text
186 passed, 0 failed, 1 pre-existing passlib warning, 78.79 s
```

The regressions prove default-to-dispatcher compatibility, selection of a
separate HTTPS data-plane origin, rejection of external HTTP, and fail-closed
handling of a missing CA bundle.

## Physical second attempt

- UFW persisted rules: 8443 and temporary 9443 are both restricted to source
  `176.12.77.31`; there is no global-source allow for either port.
- Direct `.31 -> 176.12.77.128:8443`: **PASS**.
- Server TLS: TLS 1.3, trusted CA, exact IP SAN, serverAuth EKU: **PASS**.
- Client auth: valid certificate accepted; absent and untrusted certificates
  rejected: **PASS**.
- Real AgentHello/CenterHello and fresh registry heartbeat over grpcio: **PASS**.
- Direct peer observed by Center: `176.12.77.31`; tunnel/proxy/forwarding: **NO**.
- Isolated HTTPS data plane: `176.12.77.128:9443`, isolated DB, verified private
  CA/IP SAN. Production `127.0.0.1:8081` and production DB were not used.
- Zero-inference job `a3cd3fcd-...`: source HTTP 200, EventAck path, verified
  result upload, ResultAck, and retention: **PASS**.
- Live interruption job `2478b1c9-...`: while 8443 had no listener, process PID
  `1645122` remained alive and outbox grew `19683 -> 54950` bytes. After
  restart, epoch `7 -> 10`, `worker_reconnected`, replay, ResultAck, unique
  sequences, unchanged Executor/claim generation, and zero duplicate process:
  **PASS**.
- New-key rotation A -> B: authenticated renewal, candidate proof, atomic
  switch and epoch `10 -> 11`: **PASS**.
- A after `ADMIN_REVOKED`: authenticated RPC `UNAUTHENTICATED`; B remains
  `ACTIVE` and accepted: **PASS**.
- Cert B claiming another worker in AgentHello: `PERMISSION_DENIED`, no session
  and no job offer created: **PASS**.
- Cross-worker HTTPS transfer request: HTTP 403: **PASS**.
- Worker inbound runtime listener: **NONE**.
- Claude/Codex/OpenRouter inference: **0/0/0**.

## Cleanup

Isolated Agent, Executor, issuer, HTTPS backend and public Gateway were stopped.
Ports 8443 and 9443 no longer listen. Production backend PID `277145`, polling
Agent PID `1575036` and Executor PID `1384880` were not restarted. Temporary
tokens, revoked-A recovery key and duplicate staging key were removed; current
B remains Worker-local mode 0600 and is not revoked.

The exact temporary `.31 -> TCP/9443` UFW rule could not be deleted because
sudo requires interactive authentication. With no 9443 listener it is inert,
but operator removal remains the sole cleanup item.
