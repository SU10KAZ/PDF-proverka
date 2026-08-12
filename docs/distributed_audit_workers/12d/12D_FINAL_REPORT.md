# 12D final report — physical completion

## Outcome

The already implemented 12D control channel and certificate lifecycle are now
physically proven on the tested path `176.12.77.31 -> 176.12.77.128:8443`.
No tunnel, SSH forwarding, proxy, VPN overlay, Cloudflare path or model
inference participated.

The first attempt remains `PORT_BLOCKED`. After the operator added the exact
source-scoped UFW allow, the second attempt passed direct TCP, server TLS,
mTLS, real grpcio bidi, Hello, heartbeat, zero-inference HTTPS E2E, live
reconnect, new-key rotation, old-certificate revocation and negative identity.

The operator completed the final firewall cleanup. The temporary source-scoped
`.31 -> TCP/9443` rule is absent, the persisted IPv4 rules are byte-for-byte
back at the captured post-8443/pre-9443 baseline, and neither 8443 nor 9443 has
a listener.

Read-only separation verification confirms that the independently pre-existing
production `cloudflared` is outside the 12D topology. PID `1263127` started at
`2026-08-11T19:52:01+03:00`, before the first 12D attempt; its unchanged
command targets only `http://127.0.0.1:8081`, with a loopback metrics listener
on `127.0.0.1:20251`. It neither listens on nor targets 8443 or 9443. Direct
Center peer and endpoint evidence independently bind the physical 12D control
path to `.31 -> .128:8443` and its data plane to `.31 -> .128:9443`.

Therefore process-wide absence of `cloudflared` is not an acceptance
requirement. The applicable gate is `TUNNEL USED BY 12D = NO`, which passes.

## Required verdicts

- **A. 12D OVERALL: PASS.**
- **B. MTLS CONTROL CHANNEL: PASS.**
- **C. CERTIFICATE LIFECYCLE: PASS.**
- **D. LINUX KEY STORAGE: PASS.**
- **E. WINDOWS DPAPI: IMPLEMENTED_NOT_PHYSICALLY_PROVEN.**
- **F. DIRECT `.31 -> CENTER :8443`: PROVEN.**
- **G. PHYSICAL REAL GRPCIO: PROVEN.**
- **H. ZERO-INFERENCE E2E: PASS.**
- **I. SECURE RECONNECT: PASS.**
- **J. PHYSICAL ROTATION: PASS.**
- **K. PHYSICAL REVOCATION: PASS.**
- **L. TUNNEL USED BY 12D: NO.**
- **M. TUNNEL REQUIRED FOR TESTED `.31 -> .128` PATH: NO.**
- **N. PRODUCTION CUTOVER: NOT_DONE.**

## Evidence summary

1. Control endpoint: direct `176.12.77.128:8443`, production-mode mTLS.
2. Server certificate: trusted private CA, exact IP SAN `176.12.77.128`,
   serverAuth EKU, TLS 1.3; no hostname override or disabled verification.
3. Valid Worker cert accepted; no-cert and untrusted-cert paths rejected.
4. Certificate URI identity and `AgentHello.worker_id` matched
   `wrk_19c87718`; Center observed direct peer `176.12.77.31`.
5. Heartbeat was persisted fresh with active grpc stream epoch 4.
6. Package bytes stayed off gRPC and used isolated verified HTTPS `:9443`.
7. Zero-inference job completed through JobOffer, source transfer, Executor,
   durable events, result upload, validation, ResultAck and retention.
8. During live Gateway loss, the same test process survived and the outbox
   grew. Epoch increased from 7 to 10, events replayed uniquely, no duplicate
   Executor/process appeared, and ResultAck completed.
9. Worker generated keypair B locally. Authenticated renewal activated serial
   `1537971d8aa0afc18d614c0700304b2f2d481f00`; epoch increased 10 to 11.
10. Old A serial `23523e...d7227` is `REVOKED/ADMIN_REVOKED` and receives
    `UNAUTHENTICATED`; B remains `ACTIVE` and its authenticated RPC succeeds.
11. Cert B with `AgentHello.worker_id=wrk_a55031f2` received
    `PERMISSION_DENIED` before transport session or job scheduling.
12. Cross-worker HTTPS transfer authorization returned HTTP 403.
13. B private key never left `.31`; current key is 0600 in a 0700 directory.
14. Claude/Codex/OpenRouter calls: `0/0/0`.
15. Worker inbound runtime listener: none.
16. Pre-existing production `cloudflared` started before 12D, targets only
    `127.0.0.1:8081`, has no 8443/9443 socket or target, and was not part of
    either the direct control-plane or isolated data-plane topology.

## Production cloudflared separation

- **PRE-EXISTING PRODUCTION CLOUDFLARED = PRESENT**
- **USED BY 12D = NO**
- **TARGET = 127.0.0.1:8081**
- Listener owned by the process: `127.0.0.1:20251` (metrics only).
- Listener/target `:8443`: none. Listener/target `:9443`: none.
- Started `2026-08-11T19:52:01+03:00`, before the first recorded 12D attempt
  at `2026-08-12T04:13:55+03:00`.
- Physical control path: direct `176.12.77.31 -> 176.12.77.128:8443`; Center
  observed peer `176.12.77.31`; Agent Gateway terminated mTLS directly.
- Physical data path: direct isolated verified HTTPS
  `176.12.77.31 -> 176.12.77.128:9443`.

This is not a violation of 12D isolation.

## Isolation defect and commits

The physical run found that package requests could not select an origin
separate from `dispatcher_url`. The fix is intentionally narrow:

- `a1003f45` — optional typed `AUDIT_WORKER_DATA_PLANE_BASE_URL`, defaulting to
  the unchanged dispatcher origin;
- `1913d92f` — optional fail-closed `AUDIT_WORKER_DATA_PLANE_CA_BUNDLE`,
  defaulting to the system trust store.

No Proto, Gateway, scheduling or certificate-lifecycle design changed. The
combined suite now passes 186/186.

## Production integrity and final network state

- Production `127.0.0.1:8081`: untouched, PID `277145`.
- Production polling Agent: active, PID `1575036`.
- Production Executor: active, PID `1384880`.
- nginx: active and unchanged; Caddy: inactive and unchanged.
- Provider auth: unchanged; real audit: not run.
- Isolated Agent/Executor/API/issuer/Gateway: stopped.
- Final listeners on 8443/9443: none.
- `UFW_RULE_8443_FINAL_STATE = PRESENT` (source `.31` only; no
  `0.0.0.0/0` source allow).
- `UFW_RULE_9443_FINAL_STATE = ABSENT`.
- Other persisted IPv4 rules: unchanged from the captured
  post-8443/pre-9443 baseline (identical SHA-256
  `cc25d5b0f86a2f15108fbf4f97f176989c3c08f36afb4c7b0017648ad0398aff`).
- SSH forwarding: absent. Insecure Gateway: absent.
- Pre-existing production `cloudflared`: present, PID `1263127`, targeting
  `127.0.0.1:8081`; untouched, no 8443/9443 socket or target, and not used by
  12D.
- `sudo -n ufw status numbered` could not render runtime numbering because
  sudo requires interactive authentication. UFW is active/enabled; persisted
  rules and host listener state were read directly.
- Push: NO. Merge: NO. Production cutover: NO. 12E automatically started: NO.

## Required chronology

- **FIRST ATTEMPT:** Center UFW blocked TCP/8443 (`PORT_BLOCKED`).
- **OPERATOR ACTION:** added source-scoped allow
  `176.12.77.31 -> TCP/8443`.
- **PHYSICAL COMPLETION:** direct TCP + TLS + mTLS + real grpcio +
  zero-inference E2E + reconnect + rotation + revocation = PASS.
- **FINAL CLEANUP:** temporary TCP/9443 rule removed; temporary listeners
  stopped.
