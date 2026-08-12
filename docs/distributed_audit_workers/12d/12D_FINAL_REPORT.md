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

One cleanup-only item prevents an unconditional overall PASS: the temporary
source-scoped `.31 -> TCP/9443` rule is still persisted because sudo requires
interactive authentication. The isolated 9443 listener is stopped, so the
rule currently exposes no service. No physical security proof needs rerun after
the operator removes that exact rule.

## Required verdicts

- **A. 12D OVERALL: PARTIAL — technical/physical acceptance PASS; one inert
  operator firewall-cleanup item remains.**
- **B. MTLS CONTROL CHANNEL: PASS.**
- **C. CERTIFICATE LIFECYCLE: PASS.**
- **D. DIRECT `.31 -> CENTER :8443`: PROVEN.**
- **E. PHYSICAL REAL GRPCIO: PROVEN.**
- **F. ZERO-INFERENCE PHYSICAL E2E: PASS.**
- **G. SECURE RECONNECT: PASS.**
- **H. PHYSICAL KEY/CERT ROTATION: PASS.**
- **I. PHYSICAL REVOCATION: PASS.**
- **J. TUNNEL USED: NO.**
- **K. TUNNEL REQUIRED FOR TESTED `.31 -> .128` PATH: NO.**
- **L. PRODUCTION CUTOVER: NOT_DONE.**

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
- `UFW_RULE_8443_FINAL_STATE = PRESENT` (source `.31` only).
- `UFW_RULE_9443_FINAL_STATE = PRESENT_INERT_PENDING_OPERATOR_REMOVAL`.
- Push: NO. Merge: NO. Production cutover: NO. Proceed to 12E: NO.

Operator cleanup command:

```bash
sudo ufw delete allow proto tcp from 176.12.77.31 to any port 9443
sudo ufw status numbered
```
