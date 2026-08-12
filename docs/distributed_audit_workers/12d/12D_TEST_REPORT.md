# 12D test report

## Immutable local suite

Command ran in detached/read-only candidate `f0ca3260` with pinned grpcio
dependencies. Result: **183 passed, 0 failed, 0 skipped**.

| Scope | Collected | Passed |
|---|---:|---:|
| Agent Stream protocol v1 | 45 | 45 |
| 12B Agent Gateway | 47 | 47 |
| 12C grpc Agent client | 30 | 30 |
| 11K bootstrap | 31 | 31 |
| 11L bootstrap | 3 | 3 |
| 12D mTLS/certificates | 27 | 27 |

The only pytest warning is the pre-existing `passlib` import of Python's
deprecated `crypt` module. No gRPC pending-task warnings remain.

The 27-test 12D suite covers ephemeral CA/profile issuance, real grpcio mTLS,
missing/untrusted/wrong-identity/time/EKU rejection, active-stream revocation
and expiry, renewal identity/idempotency/lost-response behavior, new-key and
server-leaf rotation, Linux permissions/symlink guards, Windows platform guard,
registry persistence, multi-root trust, protected issuer socket, issuer startup
migration, decommission revocation, automatic repeated renewal scheduling,
public-insecure refusal and secret scan.

## Physical checks

- Worker-local P-256 key + CSR and protected initial enrollment: **PASS**.
- Center temporary production-mode mTLS bind `0.0.0.0:8443`: **PASS**.
- Center loopback TLS 1.3/server IP SAN: **PASS**.
- Missing client certificate at public Gateway: **PASS, rejected**.
- `.31 → Center :22`: **PASS**.
- `.31 → Center :8443`: **FAIL, timeout (rc 124)**.
- Real physical grpcio stream, job E2E, reconnect, rotation, revoked reconnect:
  **NOT TESTED** because TCP failed before TLS.
- Windows DPAPI physical test: **NOT TESTED**, no Windows host.
- Claude/Codex/OpenRouter inference: **0/0/0**.

No skipped physical step is reported as PASS.
