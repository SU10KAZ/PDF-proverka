# 12D immutable security review

Reviewed implementation candidate:
`f0ca3260213efdf9bf1b9605c35f3d27ec15c8ba` in detached read-only worktree
`/tmp/12d-immutable-review-f0ca3260`. The combined suite passed 183/183.

Earlier candidates were rejected and replaced, never edited in place:

- `60129d31`: bootstrap read issuer key, decommission missed certificates,
  and Gateway lacked issuer unit dependency.
- `b7570ef9`: physical startup exposed missing issuer-side schema migration.
- `5eef3977`: cryptography 46 exposed eager deprecated datetime fallback.
- `912ba9b2`: clean test review exposed incomplete gRPC task draining.
- `b9351c50`: physical-readiness review found renewal was callable but not
  automatically scheduled by Agent.
- `f0ca3260`: all above fixed; suite exits without pending gRPC tasks.

## Nine lenses

1. **PKI / CA separation — PASS.** Offline root and online intermediate are
   separate; Gateway has no signing-key path; issuer socket checks peer UID.
2. **Worker private-key lifecycle — PASS.** Worker-only generation, Linux
   owner/mode/symlink checks, DPAPI implementation, no key transport.
3. **Certificate ↔ worker identity — PASS.** Canonical URI SAN is checked at
   CSR, TLS auth context, registry, AgentHello and renewal boundaries.
4. **Renewal / rotation — PASS_LOCAL.** Agent auto-scheduling, repeated new-key
   cycles, staging, candidate handshake, idempotency and activation are covered;
   physical run was network-blocked.
5. **Revocation / expiry — PASS_LOCAL.** Registry status is enforced on new
   RPC and periodically on active streams; physical reconnect was not reached.
6. **Gateway TLS configuration — PASS.** Public insecure mode fails closed,
   key/profile/owner mismatch fails startup, client cert is mandatory.
7. **Data-plane authorization — PASS.** Bytes remain HTTPS; tuple-bound
   transfer authorization and cross-worker denial remain regression-covered.
8. **Bootstrap / resume / leakage — PASS.** Only CSR crosses SSH/admin plane;
   initial issuance uses protected issuer socket and deterministic request ID.
9. **Direct public / no tunnel — FAIL_EXTERNAL.** No tunnel was used, but UFW
   omitted 8443 and the source-scoped temporary rule required unavailable
   interactive sudo authentication. `.31 → :8443` timed out.

Software security review verdict: **PASS**. 12D physical acceptance verdict:
**FAIL (`PORT_BLOCKED`)**. These are deliberately separate verdicts.
