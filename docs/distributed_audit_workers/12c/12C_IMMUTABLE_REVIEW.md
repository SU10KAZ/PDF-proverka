# 12C immutable adversarial review

Base: `61e077549ca316ffb5974835106556eac671c571` (12B). Reviewed final code
candidate: `349c229940ad2e3562fb0d33d88670f3dfb955c6`, in detached worktree
`.claude/worktrees/12c-immutable-final2-review`. The candidate worktree was not
edited during review.

## Candidate history and findings

- `d1c4ce596714bac2f1a2f0bb2b756875f8a22fa1`: first immutable review found
  negotiated EventBatch bound, CancelAck replay identity, controlled transport
  switch, exact-attempt HTTPS upload authorization, strict target parsing and
  restarted-instance registration gaps.
- `21b40fabd968f5d061a003e0bf2e9f3811b3b26a`: review found premature backoff
  reset, permanent protocol-version stop, same-stream duplicate-offer filtering
  and ambiguous source transfer selection.
- `8e6e5c5e9d526dc304fe36b0f40a4c619c2be4fc`: review found an in-memory-only
  duplicate guard and an uncovered JobAccept→local-enqueue crash window.
- `349c229940ad2e3562fb0d33d88670f3dfb955c6`: all findings fixed; no new
  findings in the final review.

## Seven lenses

1. **Polling/gRPC semantic parity — PASS.** One `WorkerAgent`, one Executor,
   one validator/package/result/retention path. Polling remains default and its
   real vertical slice passes.
2. **No duplicate Executor — PASS.** Active threads, durable attempt metadata
   and `worker.db` are all consulted. Re-offers reach Agent core; verified or
   accepted crash recovery resends JobAccept and idempotently enqueues once;
   terminal attempts are never recreated.
3. **EventOutbox/reconnect — PASS.** Existing disk outbox is reused. Negotiated
   batches are bounded; ACK advances the cursor; lower Center cursor rewinds to
   retained segments; impossible upper cursor fails closed; disconnect replay
   is deduplicated.
4. **Cancel/result ACK/retention — PASS.** Cancel uses the durable local command
   queue and replayed identity. Result bytes remain HTTPS; ResultReady is
   replayable; rejection keeps data; retention is written only from ResultAck.
5. **Durable epoch/crash windows — PASS.** Epoch is atomically persisted before
   each connection attempt. Agent/Gateway restart, pre-dispatch recovery,
   EventAck and ResultAck loss windows have regression evidence.
6. **Transport ownership/no dual lease — PASS.** A live gRPC connection fences
   polling in the shared claim transaction. Controlled restart can select
   polling only after the active connection id is cleared. There is no automatic
   fallback.
7. **Security/no admin-plane creep — PASS.** `test_insecure` is loopback-only;
   public targets and port 8443 fail closed; HTTPS verification remains enabled;
   no update/restart/shell RPC, certificate, firewall, proxy or production
   service change was added.

Immutable execution evidence: `tests/test_agent_grpc_client_12c.py` — **30
passed** including the real 60-second idle stream and four real runtime E2Es;
12A/12B/11K/11L plus polling vertical slice — **129 passed**. Python compile,
JSON parsing and `git diff --check` passed. Real inference was Claude 0, Codex 0,
OpenRouter 0. Physical `.31` proof remains safely deferred to 12D because 12C
has neither mTLS nor a proven TLS HTTP/2 bidi tunnel.
