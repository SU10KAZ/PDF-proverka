# Immutable security review

Reviewed code candidate: `6431c004a61486b10687e1f8f593627b16120e04`.

Review environment: detached clean worktree `/tmp/11k-review-6431c004`; each lens was a separate process of `scripts/review_worker_bootstrap_11k.py` with the full expected commit supplied. The script refuses a mismatched commit or dirty worktree. Candidate regression recheck in the same worktree: `tests/test_worker_bootstrap_11k.py` = 26 passed.

| # | Independent lens | Result | Verified boundary |
|---|---|---|---|
| 1 | SSH credential handling | PASS | opaque auth ref/agent only; key mode <=0600; no credential in worker runtime |
| 2 | Remote host verification | PASS | strict=yes; out-of-band fingerprint; only matching key line; atomic 0600 known_hosts |
| 3 | Registration token lifecycle | PASS | SHA-256-only DB; TTL; atomic one-time consume; instance scope; stdin exchange |
| 4 | Provider secret handling | PASS | direct remote TTY; hidden OpenRouter input; local 0600; no center/API/output capture |
| 5 | Bootstrap logs/state | PASS | request allowlist; recursive redaction; sanitized 422; no plaintext token DB |
| 6 | Rollback/destructive operations | PASS | validated root; allowlisted uninstall move; config snapshot restore; provider/data preservation |
| 7 | Existing VPS isolation | PASS | full-root hashed units; no production constants; no firewall/sshd mutation; owned units only |

No finding required a code change after this immutable review. This document is committed as a docs-only child of the reviewed candidate; the exact evidence tip is reported by `git rev-parse HEAD` in the final handoff.

Final immutable security review verdict: **PASS (7/7)**.
