# 11K final report

## Verdicts

- **BOOTSTRAP SYSTEM: PASS** for architecture/installer logic and isolated tests.
- **REAL CLEAN FOREIGN VPS: NOT_PROVEN** — no second disposable physical VPS was available.

Base: `1367bc6f8044cb24817b2ba2620e1742f629f7fa`. Immutable code candidate: `6431c004a61486b10687e1f8f593627b16120e04`. Seven-lens detached review: PASS. The exact docs-only evidence tip is reported in the final handoff.

## Delivered

One CLI command or one admin API request starts the persistent workflow. Operator supplies SSH host/port/user, auth reference, trusted fingerprint, install root, display name, center HTTPS URL and provider profile. Directories, deterministic transfer, venv, policy, config, namespaced Agent/Executor units, registration/approval/claim, heartbeat, capability/revision checks and protocol self-test are automatic.

Human-only steps are Claude browser auth, Codex browser/device auth and hidden OpenRouter input directly on the VPS. The integrated `provider-auth` command resumes the same session after success. Center never sees provider credentials.

Claude/Codex install is automatic when the approved pinned artifacts are present in central inventory; compatible existing CLI/auth is preserved. OpenRouter provisioning is worker-local mode 0600. Registration token is TTL/scoped/one-time/hash-only/stdin. Runtime needs no inbound port and uses outbound HTTPS.

Repeat install is idempotent both by request key and by a stable non-secret installation identity. Update, repair, rollback, status, validate, uninstall and deregister use the same manager; uninstall also revokes the known center identity. Provider auth/data and unrelated services are preserved by default. Credential leaks: 0 in adversarial fixtures. Production changes: NO. Real audit provider calls: 0.

Evidence: 752 focused tests PASS, deterministic candidate archive/manifest hashes match across two builds, immutable security review 7/7 PASS. Repository-wide collection has only the separately documented clean-worktree fixture/setup blockers; no unexplained 11K regression is present.

## 9-line operator workflow

1. Prepare approved bundle + pinned CLI artifact inventory on center.
2. Obtain the VPS SSH host-key fingerprint out of band.
3. Run `audit_worker_bootstrap.py install --spec ...`.
4. Watch the single session progress.
5. Complete any displayed Claude/Codex login directly on the VPS.
6. Enter OpenRouter at the hidden remote prompt if that provider is required.
7. The provider-auth command resumes the same session (or use explicit `resume` after an interruption).
8. Wait for heartbeat, revision/capability gate and fake protocol job ACK.
9. Receive READY.

## Remaining real-world gate

Rent/reset one disposable Ubuntu/Debian VPS, place the two approved pinned CLI artifacts in central inventory, obtain its fingerprint, run the one command, perform provider logins, verify a physical reboot, and archive the session evidence. No code redesign is expected; this is the missing external proof.
