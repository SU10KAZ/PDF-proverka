# Operator UX

1. Submit host/user/auth-ref/fingerprint/install-root/center URL once.
2. SSH identity and VPS preflight pass.
3. Release, dependencies, policy, config and units install automatically.
4. Existing provider auth is detected; missing pinned CLI installs automatically.
5. If Codex/Claude needs login, run the displayed `provider-auth` command; it attaches the trusted-host TTY and resumes the same session after success.
6. If OpenRouter is required, the same command accepts hidden remote input and resumes; center never sees it.
7. A manual `resume <same-session-id>` remains available after interruption or browser/device completion.
8. Worker registers/claims automatically, services start, heartbeat and revision appear.
9. A no-inference protocol job completes and is ACKed.
10. Status becomes READY.

No manual mkdir, copy, `worker.env`, systemd edit, worker ID, center URL edit or registration approval.
