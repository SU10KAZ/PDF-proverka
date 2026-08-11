# Known issues / honest evidence boundary

1. No second physical clean foreign VPS was available: `REAL CLEAN FOREIGN VPS = NOT_PROVEN`.
2. SSH password mode is intentionally not implemented; agent or central private-key reference is required. This avoids accepting a password into API/state.
3. Pinned Claude/Codex standalone binaries must be preloaded in the central artifact inventory with version and SHA-256. Bootstrap never downloads `latest` or executes `curl | sh`.
4. First target is non-root Ubuntu/Debian x86_64/aarch64 with user-systemd. Linger requires either already enabled or passwordless sudo. Root-only hosts require prior creation of a non-root runtime user.
5. Physical reboot was not performed; autostart evidence is unit enablement + linger, not a reboot on `.31`.
6. No frontend form was added; typed API and progress events are ready for it.
7. `.31` read-only inventory found its legacy units disabled/inactive and OpenRouter absent. No claim is made that production is READY, and no production state was changed.
8. Fake clean-host installer and fake HTTPS runtime are strong isolated tests but not a single physical host image; the next gate is a disposable real VPS.
9. A minimal host must already satisfy the hard preflight tools (`python3`, `tar`, `sha256sum`, `curl`, `systemctl`) and support Python venv/pip. Bootstrap installs pinned Python requirements and optional pinned provider CLIs, but deliberately does not run an OS upgrade or an unconstrained package-manager transaction.
10. The repository-wide pytest collection also needs ignored geometry corpora and `norms/tools/venv`; a clean worktree lacks them. The final evidence therefore reports those baseline setup errors separately from the 752 passing tests covering changed contours.
