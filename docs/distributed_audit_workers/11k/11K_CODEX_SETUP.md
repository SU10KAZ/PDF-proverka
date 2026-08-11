# Codex

- Supported pin: Codex CLI 0.147.0, supplied as a pre-hashed central standalone artifact.
- Detection: `~/.local/bin/codex` first, then PATH; version must contain the pin.
- Auth check: official `codex login status`, zero inference.
- Existing compatible installation/auth is preserved.
- Missing auth: `codex_login_required`; `provider-auth <session> codex` runs `codex login --device-auth` directly on the VPS TTY and resumes the same session. Neither device code nor token is stored.
