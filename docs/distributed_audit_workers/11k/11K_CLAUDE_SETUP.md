# Claude

- Supported pin: Claude Code 2.1.220, supplied as a pre-hashed central standalone artifact.
- Detection: `~/.local/bin/claude` first, then PATH; version must contain the pin.
- Auth check: official `claude auth status`, zero inference.
- Existing compatible installation/auth is not rewritten.
- Missing auth: `claude_login_required`; `provider-auth <session> claude` runs `claude auth login --claudeai` on the worker user's TTY and resumes the same session. No URL/code/token is persisted.
