# Provider setup flow

The release installs hashed policy `provider_policy.approved.json` and the worker advertises capabilities from that local policy. Checks are zero-inference: executable/version/auth metadata for Claude/Codex and `lstat`/mode for the OpenRouter credential.

Missing Claude/Codex uses center-preloaded standalone artifacts listed with exact version and SHA-256. Allowed pins are Claude 2.1.220 and Codex 0.147.0; `latest` and `curl | sh` are absent. Existing compatible binaries and auth homes are preserved.

Missing auth produces one persistent `action_required` session. `provider-auth` attaches the operator TTY directly to the VPS; output/code/token is not captured by center. After a successful action it resumes automatically, rechecks status and skips core install.

When all three are ready, approved policy covers exact presets `claude_gpt_codex` and `codex_exec`. A missing required provider keeps the session out of READY.
