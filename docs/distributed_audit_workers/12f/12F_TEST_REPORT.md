# 12F test report

No candidate regression or live test was started. The immutable 12E base gate
was verified as `12f_allowed=true`, but the subsequent production read-only
preflight failed before candidate deploy: the unchanged `:8081` runtime does
not expose an enabled distributed control plane and has no production
`workers.db`.

Test count in 12F: `0`. Production jobs: `0`. Provider inference:
Claude/Codex/OpenRouter `0/0/0`.

This is intentional fail-closed behavior, not a test pass. The 12E evidence is
not promoted into a 12F production result.
