# Known issues and non-gates

- `DEDICATED VPS REBOOT PROOF = NOT_YET_TESTED`. Shared `.31` must not be
  rebooted. Process/service recovery is the 12E scope; a dedicated Worker VPS
  can prove host reboot separately. This is not a first-canary blocker if all
  process-level gates pass.
- `WINDOWS DPAPI = IMPLEMENTED_NOT_PHYSICALLY_PROVEN` remains a platform
  evidence limitation from 12D.
- The pre-existing production cloudflared targeting `127.0.0.1:8081` remains
  untouched and is not a 12E transport component.
- 12E is currently incomplete; the explicit blockers are in
  `12E_12F_ENTRY_GATE.json`.
