# 12F.1 Phase A test report

Reference final-12E/12F runtime result:

- `658 passed, 1 skipped` in `141.45 s`;
- one passlib `crypt` deprecation warning;
- 12A protocol, 12B Gateway, 12C Agent, 12D mTLS, 12E reliability,
  polling/Center, bootstrap/update, execution backend, ownership, EventOutbox,
  retention and central-handoff groups were included;
- the first sandboxed attempt was stopped at the first real local gRPC socket
  because the environment denied socket creation; the exact suite passed when
  rerun under the approved network-capable test scope.

Isolated HTTP dry-run additionally passed health, portal protection, core
project routes, distributed routes, canonical DB initialization, synthetic
legacy polling register/approve/claim/heartbeat/no-job poll and scheduler-off
checks. Startup-to-health was `1.992732 s`; schema 0→11 initialization was
`0.006007 s`.

These results qualify the reference 12E runtime only. Candidate tests are not
marked PASS because no immutable integration with the mutable production
baseline exists. Provider inference remained Claude/Codex/OpenRouter `0/0/0`.
