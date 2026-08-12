# 12F.1A verification report

Candidate tests were not run because no exact baseline or candidate may be
built after the hard source-equivalence gate fails.

Completed checks:

- 100-file hash/metadata classification; zero category `UNKNOWN`;
- exact 43-file historical core reconciliation plus two later paths;
- disk manifest before/after equality around restricted forensic capture;
- 11 production-only commits classified, zero commit `UNKNOWN`;
- 75 dirty source paths mapped; four operator-purpose/runtime-membership
  `UNKNOWN`;
- Center and Worker `.31` DNS/TCP/TLS/health proof for `auditmanager.app`;
- JSON syntax and Git whitespace validation for evidence artifacts.

Not run: baseline boot, candidate boot, rollback dry run, migrations, role
negative tests, permission staging tests, or 12A–12E exact-candidate suites.
Provider calls remained Claude/Codex/OpenRouter `0/0/0`.
