# 12F PKI shared-state fix production deploy report

## Verdict

`PKI FIX PRODUCTION DEPLOY = FAIL_ROLLED_BACK`.

Exact candidate `5e3750a26f65a884c1c644c6bc25c490f4b665ef` was
materialized from the reviewed immutable release, selected by the production
pointer, and started as PID `2521866`. Its manifest, tree, bundle, runtime cwd
and release-specific venv were all exact. Core `/api/info` and the public API
returned HTTP 200, but authenticated production polling requests from Worker
`.31` returned HTTP 500. This is an explicit rollback trigger, so the backend
was immediately returned to exact `e6015d33`; no retry was attempted.

Final production Center state is `E6015D33_ROLLBACK`, PID `2522606`, healthy.
Final Worker polling is healthy, but candidate polling acceptance is `FAIL`.
Issuer and Gateway are inactive as required. `PKI OPERATOR RETRY = BLOCKED`.

## Timeline

- `07:12:56 +03:00`: restricted WAL-safe backup created.
- `07:23:22 +03:00`: final live freeze passed; Worker online/idle, queues zero,
  source write FDs and external deployers zero.
- `07:26:13.693777515 +03:00`: controlled candidate backend restart invoked.
- `07:26:35 +03:00`: candidate PID `2521866` started from immutable
  `pki-shared-5e3750a2`.
- `07:26:37.431634062 +03:00`: first candidate `/api/info` HTTP 200; measured
  restart-to-first-200 window `23.731 s`.
- `07:26:55 +03:00`: first observed authenticated Worker polling HTTP 500.
- `07:27:22 +03:00`: polling still HTTP 500.
- `07:27:23.430209168 +03:00`: immediate rollback invoked.
- `07:27:25 +03:00`: exact `e6015d33` PID `2522606` served both `/api/info`
  and authenticated Worker polling with HTTP 200.

The rollback restart recovered API health in approximately two seconds by
one-second journal resolution. SIGKILL was never used.

## Root cause and missing regression boundary

The candidate's `database._enforce_state_permissions()` unconditionally calls
`os.chmod(path, 0o2770)` for shared directories on every lazy database access.
The production backend unit has `RestrictSUIDSGID=true`; setting the directory
setgid bit is rejected with `EPERM`, even though the application can otherwise
read and write its persistent state. The resulting exception occurs before the
authenticated Worker token lookup and turns `/api/v1/worker/commands` into
HTTP 500.

The isolated regression tests used caller-owned temporary directories without
the hardened systemd sandbox. They proved typed config and ordinary DAC repair,
but did not prove the combined boundary:

`shared state + 02770 + RestrictSUIDSGID=true + production service identity`.

Another production deploy requires a new reviewed fix and a regression test
covering that exact boundary. Plausible remediation choices change either the
permission-enforcement algorithm or the service/deployment contract; neither
is authorized in this deploy-only step.

## Backup and rollback integrity

The backup is
`/home/coder/auditmanager/backups/pki-fix-deploy-20260814T071256+0300`, owned
`coder:coder`, directory mode `0700`, artifact mode `0600`. It contains a
SQLite online backup, backend phase/secrets config, backend/issuer/Gateway unit
definitions, rollback manifest, safe PKI metadata/hashes/fingerprints and UFW
status/hashes. Backup DB schema is 13, integrity is `ok`, SHA-256 is
`a4b7ade374d28febeae070c5ffe20f9651f6dd4d8659f2b95057826470131ee6`.

No DB restore was needed. Backup-to-final counts for workers, tokens, jobs,
attempts, offers, commands, admin actions, certificates and re-enrollment
records are identical. Only normal Worker heartbeat timestamps advanced.

## Final production state

- backend: exact `e6015d33`, PID `2522606`, NRestarts 0, HTTP 200;
- DB: `/var/lib/auditmanager/distributed_workers/workers.db`, schema 13,
  integrity `ok`, WAL;
- Worker: `wrk_19c87718` / `inst_boot_e129036dddf5c59049080ddd15624e72`,
  online, idle, POLLING ownership;
- Agent: PID `2212836`, NRestarts 0; Executor: PID `1384880`, NRestarts 0;
- active attempts/processes/EventOutbox unwritten/pending commands: `0/0/0/0`;
- logical jobs/offers/commands: `0/0/0`; scheduler disabled;
- issuer/Gateway: inactive; listeners `:8443` and `:9443`: absent;
- existing production PKI: not deleted, overwritten, regenerated or rotated;
- UFW hashes unchanged; nginx PID `57095` and cloudflared PID `1263127`
  unchanged; Caddy absent;
- Claude/Codex/OpenRouter runtime inference: `0/0/0`.

The post-rollback observation ran for `604.114 s` with 11 one-minute
samples. Every sample had PID `2522606`, NRestarts 0, HTTP 200, advancing
Worker heartbeat, zero queues, inactive issuer/Gateway and no `:8443/:9443`
listener. The rollback journal contained 653 authenticated Worker polling
HTTP 200 responses, zero polling HTTP 500 responses and zero error/traceback
lines during the observation window.

Production source, production DB contents, Worker identity/token, Agent,
Executor, issuer, Gateway, firewall, nginx and cloudflared were not manually
modified by this deployment. The only production mutations were the reviewed
phase env/pointer switch and two controlled backend restarts; pointer and env
were restored by rollback.

## PKI operator retry gate

The current `/tmp/12f_prepare_production_pki.sh` SHA-256 is
`89129b4445e120a636fd644ea89dce800ae69d1cf26d85f8940cf2b039bd360d`.
The file matches the immutable review, but its expected-release,
running-backend and shared-state guards correctly fail after rollback.

`READY_FOR_PKI_OPERATOR_RETRY = NO`.

Do **not** run the PKI preparation script. A new code/release review and new
explicit production deploy authorization are required before retry.

Push and merge were not performed.
