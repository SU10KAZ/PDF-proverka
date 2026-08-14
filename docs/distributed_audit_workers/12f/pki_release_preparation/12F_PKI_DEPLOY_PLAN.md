# Future controlled PKI shared-state fix deployment

This plan was prepared but not executed. A new explicit operator authorization
is required for the release switch, backend environment change and restart.

1. Reverify production still runs exact `e6015d33`, API is 200, PID/restart
   state is stable, schema is 13/integrity `ok`, scheduler is disabled, and
   issuer/Gateway are inactive.
2. Back up the production `workers.db` using a consistent SQLite snapshot,
   backend configuration, installed systemd units/environments and persistent
   `/etc/auditmanager/pki` state. Record hashes and ownership/modes without
   exposing private-key content.
3. Reverify durable release `pki-shared-5e3750a2`: commit, manifest, archive,
   bundle and dependency hashes from `12F_PKI_RELEASE.json`.
4. Add exact `DISTRIBUTED_WORKERS_SHARED_STATE=true` to the controlled backend
   environment while keeping `DISTRIBUTED_AUDIT_EXECUTION_ENABLED=false`.
5. Atomically switch `/home/coder/auditmanager/current` to the exact new
   release and perform one controlled backend `:8081` restart.
6. Verify process cwd/commit, API/core/distributed routes, schema 13/integrity,
   Worker `wrk_19c87718`, authenticated polling heartbeat, zero jobs/offers,
   and shared-state `2770/0660` with effective service ACLs.
7. Confirm scheduler remains disabled, production issuer remains inactive,
   Gateway remains inactive, and `:8443/:9443` remain absent during a short
   stability observation.
8. Only after a separate PKI operator approval verify SHA-256
   `89129b4445e120a636fd644ea89dce800ae69d1cf26d85f8940cf2b039bd360d`
   and run exactly `sudo bash /tmp/12f_prepare_production_pki.sh`.
9. Expect `12F_PKI_PREP_PASS` and safe receipt
   `/tmp/12f_pki_prepare_receipt.json`; confirm issuer active from exact
   `/opt/.../pki-shared-5e3750a2`, existing CA fingerprints preserved, and
   Gateway still inactive.
10. Worker certificate issuance, Gateway start, gRPC cutover and canary remain
    later separately authorized actions.

Immediate backend rollback is exact durable release
`/home/coder/auditmanager/releases/reenrollment-e6015d33`. Schema stays 13;
do not downgrade or delete the DB. Rollback stops the new issuer/Gateway
contour and restores the old backend pointer/config before one controlled
restart. Persistent PKI state under `/etc/auditmanager/pki` is retained.
