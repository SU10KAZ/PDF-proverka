# 12F PKI shared-state immutable release review

Review target: commit `5e3750a26f65a884c1c644c6bc25c490f4b665ef`,
tree `d9a9c42732337b567d4fe7efe6c339e0612dcc3c`, durable inactive release
`/home/coder/auditmanager/releases/pki-shared-5e3750a2`.

1. **Production e6015d33 compatibility — PASS.** The fix commit has exact
   production `e6015d33` as its direct parent. Schema remains 13 and the
   re-enrollment implementation is unchanged.
2. **PKI shared-state correctness — PASS.** Shared mode is strict opt-in,
   defaults remain private, ACL masks remain effective, and SQLite main/WAL/
   SHM/backup paths receive verified `0660`; state directories receive `2770`.
3. **Immutable launcher paths — PASS.** The future rendered issuer and Gateway
   units use exact `/opt/auditmanager/releases/pki-shared-5e3750a2` paths and
   the production state root `/var/lib/auditmanager/distributed_workers`.
4. **No duplicate CA — PASS.** A two-pass old-partial-state retry preserved all
   eight PKI files and created neither a second root nor a second intermediate.
5. **PKI boundary/CA key separation — PASS.** The issuing key remains issuer-
   owned `0600`; Gateway gets no issuing-key environment or filesystem read.
6. **Re-enrollment preservation — PASS.** Identity security/E2E, Center routes,
   polling heartbeat and exact-pair behavior passed in the 100-test critical
   group on both the integration tree and durable artifact.
7. **Rollback — PASS.** Isolated `e6015d33 -> 5e3750a2 -> e6015d33` retained the
   schema-13 logical DB digest and synthetic Worker registry.
8. **Operator script fail-closed — PASS.** Expected release guard is line 48,
   running-process guard line 66, shared-state activation guard precedes the
   first production mutation at line 108. Current `e6015d33` cannot pass.
9. **No secret leakage — PASS.** No provider credential, private key, portal
   password or raw Worker token was written to source, release metadata or
   evidence.
10. **Production containment — PASS.** `current` still points to
    `reenrollment-e6015d33`; PID/restarts, DB, Agent, Executor, issuer, Gateway,
    listeners, nginx and pre-existing cloudflared remain unchanged.

Detached `git archive` SHA-256
`6cbd2c7cd2ccd782e48351d7b583ec1ef412229e001efa9fbbe62aff35c76526`
matched the durable app byte-for-byte. Bundle verification passed. There are
no non-symlink writable paths in the release. Mutable development roots occur
only as pre-existing data-path documentation/configuration; no runtime
`PYTHONPATH`, `WorkingDirectory`, `ExecStart` or import depends on them.

Security/release verdict: **PASS**.
