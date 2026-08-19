# 12F production PKI failure timeline

- **Initial script** — `/tmp/12f_prepare_production_pki.sh`, SHA-256
  `31cf3e9f6ef429fbc4cb9ba720ae82fb5719861327529e03b398b1e91b1479e9`,
  failed in the shell launcher with `ModuleNotFoundError: backend`.
- **Second script** — SHA-256
  `c40ca0c757c2ebb3f00620c12f80182e755d92e763da6581c03b2544934ac659`,
  used the immutable `reenrollment-e6015d33` Python/module root and created one
  complete production PKI set. The systemd issuer imported the module from the
  exact immutable release but restarted 12 times.
- **Exact service failure** — every recorded attempt stopped at
  `issuer_service.py:36` with `PermissionError: [Errno 13] Permission denied:
  '/etc/auditmanager/pki/issuer/issuing-ca-key.pem'`.
- **Operator cleanup** — the restart loop was stopped. Final read-only state at
  `2026-08-14T00:43:07+03:00` is loaded/enabled but inactive/dead, PID 0.
- **Read-only diagnosis** — `/tmp/12f_pki_readonly_diagnose.sh`, SHA-256
  `9477c8dd6dceef94d9931e94ebb6dc25f653262d89caaa5415e1ef8fa9ed2970`,
  returned `12F_PKI_READONLY_DIAG_PASS` and wrote
  `/tmp/12f_pki_root_diagnosis.txt`.
- **Deeper gate found** — active `e6015d33` resets the shared state directory
  to `0700` and DB to `0600`; this collapses the POSIX ACL mask for issuer and
  Gateway. Opening only `/etc/auditmanager` would therefore expose a second
  deterministic startup failure at the certificate registry.
- **Recovery implementation** — opt-in typed shared-state policy, canonical
  immutable launcher variables, read-only complete-PKI validator, launcher
  regression, isolated signing E2E and retry-idempotency proof were added.
- **Production action** — none. No issuer start, certificate issuance, Gateway
  start, backend restart, Worker switch or canary occurred in this pass.
