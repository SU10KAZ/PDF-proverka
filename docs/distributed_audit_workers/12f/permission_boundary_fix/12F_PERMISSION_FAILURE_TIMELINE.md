# 12F permission-boundary failure timeline

- 2026-08-14 07:26:13 MSK — reviewed candidate `5e3750a2` selected and the
  production backend restart began.
- 07:26:37 — core API first returned HTTP 200. Issuer and Gateway remained
  inactive; scheduler remained disabled.
- 07:26:55 — the first authenticated production polling
  `GET /api/v1/worker/commands` returned HTTP 500. The traceback showed
  `os.chmod(path, 02770)` rejected with `EPERM` under
  `RestrictSUIDSGID=true`.
- 07:27:23 — rollback to `e6015d33` began. No DB restore was required.
- 07:27:25 — polling returned HTTP 200 again. The production Worker, Agent and
  Executor had not restarted and identity was preserved.
- 08:15 — read-only reconstruction confirmed three service identities and the
  need for shared gid 984 inheritance. No production mutation was made.
- 08:50 — an isolated real user-systemd service running exact old immutable
  `5e3750a2` reproduced commands/heartbeat HTTP `500/500` and the same EPERM.
- 08:50 — the identical hardened boundary running exact new immutable
  `dd8c760e` returned commands/heartbeat HTTP `200/200`; no EPERM or traceback.
- 08:51 — an intentionally mode-`0770` state root caused typed startup failure,
  exit status 3 and no listener.
- 08:53 — isolated rollback `e6015d33 → dd8c760e → e6015d33` passed with the
  same schema-13 logical DB hash.

The historical failed deployment remains evidence. It is not reclassified or
deleted; the new candidate supersedes it from the same proven production parent.
