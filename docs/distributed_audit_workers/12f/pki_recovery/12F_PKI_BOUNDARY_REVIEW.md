# 12F PKI boundary review

The existing material preserves the 12D boundary. The offline root key is
`root:root 0600`; the online issuing key is owned by the dedicated issuer UID
and is `0600`. The Gateway server key is a different `0600` file owned by the
Gateway UID. Gateway receives only its server chain, Worker trust bundle and a
local Unix-socket client path.

The installed Gateway unit has no `ReadOnlyPaths`/`ReadWritePaths` grant to the
offline-root directory. Although its shared group can traverse the issuer
directory after the ancestor fix, the issuing key remains owner-only `0600`, so
the Gateway UID cannot read it. The recovery must not change that file mode or
owner.

The new shared-state configuration applies only to
`/var/lib/auditmanager/distributed_workers`: `2770` directories and `0660`
SQLite files preserve explicitly provisioned named-user ACL masks. It does not
broaden PKI key access. The default remains private `0700/0600` when the typed
flag is absent.

Verdict: **BOUNDARY PASS IN CODE/EXISTING MATERIAL**. Production activation is
blocked until the same patch is deployed from the active `e6015d33` lineage.
