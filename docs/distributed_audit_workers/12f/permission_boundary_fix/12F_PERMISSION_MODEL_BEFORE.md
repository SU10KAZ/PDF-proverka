# 12F permission model before the boundary fix

Captured read-only on 2026-08-14 before changing candidate code. Production was the
proven `e6015d33bf4fa6b8986a21fa4b9e33c10ec3139f` release throughout this
reconstruction.

## Runtime identities

| Process | User | Primary group | Relevant hardening |
|---|---|---|---|
| production backend | `coder` (uid 1001) | `coder` (gid 1001) | `RestrictSUIDSGID=yes`, `NoNewPrivileges=yes`, `ProtectSystem=strict`, `ProtectHome=read-only`, `UMask=0077` |
| certificate issuer | `web-ocr-cert-issuer` (uid 997) | `web-ocr-agent-gateway` (gid 984) | `RestrictSUIDSGID=yes`, `NoNewPrivileges=yes`, `ProtectSystem=strict`, `ProtectHome=yes`, `UMask=0007` |
| Agent Gateway | `web-ocr-agent-gateway` (uid 999) | `web-ocr-agent-gateway` (gid 984) | `RestrictSUIDSGID=yes`, `NoNewPrivileges=yes`, `ProtectSystem=strict`, `ProtectHome=yes` |

`coder` is not a member of gid 984. The backend can own the shared-state tree;
issuer and Gateway reach it through the dedicated group. The issuer and Gateway
units were inactive during reconstruction.

## Persistent paths

| Path | Observed owner:group / mode | Desired owner:group / mode | Backend R/W | Issuer R/W | Gateway R/W | SGID | Reason |
|---|---|---|---|---|---|---|---|
| `/var/lib/auditmanager` | `coder:coder` / `0710` | unchanged, with exact traverse ACL for uid 997 and uid 999 | yes/yes below child | traverse only | traverse only | no | Container is not a multi-writer directory. |
| `/var/lib/auditmanager/distributed_workers` | `coder:coder` / `0700`; named service ACLs masked ineffective after rollback | `coder:web-ocr-agent-gateway` / `02770`, exact access/default ACL | yes/yes | yes/yes | yes/yes | yes | Three approved identities share DB state; inherited gid must be deterministic under different umasks. |
| `.../incoming` | `coder:coder` / `0700` | `coder:web-ocr-agent-gateway` / `02770`, inherited/default ACL | yes/yes | no/no | no/no | yes | Created artifacts must remain in the shared group; only backend data-plane code writes payloads. |
| `.../source_packages` | absent in rolled-back baseline or private when created | `coder:web-ocr-agent-gateway` / `02770`, inherited/default ACL | yes/yes | no/no | yes/no | yes | Gateway reads manifests to construct authenticated job offers. |
| `.../result_packages` | absent in rolled-back baseline or private when created | `coder:web-ocr-agent-gateway` / `02770`, inherited/default ACL | yes/yes | no/no | no/no | yes | Backend receives, validates and imports results. |
| `.../event_logs` | absent in rolled-back baseline or private when created | `coder:web-ocr-agent-gateway` / `02770`, inherited/default ACL | yes/yes | no/no | yes/yes through domain calls | yes | Gateway event ingestion invokes the same domain writer. |
| `.../workers.db{,-wal,-shm}` | `coder:coder` / `0600` where present | approved creator:`web-ocr-agent-gateway` / `0660` | yes/yes | yes/yes | yes/yes | n/a | SQLite registry and worker state are shared. File ownership may be any approved runtime uid; group and mode are the cross-identity invariant. |
| `/etc/auditmanager/pki` | `root:root` / `0700` | parent remains deployment-owned; child identities remain split | no/no | scoped read only | scoped read only | no | PKI is explicit material, not a multi-writer data directory. |
| `.../offline-root` | `root:root` / `0700`; key `0600` | unchanged | no/no | no/no | no/no | no | Offline root must not be exposed. |
| `.../issuer` | uid 997:gid 984 / `0750`; signing key `0600` uid 997 | unchanged | no/no | yes/yes only where issuer contract allows | no/no | no | Gateway must never receive CA signing-key access. |
| `.../gateway` | uid 999:gid 984 / `0700`; key `0600` uid 999 | unchanged | no/no | no/no | yes/yes own material | no | Leaf identity is private to Gateway. |
| `.../trust` | `root`:gid 984 / `0750`; bundle `0640` | unchanged | no/no | yes/no | yes/no | no | Read-only trust distribution needs no inheritance. |

## Decision

SGID is required for the distributed-worker state tree, because files and nested
directories are created by different approved identities and all must inherit gid
984 even with service-specific umasks. Only a privileged deployment/bootstrap
step may establish owner, group, SGID and default ACLs. Hardened runtime validates
those invariants and fails before serving traffic if they are wrong. It must never
attempt to add SGID.

The production rollback left the state private to `coder`; that is safe for the
currently polling-only topology but is not the future shared-state target. No live
mode, ownership or ACL was changed during this reconstruction.
