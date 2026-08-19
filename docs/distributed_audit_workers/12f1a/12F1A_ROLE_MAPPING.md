# 12F.1A worker authorization mapping

Machine and human authorization remain separate.

| Operation | Principal | Required permission |
| --- | --- | --- |
| subsystem/worker/job/result status | signed portal session | `distributed_workers.view` |
| worker registration | machine | one-time TTL/instance-scoped `wbt_` token |
| token claim | approved worker instance | one-time claim secret |
| heartbeat, polling, event/ACK, source/result | worker bearer identity | own worker/job/attempt scope; execution token where required |
| bootstrap session create/update/resume | signed portal session | `distributed_workers.admin` |
| worker approve/reject/revoke/token rotation | signed portal session | `distributed_workers.admin` |
| manual job/audit launch, cancel, mark-lost, deletion request | signed portal session | `distributed_workers.operate` |
| transport ownership mutation | machine protocol only | fenced worker transport identity; no human override route |
| certificate enrollment/renewal | scoped worker/bootstrap identity | protected issuer RPC policy |
| certificate revocation through worker revoke | signed portal session | `distributed_workers.admin` |

Roles are hierarchical: viewer → view; operator → view+operate; admin →
view+operate+admin. Anonymous and ordinary authenticated users not explicitly
listed are denied fail-closed.

Two product decisions remain:

- assign the six existing production portal subjects to viewer/operator/admin;
- decide whether a human worker drain action is required. The current API has
  no explicit drain mutation route; inventing one or assigning its role here
  would exceed evidence.

Therefore the permission model is understood, but production role mapping is
not ready.
