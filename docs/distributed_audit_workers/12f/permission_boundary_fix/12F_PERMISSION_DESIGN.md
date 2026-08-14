# Shared-state permission design

## Boundary separation

1. **Deployment/bootstrap:** the explicit operator tool
   `scripts/manage_distributed_worker_state.py prepare` creates only the known
   static state directories, assigns the exact backend owner and shared group,
   sets `02770`, installs an exact default ACL, repairs only known SQLite files,
   and gives the two service UIDs traverse-only access to the container parent.
   It is idempotent, non-recursive and fail-closed.
2. **Application startup:** backend lifespan, issuer startup and Gateway startup
   validate plain paths, exact directory uid/gid/mode/default ACL and DB
   accessibility before serving traffic.
3. **Request-time DB access:** performs stat validation only. It never calls
   `chown` and never chmods a directory or adds SGID. A creator may complete
   `0660` only on its own plain file with the exact shared gid and no unsafe
   bits; all other mismatches fail closed.

## SGID decision

SGID is required. Backend uid 1001 owns the tree but is not a member of the
service gid 984; issuer uid 997 and Gateway uid 999 use gid 984. New DB
sidecars, event logs and package directories therefore need deterministic group
inheritance across different umasks. `02770` is a deployment invariant, not an
application repair operation.

## Alternatives rejected

- Catching `PermissionError` would hide an unusable or unsafe state tree.
- Disabling `RestrictSUIDSGID` would weaken the boundary and leave the wrong
  responsibility in runtime.
- A privileged `ExecStartPre` would silently grant privilege on every restart.
- systemd-tmpfiles would introduce a second provisioning mechanism and does not
  cover existing SQLite files, exact parent ACLs or detached validation. The
  existing operator/deployment-script architecture is the canonical boundary.

## PKI separation

The PKI tree is not placed under the shared creator model. Offline root remains
root-only; the issuing key stays uid 997 mode `0600`; Gateway key stays uid 999
mode `0600`; trust material is read-only. Gateway never gets signing-key access.
