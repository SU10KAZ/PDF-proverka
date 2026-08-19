# RestrictSUIDSGID analysis

The failed candidate `5e3750a2` put an unconditional
`chmod(path, 02770)` in `database._enforce_state_permissions()`. That function is
reached from `ensure_ready()`, and `ensure_ready()` is reached by every database
read/write context, including authenticated command polling. Under the actual
backend unit, `RestrictSUIDSGID=yes` rejects an attempt to add or reassert SGID
with `EPERM`; the backend therefore served normally until the first Worker DB
request and then returned HTTP 500.

This is an architectural boundary defect, not a reason to weaken systemd. The
property remains enabled. Directory metadata is a deployment invariant and is
created by an explicit privileged, idempotent, path-scoped setup action. Runtime
uses `lstat/stat` validation and fails at application startup with a typed
configuration error if owner, shared gid, file type or mode is wrong.

Ordinary data-file creation is distinct. SQLite sidecars and data-plane artifacts
are runtime data. SGID/default ACL inheritance supplies their shared gid. A file's
creator may add only the missing owner/group read/write bits when the path is a
plain regular file, already has the exact shared gid, is owned by that process and
has no executable or `other` permissions. Wrong group, unsafe bits, non-regular
objects or non-owner corrections fail closed. This exception never changes a
directory, never changes ownership and never sets SGID.
