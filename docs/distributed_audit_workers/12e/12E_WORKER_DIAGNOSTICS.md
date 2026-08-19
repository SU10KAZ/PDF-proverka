# 12E Worker diagnostics

Operator command:

```text
python -m audit_worker doctor --root <isolated-worker-root>
```

The command is offline and read-only: it does not register, call a provider,
connect to Center, change transport, create directories, migrate SQLite, or
print tokens, keys, credentials or raw exception text.  SQLite is opened only
with `mode=ro`; an absent Worker root remains absent.

It reports the required durable operational facts:

- transport mode, gRPC connection state, Gateway status and connection epoch;
- last connect/disconnect and typed disconnect reason;
- certificate expiry when local mTLS material is readable;
- last heartbeat and typed heartbeat error;
- active attempts, slots, executor state, outbox count and ACK cursor;
- pending cancel/result count, data-plane configuration state and disk free;
- whether a token exists, never its value.

The Agent persists only a constrained diagnostic schema.  Its state-file
read/modify/write operations are file-locked so an epoch reservation cannot
erase a concurrent disconnect observation.
