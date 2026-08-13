# Conflict matrix

| Condition | Result | Typed reason |
|---|---|---|
| Authorization absent | reject | `AUTH_NOT_FOUND` |
| Token digest mismatch | reject | `TOKEN_INVALID` |
| Authorization expired | reject, persist expired | `AUTH_EXPIRED` |
| Authorization revoked | reject | `AUTH_REVOKED` |
| Consumed with different request/key | reject | `AUTH_CONSUMED` |
| Consumed with exact request/key | safe state, no credential | `IDEMPOTENT_COMPLETED` |
| Requested Worker differs from stored pair | reject | `WORKER_ID_MISMATCH` |
| Requested instance differs from stored pair | reject | `INSTANCE_ID_MISMATCH` |
| Worker ID already has another instance | reject | `WORKER_ALREADY_BOUND_OTHER_INSTANCE` |
| Instance belongs to another Worker | reject | `INSTANCE_ALREADY_BOUND_OTHER_WORKER` |
| Exact Worker already exists outside this completion | reject | `WORKER_ALREADY_EXISTS` |
| Invalid Worker format | reject | `INVALID_WORKER_ID` |
| Invalid instance format | reject | `INVALID_INSTANCE_ID` |
| Invalid configured TTL | reject | `INVALID_TTL` |
| Same ADMIN key with different body | reject | `IDEMPOTENCY_KEY_REUSED` |
| Persisted rows disagree after completed retry | reject | `REGISTRY_INCONSISTENT` |

There is no automatic reassignment and no empty-registry shortcut.
