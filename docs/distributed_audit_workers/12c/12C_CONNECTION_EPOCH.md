# Durable connection epoch

Before every stream attempt `WorkerStateStore.reserve_connection_epoch()` takes
an inter-process file lock, reads `worker_state.json`, increments the epoch and
atomically persists it before the socket is opened. A crash after reservation
therefore burns an epoch; it cannot reuse one. Unit coverage proves initial,
increment, restart persistence and concurrent uniqueness. Gateway restart and
Agent restart E2Es prove that the next accepted stream uses a higher epoch.
