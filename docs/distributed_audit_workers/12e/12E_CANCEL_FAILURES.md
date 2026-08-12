# Cancel failures

`command_id` is the cancellation idempotency boundary. Local regressions prove
that an unacknowledged CancelCommand is replayed after reconnect and that the
same acknowledgement is not a second semantic cancellation.

The still-open C23 fault is a process-scoped Gateway SIGKILL after central
command persistence and before delivery. C24 is held pending its relevant
executor regression rerun; it must preserve a finished result when a late
cancel arrives.
