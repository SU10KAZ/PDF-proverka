# Durable connection epoch

The fence key is `worker_id`; the approved `worker_instance_id` must also match. `worker_transport_sessions.last_connection_epoch` is committed in the same transaction that replaces `active_connection_id`.

- first positive epoch: accepted;
- strictly greater epoch: accepted and the old in-process stream receives a typed stale/superseded error before close;
- equal or lower epoch: rejected;
- gateway restart: the stored epoch remains, so equal/lower remains rejected;
- stale disconnect cleanup: its conditional update cannot clear a newer connection id.

This is a connection fence, not an execution failure signal. Active jobs survive stream loss.
