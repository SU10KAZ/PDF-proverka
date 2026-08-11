# Cancel flow

Gateway CancelCommand enters the existing local command queue. The separate
Executor performs ownership checks and cancellation; the Agent merely translates
its durable result to CancelAck. Duplicate commands remain idempotent in
`worker.db`. A disconnected Agent does not lose the server command: Gateway
re-emits pending cancel state after reconnect. A locally completed but previously
reported command is made reportable again only when that duplicate central
command proves the fire-and-stream CancelAck was not committed. After Agent
restart, CancelAck waits for replayed command identity instead of inventing it.
