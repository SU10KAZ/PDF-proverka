# 12A — idempotency matrix

| Message | Repeat | Identity/key | Receiver behavior |
|---|---|---|---|
| AgentHello | yes after reconnect | worker_id + connection_epoch | greater epoch accepts/supersedes; equal/lower rejects |
| Heartbeat | yes | connection_id + latest timestamp | observational overwrite; no job transition by silence |
| JobAccept | yes | job_id + attempt_id + assignment_generation | return current compatible state; conflicting revision/hash rejects |
| JobDecline | yes | job_id + attempt_id | same terminal disposition no-op; conflict is explicit |
| ProgressUpdate | yes | job/attempt/stage/action + observation | observational; never exactly-once authority |
| WorkerEvent | yes | job_id + attempt_id + event sequence | insert once; duplicate skipped; ACK contiguous cursor |
| CancelCommand | yes | command_id | do not start second cancel; replay stored/current stage |
| CancelAck | yes | command_id + stage | monotonic command result; never regress cancelled |
| ResultReady | yes | job_id + attempt_id + result sha256 | resume/find same upload/result; conflicting identity rejects |
| ResultAck | yes | attempt_id + result sha256 | same validated outcome/retention returned; countdown not restarted |

Envelope `message_id` supports diagnostics/dedup of transient messages but does not replace domain keys. Event exactly-once depends on EventOutbox sequence; result exactly-once depends on attempt/result identity and central persistence.
