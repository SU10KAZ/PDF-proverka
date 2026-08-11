# 12A — каталог сообщений

## Agent → Center

| Message | Назначение / identity |
|---|---|
| `AgentHello` | worker/instance, supported versions, revisions, full capabilities, slots, active attempts, cursors, epoch |
| `Heartbeat` | lightweight health/slots/attempt refs/resources/capability revision; no full metadata |
| `CapabilitiesChanged` | полный sanitized snapshot при изменении |
| `JobAccept` | job/attempt/worker, routing hash/revision, verified source, planned stages |
| `JobDecline` | job/attempt/worker, typed reason + safe detail |
| `ProgressUpdate` | observational job/attempt/stage/action counters/status |
| `EventBatch` | job/attempt, first sequence, contiguous typed events from EventOutbox |
| `JobStatusUpdate` | exact domain JobState plus sanitized job error |
| `ResultReady` | result transfer metadata/hashes/routing/revision/safe summaries; no bytes |
| `CancelAck` | command/job/attempt and typed cancel stage |
| `ErrorStatus` | protocol/application safe error, retryability/correlation |

## Center → Agent

| Message | Назначение / identity |
|---|---|
| `CenterHello` | negotiated version/connection/timings/limits/resume/revision/policy/duplicate policy |
| `JobOffer` | persisted assignment identity, frozen route, source descriptor, provider requirements, lease |
| `CancelCommand` | idempotent command/job/attempt, safe reason/deadline |
| `EventAck` | highest contiguous EventOutbox sequence, accepted/duplicate counts |
| `ResultAck` | result hash, validated acceptance/storage status and retention timestamps |
| `ResultRejected` | result hash, typed validation reason, safe detail/retryability |
| `ErrorStatus` | sanitized protocol/application response |

`AgentToCenter` and `CenterToAgent` use `oneof`; unknown critical action must fail closed. There is no RunShell/Exec/Eval/Install/Edit/Restart message.
