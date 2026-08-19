# 12A — connection/reconnect state machine

```text
DISCONNECTED → CONNECTING → HELLO_SENT → READY
      ↑                        │           │
      └──── network loss ──────┴───────────┘

READY may concurrently carry N attempts:
JOB_OFFERED → JOB_ACCEPTED → RUNNING → HANDOFF_READY
→ HTTPS RESULT_UPLOADING → RESULT_READY → RESULT_ACKED
```

При обрыве worker продолжает локальный Executor/job, EventOutbox продолжает durable append. Reconnect посылает новый persistent `connection_epoch`, active attempts и last-written/last-acked cursors. `CenterHello.resume_cursors` задаёт authoritative highest contiguous ACK; agent replay only unacked tail, затем normal delivery.

Center/gateway restart только обрывает stream. Agent reconnects; active attempt и Executor не перезапускаются. Agent restart обнаруживает локальные processes/attempts, сообщает их в hello и читает disk EventOutbox. Независимость Agent/Executor сохраняется.

Duplicate policy: strictly greater worker-persisted epoch supersedes old connection; equal/lower rejected as stale. Old connection перестаёт иметь authority после fencing. Как gateway кластер атомарно хранит/fences epoch — обязательная реализация 12B.

Backpressure: CenterHello announces max control bytes, max batch count и max unacked event window. Agent pauses new event sends when window исчерпан, но продолжает EventOutbox append; transport flow control дополняет, но не заменяет durable limit.
