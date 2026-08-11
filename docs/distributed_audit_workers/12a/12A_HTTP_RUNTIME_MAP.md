# 12A — фактический HTTP runtime

Источник истины — `backend/app/api/routers/audit_worker_agent.py`, модели `backend/app/models/distributed_workers.py` и сервисы `backend/app/services/distributed_workers/`. Базовый путь — `/api/v1/worker`; worker инициирует только исходящие HTTPS-запросы.

| Действие | HTTP | Главная семантика |
|---|---|---|
| Регистрация / claim | `POST /register`, `POST /claim`, `PUT /registration` | Одноразовый claim-secret → постоянный bearer; capabilities/revision обновляются отдельно |
| Liveness | `POST /heartbeat`, `POST /resources` | Connectivity — отдельная ось; пропажа heartbeat не означает failed |
| Lease/claim | `POST /jobs/next` | Центр атомарно выбирает и фиксирует attempt до возврата `JobAssignment` |
| Source | `GET /jobs/{job_id}/source` | Большой неизменяемый архив; attempt token; worker проверяет hash/manifest до accept |
| Accept/decline | `POST /jobs/{job_id}/accept|reject` | Accept переводит уже закреплённую попытку в `accepted_by_worker` |
| Events | `POST /events` | Durable EventOutbox, монотонный `seq`, dedupe `(job, attempt, seq)`, contiguous ACK |
| Result data plane | `POST /uploads`, `GET /uploads/{id}`, `PUT .../chunks/{idx}`, `POST .../complete` | Resumable upload, пропущенные chunks, затем центральная валидация |
| Commands | `GET /commands`, `POST /commands/next`, `POST /commands/{id}/ack` | Закрытые business-команды; cancel идемпотентен по command_id |
| Reconnect | `POST /reconcile` | Центр возвращает authoritative verdict/cursor/retention по локальным attempts |

Полная машинно-читаемая инвентаризация каждого action, request/response, identifiers, persistence, idempotency, retry и security находится в `12A_HTTP_RUNTIME_MAP.json`.

## Ключевые восстановленные свойства

- Job identity: строковые `job_id`, `attempt_id`, `attempt_no`; попытки имеют собственный execution token и disposition.
- Текущий claim — не «предложение без записи»: `/jobs/next` атомарно фиксирует назначение. Потеря ответа не разрешает отдать тот же attempt другому worker без центрального lease/recovery решения.
- Routing plan — самостоятельная immutable доменная сущность с canonical representation, schema version и hash. `AuditPipelineParams` валидирует её настоящим `RoutingPlan.from_dict` и validator.
- EventOutbox остаётся на диске worker. Его sequence не заменяется порядковым номером соединения.
- Result complete ACK выдаётся после проверки archive/hash/manifest/boundary/artifacts. Только тогда записывается `result_acknowledged_at` и начинается retention; до этого auto-delete запрещён.
- Старый/отозванный attempt может дослать историю и результат в `superseded_result_received`, но не меняет актуальную попытку и не публикуется.
- Cancel до dispatch может завершиться сразу. После dispatch центр ждёт ACK worker, доказывающий остановку; `mark-lost` не фабрикует `failed`.
- Provider advertisement содержит только sanitized auth/quota/account-group/capability metadata; credentials остаются локальными.

## Ошибки и retry

Bearer/attempt ownership/revision/capability mismatch закрываются 401/403/409. Повтор с одинаковым `Idempotency-Key` и телом возвращает сохранённый ответ; тот же ключ с другим телом — 409. Network/5xx повторяется с тем же identity. Validation и policy failures не маскируются transport retry. Подробности приведены в idempotency/retry matrices.
