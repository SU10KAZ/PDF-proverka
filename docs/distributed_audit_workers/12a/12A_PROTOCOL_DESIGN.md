# 12A — дизайн Agent Stream Protocol v1

Namespace: `auditmanager.agent_stream.v1`. Один будущий service `AgentStreamService`, один bidi RPC `Connect`. 12A — только wire/application contract, generated Python types, descriptor, adapters и тесты; socket/runtime отсутствуют.

## Единая доменная модель

Proto не создаёт второй lifecycle. `JobState` и `WorkerEventType` зеркалят закрытые текущие enums. Чистые adapters переводят HTTP/domain dictionaries ↔ proto; pipeline и business services не импортируют generated types.

`JobOffer` соответствует факту уже выполненного атомарного claim в центральной persistence. Поэтому offer не создаётся из произвольного queue candidate. `JobAccept` — подтверждение worker после проверки source hash/manifest; только оно переводит attempt в `accepted_by_worker`.

## Envelope и ordering

Оба envelope несут protocol version, message/worker/connection/correlation IDs, timestamp и connection-local `stream_sequence`; payload — закрытый `oneof`. Stream sequence нужен для порядка и диагностики внутри соединения. Exactly-once events опираются только на durable EventOutbox sequence и `EventAck.highest_contiguous_sequence`.

## Handshake и resume

Первое сообщение worker — `AgentHello`: supported major versions, identities/revisions, full capabilities, slots, active attempts, EventOutbox cursors и persistent monotonically increasing `connection_epoch`. Center отвечает `CenterHello`: выбранная версия, connection ID, timings/limits, required revisions/policy and resume cursors.

Unsupported major rejected. Новый stream одного worker принимается только при strictly greater epoch и supersedes old; equal/lower is stale. Worker сохраняет epoch до connect. Реализация durable fencing относится к 12B.

## Routing plan: вариант B

Выбран bounded canonical JSON + schema/version/hash. Это сохраняет единственный authoritative typed domain `RoutingPlan` и исключает рассинхронизацию со вторым protobuf-графом. Center формирует и валидирует plan, worker валидирует те же canonical bytes/hash; результат возвращает тот же `routing_plan_hash`. Provider requirement остаётся логическим provider/capability/allowed stages/max inferences; exact model ID и credentials отсутствуют.

## Limits

- control envelope: не более 1 MiB в adapter policy;
- canonical JSON: не более 256 KiB;
- строка внутри flexible JSON: не более 4096 UTF-8 bytes;
- EventBatch: максимум 256 contiguous events;
- CenterHello объявляет negotiated maximums и unacked window.

Production-конкретные lower limits и gateway backpressure настраиваются в 12B, не изменяя v1 wire fields.
