# 12A — known issues / вопросы 12B

Не являются blocker строгого v1 contract, но обязательны до gateway cutover:

1. Реализовать Agent Gateway listener/deployment/health/metrics на :8443 без открытия его в 12A.
2. Спроектировать CA, certificate issuance/rotation/revocation и проверку `certificate worker identity == AgentHello.worker_id`.
3. Выбрать и реализовать durable atomic connection-epoch fencing для нескольких gateway instances; определить nonce/replay lifetime.
4. Определить production heartbeat/lease/control-size/batch/unacked-window/backpressure limits и overload behavior.
5. Связать mTLS stream identity с authorization к существующему HTTPS data plane без передачи execution token/secret URL в protobuf.
6. Реализовать gateway Proto↔Domain adapter calls, observability/redaction, graceful drain, HA/restart и fallback/cutover configuration.
7. Windows secret storage: DPAPI machine scope. Linux private-key/secret storage и service-account permissions выбрать на mTLS/bootstrap этапе.
8. Формализовать registry/migration rules для canonical routing/job/event JSON schemas и unknown critical feature behavior.
9. Решить выдачу/повтор offer при lease expiry и stale connection через существующий attempt service, не создавая второй scheduler.
10. Провести отдельные integration/network/security/load tests; текущая работа намеренно не открывает socket и не проверяет live gRPC.

HTTP polling остаётся fallback до доказанного 12B cutover. Production config/DB/firewall/proxy не менялись.
