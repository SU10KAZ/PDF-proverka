# 12A — решение о больших пакетах

V1 использует control/data split. Source/result packages размером 100–300 MB и больше не проходят через `Connect`. Control stream передаёт только `PackageTransferDescriptor`; существующий HTTPS data plane сохраняет resumable chunks, received-chunk bitmap, per-chunk/result hashes и central validation.

Причины: действующий механизм уже доказал resume/idempotency; помещение больших bytes в общий bidi stream создаёт head-of-line blocking и вторую competing file protocol. Opaque `transfer_id` не раскрывает signed/credential URL. Возможная будущая замена data transport допустима добавлением нового enum protocol и отдельной совместимой negotiation, но не меняет job semantics.
