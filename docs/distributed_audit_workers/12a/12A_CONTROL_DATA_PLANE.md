# 12A — три плоскости

| Plane | В 12A/v1 | Данные |
|---|---|---|
| Control | Будущий gRPC bidi stream | hello, capabilities change, lightweight heartbeat, job offer/accept/decline/status/progress, events/ACK, cancel/ACK, result metadata/ACK/reject |
| Data | Существующий resumable HTTPS | source archive download; result upload session/status/indexed chunks/complete |
| Admin | SSH/bootstrap | install, approval/registration, repair, service/config/update operations |

`PackageTransferDescriptor` содержит opaque `transfer_id`, direction, protocol, size/hashes/manifest/compression/chunk size. В нём нет URL, credential или bytes. Endpoint строится из локальной trusted center/gateway configuration.

Stream не содержит PDF/tar, upload chunks, arbitrary file paths, install/edit/restart/shell commands. SSH не используется как runtime job channel. Provider retries/inference остаются внутри worker ProviderAdapter и не управляются центром сообщением «повтори model call».

После 12A фактический runtime остаётся polling HTTPS. Нет listener, :8443, firewall/Caddy/nginx change, mTLS, gateway или cutover.
