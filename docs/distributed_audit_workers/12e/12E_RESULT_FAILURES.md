# Result / acknowledgement failures

Result receipt, validated acceptance and retention are distinct durable
states. Local C06/C19 evidence proves that a persisted central acceptance can
reissue ResultAck after the stream is lost. The worker retains the package
until `retention_until` is received.

Remaining C18/C20 evidence must inject a retryable validator outage and an
isolated Agent crash immediately after ACK delivery but before local retention
persistence. Neither condition may cause a second import or a premature
deletion.
