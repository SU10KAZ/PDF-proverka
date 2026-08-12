# Agent Stream Protocol

`auditmanager.agent_stream.v1` is the versioned application contract for a future Audit Worker ↔ Center Agent Gateway bidirectional control stream.

The sole service is `AgentStreamService`; its sole RPC is `Connect(stream AgentToCenter) returns (stream CenterToAgent)`. Stage 12A defines and compiles this service but implements no server, client, listener, mTLS, or transport switch. Production remains on outbound HTTPS polling.

## Compatibility rules

- Never reuse a field number. Removed fields and enum values are reserved.
- Every enum starts with `*_UNSPECIFIED = 0`; receivers fail closed when an unknown value controls a critical action.
- Additive fields and non-critical `oneof` variants are allowed inside v1. A required unsupported feature is a protocol error, never silent best effort.
- An incompatible semantic or wire change requires package `auditmanager.agent_stream.v2` and explicit major-version negotiation.
- IDs remain canonical strings. Time uses `google.protobuf.Timestamp`; intervals use `Duration`.
- `stream_sequence` is connection-local ordering/diagnostics. Durable replay and exactly-once event ingestion use the separate EventOutbox sequence.

## Planes and safety

The stream is control plane only. Source/result package bytes remain on resumable HTTPS and are named by opaque `transfer_id`. SSH/bootstrap is the admin plane. The schema intentionally has no credential, private-key, arbitrary path, executable, shell, eval, provider-prompt, or package-content field.

Generated `*_pb2.py` and `agent_stream_v1.desc` are committed. Recreate them with the pinned dev-only packages in `requirements-proto.txt`:

```bash
python3 -m pip install --target /tmp/agent-stream-proto-tools -r requirements-proto.txt
PYTHONPATH=/tmp/agent-stream-proto-tools python3 scripts/generate_agent_stream_proto.py
```

No generated gRPC runtime stub is committed in 12A: the service descriptor is available for the future 12B gateway while current runtime requirements remain unchanged.
