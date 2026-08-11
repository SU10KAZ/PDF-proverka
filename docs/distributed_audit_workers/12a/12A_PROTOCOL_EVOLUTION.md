# 12A — evolution/versioning

Version negotiation is explicit: AgentHello advertises supported major numbers; CenterHello selects one. V1 accepts only major 1; unknown major is rejected, never silently approximated.

Inside v1:

- field numbers are permanent; removed fields/numbers become `reserved`;
- enums use zero `UNSPECIFIED`; numbers are never reassigned;
- additive optional/repeated fields are compatible;
- an old receiver may ignore a non-critical unknown field/oneof variant, but any feature required to execute safely must fail closed with `PROTOCOL_VERSION_UNSUPPORTED`, `PROTOCOL_VIOLATION`, `REVISION_MISMATCH` or `POLICY_MISMATCH`;
- exact semantic break creates `auditmanager.agent_stream.v2` and a dual-negotiation period;
- descriptor snapshot fixes service shape and critical field numbers; committed descriptor is reproduced by pinned compiler.

Canonical JSON subcontracts have independent schema names/versions and SHA-256. They are allowed only for bounded existing authoritative domain schemas; their incompatible evolution also requires explicit schema version handling.
