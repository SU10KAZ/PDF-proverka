# Unified change synthesis validation boundary

`unified_change_synthesis_v1.schema.json` is the structural contract. It
closes object fields, enums, source/evidence composition, deterministic
TEXT-before-GRAPHIC positions where the producer emits one atom per source,
and fixed flags such as `provenance.uses_llm=false`.

`validate_synthesis()` is the authoritative semantic validator. JSON Schema
cannot recompute SHA-256-derived `change_id`, `review_evidence_id`,
`content_signature`, or group IDs. It also cannot establish that a
`change_ids` value names an object in another array, that contested evidence
is exactly the evidence of its referenced changes, or that every
`CONTRADICTORY` change belongs to one contested group. Those invariants are
therefore implemented only once, in the Python validator; no second semantic
validator is embedded in JSON Schema or test code.

A consumer must run both layers when accepting an untrusted synthesis:

1. JSON Schema validation for structural errors.
2. `validate_synthesis()` for derived identity and cross-reference semantics.

The producer calls `validate_synthesis()` before returning its result. Tests
also pass real producer output through the committed JSON Schema.
