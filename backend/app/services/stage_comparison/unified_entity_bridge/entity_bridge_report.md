# Entity Bridge v1

`unified_entity_bridge` is an additive deterministic layer between supplied
TEXT entities and SYSTEM_GRAPH nodes. It does not extract entities from free
text, call an LLM, modify source artifacts, or merge project changes.

## Contract

The versioned envelope is `entity-bridge.v1` / `text_graphic_entity_links`.
Every candidate link contains:

- stable `entity_link_id`;
- unchanged `text_entity_id` and `graphic_entity_id`;
- `SAME_ENTITY`, `POSSIBLE_ENTITY`, or `UNKNOWN`;
- `HIGH`, `MEDIUM`, `LOW`, or `UNKNOWN` confidence;
- rule-level evidence with original tokens, canonical tokens, context, and the
  deterministic level which produced the result.

`entity_links.schema.json` describes the JSON shape. `validate_entity_links`
also checks referential integrity, unique pairs, relation/confidence
compatibility, and exact diagnostic counts.

## Rule order

1. `EXACT_CANONICAL_IDENTITY_MATCH` — HIGH, unless conflict/ambiguity exists.
2. `NORMALIZED_DESIGNATION_MATCH` — HIGH for supported spelling/separator
   variants.
3. `DESIGNATION_CONTEXT_MATCH` — HIGH for local designations such as QF/QS
   only when system or parent-group context agrees.
4. `FUNCTIONAL_ROLE_MATCH` — POSSIBLE/MEDIUM only for an explicitly supplied
   role and only without conflicts.

Page, bbox, proximity, and a bare number are never identity rules. Different
explicit designations, context/type conflicts, graph extraction conflicts, and
one-to-many/many-to-one candidates produce UNKNOWN. No candidate produces no
link and leaves the source ID in diagnostics as unresolved.

## Reference artifact

`entity_links.json` is a contract example, not a production conclusion. It
links the three controlled pairs requested for the bridge illustration:

- `ВРУ-А ↔ VRU-A`;
- `ЩР-1 ↔ PANEL-1` via the normalized `ЩР1` label;
- `Помещение 101 ↔ ROOM-101`.

The artifact contains 3 HIGH links and 0 UNKNOWN links. Conflict and ambiguity
coverage lives in the test suite rather than being mixed into this compact
reference artifact.

## Existing-data boundary

Current Stage 5.3 artifacts do not publish a standalone `text_entities` array.
The existing AR evidence contains 26 distinct room designations but the checked
corpus has no AR SYSTEM_GRAPH entity set, so none can be evaluated cross-modal.

For IOS/GRSh, the checked Stage 5.3 evidence exposes `ВРУ-А` and `ЩР-6`, while
the real right SYSTEM_GRAPH has 73 nodes. `ВРУ-А` has two LOAD representations,
so the bridge returns 2 UNKNOWN candidate links and does not choose one. `ЩР-6`
has no graph candidate. The real QF1 graph probe is UNKNOWN without parent
context. QF/QS tokens present only in excluded/raw text records were not
promoted into Stage 5.3 entities for this check.

The bridge contract and matcher are ready for a later synthesis consumer. A
production synthesis run still needs an upstream Stage 5.3 entity producer,
logical graph-entity deduplication or explicit parent roles, and AR graph
entities. Until then UNKNOWN/unresolved is the intended result.
