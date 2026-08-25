# G2.4.2 — Entity Bridge report

## Verdict

An independent TEXT entity ↔ GRAPHIC entity layer has been added without
changing Stage 5.3, GraphicChangeLedger MODE_1/MODE_2, SYSTEM_GRAPH comparator,
or the G2.4.1 unified evidence envelope.

The bridge is deterministic and evidence-first. It can prove strong identity
for canonical aliases and normalized designations, return a guarded possible
link for explicit functional roles, and fail closed for conflicts or ambiguous
cardinality. It does not infer entities from prose and does not create a unified
project change.

## 1. Can TEXT and GRAPHIC be linked?

Yes, when both sources supply entity-level records and the identity evidence is
unique. The reference contract artifact proves these mappings:

| TEXT | GRAPHIC | Result | Rule |
|---|---|---|---|
| `ВРУ-А` | `VRU-A` | SAME_ENTITY / HIGH | exact canonical identity |
| `ЩР-1` | label `ЩР1` | SAME_ENTITY / HIGH | normalized designation |
| `Помещение 101` | `ROOM-101` | SAME_ENTITY / HIGH | exact canonical identity |

Source IDs remain unchanged. The link receives its own stable `eln_*` ID, and
its evidence stores both original spellings and the canonical form.

## 2. Which rules work best?

The order is fixed and versioned:

1. `EXACT_CANONICAL_IDENTITY_MATCH` is the strongest rule. Explicit aliases
   such as `ВРУ`/`VRU`, `ЩР`/`PANEL`, and `Помещение`/`ROOM` are normalized; no
   general fuzzy transliteration is used.
2. `NORMALIZED_DESIGNATION_MATCH` handles separators, spaces, case, and
   supported designation aliases, for example `ЩР-1` and `ЩР1`.
3. `DESIGNATION_CONTEXT_MATCH` is required for locally repeated identifiers
   such as QF/QS. `system` or `parent_group/section` must agree. Page or bbox is
   insufficient.
4. `FUNCTIONAL_ROLE_MATCH` only consumes an explicit role on both sides. It
   returns POSSIBLE_ENTITY/MEDIUM, not SAME_ENTITY, because the same role can
   belong to several objects.

Conflict rules override positive spelling:

- different designation suffixes (`ВРУ-А` versus `ВРУ-Б`);
- different explicit node types;
- conflicting system/parent context;
- graph-side extraction conflicts;
- one TEXT → several GRAPHIC or several TEXT → one GRAPHIC.

No first-candidate selection exists.

## 3. How many HIGH links?

There are two deliberately separate measurements.

### Contract/reference corpus

`entity_links.json` contains:

- 3 HIGH / SAME_ENTITY;
- 0 MEDIUM;
- 0 UNKNOWN.

### Existing project artifacts

No HIGH cross-modal link was publishable from the currently available complete
entity inputs:

| Area | TEXT input visible now | GRAPHIC input visible now | HIGH |
|---|---:|---:|---:|
| AR rooms/markings | 26 distinct room designations in Stage 5.3 evidence | no AR SYSTEM_GRAPH set | 0 |
| IOS ВРУ/ЩР | 2 checked designations (`ВРУ-А`, `ЩР-6`) | 73 right GRSh graph nodes | 0 |
| GRSh QF/QS/sections | no standalone Stage 5.3 entity array | 73 right / 82 left graph nodes | 0 |

Zero here is not treated as evidence that the objects differ. It records an
input/readiness limitation.

## 4. How many UNKNOWN?

Mandatory controlled behavior is covered individually:

- `QF1` without parent/system context → 1 UNKNOWN;
- `ВРУ-А ↔ ВРУ-Б` → 1 UNKNOWN/CONFLICT;
- one TEXT `ВРУ-А` → two identical graph candidates → 2 UNKNOWN;
- two TEXT aliases → one graph node → 2 UNKNOWN;
- fire-pump role → POSSIBLE_ENTITY/MEDIUM only when the role is explicit;
  without it there is no candidate link.

On existing IOS/GRSh data:

- Stage 5.3 `ВРУ-А` meets two real right-graph LOAD nodes, so the result is
  2 UNKNOWN candidate links with `ONE_TEXT_TO_MULTIPLE_GRAPHIC` provenance;
- `ЩР-6` has no graph candidate, so it remains unresolved without manufacturing
  an UNKNOWN pair;
- a QF1 diagnostic against the real graph produces 1 UNKNOWN when parent
  section context is absent.

AR has 0 UNKNOWN pairs because no AR graph entity set exists. The 26 TEXT room
designations are not evaluated, not contradicted.

## 5. Where are additional rules or inputs needed?

The missing production inputs are clearer than a need for looser matching:

1. Stage 5.3 needs an additive, source-grounded `text_entities` producer. The
   current artifact contains fragments/before/after text but no standalone
   entity array matching the G2.4.2 input contract.
2. GRAPHIC needs a logical entity role or deduplication key. In the current GRSh
   graph one consumer may appear as OUTGOING_DEVICE and LOAD and may repeat by
   bus section. The bridge correctly refuses to collapse these representations.
3. QF/QS require explicit parent section/system on the TEXT side. Same page or
   nearby coordinates must not substitute for it.
4. AR needs SYSTEM_GRAPH-style room/marking nodes before its 26 room
   designations can be checked.
5. Discipline-specific alias tables can be extended only with reviewed,
   deterministic aliases. Fuzzy or semantic guessing should remain outside the
   production bridge.

## 6. Is the layer ready for unified change synthesis?

The contract and deterministic mechanics are ready as an input layer:

- versioned schema and validator;
- stable link IDs and unchanged source IDs;
- exact provenance for every candidate link;
- honest UNKNOWN/unresolved outcomes;
- SYSTEM_GRAPH node adapter;
- no dependency on UI, LLM, or change-merging code.

The current corpus is not yet ready for automatic synthesis. Synthesis must wait
for the upstream TEXT entity producer and sufficient graph role/context to turn
real ambiguities into unique entity links. `POSSIBLE_ENTITY` and `UNKNOWN` must
not authorize automatic change merging.

## Artifacts

- `unified_entity_bridge/entity_normalizer.py` — lossless canonical forms;
- `unified_entity_bridge/entity_bridge.py` — matcher, SYSTEM_GRAPH adapter, and
  validator;
- `unified_entity_bridge/entity_links.schema.json` — JSON Schema;
- `unified_entity_bridge/entity_links.json` — three-link reference artifact;
- `unified_entity_bridge/entity_bridge_report.md` — package-level operating
  boundary;
- `tests/test_stage_comparison_unified_entity_bridge.py` — mandatory,
  ambiguity, provenance, non-mutation, and real-graph checks.

## Non-regression boundary

No source contract or producer was edited. The tests load the existing Stage
5.3 artifact, call its existing public view, run the bridge independently, and
assert that the original object is byte-for-byte unchanged in memory. The real
SYSTEM_GRAPH adapter and matcher are also checked for input non-mutation.

## Verification

- Entity Bridge tests: 20 passed;
- complete Stage Comparison suite plus SYSTEM_GRAPH comparator tests: 362 passed;
- dense-sectioned-board profile tests: 9 passed;
- combined checked set: 371 passed;
- `git diff --check`: clean.
