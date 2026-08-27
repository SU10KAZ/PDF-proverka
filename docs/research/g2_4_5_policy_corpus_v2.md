# G2.4.5 Policy Corpus v2

**Status:** corrected, tracked corpus for G2.4.5 hardening.  
**Contract:** `unified-change-policy-v1`; this document corrects evidence and
test representation without rewriting the historical v1 corpus.  
**Date:** 2026-08-27.

The historical `docs/research/g2_4_5_policy_corpus.md` remains frozen and is
not a dependency of this document or its tests. The machine-readable source of
the cases below is
`tests/fixtures/g2_4_5_policy_cases_v2.json` (`g2.4.5-policy-cases-v2`).

## 1. Evidence boundary and provenance

### GRAPHIC truth

All GRAPHIC facts in v2 use only:

`experiments/g2_4_4_3_correct_sides/ios/`

in the accepted direction **LEFT → RIGHT**:

| Side | block_id | nodes / edges | outgoing devices |
|---|---|---:|---:|
| LEFT | `blk_2d72a6705eaf4d8c9ee1d6ff459b15a6` | 73 / 99 | 27 |
| RIGHT | `blk_039909ec039649a1b8209f059c95167b` | 82 / 111 | 30 |

The corrected comparison has exactly four events:

| change_id | type | corrected fact |
|---|---|---|
| `chg_1d5f0b80d8a1` | `UNCERTAIN_STRUCTURAL_CHANGE` | reserve recognition 0 → 2, not asserted as a material change |
| `chg_6edbdea8fb72` | `GROUP_COUNT_CHANGED` | repeated outgoing-device group 27 → 30 |
| `chg_1b601fa171f2` | `NODE_TYPE_CHANGED` | `QS1 (SWITCH_DISCONNECTOR)` → `QF3 (CIRCUIT_BREAKER)` |
| `chg_a7346590ab6f` | `UNCERTAIN_STRUCTURAL_CHANGE` | unresolved correspondence, not added/removed nodes |

Regression totals are therefore:

- `changes_total = 4`;
- `DETAIL_LEVEL_INCREASED = 0`;
- `NODE_ADDED = 0`;
- `NODE_REMOVED = 0`.

No event ID from the reversed GRAPHIC artifacts is carried into v2.

### TEXT provenance

TEXT facts remain sourced from the existing complete Stage 5.3 pairs:

- ИОС: `comparison/sessions/121d764109184c13/pairs/p26c08b83a6/high_level_project_changes.json`;
- АР-1: `comparison/sessions/121d764109184c13/pairs/p570d156f57/high_level_project_changes.json`;
- АР-2: `comparison/sessions/121d764109184c13/pairs/p16b108b9f5/high_level_project_changes.json`.

Those TEXT artifacts are not regenerated here. Their evidence IDs are checked
directly by the v2 replay test. Reuse of TEXT material does **not** prove a
TEXT+GRAPHIC join: where common scope or subject identity is unproven, the case
has no integrated relation verdict.

## 2. Hardened policy rules represented by v2

### Dimension-aware GRAPHIC coverage

`GRAPHIC_COVERAGE_DIMENSION_MAP` now mirrors what
`graphic-coverage-policy-v2` can observe:

- observable: `STRUCTURE`, `CONNECTION`, `TYPE`, `QUANTITY`;
- not observable: `OPERATION`, `PARAMETER`, `METHOD`, `PRINCIPLE`, `SPACE`.

For the second group, `CHECKED/CHECKED` supplied by a caller cannot make M7
pass. `check_source_validity(...)` returns `NOT_APPLICABLE` with
`graphic_route_cannot_observe_dimension`; consequently
`evaluate_source_relation(...)` cannot return `CORROBORATING` or
`CONTRADICTORY` for those GRAPHIC dimensions.

### Required proof inputs

`scope_compatible`, `subject_relation`, and `document_binding_state` are
required keyword-only inputs to `evaluate_source_relation(...)`. Omission is a
contract error, not an implicit proof.

### Freshness

If a saved scope/coverage artifact contains non-empty
`parent_page_relations`, the current caller must supply the current relation
evidence. `None`, `[]`, and any changed list make the saved artifact stale.
The saved list is never substituted for omitted current evidence.

### UNKNOWN_DIMENSION identity

`UNKNOWN_DIMENSION` cannot receive an ordinary stable unified `change_id`.
Such an atom stays review-required and may receive only a
`review_evidence_id(identity_cell, evidence_ref)`, which includes the atom's
own evidence reference. Two unresolved atoms therefore cannot coalesce merely
because scope, subject, and direction happen to match.

### M2 and M3

The live decision gates remain `M1, M2, M7, M8`. Observation-only gates remain
`M3, M4, M5, M6`.

- M2 is an already-proven subject-identity input.
- M3 separately records link strength on each side.
- M3 does not derive or upgrade M2 in G2.4.5 v1.
- The contradiction helper may inspect M4/M5 because contradiction requires a
  common dimension and opposite direction; that does not make M4/M5 merge
  gates.

### Direct PAGE comparison

When the user explicitly selects LEFT PAGE and RIGHT PAGE, the product compares
those pages. Parent relations, same-sheet checks, П/РД classification, and
chronology are not preconditions for Direct PAGE Comparison. Binding and
provenance gates apply only when automatically integrating evidence across
sources.

## 3. Machine-readable case design

Every case has:

- a stable `case_id` and this corpus marker;
- repository-relative source paths plus source IDs where an ID exists;
- observed structured facts;
- zero or more calls to production policy functions with literal inputs;
- expected policy facts and, only when derivable, an expected result;
- a reason and an explicit representability status.

The statuses are:

- `REPRESENTABLE_IN_V1`: one complete relation result is derivable;
- `POLICY_FACTS_ONLY_IN_V1`: individual gates are derivable, but no overall
  relation is asserted;
- `NOT_REPRESENTABLE_IN_V1`: replay would require an invented dimension,
  subject relation, scope, outcome, or an unsupported source pairing;
- `NEGATIVE_CONTROL`: the requested real candidate is absent.

Tests replay only declared calls. They do not replace missing facts to obtain a
desired verdict.

## 4. Cases A1–A19

<!-- policy-case:A1 -->
**A1 — NOT_REPRESENTABLE_IN_V1.** TEXT `ev_e49035bd602be7cb`
describes a documentation-list split on page 4; GRAPHIC
`chg_6edbdea8fb72` is a page-index-0 group count 27 → 30. Common scope is not
proven, and mapping documentation structure to project `STRUCTURE` would invent
a dimension. The historical `COMPLEMENTARY` verdict is not retained.

<!-- policy-case:A2 -->
**A2 — REPRESENTABLE_IN_V1 → SINGLE_SOURCE.** Corrected GRAPHIC
`chg_1b601fa171f2` proves `QS1 → QF3`; no TEXT atom addresses the section tie.

<!-- policy-case:A3 -->
**A3 — REPRESENTABLE_IN_V1 → SINGLE_SOURCE / REVIEW_REQUIRED outcome.**
TEXT `ev_b9db67e604bed43e` is itself review-required; GRAPHIC is `NOT_CHECKED`.
Silence is not contradiction.

<!-- policy-case:A4 -->
**A4 — REPRESENTABLE_IN_V1 → SINGLE_SOURCE / REVIEW_REQUIRED outcome.**
TEXT `ev_36b401845313b4bb` concerns `PARAMETER`, which the GRAPHIC route cannot
observe.

<!-- policy-case:A5 -->
**A5 — POLICY_FACTS_ONLY_IN_V1.** For the accepted section fixture, TYPE has
`CHECKED/CHECKED` while CONNECTION has `NOT_CHECKED/NOT_CHECKED`; M7 passes only
for TYPE. This is a coverage fact, not a source-relation verdict.

<!-- policy-case:A6 -->
**A6 — POLICY_FACTS_ONLY_IN_V1.** Correct orientation yields one TEXT `VRU_1`
identity versus two ambiguous GRAPHIC candidates on RIGHT. M8 fails 1→2.

<!-- policy-case:A7 -->
**A7 — POLICY_FACTS_ONLY_IN_V1.** `MSB_ТП` has eight ambiguous GRAPHIC
candidates on RIGHT, so M8 fails 1→8. M3's `UNKNOWN` evidence cannot be turned
into an M2 fact; no `UNRELATED` result is manufactured.

<!-- policy-case:A8 -->
**A8 — NOT_REPRESENTABLE_IN_V1.** Corrected GRAPHIC truth contains zero
`DETAIL_LEVEL_INCREASED` events. The reversed-artifact case is retired.

<!-- policy-case:A9 -->
**A9 — NOT_REPRESENTABLE_IN_V1.** TEXT says five ЩР sheets are absent while
GRAPHIC shows a group delta of three. No subject identity reconciles those
counts; creating the candidate would be the unsupported inference.

<!-- policy-case:A10 -->
**A10 — REPRESENTABLE_IN_V1 → UNRELATED.** TEXT city-stamp evidence
`ev_8bd289a59355afae` and the section-tie type change are provably different
subjects even though their page scope is compatible.

<!-- policy-case:A11 -->
**A11 — REPRESENTABLE_IN_V1 → REVIEW_REQUIRED.** Corrected GRAPHIC
`chg_1d5f0b80d8a1` has confidence 0.35 and explicitly does not assert a
material reserve-count change.

<!-- policy-case:A12 -->
**A12 — REPRESENTABLE_IN_V1 → REVIEW_REQUIRED.** Corrected GRAPHIC
`chg_a7346590ab6f` records unresolved correspondence, not added/removed nodes.

<!-- policy-case:A13 -->
**A13 — POLICY_FACTS_ONLY_IN_V1.** A checked unchanged GRAPHIC subject can
pass M7 for TYPE. A coverage-only no-change observation is not passed through
the TEXT×GRAPHIC relation function as a fabricated change atom.

<!-- policy-case:A14 -->
**A14 — NOT_REPRESENTABLE_IN_V1.** Repeated-group QUANTITY versus an
individual GRAPHIC entity is a GRAPHIC-internal pairing. The v1 relation API is
TEXT×GRAPHIC, and individual quantity coverage is not applicable.

<!-- policy-case:A15 -->
**A15 — NOT_REPRESENTABLE_IN_V1.** Three MSB documentation facts and four
corrected GRAPHIC content events have neither a proven common subject nor a
resolved common scope. The historical `COMPLEMENTARY` verdict is not retained.

<!-- policy-case:A16 -->
**A16 — NEGATIVE_CONTROL.** No real contradictory candidate satisfies common
scope, M2, common observable dimension, checked coverage, and opposite
directions.

<!-- policy-case:A17 -->
**A17 — NEGATIVE_CONTROL.** There is no real automatic-merge candidate, and
G2.4.5 exposes no merge executor.

<!-- policy-case:A18 -->
**A18 — POLICY_FACTS_ONLY_IN_V1.** One aggregate TEXT record faces four, not
six, corrected GRAPHIC events. M8 fails 1→4; no overall relation is asserted.

<!-- policy-case:A19 -->
**A19 — NOT_REPRESENTABLE_IN_V1.** It is GRAPHIC×GRAPHIC, while
`evaluate_source_relation(...)` is TEXT×GRAPHIC. The corrected pair also lacks
the old detail events. The function is deliberately not called.

## 5. Cases B1–B9

<!-- policy-case:B1 -->
**B1 — REPRESENTABLE_IN_V1 → SINGLE_SOURCE.** Confirmed AR-1 parameter change;
GRAPHIC absent.

<!-- policy-case:B2 -->
**B2 — REPRESENTABLE_IN_V1 → SINGLE_SOURCE.** Confirmed AR-1 space-program
change with two-sided TEXT proof; GRAPHIC absent and SPACE unobservable.

<!-- policy-case:B3 -->
**B3 — REPRESENTABLE_IN_V1 → SINGLE_SOURCE / DETAIL_ONLY.** Retained TEXT
detail provenance is `hlc_78243ba30fcf16c1` / `ev_2988677dcc6f4049` from
АР-1. It is not claimed to join any GRAPHIC event.

<!-- policy-case:B4 -->
**B4 — REPRESENTABLE_IN_V1 → REVIEW_REQUIRED.** The upstream classifier failed
closed on an incompatible AI type.

<!-- policy-case:B5 -->
**B5 — REPRESENTABLE_IN_V1 → REVIEW_REQUIRED.** A material removal is not
proven by absence from one fragment.

<!-- policy-case:B6 -->
**B6 — REPRESENTABLE_IN_V1 → SINGLE_SOURCE.** The room atom is present only on
LEFT. Its dimension stays `UNKNOWN_DIMENSION`; the aggregate's dimension is not
copied onto it.

<!-- policy-case:B7 -->
**B7 — NOT_REPRESENTABLE_IN_V1.** `NO_HIGH_LEVEL_CHANGE` has no corresponding
v1 `Outcome`. Replacing it with `VALID/MATERIAL_CHANGE`, as the old replay did,
would change the fact.

<!-- policy-case:B8 -->
**B8 — REPRESENTABLE_IN_V1 → SINGLE_SOURCE.** АР-2 has a confirmed TEXT
parameter fact and no downstream GRAPHIC/entity/scope artifacts.

<!-- policy-case:B9 -->
**B9 — REPRESENTABLE_IN_V1 → REVIEW_REQUIRED.** АР-2 unresolved TEXT evidence
remains review-required with no GRAPHIC source.

## 6. Derived distribution

Only the 14 fully representable relation cases contribute to the relation
distribution:

| Derived verdict | Count | Cases |
|---|---:|---|
| `SINGLE_SOURCE` | 8 | A2, A3, A4, B1, B2, B3, B6, B8 |
| `REVIEW_REQUIRED` | 5 | A11, A12, B4, B5, B9 |
| `UNRELATED` | 1 | A10 |
| `CORROBORATING` | 0 | — |
| `COMPLEMENTARY` | 0 | — |
| `CONTRADICTORY` | 0 | — |

The remaining 14 entries are five policy-fact-only cases, seven explicitly
unrepresentable cases, and two negative controls. They are not silently added
to a verdict bucket.

## 7. Material differences from historical v1

1. GRAPHIC is oriented 73/99 → 82/111 and 27 → 30; node type is QS1 → QF3.
2. Corrected GRAPHIC truth has four changes and no detail-level, node-added, or
   node-removed event.
3. GRAPHIC coverage is dimension-aware; five semantic dimensions fail closed.
4. A1 and A15 no longer claim integration without proven scope and M2.
5. A5 is a per-dimension coverage case, not a hand-built overall verdict.
6. A7 exposes only the factual M8 failure and keeps M2 unproven.
7. A19 is explicitly outside the TEXT×GRAPHIC v1 relation contract.
8. B7 is not translated from `NO_HIGH_LEVEL_CHANGE` into a material change.
9. Required scope/subject/binding inputs have no optimistic defaults.
10. The tracked JSON fixture, not historical v1, is the test dependency.

## 8. Remaining limitations before a later synthesizer

- There is no production dimension for documentation/package structure.
- `NO_HIGH_LEVEL_CHANGE` has no closed v1 source-fact/outcome representation.
- There is no GRAPHIC×GRAPHIC relation/deduplication contract.
- M2 must be supplied by a proven upstream subject-identity decision; M3 does
  not supply that decision.
- Correct-sides scope/coverage is replayed in tests but is not written back over
  historical experiment artifacts.

These limitations are explicit review boundaries, not permission to infer
missing facts. This corpus completes G2.4.5 hardening only; it does not begin
G2.4.6.
