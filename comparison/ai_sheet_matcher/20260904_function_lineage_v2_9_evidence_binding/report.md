# Function Lineage v2.9 — deterministic function-local evidence attribution

Research only. No model calls, no deploy, no shadow, no materialization, no production module changed.

The layer under test replaces `sheet fact -> every function on the sheet` with an attribution to one function fragment. Nothing binds by proximity: a fact binds only when a delimited structural unit that names exactly one function class contains it.

## What binds, over the whole frozen corpus

| Corpus | Sheets | Facts | PROVEN | PARTIAL | AMBIGUOUS | UNKNOWN |
|---|---|---|---|---|---|---|
| IOS1.1 | 45 | 1850 | 357 | 119 | 1374 | 0 |
| IOS2.1 | 42 | 1766 | 609 | 222 | 930 | 5 |
| IOS3.1 | 16 | 597 | 68 | 62 | 466 | 1 |

`PARTIAL` here is `SHEET_SHARED`: the fact is documented, its owner is not. `AMBIGUOUS` is a value several fragments claim.

## Which structural channel actually carries a binding

| Channel | Available | Confers ownership | Finding |
|---|---|---|---|
| A_BLOCK_CONTAINMENT | yes | no | a content block is the sheet on almost every page, so block containment would restate sheet == fragment |
| B_VALIDATED_GRAPHIC_REGION | yes | no | geometry is one axis-aligned rectangle per block and every polygon is null; there is no sub-block region to own a fact |
| C_CONNECTOR_OR_CALLOUT | no | no | no line, leader or connector is extracted for these documents; blocks.json carries rectangles only |
| D_TABLE_ROW_OR_COLUMN | yes | yes | table rows exist in quantity, but the extractor reads facts from block descriptions, so almost no fact is hosted by a row |
| E_LOCAL_SCHEME_REGION | no | no | no sub-block scheme region is extracted |
| F_EQUIPMENT_LABEL_TO_EQUIPMENT_FRAGMENT | no | no | there is no equipment fragment to bind to: a fragment is one function class on one page |
| G_FUNCTION_LABEL_TO_FUNCTION_FRAGMENT | yes | yes | the only channel that binds: a structural unit that names exactly one function class owns the facts it contains |

## The binding reaches the wrong facts

| Field | PROVEN | PARTIAL | AMBIGUOUS | UNKNOWN |
|---|---|---|---|---|
| serviced_object | 0 | 18 | 30 | 0 |
| building | 0 | 11 | 21 | 0 |
| corpus | 0 | 11 | 21 | 0 |
| section | 0 | 7 | 9 | 0 |
| zone | 10 | 7 | 29 | 1 |
| floors | 26 | 12 | 143 | 5 |
| systems | 307 | 123 | 682 | 0 |
| consumers | 11 | 17 | 169 | 0 |
| equipment_roles | 135 | 46 | 357 | 0 |
| upstream | 89 | 17 | 237 | 0 |
| downstream | 200 | 43 | 423 | 0 |
| stable_entities | 256 | 91 | 645 | 0 |
| cross_sheet_functional_references | 0 | 0 | 4 | 0 |

Every scope fact — `serviced_object`, `building`, `corpus`, `section` — has **zero** proven fragment-local bindings in all three corpora. Those are exactly the fields both blocked tiers need.

## Phase 1 — the blocked populations

| Population | Count |
|---|---|
| fragments | 313 |
| fragments with any proven local fact | 63 |
| fragments with a proven *deciding* local fact | 19 |
| MERGED candidates PARTIAL before | 69 |
| of them with a deciding local fact on both sides | 0 |
| of them with no deciding local fact at all | 59 |
| contested identity clusters | 39 |
| of them separated by a deciding local fact | 4 |
| of them with no local fact at all | 29 |
| stable NEED_MORE_EVIDENCE merge tasks | 18 |
| of them single-candidate | 14 |

## Is a fragment a region of the page?

No. Of 169 pages hosting more than one function, 134 have fragments whose evidence sentences overlap: 444 sentences belong to several fragments at once against 1196 that belong to one. A fragment is the set of page sentences matching one function class, so the binding layer had to be built on structural units of the document rather than on the fragments themselves.

## Phase 4 — the tiers, before and after

### Merge certificate

| Status | before | after (facts) | after (facts + title) |
|---|---|---|---|
| CERTIFIED | 0 | 0 | 0 |
| PARTIAL | 69 | 125 | 125 |
| AMBIGUOUS | 0 | 0 | 0 |
| CONTRADICTORY | 56 | 0 | 0 |
| UNKNOWN | 0 | 0 | 0 |

`AUTO_MERGED_CERTIFIED` entrants: **0** before, **0** after.

The overlay does not certify a single merge, and it destroys **56** documented refutations (105 contradicted dimensions): every `CONTRADICTORY` certificate becomes `PARTIAL`, because the scope facts that refuted it were sheet-shared and the overlay takes them away. Losing a refusal is a loss of safety, not a gain in coverage.

### Instance identity

| Corpus | identity PROVEN before | after (facts) | after (facts + title) | UNIQUELY_IDENTIFIED before | after (facts) | after (facts + title) |
|---|---|---|---|---|---|---|
| IOS1.1 | 73 | 73 | 0 | 0 | 0 | 0 |
| IOS2.1 | 7 | 7 | 5 | 3 | 0 | 0 |
| IOS3.1 | 0 | 0 | 0 | 5 | 0 | 0 |

`AUTO_ONE_TO_ONE_CERTIFIED` entrants: **0** before, **0** after. contention is a property of the frozen candidate inventory and no binding changes it; the tier still needs both sides proven.

The identity coverage that existed rested on the sheet. The primary mark is the stamp `Name`: leave it in place and the `PROVEN` count does not move at all; take it away and identity collapses. That is the measurement of how much of the current identity layer is a sheet fact wearing a function's name.

## Phase 3 — what the recovery separates

| Corpus | Multi-function sheets | Sheets where recovery separates siblings |
|---|---|---|
| IOS1.1 | 40 | 22 |
| IOS2.1 | 24 | 8 |
| IOS3.1 | 14 | 6 |

## Phase 5 — corpus safety

| Check | Value |
|---|---|
| candidate recall unchanged | True |
| scope baseline unchanged | True |
| cross-granularity competition after scoping | 0 |
| RIGHT_MAP_CONFLICT | 0 |
| search failures | 0 |
| group generation failures | 2 |
| binding invariant violations | 0 |
| overlay states an unproven value | 0 |
| ownership justified by absence of rivals | 0 |

## Phase 6 — negative controls

| Control | Instances | Expected | Violations |
|---|---|---|---|
| SHARED_ADDRESS_ON_A_MULTI_FUNCTION_SHEET | 5 | PARTIAL | 0 |
| LABEL_NEAR_TWO_FRAGMENTS_WITHOUT_A_STRUCTURAL_RELATION | 2782 | AMBIGUOUS | 0 |
| EXPLICIT_CALLOUT_TO_ONE_FRAGMENT | 0 | PROVEN | 0 |
| TABLE_ROW_ABOUT_ONE_FUNCTION | 4 | PROVEN | 0 |
| PROXIMITY_ONLY | 198 | PARTIAL | 0 |
| MISSING_GEOMETRY | 6 | UNKNOWN | 0 |

`EXPLICIT_CALLOUT_TO_ONE_FRAGMENT` has no instance because no connector or leader geometry is extracted for these documents. The control is declared unavailable rather than quietly passed.

## Phase 7 — determinism

Two independent replays, byte-identical: **True**. No page-specific or file-specific rule. Model calls: 0.

## Verdict

**B** — a general deterministic binding layer exists and is sound — it attributes 1034 of 4213 documented values to exactly one fragment and never binds by proximity — but it reaches the wrong facts: no scope fact in these corpora is fragment-local, so no merge and no 1:1 becomes certifiable, and restating the passports on proven facts alone erases 56 documented refutations.

Two findings come with it and are not softened into the main verdict:

* **D** — a fragment is not a region: it is the set of page sentences matching one function class, and on 134 of 169 multi-function pages those sets overlap; there is no equipment fragment and no page ever hosts two fragments of the same class.
* **E** — for the scope fields specifically the source really does not carry the information per function: the object is printed once per sheet, in the stamp, for every function at once.

The layer is real and it is sound; it is not sufficient. A non-empty tier was never the goal, and it was not reached: `AUTO_MERGED_CERTIFIED` and `AUTO_ONE_TO_ONE_CERTIFIED` both stay at zero, which is the correct outcome for evidence that does not exist.

## Files

- `experiments/function_lineage_v2/evidence_binding.py` — the measurement
- `tests/test_function_evidence_binding.py` — the controls
- artifact: `comparison/ai_sheet_matcher/20260904_function_lineage_v2_9_evidence_binding/`
