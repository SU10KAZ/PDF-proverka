# PDF Evidence V1 — native PDF text and geometry preservation

Research only.  No model calls, no deploy, no shadow, no materialization, no production module changed.

## What the layer is

A deterministic evidence layer over the source PDF.  Every unit is a printed string with its rectangle, its decoding provenance, the structural region that owns it, and — derived from those — the scope at which it applies and what it is allowed to assert.  The producer has two claim values, `POSITIVE_PRESENCE` and `SUPPORT_ONLY`, and no vocabulary for an absence.

Corpus: 278 pages of six frozen documents.  Units: **48578**.  Table cells: 16386.  Vector segments compacted 69.4× into welded edges.

## The layer, per document

| Document | Pages | Units | Positive presence | Support only | Fragment-local | Sheet-shared | Document-shared | Unknown scope | Table cells |
|---|---|---|---|---|---|---|---|---|---|
| IOS1.1/LEFT | 60 | 20149 | 20149 | 0 | 8787 | 613 | 108 | 10641 | 5587 |
| IOS1.1/RIGHT | 48 | 11003 | 10942 | 61 | 2726 | 729 | 133 | 7415 | 826 |
| IOS2.1/LEFT | 52 | 3828 | 3794 | 34 | 1546 | 266 | 93 | 1923 | 3329 |
| IOS2.1/RIGHT | 63 | 6796 | 6769 | 27 | 2791 | 257 | 119 | 3629 | 5341 |
| IOS3.1/LEFT | 26 | 3512 | 3501 | 11 | 782 | 138 | 48 | 2544 | 724 |
| IOS3.1/RIGHT | 29 | 3290 | 3277 | 13 | 321 | 90 | 66 | 2813 | 579 |

## What the recovery adds

A string counts as recovered only when nothing downstream had it: not in the recognized Markdown of its page, and — for the annotation channel — not in the page's own text layer either.

| Document | Single-span units | of those only in the PDF | Joined units | of those only in the PDF | SHX annotations | already in the text layer | in the Markdown | recovered by this channel |
|---|---|---|---|---|---|---|---|---|
| IOS1.1/LEFT | 5197 | 2914 | 687 | 498 | 4765 | 0 | 112 | 4653 |
| IOS1.1/RIGHT | 8221 | 5956 | 604 | 508 | 0 | 0 | 0 | 0 |
| IOS2.1/LEFT | 2483 | 907 | 133 | 72 | 445 | 0 | 389 | 56 |
| IOS2.1/RIGHT | 4861 | 1639 | 137 | 60 | 0 | 0 | 0 | 0 |
| IOS3.1/LEFT | 1321 | 879 | 121 | 92 | 137 | 0 | 31 | 106 |
| IOS3.1/RIGHT | 1488 | 756 | 22 | 9 | 155 | 0 | 37 | 118 |

Recovered by the annotation channel alone: **4933** printed strings that exist in no other layer.  AutoCAD draws SHX shape text as vectors and writes the readable string into a comment annotation; there is no glyph in the text layer at all, so no reader that only reads text has ever seen them.

## CAD font decoding, audited

Fonts with a proven repair: **3**.  Refused: **1**.  Units repaired: 38.  Units left unresolved (kept as printed, downgraded to support): 81.

| Document | Font | Block chars | Distinct codes | Covered by 581 | Yield-optimal shift | its coverage | Confirmed by the Markdown | Repair | Reason |
|---|---|---|---|---|---|---|---|---|---|
| IOS1.1/LEFT | ISOCPEUR | 218 | 37 | 0.9862 | 565 | 0.9862 | 5/14 | applied | the_constant_covers_this_font |
| IOS1.1/LEFT | ISOCPEURItalic | 19 | 15 | 1.0 | 567 | 1.0 | 0/3 | applied | the_constant_covers_this_font |
| IOS1.1/RIGHT | ArialMT | 59 | 1 | 0.0 | 599 | 1.0 | 0/0 | refused | one_codepoint_identifies_no_displacement |
| IOS2.1/LEFT | ISOCPEUR | 64 | 15 | 0.6562 | 542 | 0.7969 | 3/11 | applied | repairs_confirmed_word_for_word_by_the_recognized_layer |

The yield-optimal column is the diagnostic, not the answer.  On `IOS1.1/LEFT` it proposes 565, which scores the same coverage as the documented 581 and renders the title of a single-line diagram as `ЎФЭЮЫШЭХЩЭРп аРбзХвЭРп беХЬР`; 581 renders it as `Однолинейная расчетная схема ВРУ-3`, and only 581 is ever confirmed by an independent reading.  Garbage is also Cyrillic, so a search that maximizes Cyrillic cannot identify the codec.

## Geometry, compacted

| Document | Raw segments | Welded edges | Compression | Regions | Table cells | Mean slanted ink | Pages mostly slanted |
|---|---|---|---|---|---|---|---|
| IOS1.1/LEFT | 3820215 | 23327 | 163.8 | 11604 | 5587 | 0.0609 | 0 |
| IOS1.1/RIGHT | 1871060 | 44318 | 42.2 | 28076 | 826 | 0.0576 | 0 |
| IOS2.1/LEFT | 277438 | 11489 | 24.1 | 4994 | 3329 | 0.0367 | 0 |
| IOS2.1/RIGHT | 392105 | 9520 | 41.2 | 3254 | 5341 | 0.0243 | 0 |
| IOS3.1/LEFT | 380732 | 6673 | 57.1 | 2580 | 724 | 0.0501 | 0 |
| IOS3.1/RIGHT | 136152 | 3845 | 35.4 | 1507 | 579 | 0.0399 | 0 |

## Page completeness

How much of what the sheet prints the recognized layer contains.  This is a statement about reading, never about the document.

| Document | Status | Pages | SUFFICIENT | PARTIAL | INSUFFICIENT | UNKNOWN | Pages with no Markdown section | Pages with no text layer | Read share |
|---|---|---|---|---|---|---|---|---|---|
| IOS1.1/LEFT | INSUFFICIENT | 60 | 7 | 18 | 31 | 4 | — | 3 | 0.2427 |
| IOS1.1/RIGHT | INSUFFICIENT | 48 | 7 | 15 | 26 | 0 | — | 0 | 0.2675 |
| IOS2.1/LEFT | INSUFFICIENT | 52 | 6 | 25 | 11 | 10 | — | 10 | 0.6619 |
| IOS2.1/RIGHT | INSUFFICIENT | 63 | 6 | 34 | 13 | 10 | — | 10 | 0.6601 |
| IOS3.1/LEFT | INSUFFICIENT | 26 | 2 | 7 | 7 | 10 | 25 | 10 | 0.3179 |
| IOS3.1/RIGHT | INSUFFICIENT | 29 | 3 | 9 | 6 | 11 | — | 11 | 0.4697 |

## Function Lineage, re-evaluated read-only

Three regimes.  `BASELINE_V3` is the v2.9 / v3.0 rule — text-layer spans only, and a value counts only when the recognized Markdown also has it.  `RECOVERED_ONLY` adds the new channels and keeps the Markdown requirement: the difference is what better extraction buys.  `ASYMMETRIC_V1` drops the Markdown requirement per decision item 3: the difference is what the contract buys.

| Field | Values | v2.9 proven | v3.0 fragment-local | BASELINE_V3 | RECOVERED_ONLY | ASYMMETRIC_V1 | sheet-shared (V1) | document-shared (V1) | not in the native layer (V1) |
|---|---|---|---|---|---|---|---|---|---|
| serviced_object | 165 | 0 | 0 | 0 | 6 | 6 | 85 | 5 | 16 |
| building | 103 | 0 | 0 | 0 | 4 | 4 | 84 | 2 | 8 |
| corpus | 103 | 0 | 0 | 0 | 4 | 4 | 84 | 2 | 8 |
| section | 62 | 0 | 0 | 0 | 2 | 2 | 1 | 3 | 8 |
| zone | 151 | 10 | 2 | 24 | 24 | 24 | 61 | 1 | 33 |
| floors | 708 | 26 | 28 | 262 | 283 | 283 | 14 | 0 | 262 |
| systems | 3212 |  |  | 102 | 119 | 119 | 284 | 24 | 1701 |
| consumers | 746 |  |  | 0 | 0 | 0 | 5 | 0 | 682 |
| equipment_roles | 1644 |  |  | 25 | 28 | 28 | 9 | 0 | 1263 |
| upstream | 1074 |  |  | 1 | 1 | 1 | 0 | 0 | 1073 |
| downstream | 1945 |  |  | 1 | 4 | 4 | 0 | 0 | 1929 |
| stable_entities | 2957 |  |  | 154 | 184 | 184 | 109 | 0 | 1052 |
| cross_sheet_functional_references | 16 |  |  | 0 | 0 | 0 | 0 | 0 | 9 |

### Is the value printed at all?

| Field | Values | printed (baseline) | printed (recovered) | share (baseline) | share (recovered) |
|---|---|---|---|---|---|
| upstream | 1074 | 1 | 1 | 0.0009 | 0.0009 |
| downstream | 1945 | 13 | 16 | 0.0067 | 0.0082 |
| consumers | 746 | 32 | 64 | 0.0429 | 0.0858 |
| serviced_object | 165 | 146 | 149 | 0.8848 | 0.903 |
| building | 103 | 92 | 95 | 0.8932 | 0.9223 |
| corpus | 103 | 92 | 95 | 0.8932 | 0.9223 |
| section | 62 | 54 | 54 | 0.871 | 0.871 |
| zone | 140 | 107 | 107 | 0.7643 | 0.7643 |
| floors | 708 | 388 | 446 | 0.548 | 0.6299 |

### What the layer could add that the passport does not have

The table above places values the passport already holds — and every one of those came from the recognized Markdown by construction, so asking whether the Markdown confirms them asks whether the Markdown contains what it produced.  This is the other question: when a scope value is printed inside a proven region, do the regions of that page disagree?  Only a page whose regions disagree could ever separate siblings.

| Field | regions with a value (baseline) | regions with a value (V1) | pages where regions disagree (baseline) | pages where regions disagree (V1) | pages with a sheet-level value (V1) |
|---|---|---|---|---|---|
| corpus | 4 | 14 | 0 | 1 | 21 |
| floors | 80 | 111 | 11 | 18 | 36 |
| section | 0 | 6 | 0 | 0 | 2 |
| serviced_object | 4 | 16 | 0 | 1 | 21 |
| zone | 12 | 12 | 0 | 0 | 0 |

### Certified tiers

| Tier | before | after | gate |
|---|---|---|---|
| AUTO_ONE_TO_ONE_CERTIFIED | 0 | 0 | an uncontended pure 1:1 task with identity PROVEN on both sides |
| AUTO_MERGED_CERTIFIED | 0 | 0 | a FULL merge certificate, decided on serviced_object, building, corpus and section |

Functions whose page prints an equipment mark inside a proven region: **152** of 313.  Functions whose *own primary mark* is printed inside a proven region: **21**.

## The regression: false removals must not come back

### Structural

| Control | Expected | Observed |
|---|---|---|
| NO_ABSENCE_VOCABULARY_IN_ANY_PRODUCED_VALUE | 0 | 0 |
| CLAIMS_STAY_INSIDE_THE_CLOSED_VOCABULARY | 0 | 0 |
| FRAGMENT_SCOPE_REQUIRES_STRUCTURAL_OWNERSHIP | 0 | 0 |
| POSITIVE_PRESENCE_REQUIRES_EXACT_GEOMETRY | 0 | 0 |
| UNRESOLVED_DECODING_NEVER_ASSERTS | 0 | 0 |

### Empirical — the naive symmetric consumer, replayed

The defect being replayed: a native string has no owner object, so comparing the strings of two linked sheets and calling the leftovers removals produces removals nobody can defend.

| Corpus | 1:1 links | Left units compared | Removals it would assert | of those printed elsewhere in the right document | of those read elsewhere in the right Markdown | V1 producer removals |
|---|---|---|---|---|---|---|
| IOS1.1 | 33 | 3763 | 3328 | 142 | 337 | 0 |
| IOS2.1 | 15 | 1128 | 914 | 499 | 72 | 0 |
| IOS3.1 | 8 | 1219 | 258 | 92 | 22 | 0 |

### Negative controls

| Control | Expected | Observed |
|---|---|---|
| PROXIMITY_NEVER_PROVES | units within reach of a region stay unowned unless a drawn relation says otherwise | {"units_within_5_em_of_a_region_and_unowned": 17512, "attributed_by_proximity": 0} |
| SHEET_SCALE_REGION_NEVER_OWNS | 0 | 0 |
| STAMP_VALUE_NEVER_FRAGMENT_LOCAL | 0 | 0 |
| LONE_REGION_IS_NOT_EVIDENCE | a page with one region attributes nothing by that fact alone | {"pages_with_exactly_one_local_region": 35, "units_left_without_a_scope_on_those_pages": 1338, "attributed_because_no_rival_existed": 0} |

## Verdict

the layer exists and holds what the source carries: 48 578 units with rectangles, decoding provenance, structural ownership and scope, under a contract that can assert presence and has no way to assert an absence.  It recovers printed content no downstream stage has ever seen, and it does not open either certified tier.

* **1** — AutoCAD SHX shape text is a whole channel of printed content that no reader of this project has ever seen: the glyphs are vectors and the readable string lives in a comment annotation with its own rectangle.
* **2** — the CAD codec cannot be found by search — maximizing the Cyrillic yield picks a shift that renders a drawing title as garbage, because garbage is also Cyrillic.
* **3** — lifting the Markdown requirement changes nothing for values the passport already holds, because those values came from the Markdown by construction; it changes what the layer can add — scope values inside a region rise from 100 to 159 and the pages whose regions disagree from 11 to 20.
* **4** — neither certified tier gains an entrant, and that is now a computed answer: 21 of 313 functions have their own primary mark printed inside a proven region, and none of them is on both sides of the one uncontended 1:1 task.

{
 "evidence": {
  "geometry_compression": 69.4,
  "pages": 278,
  "printed_strings_absent_from_the_recognized_layer": 19223,
  "strings_recovered_by_joining_spans": 1239,
  "strings_recovered_from_annotations": 4933,
  "table_cells": 16386,
  "units": 48578
 },
 "findings": [
  {
   "id": "1",
   "statement": "AutoCAD SHX shape text is a whole channel of printed content that no reader of this project has ever seen: the glyphs are vectors and the readable string lives in a comment annotation with its own rectangle"
  },
  {
   "id": "2",
   "statement": "the CAD codec cannot be found by search — maximizing the Cyrillic yield picks a shift that renders a drawing title as garbage, because garbage is also Cyrillic"
  },
  {
   "id": "3",
   "statement": "lifting the Markdown requirement changes nothing for values the passport already holds, because those values came from the Markdown by construction; it changes what the layer can add — scope values inside a region rise from 100 to 159 and the pages whose regions disagree from 11 to 20"
  },
  {
   "id": "4",
   "statement": "neither certified tier gains an entrant, and that is now a computed answer: 21 of 313 functions have their own primary mark printed inside a proven region, and none of them is on both sides of the one uncontended 1:1 task"
  }
 ],
 "function_lineage": {
  "fragment_local_asymmetric_v1": 659,
  "fragment_local_baseline_v3": 569,
  "fragment_local_recovered_only": 659,
  "pages_where_regions_disagree_asymmetric_v1": 20,
  "scope_regions_with_a_value_asymmetric_v1": 159,
  "scope_regions_with_a_value_baseline": 100
 },
 "guards_failed": [],
 "kind": "verdict",
 "layer_built": true,
 "model_calls": 0,
 "regression": {
  "demonstrably_false_of_those": 1164,
  "naive_consumer_removal_claims": 4500,
  "producer_removal_claims": 0
 },
 "schema_version": "pdf-evidence.v1",
 "statement": "the layer exists and holds what the source carries: 48 578 units with rectangles, decoding provenance, structural ownership and scope, under a contract that can assert presence and has no way to assert an absence.  It recovers printed content no downstream stage has ever seen, and it does not open either certified tier",
 "tiers": {
  "AUTO_MERGED_CERTIFIED": {
   "after": 0,
   "before": 0
  },
  "AUTO_ONE_TO_ONE_CERTIFIED": {
   "after": 0,
   "before": 0
  }
 }
}

## Files

- `experiments/pdf_evidence_v1/` — the layer
- `tests/test_pdf_evidence_v1.py` — the controls
