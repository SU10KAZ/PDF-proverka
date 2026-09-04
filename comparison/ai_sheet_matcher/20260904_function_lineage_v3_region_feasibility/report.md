# Function region / evidence geometry feasibility (v3.0)

Research only.  No model calls, no deploy, no shadow, no materialization, no production module changed.

## Verdict

**B** — part of the needed geometry exists in the source and the current preprocessing loses it; the extraction and the data contract have to change

## What the source carries

| Document | Pages | Rotations | Spans | Vector segments | Table cells | Leaders | Annotations | Pages w/o text layer |
|---|---|---|---|---|---|---|---|---|
| IOS1.1/LEFT | 60 | 0°×44, 90°×5, 270°×11 | 14367 | 3820215 | 5587 | 385 | 7510 | 3 |
| IOS1.1/RIGHT | 48 | 0°×48 | 13296 | 1871060 | 826 | 4203 | 1 | 0 |
| IOS2.1/LEFT | 52 | 0°×39, 90°×1, 270°×12 | 3513 | 277438 | 3329 | 270 | 516 | 10 |
| IOS2.1/RIGHT | 63 | 0°×52, 90°×1, 270°×10 | 7023 | 392105 | 5341 | 1192 | 2 | 10 |
| IOS3.1/LEFT | 26 | 0°×16, 270°×10 | 3846 | 380732 | 724 | 294 | 137 | 10 |
| IOS3.1/RIGHT | 29 | 0°×18, 270°×11 | 3174 | 136152 | 579 | 156 | 157 | 11 |

## What the preprocessing keeps

| Channel | In the PDF | In blocks.json | In the Markdown |
|---|---|---|---|
| text span rectangle | every span carries a bbox, a font and a size | absent — blocks.json holds one rectangle per block | absent — a page is a stream of lines |
| paragraph rectangle | PyMuPDF text blocks, one rectangle per paragraph | absent | paragraph order is kept, position is not |
| table cell rectangle | derivable from the drawn lattice of rulings | absent | a table becomes prose or a pipe table without geometry |
| vector line | millions of segments per document | absent | absent |
| leader / callout | a stroke drawn along its label, recoverable deterministically | absent | absent |
| page rotation | /Rotate per page, 0/90/270 all present in this corpus | kept as a number, unused by the fact extractor | absent |
| CAD font encoding | ISOCPEUR subsets shift Cyrillic by a constant | absent | recovered by OCR, without position |
| annotation | 8323 annotations across the six documents | absent | absent |

## Recognition loses content, not only position

| Document | Printed strings | In the Markdown | Share | Drawing pages share |
|---|---|---|---|---|
| IOS1.1/LEFT | 5543 | 2379 | 0.4292 | 0.2495 |
| IOS1.1/RIGHT | 8926 | 2523 | 0.2827 | 0.2406 |
| IOS2.1/LEFT | 2656 | 1668 | 0.628 | 0.559 |
| IOS2.1/RIGHT | 5044 | 3335 | 0.6612 | 0.6066 |
| IOS3.1/LEFT | 1472 | 513 | 0.3485 | 0.1802 |
| IOS3.1/RIGHT | 1525 | 749 | 0.4911 | 0.3853 |

## The prototype

278 pages, 45219 printed strings, 12866 attributed to exactly one region (28.4%).

| Relation | Spans |
|---|---|
| UNKNOWN | 28983 |
| CONNECTED_CALLOUT | 6500 |
| TABLE_CELL | 6240 |
| SHEET_SHARED | 2946 |
| AMBIGUOUS | 424 |
| DIRECT_CONTAINMENT | 126 |

| Region kind | Count |
|---|---|
| EDGE_GROUP | 51213 |
| TABLE | 407 |
| BOX | 286 |
| SHEET_FRAME | 56 |
| TEXT_SECTION | 51 |
| STAMP | 2 |

## Representative pages

the first page in corpus order (IOS1.1, IOS2.1, IOS3.1; LEFT then RIGHT; ascending page) that satisfies the case predicate.

| Case | Page | Rot | Spans | Segments | Attributed |
|---|---|---|---|---|---|
| SINGLE_FUNCTION | IOS1.1/LEFT p.24 | 270 | 582 | 522673 | 0.985 |
| MANY_FUNCTIONS | IOS1.1/LEFT p.25 | 270 | 39 | 165777 | 0.718 |
| TABLE | IOS1.1/LEFT p.3 | 0 | 39 | 564 | 0.923 |
| SCHEME | IOS1.1/LEFT p.24 | 270 | 582 | 522673 | 0.985 |
| REPEATED_SCHEMES | IOS1.1/LEFT p.24 | 270 | 582 | 522673 | 0.985 |
| MIXED_TEXT_AND_GRAPHICS | IOS1.1/LEFT p.24 | 270 | 582 | 522673 | 0.985 |
| NO_TEXT_LAYER | IOS1.1/LEFT p.58 | 270 | 0 | 0 | 0.0 |
| ROTATED | IOS1.1/LEFT p.24 | 270 | 582 | 522673 | 0.985 |

Cases with no instance in the corpus: SAME_CLASS_TWICE.

## Fragment-local recovery, field by field

| Field | Values | v2.9 PROVEN | v3.0 region-local | v3.0 fragment-local | sheet-shared | not in text layer |
|---|---|---|---|---|---|---|
| serviced_object | 165 | 0 | 0 | 0 | 16 | 19 |
| building | 103 | 0 | 0 | 0 | 10 | 11 |
| corpus | 103 | 0 | 0 | 0 | 10 | 11 |
| section | 62 | 0 | 0 | 0 | 6 | 8 |
| zone | 151 | 10 | 24 | 2 | 1 | 36 |
| floors | 708 | 26 | 233 | 28 | 9 | 320 |

## Discriminating power of the region layer

| Measure | Markdown-confirmed | Ceiling (text layer alone) |
|---|---|---|
| corpus:pages_with_disagreeing_regions | 0 | 1 |
| corpus:regions_with_value | 4 | 10 |
| floors:pages_with_disagreeing_regions | 11 | 18 |
| floors:regions_with_value | 80 | 111 |
| section:regions_with_value | 0 | 1 |
| serviced_object:pages_with_disagreeing_regions | 0 | 1 |
| serviced_object:regions_with_value | 4 | 11 |
| zone:regions_with_value | 12 | 12 |

## The tiers do not move

| Tier | before | after |
|---|---|---|
| AUTO_MERGED_CERTIFIED | 0 | 0 |
| AUTO_ONE_TO_ONE_CERTIFIED | 0 | 0 |

both tiers are decided on serviced_object, building, corpus and section.  The region layer places none of the documented values of those fields inside a delimited region, and the values it can add from the text layer disagree between regions on one page of 278.  Zero discriminating evidence remains zero certificates.

## Sensitivity of the two leader parameters

| gap (em) | overlap | attributed | ambiguous | unknown |
|---|---|---|---|---|
| 0.0 | 0.8 | 8176 | 272 | 33825 |
| 0.15 | 0.8 | 12033 | 348 | 29892 |
| 0.3 | 0.8 | 12866 | 424 | 28983 |
| 0.6 | 0.8 | 13345 | 551 | 28377 |
| 0.3 | 0.5 | 13211 | 537 | 28525 |
| 0.3 | 1.0 | 12290 | 346 | 29637 |

## Controls

* **PROXIMITY_NEVER_PROVES** — expected 0, observed 0.  9347 spans lie within five em of a boundary and are left UNKNOWN because no boundary runs along them; a nearest-region rule would have claimed every one
* **SHEET_SCALE_REGION_NEVER_OWNS** — expected 0, observed 0.
* **STAMP_VALUE_NEVER_FRAGMENT_LOCAL** — expected 0, observed 0.
* **NO_GRAPHIC_OWNERSHIP_WITHOUT_INK** — expected 0, observed 0.
* **LONE_REGION_IS_NOT_EVIDENCE** — expected every attribution justified by an explicit relation, observed {'pages_with_exactly_one_local_region': 35, 'attributions_on_those_pages': 795, 'justified_by_absence_of_a_rival': 0}.
* **FRAGMENT_LOCAL_REQUIRES_A_CLAIM** — expected every fragment-local value sits in a region naming exactly one class, observed {'fragment_local_values': 86, 'with_region_naming_exactly_one_class': 86}.

## Secondary findings

* **D** — the fragment model is the wrong unit: geometry attributes a fact to an equipment region — a feeder, a table row, a panel — and there is no such fragment to receive it
* **E'** — for scope fields the source really is sheet-level on these sheets: the object is a title above the whole drawing or a stamp entry, and regions disagree on one page of 278
* **F** — recognition loses content, not only position: on drawing pages most printed strings never reach the Markdown, so the fact ceiling is set by the recognizer and not by the document

