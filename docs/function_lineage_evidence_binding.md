# Deterministic function-local evidence attribution (v2.9)

Research only. No model calls, no deploy, no shadow, no materialization, no
production module changed.

## Why the track was opened

Both blocked tiers fail for the same architectural reason. `_passport_values`
in `function_lineage_shadow.py` builds a passport from
`(sheet, function_class)` and copies **every** page-level fact into **every**
function on that page. A sheet fact therefore becomes a fact of each function
on the sheet, and two functions on one page are indistinguishable by
construction.

The question was whether the fact can instead be attributed to one function
fragment, deterministically, from structure that already exists.

## The three rules the layer obeys

1. **Proximity is never proof.** Nothing binds because it is the nearest text.
   A fact binds only when a delimited structural unit that names exactly one
   function class *contains* it, or when a table row inherits a caption that
   names exactly one class, or a list item inherits its lead-in.
2. **The block is the sheet.** 250 of 277 pages carry a single content block,
   so inheriting ownership from a block would restate `sheet == fragment`
   under a new name. Block scope always resolves to `SHEET_SHARED`.
3. **A lone candidate is not evidence.** A page hosting exactly one function
   does not thereby own its facts — ownership needs a claim, never an absence
   of rivals. Measured: 751 proven bindings on single-function sheets, 751 of
   them justified by an explicit claim, 0 by the absence of a rival.

## What the geometry actually offers

| Channel | Available | Confers ownership | Why |
|---|---|---|---|
| block containment | yes | no | a content block is the whole sheet |
| validated graphic region | yes | no | one axis-aligned rectangle per block, every `polygon_points` null |
| connector / callout | **no** | no | no line, leader or connector is extracted; `blocks.json` carries rectangles only |
| table row / column | yes | yes | 2223 rows exist, but the extractor reads facts from block descriptions, so only 61 facts are hosted by a row |
| local scheme region | **no** | no | no sub-block region is extracted |
| equipment label → equipment fragment | **no** | no | there is no equipment fragment: a fragment is one function class on one page |
| function label → function fragment | yes | **yes** | the only channel that binds |

## A fragment is not a region

Of 169 pages hosting more than one function, **134** have fragments whose
evidence sentences overlap: 444 sentences belong to several fragments at once
against 1196 that belong to one. `_page_source` fills each fragment with every
page sentence matching that class, and nothing forces the sets apart. The
binding layer therefore had to be built on structural units of the document —
paragraphs, headings, list items, table rows, `[IMAGE]` field groups — and not
on the fragments themselves.

## What binds

| Corpus | Sheets | Facts | PROVEN | SHEET_SHARED | AMBIGUOUS | UNKNOWN |
|---|---:|---:|---:|---:|---:|---:|
| IOS1.1 | 45 | 1850 | 357 | 119 | 1374 | 0 |
| IOS2.1 | 42 | 1766 | 609 | 222 | 930 | 5 |
| IOS3.1 | 16 | 597 | 68 | 62 | 466 | 1 |
| **ALL** | 103 | 4213 | **1034** | 403 | 2770 | 6 |

## …and what it does not reach

| Field | PROVEN | SHEET_SHARED | AMBIGUOUS |
|---|---:|---:|---:|
| `serviced_object` | **0** | 18 | 30 |
| `building` | **0** | 11 | 21 |
| `corpus` | **0** | 11 | 21 |
| `section` | **0** | 7 | 9 |
| `zone` | 10 | 7 | 29 |
| `floors` | 26 | 12 | 143 |

Every scope fact has **zero** proven fragment-local bindings in all three
corpora. Those are exactly the fields both blocked tiers need: the merge
certificate fails on `TARGET_CONSOLIDATION` and
`SERVICED_OBJECT_COMPATIBILITY`, the 1:1 tier needs an instance a sibling
cannot wear.

## The tiers do not move — and the overlay costs safety

| | before | after (facts) | after (facts + title) |
|---|---:|---:|---:|
| merge `CERTIFIED` | 0 | 0 | 0 |
| merge `PARTIAL` | 69 | 125 | 125 |
| merge `CONTRADICTORY` | **56** | **0** | **0** |
| `AUTO_MERGED_CERTIFIED` | 0 | 0 | 0 |
| `AUTO_ONE_TO_ONE_CERTIFIED` | 0 | 0 | 0 |

Restating the passports on proven facts alone certifies nothing and **destroys
all 56 documented refutations** (105 contradicted dimensions): the scope facts
that refuted those merges were sheet-shared, and the overlay takes them away.
Losing a refusal is a loss of safety, not a gain in coverage. That is the
reason the overlay is a measurement and is not wired into anything.

The identity side gives the same answer from the other direction. The v2.7
primary mark is `source_sheet.title` — the stamp `Name`, a sheet-level fact.
Leave it in place and `PROVEN` identity does not move at all (73 → 73 on
IOS1.1); take it away and identity collapses (73 → 0, `UNIQUELY_IDENTIFIED`
8 → 0 across the corpora). The identity coverage that exists today is a sheet
fact wearing a function's name.

## Safety and controls

Candidate recall unchanged, scope baseline unchanged, cross-granularity
competition 0, `RIGHT_MAP_CONFLICT` 0, search failures 0, group generation
failures 2 (the frozen v2.4 baseline), binding invariant violations 0, overlay
states an unproven value 0 times, ownership justified by absence of rivals 0.

All six negative controls hold with 0 violations.
`EXPLICIT_CALLOUT_TO_ONE_FRAGMENT` has no instance to exercise and is declared
**structurally unavailable** rather than quietly passed — there is no connector
geometry for these documents.

Two independent replays are byte-identical. No page-specific or file-specific
rule. Model calls: 0.

## Verdict

**B** — the general deterministic Function-local Evidence Binding layer exists
and is sound. It attributes 1034 of 4213 documented values to exactly one
fragment and never binds by proximity. It is not sufficient: it reaches the
wrong facts, opens no production-relevant tier, and the overlay built on it
would cost 56 refutations.

Two findings ride with it and are not softened into the main verdict:

* **D** — a defect of granularity, not of this layer: a fragment is the set of
  page sentences matching one function class, those sets overlap on 134 of 169
  multi-function pages, there is no equipment fragment, and no page ever hosts
  two fragments of the same class.
* **E** — for the scope fields specifically the source really does not carry
  the information per function: the object is printed once per sheet, in the
  stamp, for every function on it at once.

A non-empty tier was never the goal and was not reached. Zero is the correct
outcome for evidence that does not exist.

## Files

- `experiments/function_lineage_v2/evidence_binding.py` — the measurement
- `tests/test_function_evidence_binding.py` — 30 tests, including the six
  Phase 6 negative controls
- artifact:
  `comparison/ai_sheet_matcher/20260904_function_lineage_v2_9_evidence_binding/`
  (`function_evidence_bindings.json`, `binding_metrics.json`,
  `fragment_local_field_recovery.json`, `one_to_one_reassessment.json`,
  `merge_certificate_reassessment.json`, `report.md`)
