# Deterministic object binding recovery for MERGED

Research only. No model calls, no deploy, no shadow, no production module
changed.

## Why the track was opened

All 69 `PARTIAL` merge certificates fail on exactly two dimensions —
`TARGET_CONSOLIDATION` and `SERVICED_OBJECT_COMPATIBILITY` — and both fail for
one reason: the functions carry no serviced-object binding. The question was
whether that binding exists in the Markdown and is recoverable
deterministically.

## The unused stamp field

Every page carries a blockquote stamp, and `_STAMP_RE` parses it correctly into
`Code`, `Stage`, `Sheet`, `Object`, `Name`, `Organization`, `Revisions`. But
`_page_source` builds `serviced_object` from `_field_values(clean_body)`, not
from the stamp, so `stamp["Object"]` is parsed and then discarded.

It must stay discarded. Where the field is filled at all it holds the project
address, one identical value per document side, and it never names a corpus or
a section:

| Corpus | Side | Pages with the field | Distinct values | Values naming an object |
|---|---|---:|---:|---:|
| IOS1.1 | LEFT | 60 | 1 | 0 |
| IOS1.1 | RIGHT | 48 | 1 | 0 |
| IOS2.1 | LEFT | 52 | 1 | 0 |
| IOS2.1 | RIGHT | 0 | 0 | 0 |
| IOS3.1 | LEFT | 25 | 1 | 0 |
| IOS3.1 | RIGHT | 29 | 1 | 0 |

Wiring it in would make both failing dimensions pass on **every** candidate at
once, certifying 69 merges on evidence that separates nothing. That is the
exact shape of a forbidden green result, so the field is refused as binding
evidence.

## The recovery gap is real

Corpus and section tokens are searched only in `evidence_text` — the stamp
name, summaries, descriptions and entity items — never in the rest of the page
body:

| Corpus | Side | Pages | Binding in the body | Extractor sees | Recoverable |
|---|---|---:|---:|---:|---:|
| IOS1.1 | LEFT | 60 | 8 | 1 | 7 |
| IOS1.1 | RIGHT | 48 | 6 | 0 | 6 |
| IOS2.1 | LEFT | 52 | 11 | 6 | 5 |
| IOS2.1 | RIGHT | 63 | 12 | 6 | 6 |
| IOS3.1 | LEFT | 25 | 10 | 5 | 5 |
| IOS3.1 | RIGHT | 29 | 7 | 4 | 3 |
| **ALL** | | 277 | 54 | 22 | 32 |

So 32 of 277 pages hold a discriminating binding the extractor does not see.
The ceiling is nonetheless low: only 54 pages of 277 name an object at all.

## But a recovered token is not a function fact

A page token may be attributed to a fragment only when the page hosts exactly
one function and names exactly one object. Otherwise `sheet == function` is
assumed, which the architecture forbids.

| State | Pages |
|---|---:|
| ATTRIBUTABLE | 0 |
| PAGE_AMBIGUOUS | 24 |
| OBJECT_AMBIGUOUS | 2 |
| ABSENT | 77 |

Not one page binding in the whole corpus is soundly attributable: of the 26
pages that both name an object and host a function, 24 host several functions
and 2 name several objects.

## What recovery would do to the certificates

The upper bound deliberately ignores attributability and lets any page token
stand for its functions — it cannot be published, it is the ceiling the sound
recovery is measured against.

| Recovery | Outcome |
|---|---|
| Upper bound | 0 candidates recoverable on both sides, 18 on one side only, 51 on neither |
| Sound | 0 of 69 bound on both sides |

`PARTIAL` certificates that would become `CERTIFIED`: **0**, under either
recovery. A one-sided binding proves nothing: a missing fact on the other side
stays `UNKNOWN` and never becomes a match.

The upper bound separates exactly one candidate, and that separation is a
string artefact — a page naming `корпус 1`, `корпус 2`, `корпус 3` against a
page naming `корпус 1,2` in one token. The values overlap in meaning and differ
only literally, so it is a false refutation and is recorded as unsound.

## Verdict

`E_DATA_LIMITED`. The binding is recoverable, but it lands on the wrong pages:
no `PARTIAL` certificate has an attributable binding on either side, so no
merge becomes provable and none is refuted soundly. Recovery is **not** wired
into the extractor, because its only measurable effect on this corpus is one
false refutation, and its most likely effect if the stamp field were included
would be 69 false certifications.

## Files

- `experiments/function_lineage_v2/object_binding.py` — the measurement
- `tests/test_function_object_binding.py` — 15 tests
- artifact: `comparison/.../20260904_function_lineage_object_binding/`
