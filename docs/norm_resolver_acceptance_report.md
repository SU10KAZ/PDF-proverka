# Norm Resolver acceptance: EM-K2 / EM-K4

Date: 2026-09-01. Resolver: `norm-resolver-v9`. The source project artifacts
were read only. Replays were written under
`/tmp/norm-resolver-acceptance.WbgGq5/EM-K2-v9` and
`/tmp/norm-resolver-acceptance.WbgGq5/EM-K4-v9`.

## Acceptance result

| Document | Findings | Candidates | Baseline VERIFIED | Resolver VERIFIED | AMBIGUOUS | NOT_VERIFIED | MISSING | WRONG_EDITION | SPECIAL_POLICY | Native verify |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| EM-K2 | 28 | 14 | 1 | 3 | 1 | 10 | 0 | 0 | 2 | 3/3 |
| EM-K4 | 54 | 14 | 4 | 5 | 1 | 8 | 0 | 0 | 3 | 5/5 |
| Total | 82 | 28 | 5 | 8 | 2 | 18 | 0 | 0 | 5 | 8/8 |

Both documents improve over their own baseline. Every published clause was
looked up a second time by `norms_api.get_paragraph`, then all eight were
independently accepted by native `norm_verify`. Model calls: **0**.

The project issue date used for edition applicability was 2025-11-11.
`ГОСТ Р 21.101-2020` remains the cited, historically applicable edition;
the current edition is exposed separately as `ГОСТ Р 21.101-2026`. The
deterministic checks retain current register status `replaced`, add
`status_at_document_date=active`, and set `needs_revision=false`.

## Manual audit of all 28 references

The labels assess whether the reference supports the engineering finding, not
merely whether the document exists. `PARTIALLY_SUPPORTED` is not counted as a
false reference, but it must not be presented as proof of every factual detail
in the finding.

| Document | Finding | Designation | Resolver | Clause | Manual result | Reason |
|---|---|---|---|---|---|---|
| K2 | F-001 | ГОСТ 32395-2020 | NOT_VERIFIED | — | UNSUPPORTED | A definition of distribution equipment cannot prove the claimed edition replacement. |
| K2 | F-002 | СП 256.1325800.2016 | AMBIGUOUS | — | PARTIALLY_SUPPORTED | Several context-specific load formulas are close; the project context does not identify one uniquely. |
| K2 | F-002 | ПУЭ 7 | NOT_VERIFIED | — | UNSUPPORTED | Retrieved PUE provisions do not establish the printed load formula. |
| K2 | F-003 | ГОСТ 21.110-2013 | NOT_VERIFIED | — | PARTIALLY_SUPPORTED | The document family is relevant to schedules, but no exact clause was established for the wrong building label. |
| K2 | F-004 | ПУЭ 7 | VERIFIED | 1.5.17 | SUPPORTED | The real provision gives the 40%/5% meter-current criteria used by the finding. |
| K2 | F-017 | ГОСТ 21.501-2018 | VERIFIED | 5.3.2 | PARTIALLY_SUPPORTED | It requires room names and areas on floor plans, but does not by itself prove every listed inconsistency. |
| K2 | F-018 | ГОСТ 21.602-2016 | NOT_VERIFIED | — | UNSUPPORTED | The close clause concerns heat-station elements, not air-curtain identifiers. |
| K2 | F-028 | ГОСТ Р 21.101-2020 | VERIFIED | 4.3.3 | SUPPORTED | It explicitly requires entries in the referenced/attached documents register. |
| K2 | F-028 | СП 52.13330.2016 | NOT_VERIFIED | — | UNSUPPORTED | This is an item that may belong in the register, not the rule requiring the register. |
| K2 | F-028 | СП 6.13130.2021 | NOT_VERIFIED | — | UNSUPPORTED | Its own “Normative references” section is not evidence for the project register. |
| K2 | F-028 | СП 59.13330.2020 | NOT_VERIFIED | — | UNSUPPORTED | Its own “Normative references” section is not evidence for the project register. |
| K2 | F-028 | СП 256.1325800.2016 | NOT_VERIFIED | — | UNSUPPORTED | This is an item that may belong in the register, not the governing register rule. |
| K2 | F-028 | СП 76.13330.2016 | NOT_VERIFIED | — | UNSUPPORTED | This is an item that may belong in the register, not the governing register rule. |
| K2 | F-028 | ГОСТ 32396-2021 | NOT_VERIFIED | — | UNSUPPORTED | Its own references section does not establish the project-document requirement. |
| K4 | F-001 | ГОСТ Р 21.101-2020 | NOT_VERIFIED | — | PARTIALLY_SUPPORTED | The standard governs document naming, but no exact clause was strong enough. |
| K4 | F-002 | ГОСТ Р 21.101-2020 | NOT_VERIFIED | — | PARTIALLY_SUPPORTED | Cross-document references are in scope, but no unique supporting provision was found. |
| K4 | F-003 | СП 256.1325800.2016 | NOT_VERIFIED | — | PARTIALLY_SUPPORTED | Load calculation provisions are relevant, but none uniquely proves the internally inconsistent totals. |
| K4 | F-004 | ПУЭ 7 | VERIFIED | 1.5.17 | SUPPORTED | The provision directly supplies the threshold contradicted by the table conclusion. |
| K4 | F-005 | ГОСТ 21.110-2013 | NOT_VERIFIED | — | PARTIALLY_SUPPORTED | The document is relevant to specifications, but no exact quantity-reconciliation clause was established. |
| K4 | F-006 | ГОСТ Р 21.101-2020 | NOT_VERIFIED | — | PARTIALLY_SUPPORTED | Drawing/reference consistency is in scope; candidate clauses remained insufficient or competing. |
| K4 | F-007 | ГОСТ Р 21.101-2020 | NOT_VERIFIED | — | UNSUPPORTED | No provision specific enough to establish the proposed air-curtain identifier was found. |
| K4 | F-008 | ФЗ 384-ФЗ | NOT_VERIFIED | — | UNSUPPORTED | The federal law does not establish the disputed ГОСТ designation. |
| K4 | F-019 | СП 256.1325800.2016 | AMBIGUOUS | — | PARTIALLY_SUPPORTED | Several cable-section provisions are close; the exact applicable one is not unique. |
| K4 | F-023 | СП 256.1325800.2016 | VERIFIED | 15.25 | PARTIALLY_SUPPORTED | It requires a fire-rated penetration, but does not choose between the conflicting listed materials. |
| K4 | F-051 | ПУЭ 7 | VERIFIED | 1.5.17 | PARTIALLY_SUPPORTED | It establishes the current-ratio criteria, not the separate assertion that only one transformer ratio may be listed. |
| K4 | F-052 | ПУЭ 7 | VERIFIED | 1.5.17 | SUPPORTED | It directly confirms that 40% of nominal meter current is the relevant maximum-load criterion. |
| K4 | F-053 | ГОСТ Р 21.101-2020 | VERIFIED | 4.3.1 | SUPPORTED | It requires the referenced and attached documents register in general data. |
| K4 | F-054 | ГОСТ Р 21.101-2020 | NOT_VERIFIED | — | PARTIALLY_SUPPORTED | The standard is relevant to document references, but no exact clause proves this identifier mismatch. |

Manual totals: **SUPPORTED 5, PARTIALLY_SUPPORTED 12, UNSUPPORTED 11**.
Among the eight references published as `VERIFIED`: **SUPPORTED 5,
PARTIALLY_SUPPORTED 3, UNSUPPORTED 0**. Therefore the acceptance failure rule
“UNSUPPORTED published as VERIFIED” was not triggered.

## Performance

| Document | Resolver total | Mean/reference | P95/reference | Mean/finding | P95/finding | AI calls |
|---|---:|---:|---:|---:|---:|---:|
| EM-K2 | 1783.7 ms | 127.1 ms | 434.8 ms | 63.6 ms | 435.0 ms | 0 |
| EM-K4 | 2007.5 ms | 143.0 ms | 435.4 ms | 37.1 ms | 316.1 ms | 0 |

`norm_resolver_report.json` also contains an aggregate for every designation.
The first document access includes the one-time JSONL index load; subsequent
lookups reuse the in-process clause map.

## Regressions and historical scan

- VK-4-2-RD regressions cover `ГОСТ 21.110-2013` vs
  `ГОСТ 21.601-2011`, and `СП 29` vs `СП 30`. A single legacy quote cannot
  validate both designations; every published quote must return the same
  `matched_code` as its reference.
- The read-only historical scan inspected **1,922** existing
  `03_findings.json` artifacts and **54,115** quoted-reference occurrences.
  It found **4,811 unique quote-digest groups** attached to more than one
  normalized designation. These are remediation candidates (the scan includes
  repeated runs/backups and legacy multi-norm fields), not 4,811 automatically
  proven bad findings. No historical data was changed. Full scan output:
  `/tmp/norm-resolver-acceptance.WbgGq5/historical_duplicate_quote_scan.json`.

## Decision

The acceptance succeeds: both per-document VERIFIED counts exceed baseline,
all published clauses and quotes come from their designated vault document,
native verification is 8/8, and the manual audit found zero unsupported
references among published `VERIFIED` references. The unresolved and ambiguous
references remain visible as such rather than being filled speculatively.
