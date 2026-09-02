# Function Lineage Matcher v1 — research result

Status: complete isolated research run. No production code, UI, mappings, source
runs or Candidate Generator v4 status changed. No deploy and no push.

## Result

`DOCUMENT_LINK` and `FUNCTIONAL_ANALOGUE` are independent namespaces. IOS 2.1
keeps documentary 17→7 and 18→8 while functional selection is evaluated against
the actual extracted graphic fragments.

- LEFT 17: DOCUMENT_LINK `[7]`; FUNCTIONAL `CONTINUED_1_TO_1 [27]`.
- LEFT 18: DOCUMENT_LINK `[8]`; FUNCTIONAL `CONTINUED_1_TO_1 [24]`.
- LEFT 19: `NEED_MORE_EVIDENCE []`; RIGHT 9 remains documentary and its corpus-1 extraction
  is not used as functional identity.
- LEFT 20: `FUNCTION_DISTRIBUTED [26, 28, 29]`.

Stable unique relations: 1→1 `8`, 1→N
`0`, N→1 `0`, distributed
`2`. Unsupported auto lineages:
`0`; unresolved tasks: `26`.

Function-level capacity avoided `3`
false physical-sheet conflicts. A RIGHT page can participate in several lineages
only through distinct fragment IDs; duplicate ownership of one fragment is rejected.

TEXT used `1327912` tokens in `18` calls, a
`81.8%` reduction from the 7,315,563-token v4
repeat. Vision was not triggered because the critical TEXT lineages did not need
it; remaining unresolved extraction/topology cases stay `NEED_MORE_EVIDENCE`.

## Acceptance gates

- PASS — `relation_namespaces_separate`
- PASS — `unsupported_auto_lineages_zero`
- PASS — `ios21_17_18_document_and_function_coexist`
- PASS — `ios21_critical_functional_results`
- PASS — `ios21_left20_distributed_stable`
- PASS — `left20_false_sheet_conflict_removed`
- PASS — `at_least_one_complex_stable`
- PASS — `tokens_below_previous`
- PASS — `production_sources_unchanged`

## Verdict

**A** — architecture confirmed; controlled production integration may be designed.

Artifacts in this directory are traceable research outputs. `manual_audit.json`
contains SUPPORTED/PARTIAL/UNSUPPORTED classification for every stable proposal;
`derived_sheet_map.json` is derived only after the lineage decisions.
