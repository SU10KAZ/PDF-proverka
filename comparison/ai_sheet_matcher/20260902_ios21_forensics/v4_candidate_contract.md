# Candidate Model v4 — functional continuity contract

Status: design only. This document does not change `production-sheet-matcher.v3`, thresholds, run artifacts, UI or deployment.

## 1. Unit of retrieval

The v4 anchor is a `LEFT_FUNCTION`, not a physical PDF page. A function may be evidenced by one or several LEFT sheets and may contain independently traceable components. Physical pages and graphic sheet numbers are different identifiers and must never be substituted for each other.

```text
LEFT_FUNCTION
  ├─ RIGHT_PAGE
  ├─ RIGHT_PAGE_GROUP
  ├─ NO_ANALOG
  └─ NEED_MORE_EVIDENCE
```

The contract must express:

- `MATCH_1_TO_1` — one LEFT function continues on one RIGHT page;
- `SPLIT_1_TO_N` — one indivisible LEFT function is split across N RIGHT pages;
- `MERGED_N_TO_1` — several LEFT functions are consolidated on one RIGHT page;
- `FUNCTION_DISTRIBUTED` — function components continue across a bounded, possibly non-contiguous group;
- `NEW_FUNCTION` — a RIGHT function has no predecessor after corpus-wide coverage proof;
- `REMOVED_FUNCTION` — a LEFT function has no successor after corpus-wide coverage proof.

`REMOVED_SHEET` is not `REMOVED_FUNCTION`; `NEW_SHEET` is not `NEW_FUNCTION`. Sheet-level changes are observations. Function-level absence requires explicit coverage evidence.

## 2. Identity and passport prerequisites

Every referenced sheet must have a `SheetPassport` and every anchor must have a `FunctionPassport`.

`SheetPassport` required fields:

```json
{
  "document_version_id": "v001",
  "side": "RIGHT",
  "physical_pdf_page": 28,
  "graphic_sheet_number": "7",
  "page_kind": "GRAPHIC_SHEET",
  "title": "Принципиальная схема систем пожаротушения. (Корпус 1).",
  "type": ["SCHEME", "FIRE_WATER_RISER_AND_PUMP_SCHEME"],
  "corpus": ["Корпус 1"],
  "zone": ["sections 1.1-1.3", "Насосная АПТ-ВПВ"],
  "floor": ["-02..6"],
  "systems": ["В2.1", "В2.2"],
  "consumers": ["fire cocks"],
  "equipment": ["zone-1 fire booster", "zone-2 fire booster"],
  "source": ["common input/meter chain"],
  "receivers": ["корпус 1 fire-water risers"],
  "related_sheets": [{"physical_pdf_page": 26, "relation": "SOURCE"}],
  "evidence_refs": ["ev_right_pdf_p28_stamp", "ev_right_pdf_p28_raster"]
}
```

`FunctionPassport` required fields:

```json
{
  "left_function_id": "lf_pe336037597_p20_pump_station",
  "source_sheet_refs": [{"physical_pdf_page": 20, "graphic_sheet_number": "5"}],
  "function_class": "WATER_SUPPLY_AND_PRESSURE_BOOSTING",
  "components": [
    {"component_id": "incoming_metering", "role": "SOURCE_AND_METER"},
    {"component_id": "domestic_boosting", "role": "DOMESTIC_PRESSURE_BOOST"},
    {"component_id": "fire_boosting", "role": "FIRE_PRESSURE_BOOST"},
    {"component_id": "apt_handoff", "role": "EXTERNAL_DOCUMENT_HANDOFF"}
  ],
  "corpus_scope": ["complex-wide"],
  "systems": ["В1", "ВПВ", "АПТ hand-off"],
  "source": ["two incoming water lines"],
  "receivers": ["domestic network", "fire-water network", "ИОС2.2"],
  "evidence_refs": ["ev_left_pdf_p20_raster", "ev_left_contents_p3"]
}
```

If `graphic_sheet_number`, title or page kind cannot be read from the OCR metadata, the resolver must attempt the PDF text layer and bounded title-block raster evidence. A missing title-block parse is an extraction state, never proof that no analogue exists.

## 3. Candidate object

```json
{
  "candidate_id": "fcand_6294159aac7851a636dd",
  "pair_id": "pe336037597",
  "direction": "LEFT_TO_RIGHT",
  "left_function_ids": ["lf_pe336037597_p20_pump_station"],
  "left_sheet_refs": [
    {"physical_pdf_page": 20, "graphic_sheet_number": "5"}
  ],
  "target_kind": "RIGHT_PAGE_GROUP",
  "decision_type": "FUNCTION_DISTRIBUTED",
  "right_sheet_refs": [
    {"physical_pdf_page": 26, "graphic_sheet_number": "1"},
    {"physical_pdf_page": 28, "graphic_sheet_number": "7"},
    {"physical_pdf_page": 29, "graphic_sheet_number": "6"}
  ],
  "component_coverage": [
    {"component_id": "domestic_boosting", "right_physical_pages": [26], "state": "SUPPORTED"},
    {"component_id": "fire_boosting", "right_physical_pages": [28], "state": "SUPPORTED"},
    {"component_id": "incoming_metering", "right_physical_pages": [29], "state": "SUPPORTED"},
    {"component_id": "apt_handoff", "right_physical_pages": [26, 28], "related_document": "ИОС2.2", "state": "BOUNDARY_ONLY"}
  ],
  "coverage": {
    "required_component_count": 3,
    "supported_component_count": 3,
    "uncovered_component_ids": [],
    "contradicted_component_ids": []
  },
  "retrieval_channels": ["CONTENTS", "FUNCTION", "EQUIPMENT_ROLE", "TOPOLOGY", "CROSS_SHEET_REFERENCE"],
  "rank_evidence": {
    "per_page_ranks": {"26": 4, "28": 15, "29": 7},
    "group_rank": 1,
    "group_score": 0.91
  },
  "authority": {
    "saved_decision_ids": [],
    "conflict_state": "NONE"
  },
  "evidence_refs": [
    "ev_right_contents_p5_sheet5_annulled",
    "ev_right_change_p8_domestic_pump_to_sheet1",
    "ev_right_change_p8_fire_pump_to_sheet7",
    "ev_right_change_p10_input_to_sheets1_6",
    "ev_right_pdf_p26_raster",
    "ev_right_pdf_p28_raster",
    "ev_right_pdf_p29_raster"
  ],
  "generator_version": "candidate-model.v4",
  "input_signature": "sha256:..."
}
```

The example `group_score` is illustrative; no v4 score was computed in this spike. The group membership and component coverage are evidence-backed.

## 4. Deterministic IDs and boundedness

`candidate_id` is a stable digest of:

```text
pair_id
+ exact document version IDs and content hashes
+ sorted left_function_ids
+ decision_type
+ sorted (RIGHT physical page, graphic sheet number) pairs
+ sorted component-to-page assignments
+ sorted evidence content hashes
+ generator version
```

The bounded payload given to any selector contains only prebuilt candidate IDs. It must also include `NO_ANALOG` and `NEED_MORE_EVIDENCE`. The selector may not invent pages, groups, component mappings or evidence.

A candidate set is bounded by explicit limits per retrieval channel and group size, not by one global top-K. Recommended construction:

1. resolve sheet identities from contents, change register, PDF text layer and title-block raster;
2. retrieve a bounded union from stamp/contents, function, object-zone, entity/equipment-role and topology channels;
3. expand through bounded cross-sheet references and shared source/receiver nodes;
4. compose groups from component coverage, allowing non-contiguous physical pages;
5. deduplicate by stable ID and rank individual and group candidates separately.

No group may be formed solely because pages are adjacent. No valid group may be rejected solely because its pages are non-contiguous.

## 5. Decision validation

### MATCH_1_TO_1

- exactly one LEFT function and one RIGHT page;
- corpus/zone scope compatible or explicitly transformed;
- function role supported by at least one primary signal and one independent corroborating signal.

### SPLIT_1_TO_N

- exactly one LEFT function and at least two RIGHT pages;
- each RIGHT page contributes a distinct required component;
- union covers the LEFT function; redundant alternatives are not a split.

### MERGED_N_TO_1

- at least two LEFT functions and exactly one RIGHT page;
- the RIGHT page explicitly covers every LEFT function or a deterministic scope container proves coverage;
- no LEFT member may be added only because it shares vocabulary.

### FUNCTION_DISTRIBUTED

- one or more LEFT functions and at least two RIGHT pages;
- component coverage table is mandatory;
- non-contiguous pages are allowed;
- source/receiver or explicit change-register/contents evidence must connect the group;
- any required component outside the bounded pool makes the group unavailable and produces `NEED_MORE_EVIDENCE`, not a partial match.

### REMOVED_FUNCTION / NO_ANALOG

`NO_ANALOG` for a LEFT function materializes as `REMOVED_FUNCTION` only when:

- all identity/function/zone/topology retrieval channels completed;
- contents/change register and related-document references were checked;
- no supported individual or group candidate remains;
- the absence proof is recorded in `exhaustive_search_evidence`;
- no saved engineer decision asserts continuity.

An annulled or missing sheet alone is insufficient.

### NEW_FUNCTION

`NEW_FUNCTION` is a RIGHT-centric coverage result. After all LEFT functions are resolved, every uncovered RIGHT function receives a reverse audit task:

```json
{
  "orientation": "RIGHT_FUNCTION_AUDIT",
  "right_function_id": "rf_...",
  "candidate_left_function_ids": [],
  "decision_type": "NEW_FUNCTION",
  "exhaustive_search_evidence": ["ev_..."]
}
```

A new physical sheet is not enough; the function must be absent from all LEFT function passports and groups.

### NEED_MORE_EVIDENCE

Mandatory when extraction failed, a required group member fell outside a retrieval channel's bound, related documents were unavailable, function coverage is incomplete, or authority and functional evidence conflict.

## 6. Authority handling

Saved `user_accepted` mappings remain authoritative for materialization, but authority must not rewrite candidate-recall facts.

If an authoritative target is absent from the bounded set:

- record `AUTHORITY_TARGET_NOT_RETRIEVED`;
- add no invented option;
- block conflicting automatic materialization;
- return `NEED_MORE_EVIDENCE` for the function task;
- surface page-kind and functional-evidence conflicts for audit.

This preserves the safety behavior observed for `17→7`, `18→8`, `19→9` while making the retrieval failure measurable.

## 7. Global consistency

Global assignment operates on function and group candidates, not a one-page/one-page matrix.

- A RIGHT page may support multiple LEFT components only when the candidate declares compatible component coverage.
- A function group is atomic during assignment; its members cannot be silently stolen by unrelated page-level matches.
- Competing candidates remain visible as explicit conflicts.
- `NEW_FUNCTION` and `REMOVED_FUNCTION` are evaluated after coverage, not inferred before assignment.

The ИОС 2.1 evidence demonstrates why: current v3 assigns RIGHT 26 to LEFT 19, RIGHT 30 to LEFT 17 and RIGHT 29 to LEFT 51, even though the functional map needs those pages in corpus/pump/meter groups.

## 8. Failure telemetry

Every rejected or missing reference must emit:

```json
{
  "left_function_id": "lf_...",
  "expected_target": {"right_physical_pages": [26, 28]},
  "top5_presence": {"26": false, "28": false},
  "top10_presence": {"26": true, "28": false},
  "full_corpus_ranks": {"26": 6, "28": 17},
  "primary_failure_class": "GROUP_CANDIDATE_MISSING",
  "contributing_failure_classes": [
    "STAMP_EXTRACTION_MISS",
    "FUNCTION_EXTRACTION_MISS",
    "SEARCH_WINDOW_MISS"
  ],
  "missing_signals": ["graphic sheet identity", "combined-to-separated fire role"],
  "evidence_refs": ["ev_..."]
}
```

Allowed failure classes are the forensic taxonomy from the task: `STAMP_EXTRACTION_MISS`, `TITLE_EXTRACTION_MISS`, `FUNCTION_EXTRACTION_MISS`, `OBJECT_ZONE_EXTRACTION_MISS`, `ENTITY_EXTRACTION_MISS`, `TOPOLOGY_EXTRACTION_MISS`, `SEARCH_WINDOW_MISS`, `CANDIDATE_RANKING_MISS`, `GLOBAL_ASSIGNMENT_DISPLACEMENT`, `GROUP_CANDIDATE_MISSING`, `FUNCTION_DISTRIBUTION_MISSING`, and `OTHER`.
