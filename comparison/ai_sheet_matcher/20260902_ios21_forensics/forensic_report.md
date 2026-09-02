# ИОС 2.1 — forensic audit of functional candidate failures

## Verdict

The functional pages are recoverable, but the current candidate model loses their identity and cannot express their cardinality.

- RIGHT physical pages `24/25/26/27/28/29/30` are graphic sheets `3/4/1/2/7/6/8`, not sheets 24-30. The order is non-monotonic.
- The RIGHT OCR/index records for all seven pages have `sheet_number=null`, `title=null`, and no `sheet_identity`, although the title blocks are readable in the PDF text layer and raster.
- Functional candidates for A sheets 1-4 are pushed to quick ranks 6-9 or outside top-10 by generic vocabulary and physical-page proximity. A sheet 6 is retrieved strongly but loses ownership during global assignment.
- B sheets 7 and 8 are new sheets but not new functions. They isolate fire-water functions that were combined with water-supply sheets in LEFT.
- A sheet 5 is an annulled sheet, not a removed function. Its function is evidence-backed as distributed across RIGHT physical pages `[26, 28, 29]` (graphic sheets `[1, 7, 6]`).
- The bounded AI could not choose saved engineer targets `17→7`, `18→8`, or `19→9`: none existed in its top-10/options. The AI choices `17→27`, `18→24`, `19→25` were available at ranks 8, 6 and 9.

No production algorithm, threshold, source run, deployment or model output was changed or created.

## Scope and method

Audited pair `pe336037597`, run `prun_68a933afd5b2bb083189d3c6`, with research spike commit `41d43625`.

Evidence was read from:

- both original v001 PDFs and their saved `document.md`/OCR artifacts;
- LEFT contents page 3;
- RIGHT contents page 5 and change register pages 7-10;
- saved `sheet_links.json` (`user_accepted` authority);
- frozen `production-sheet-matcher.v3` top-5/deep candidates;
- saved read-only top-10 regeneration from the existing research spike;
- saved bounded AI decisions and option catalog.

Forensic full-corpus ranks were computed read-only with the same deterministic v3 pass-1 scorer over the 63 RIGHT pages. This was not a model call, did not persist a new matcher run, and did not change production artifacts.

## Real page resolution

### LEFT

| Graphic sheet | Physical PDF page | Title / primary function | Corpus / zone / floor | Systems | Source → receivers |
|---:|---:|---|---|---|---|
| 1 | 16 | Water-supply risers, combined domestic/fire component | Корпус 1; parking/technical levels through floors 1-6 | В1 combined, Т3, Т4 | shared pump/meter chain → apartments, МОП, fire cocks |
| 2 | 17 | Water-supply risers, combined domestic/fire component | Корпус 2; -02..6 | В1 combined, Т3, Т4 | parking header → apartments, МОП, fire cocks |
| 3 | 18 | Water-supply risers | Корпус 3; -02..5 | В1/Т1, Т3, Т4 | basement header → apartments/service rooms/fire points |
| 4 | 19 | Domestic/hot/fire-water risers on one sheet | Корпус 4; -02..16 | В1, В2, Т3, Т4 | parking/ИТП → apartments and fire cocks |
| 5 | 20 | Pump-station scheme: input, metering, domestic boost, fire boost, APT hand-off | complex-wide underground pump/meter rooms | В1 combined, ВПВ, APT boundary | two DN150 inputs → domestic, fire and ИОС2.2 branches |
| 6 | 21 | Detailed water-meter node and room plan | meter room; pipe axis -2.51 | В1 input/metering | external input → sheet-5 pump/distribution chain |

### RIGHT

| Graphic sheet | Physical PDF page | State | Title / primary function | Corpus / zone / floor | Systems | Source → receivers |
|---:|---:|---|---|---|---|---|
| 1 | 26 | present | Water-supply risers plus domestic/water-treatment pumps | Корпус 1, sections 1.1-1.3, pump rooms; -02..6 | zoned В1, Т3/Т4, improved water | common input → domestic risers; links to fire/APT branches |
| 2 | 27 | present | Zoned water-supply risers | Корпус 2, sections 2.1-2.2; -02..6 | В1/Т3/Т4, improved and fire-related branches | parking headers → корпус 2 consumers |
| 3 | 24 | present | Zoned water-supply risers | Корпуса 3 and 3.1; -02..5 | domestic/hot/improved/fire branches | basement headers → корпуса 3/3.1 consumers |
| 4 | 25 | present | Domestic/hot/improved-water risers; fire part separated | Корпус 4; -02..16 | В1, Т3/Т4, improved water | technical headers → domestic consumers |
| 5 | — | **annulled** | Old pump-station sheet container has no current physical page | complex-wide | — | functions continue on sheets 1, 6, 7 |
| 6 | 29 | present | Revised water-meter node and room plan | meter room; pipe axis -3.49 | В1 input/metering | PE225 inputs → downstream pump/distribution chain |
| 7 | 28 | present | Fire-water risers and zone fire/jockey pumps | Корпус 1 and fire-pump room; -02..6 | В2.1, В2.2 | common input → корпус 1 and zone fire headers |
| 8 | 30 | present | Fire-water risers | Корпус 4; -02..16 | В2.1, В2.2 | pumps/headers on sheet 7 → корпус 4 fire cocks |

Full Sheet and Function Passports, including equipment, consumers and related sheets, are in `reference_map.json`.

## Reference relations and candidate availability

`T5` is the frozen production quick top-5. `T10` is the saved deterministic top-10 regeneration. `D5`/`D10` are the corresponding deep sets. `full#` is the forensic rank across all RIGHT pages. A group is present only if a prebuilt option contains all required pages.

| Reference relation | LEFT physical page | RIGHT physical page/group | Engineer mapping | Current top-5 | Current top-10 | Correct candidate present? | Rank | Failure class | Missing signal | Recommended fix |
|---|---:|---|---|---|---|---|---|---|---|---|
| A1 → B1; complete old combined function → B1+B7 | 16 | 26 + 28 | none | 16,15,17,30,18 | 16,15,17,30,18,26,13,19,11,24 | p26: T10 yes; p28: no; group: no | p26 T10#6/D10#3; p28 full#17 | `CANDIDATE_RANKING_MISS`; `SEARCH_WINDOW_MISS`; `GROUP_CANDIDATE_MISSING` | sheet/title/stamp; корпус 1; combined В1 → separated В2 role; topology | multi-channel retrieval plus function-component group composition |
| A2 → B2 | 17 | 27 | **17→7** authoritative | 17,30,16,18,15 | 17,30,16,18,15,19,20,27,11,4 | T5 no; T10/D10 yes | T10#8/D10#2 | `CANDIDATE_RANKING_MISS` | sheet 2; корпус/sections; system aliases; title | extract schematic stamp/title and object scope |
| A3 → B3 | 18 | 24 | **18→8** authoritative | 18,26,17,19,30 | 18,26,17,19,30,24,27,53,16,20 | T5 no; T10/D10 yes | T10#6/D10#3 | `CANDIDATE_RANKING_MISS` | sheet 3; корпус 3 with allowed 3.1 scope expansion | identity/zone channel plus scope-containment evidence |
| A4 → B4; complete old combined function → B4+B8 | 19 | 25 + 30 | **19→9** authoritative | 19,30,18,20,26 | 19,30,18,20,26,17,21,24,25,27 | both pages individually yes; group no | p25 T10#9/D10#5; p30 T5#2/D5#1 | `GROUP_CANDIDATE_MISSING`; `GLOBAL_ASSIGNMENT_DISPLACEMENT` | non-contiguous group; domestic/fire component coverage; sheet/title | compose groups by function, make group atomic in assignment |
| A5 → distributed pump/meter function | 20 | 26 + 28 + 29 | **20→10** authoritative | 20,19,21,26,18 | 20,19,21,26,18,22,29,24,25,30 | p26/p29 yes; p28 no; group no | p26 T5#4/D5#1; p28 full#15; p29 T10#7/D10#4 | `SEARCH_WINDOW_MISS`; `FUNCTION_DISTRIBUTION_MISSING`; `GROUP_CANDIDATE_MISSING` | vendor-independent pump role; source/receiver graph; cross-sheet references | retrieve by equipment role and build `[26,28,29]` component group |
| A6 → B6 | 21 | 29 | none | 21,29,20,22,19 | 21,29,20,22,19,23,18,11,4,24 | T5/T10/D5/D10 yes | T5#2/D5#1 | `GLOBAL_ASSIGNMENT_DISPLACEMENT` | exclusive ownership of meter function | assign functions/groups; allow compatible component reuse |

## Signal evidence for functional edges

All values below are from the unmodified v3 scorer. `—` means not extracted. Every RIGHT graphic page in this table had `stamp_relation=UNKNOWN`, `title=—`, and `graphic=—`.

| LEFT→RIGHT | Function | Entities | Topology | Type | Deep score/status | Result |
|---|---:|---:|---:|---:|---|---|
| 16→26 | .278 | 0 | .083 | 1.0 | .187 / NO_MATCH | rank 6; only in T10 |
| 16→28 | .056 | 0 | .031 | 1.0 | .101 / NO_MATCH | full rank 17; outside T10 |
| 17→27 | .176 | 0 | .146 | 1.0 | .173 / NO_MATCH | rank 8; only in T10 |
| 18→24 | .118 | — | .229 | 1.0 | .258 / NO_MATCH | rank 6; only in T10 |
| 19→25 | .222 | 0 | .043 | 1.0 | .158 / NO_MATCH | rank 9; only in T10 |
| 19→30 | .444 | 1.0 | .057 | 1.0 | .537 / POSSIBLE | strong edge, wrong global owner |
| 20→26 | .400 | 0 | .052 | 1.0 | .217 / NO_MATCH | T5 present, but component/group semantics missing |
| 20→28 | .133 | 0 | .031 | 1.0 | .126 / NO_MATCH | full rank 15; outside T10 |
| 20→29 | .200 | .100 | .021 | 1.0 | .174 / NO_MATCH | rank 7; only in T10 |
| 21→29 | .647 | .370 | .469 | 1.0 | .530 / POSSIBLE | strong edge, wrong global owner |

The apparently perfect quick candidates at the same physical page (`16→16`, `17→17`, etc.) are not semantic matches. When substantive fields are absent on those front-matter pages, physical-page proximity is the only available signal and is normalized to a score of 1.0. This crowds out the actual graphics whose physical positions changed.

## Can the bounded AI choose the engineer-accepted pages?

No.

| Saved mapping | T5 | T10 | D5/D10 | Full-corpus quick rank | Prebuilt option | RIGHT page kind |
|---|---|---|---|---:|---|---|
| 17→7 | absent | absent | absent | 21 | absent | change register |
| 18→8 | absent | absent | absent | 25 | absent | change register |
| 19→9 | absent | absent | absent | 23 | absent | change register |
| 20→10 | absent | absent | absent | 21 | absent | change register / sheet-5 annulment record |

The model contract accepted only prebuilt option IDs. Therefore it was technically impossible for any pass to emit those RIGHT pages. The saved authority gate still did its job: it blocked materialization of conflicting AI choices. Authority and functional equivalence must remain separate facts; the functional evidence identifies RIGHT 27/24/25 as the graphic analogues while the saved mappings remain authoritative state.

## Root causes

### 1. Stamp and title extraction

The raster and PDF text layer explicitly show `.ГЧ`, sheet number and full title on RIGHT graphic pages. The saved OCR stamp metadata instead reports `.ТО` and blank `Sheet`/`Name`. `extract_sheet_identities` returns no identity because its proven title grammar covers plan/roof/section/facade types, not these principle schematics.

Impact: ten of ten required functional edges have no stamp or title signal. Graphic sheet identity cannot rescue the non-monotonic order `3,4,1,2,7,6,8`.

Classes: `STAMP_EXTRACTION_MISS`, `TITLE_EXTRACTION_MISS`.

### 2. Function, object and entity normalization

Corpus/section names and functional equipment are present in source text, but are not first-class matcher fields. Old combined `В1` and new zoned `В1.1/В1.2` plus `В2.1/В2.2` are compared as literal tokens. Replaced ALPHA pumps and Wilo pumps have the same role but no shared equipment code, producing `entities=0` for the key pump edge `20→28`.

Classes: `FUNCTION_EXTRACTION_MISS`, `OBJECT_ZONE_EXTRACTION_MISS`, `ENTITY_EXTRACTION_MISS`.

### 3. Topology extraction

Source→pump→header→riser→consumer continuity and cross-sheet references are not represented as a comparable graph. Eight of ten required edges have topology below .20 even though the raster/change register explains continuity.

Class: `TOPOLOGY_EXTRACTION_MISS`.

### 4. Search window and ranking

Two functional edges fall outside top-10: `16→28` at full rank 17 and `20→28` at full rank 15. Five more required functional edges appear only at ranks 6-9. Four saved authority targets are also outside top-10 at ranks 21-25.

Classes: `SEARCH_WINDOW_MISS`, `CANDIDATE_RANKING_MISS`.

### 5. Group construction

The research option builder makes single-LEFT split groups only from contiguous physical RIGHT pages. Its `FUNCTION_DISTRIBUTED` options are limited to adjacent LEFT pairs. It cannot construct any required non-contiguous group:

- `[16]→[26,28]`;
- `[19]→[25,30]`;
- `[20]→[26,28,29]`.

Classes: `GROUP_CANDIDATE_MISSING`, `FUNCTION_DISTRIBUTION_MISSING`.

### 6. Global assignment

The page-to-page assignment optimizes a one-to-one matrix and gives key pages to the wrong LEFT page:

- RIGHT 26 is assigned to LEFT 19 instead of participating in корпус-1/pump continuity;
- RIGHT 30 is assigned to LEFT 17 instead of the corpus-4 group anchored at LEFT 19;
- RIGHT 29 is assigned to unrelated LEFT 51 instead of the meter/pump functions at LEFT 20/21.

Class: `GLOBAL_ASSIGNMENT_DISPLACEMENT`.

## A sheet 5 — annulled sheet, continuing function

RIGHT contents page 5 marks sheet 5 `Аннул.` and change-register page 10 states `Лист 5 – Аннулирован`. The same change register provides positive continuity evidence:

| Old component on LEFT physical 20 | RIGHT destination | Evidence |
|---|---|---|
| Incoming/common metering | physical 29 / sheet 6 | changed input diameter/location is assigned to sheets 1 and 6; page 29 contains PE225 input and ВСХНд-65 node |
| Domestic pressure boosting | physical 26 / sheet 1 | change register puts revised ХПВ zone pumps on sheet 1; raster lists Wilo COR domestic and water-treatment pumps |
| Internal-fire pressure boosting | physical 28 / sheet 7 | change register puts revised ВПВ pumps/jockey pumps on sheet 7; raster contains both fire zones |
| APT | boundary on physical 26/28; detailed in ИОС2.2 | old and new sheets both hand this branch to the related document; it is not a standalone ИОС2.1 replacement page |

The equipment appendices on physical pages `32,34,36,38,40,42,44,46` corroborate pump selection but are supporting pages, not group members.

It is therefore deterministic to build the bounded forensic candidate:

```text
FUNCTION_DISTRIBUTED(fcand_6294159aac7851a636dd)
LEFT physical [20] / graphic [5]
RIGHT physical [26,28,29] / graphic [1,7,6]
```

This group is supported by contents + change register + three graphic rasters. It is not available under the existing bounded option rules because page 28 is outside top-10 and the group is non-contiguous.

## Counts

Functional-reference lens:

- candidate generation/search-window failures: **2 required edges** (`16→28`, `20→28`);
- ranking failures: **5 required edges** present only at ranks 6-9;
- group-candidate failures: **3 groups**;
- extraction-affected edges: **10/10** (non-exclusive: stamp 10, title 10, weak/missing function 7, object/zone 6, entity 8, topology 8);
- global-assignment displacement: **3 key RIGHT pages**.

Authority-availability lens:

- saved engineer mapping failures: **4/4 edges** absent from top-10;
- bounded edge omissions including the two functional misses: **6**.

These lenses overlap; the numbers must not be summed.

## Highest expected Recall gain

1. Extract graphic-sheet number, title and `.ГЧ` identity for schematic title blocks.
2. Resolve page order from contents/change register and preserve physical page separately from graphic sheet number.
3. Normalize corpus, section, zone and floor as first-class fields.
4. Canonicalize old combined versus new zoned system roles (`В1` ↔ `В1.x` + `В2.x`).
5. Normalize equipment purpose independently of vendor/model replacement.
6. Build source→equipment→header→receiver topology and ingest explicit cross-sheet references.
7. Retrieve a bounded union of identity, function, zone, entity-role and topology channels.
8. Compose non-contiguous groups from component coverage, not physical adjacency.
9. Perform assignment on function/group candidates and keep groups atomic.
10. Surface authority-versus-function/page-kind conflict while preserving the saved authority gate.

The proposed v4 contract and validation rules are in `v4_candidate_contract.md`.

## Limitations

- The supplied functional map remains a hypothesis; this audit strengthens it with document evidence but does not convert it into an engineer approval.
- Saved `user_accepted` links are treated as authoritative for materialization even where the linked RIGHT page is a change-register page.
- Full-corpus ranks are diagnostic evaluations of the unchanged deterministic scorer, not a proposed threshold and not a production run.
- No claim of `NEW_FUNCTION` or `REMOVED_FUNCTION` is made from sheet presence alone.
