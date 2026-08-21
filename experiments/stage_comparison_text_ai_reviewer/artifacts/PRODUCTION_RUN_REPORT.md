# Stage 4 production run: AI reviewer П ↔ РД

## Scope and source

- Run date: 2026-08-21.
- Project: `272_Sadovnicheskaya_76_Balchug_Esteyt` (Balchug Estate).
- Session: `121d764109184c13`.
- Pair: `p570d156f57`.
- П: `13АВ-РД-АР0.1-ПА_V2.pdf`.
- РД: `13АВ-РД-АР0.1-ПА_V3.pdf`.
- Model: `codex/gpt-5.6-luna`, deterministic hint, reasoning effort `medium`.
- Reviewer source signature: `5f12da5cdccf57c4352cc05c5cbece77afdd0c38aa369ea4ec443f23b7bb0b70`.

Only text source fragments and deterministic preliminary decisions were sent to
the model. No PDF, raster, images, vector graphics, norms, findings, or other
disciplines were sent. `sheet_links.json` was read as input and was not changed.

## Artifact separation

The deterministic artifacts remained the RAW evidence:

| Artifact | SHA-256 after the run |
|---|---|
| `text_comparison.json` | `d60ca8c316b7f1840cca19c7abf83c90e50555849f8e4d28266098287ee6e169` |
| `text_exclusions.json` | `082057e56bc57800d2c275d705012e9396ff80efdc7ea6fe1066dc0a362683d9` |
| `text_differences.json` | `0cf97803e1c1410146044f21051279e06088317bf1ecd37dda3ff26f8ffd4e79` |
| `sheet_links.json` | `700d79391c1d092b098add2e90beadd854e11b74acc1f40007275f972d048baf` |

The new artifacts are separate:

| Artifact | SHA-256 |
|---|---|
| `text_ai_review.json` | `a2bb1f1498ea92563b68160bbeaa646a981228b85854c47de97b5a370ae0d159` |
| `text_final_comparison.json` | `f2ef4fba9d138753948bf414cee768fb32e427717a3ca4f617c619b125062ab8` |

## Before and after

Deterministic counts are classification records from Stages 2 and 3. AI counts
are final semantic decisions; they are not expected to have the same total
because the reviewer supports 1→N and N→1 grouping.

| Status | Deterministic | After AI |
|---|---:|---:|
| SAME | 296 | 193 |
| MOVED | 16 | 16 |
| CHANGED | 38 | 41 |
| REMOVED | 70 | 59 |
| ADDED | 174 | 32 |
| AMBIGUOUS / UNCERTAIN | 8 | 189 |

All 11 sheet groups completed; no group used deterministic fallback. The final
artifact contains 530 semantic decisions covering 1,024 source fragments:
328 preliminary decisions were confirmed without a status change, 202 were
reclassified, and 189 ended in conservative `UNCERTAIN`.

Transitions:

| Transition | Count |
|---|---:|
| SAME → SAME | 191 |
| SAME → CHANGED | 10 |
| SAME → UNCERTAIN | 135 |
| MOVED → MOVED | 16 |
| CHANGED → CHANGED | 30 |
| CHANGED → UNCERTAIN | 8 |
| REMOVED → REMOVED | 59 |
| REMOVED → UNCERTAIN | 3 |
| ADDED → ADDED | 32 |
| ADDED → UNCERTAIN | 38 |
| AMBIGUOUS → SAME | 2 |
| AMBIGUOUS → CHANGED | 1 |
| AMBIGUOUS → UNCERTAIN | 5 |

No production decision was emitted as a combined `REMOVED_ADDED` transition;
those reclassifications are nevertheless covered directly by the controlled
benchmark cases. A deterministic `CHANGED → model SAME` proposal is deliberately
closed to `UNCERTAIN`, so accepted `CHANGED → SAME` is zero by safety policy.

## Execution and recovery

Large groups were split into at most 40 preliminary items while assigning each
required fragment ID to exactly one chunk. The accepted artifact represents 21
model calls. Accepted usage was 414,316 input tokens, 49,977 output tokens and
6,912 cached tokens. Sum of accepted call durations was 1,039.385 s, or 94.49 s
per sheet group (49.49 s per transport chunk). Failed recovery attempts are not
included in these accepted-artifact totals.

The first large-response passes exposed duplicate, foreign and incomplete IDs.
The validator was not relaxed: same-decision duplicate IDs are visibly
deduplicated, foreign/cross-decision duplicate/incomplete coverage is rejected,
and failed parent groups receive no partial masks. Same-model recovery reused
completed groups and eventually completed 11/11 groups with 0 failed chunks.

An immediate rerun returned in 0.0548 s. SHA-256 and mtime of both AI artifacts
were unchanged, demonstrating the completed-run idempotence path and absence of
a new model call.

## Representative decisions

Production examples:

- Deterministic `SAME → CHANGED`: `У3 - ...` versus `УЗ - ...`; the reviewer
  retained the changed engineering designation instead of masking it.
- Deterministic `SAME → CHANGED`: the referenced set changed from
  `13АВ-РД-КЖ` to `1ЗАВ-РД-КЖ`.
- Deterministic `MOVED → MOVED`: `3.МОП.1 Лестничная клетка 17,40` was confirmed
  on actual РД page 17, and the final overlay points to that page.
- Deterministic `AMBIGUOUS → SAME`: `Марака помещения` versus `- Марка
  помещения` was treated as an obvious text-recognition/typing difference.
- Deterministic `CHANGED → UNCERTAIN`: `Проеход блока кладовых` versus `Проезд
  блока кладовых`; the model proposed SAME, but the safety gate refused to mask
  a deterministic change.

Required semantic examples validated by the selected model in the benchmark:

- Paraphrase: `Для помещения предусматривается вытяжная вентиляция` versus
  `Удаление воздуха ... осуществляется системой вытяжной вентиляции`:
  `REMOVED_ADDED → SAME`.
- Method change: `по 3-кратному воздухообмену` versus `по количеству выделяемых
  вредностей`: `REMOVED_ADDED → CHANGED`.
- Numerical change: `n = 3` versus `n = 5`: `CHANGED → CHANGED`.
- Equivalent formula: `Q = L × n` versus `Q=n·L`:
  `REMOVED_ADDED → SAME`.
- Changed formula: `Q = V × n` versus `Q = G / (Cп - Cн)`:
  `CHANGED → CHANGED`.
- False MOVED guard: exhaust air from toilets versus smoke exhaust from a
  corridor was not accepted as MOVED; the model separated it into REMOVED and
  ADDED. Ground truth was the still more conservative `UNCERTAIN`, so this
  remains a benchmark error but not a dangerous false MOVED.
- Same words, different context: identical `20 °C` statements for a living room
  and a server room became `SAME → UNCERTAIN`.

## Safety outcome

Final grey masks are produced only for accepted high-confidence AI `SAME` and
`MOVED`. The final result has 193 SAME and 16 MOVED decisions; CHANGED, REMOVED,
ADDED, UNCERTAIN and any failed group cannot create a mask. The model neither
changes `sheet_links.json` nor creates engineering findings.

## Verification

- `python -m pytest -q tests/test_stage_comparison_*.py`: 132 passed.
- `npx vitest run tests/stage_comparison_*.test.js`: 63 passed.
- `npm run build`: production bundle built successfully.
- The Stage 4 reviewer test module covers every one of the 27 required cases,
  plus strict coverage, confidence/policy gates and chunk partitioning.
- A repository-wide frontend run reached 374 passed and 7 failed. The seven
  failures are pre-existing expectations in `section_optimization_card`,
  `opt_norm_badge`, `stage_algorithm_guide` and `md_page_alignment`; none of
  those contours is changed by Stage 4. The complete comparison UI subset is
  green as reported above.
