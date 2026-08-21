# Stage 4.2 — accepted sheet-link page membership

Baseline commit: `39da504e8da11028204f53aa340a079f2e019563`.

## Exact rule

The prompt now states that `left_pages` and `right_pages` define the accepted current sheet group. Different absolute PDF pages inside that group are `SAME` when their fragments are semantically equivalent. `MOVED` is allowed only when the matching fragment lies outside the accepted opposite-side page set; sheet number, PDF-page equality and order are not used.

The backend converts a high-confidence model `MOVED` to `SAME` only when all referenced left and right pages belong to the accepted group and provenance is supported. The original proposal remains in `model_*`. A deterministic `CHANGED` conflict still fails closed to `UNCERTAIN`; unsupported explanations also remain `UNCERTAIN`. A genuine exactly-one-side-outside match remains `MOVED`.

## Controlled benchmark

| Metric | BEFORE | AFTER |
|---|---:|---:|
| Accuracy | 0.8718 | 0.8718 |
| Accepted False SAME | 0 | 0 |
| Accepted False MOVED | 0 | 0 |
| Raw False SAME | 1 | 1 |
| Raw False MOVED | 0 | 0 |
| Corrected deterministic errors | 8 | 8 |
| Harmful reclassifications | 2 | 2 |
| UNCERTAIN | 3 | 3 |
| JSON failures | 0 | 0 |
| Input tokens | 46900 | 48974 |
| Output tokens | 4312 | 4511 |
| Average group runtime, sec | 3.485 | 3.785 |

Acceptance gate: accepted False SAME = 0 and accepted False MOVED = 0; accuracy and harmful reclassifications are unchanged.

## 140-case MOVED_PAGE_SEMANTICS replay

| Status | BEFORE | AFTER |
|---|---:|---:|
| SAME | 0 | 120 |
| MOVED | 0 | 0 |
| CHANGED | 0 | 1 |
| UNCERTAIN | 140 | 19 |

Raw model status changed from 140 MOVED to {"CHANGED": 1, "SAME": 137, "UNCERTAIN": 2}. The replay used 7 calls, 146655 input and 15568 output tokens in 325.325 seconds; validation errors: 0.

## Full production rerun

Project `272_Sadovnicheskaya_76_Balchug_Esteyt`, session `121d764109184c13`, pair `p570d156f57`.

| Status | BEFORE | AFTER |
|---|---:|---:|
| SAME | 193 | 314 |
| MOVED | 16 | 16 |
| CHANGED | 41 | 22 |
| REMOVED | 59 | 62 |
| ADDED | 32 | 86 |
| UNCERTAIN | 189 | 91 |

The original 140-case production cohort changed from 140 UNCERTAIN to {"SAME": 120, "UNCERTAIN": 20}. The final 16 MOVED decisions are all genuine membership cases with exactly one referenced side outside the accepted group; both-inside MOVED: 0.

### Original non-page 49-case cohort

| Baseline cause | BEFORE | AFTER statuses |
|---|---:|---|
| VALIDATOR_REJECTED | 39 | `{"SAME": 1, "UNCERTAIN": 38}` |
| OCR_NOISE | 5 | `{"CHANGED": 1, "UNCERTAIN": 4}` |
| MULTIPLE_CANDIDATES | 3 | `{"REMOVED": 3}` |
| TABLE_STRUCTURE | 2 | `{"UNCERTAIN": 2}` |

### Remaining UNCERTAIN taxonomy

| Cause | Count |
|---|---:|
| OCR_NOISE | 21 |
| OTHER | 1 |
| VALIDATOR_REJECTED | 69 |

### Production performance

| Metric | BEFORE | AFTER |
|---|---:|---:|
| Input tokens | 414316 | 441762 |
| Output tokens | 49977 | 54152 |
| Cached tokens | 6912 | 0 |
| Represented model calls | 21 | 23 |
| Fresh model calls | 5 | 3 |
| Runtime, sec | 1039.385 | 1127.612 |

The completed persisted AFTER artifact represents 23 calls. BEFORE represented 21 calls because one historical group was stored as a single legacy unchunked call; the already-existing current policy reconstructs it as three chunks. No chunking code or limit changed in Stage 4.2.

Including rejected fail-closed responses and recoveries, this execution made 32 actual calls, used 631007 input and 78384 output tokens, and accumulated 1626.485 seconds of model-call duration across four attempts.

Deterministic comparison, differences, accepted links and sheet suggestions are unchanged: `True`.

## Verification

- Targeted reviewer module: `45 passed`.
- Extended Stage Comparison regression selection: `141 passed`.
- The seven required page-membership cases are covered, plus a safety test that an inside-group MOVED cannot mask deterministic CHANGED.
- Production model/chunking/preprocessing/sheet links/UI were not changed.

## Recommendation

B. Use a separate small Stage 4.3 for the next concrete cause: the remaining validator-rejected unsupported explanation/provenance cases (69 current UNCERTAIN; 38 of the original 39-case validator cohort remain). Do not combine that work with this accepted page-membership fix.
