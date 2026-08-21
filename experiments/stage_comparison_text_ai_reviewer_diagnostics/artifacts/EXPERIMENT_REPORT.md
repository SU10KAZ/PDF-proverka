# Stage 4.1 — аудит UNCERTAIN и performance profile

## Диагноз простыми словами

Из 189 UNCERTAIN только **3** выбрала сама модель; **186** созданы safety policy backend. Главная системная причина — модель трактовала различающиеся абсолютные PDF-страницы двух уже связанных стадий как MOVED. Validator правильно не разрешил маски, но итогом стали массовые технические UNCERTAIN.

94,49 с на production sheet group не противоречат benchmark 3,485 с: production содержит 530 решений, или 48.182 на группу, benchmark — только 1.444. На одно решение production быстрее: 1.961 с против 2.413 с. Локальный Python/IO не является bottleneck. Production input/output больше benchmark в 8.834×/11.59×, потому что решений в 13,6× больше.

## UNCERTAIN taxonomy

| Причина | Количество | Доля |
|---|---:|---:|
| MOVED_PAGE_SEMANTICS | 140 | 74.07% |
| VALIDATOR_REJECTED | 39 | 20.63% |
| OCR_NOISE | 5 | 2.65% |
| MULTIPLE_CANDIDATES | 3 | 1.59% |
| TABLE_STRUCTURE | 2 | 1.06% |
| MODEL_UNCERTAIN | 0 | 0.00% |
| MISSING_CONTEXT | 0 | 0.00% |
| FRAGMENTATION_1_TO_N | 0 | 0.00% |
| FRAGMENTATION_N_TO_1 | 0 | 0.00% |
| FRAGMENTATION_N_TO_M | 0 | 0.00% |
| CHUNK_BOUNDARY | 0 | 0.00% |
| FORMULA_STRUCTURE | 0 | 0.00% |
| WEAK_PROVENANCE | 0 | 0.00% |
| CONFLICTING_EVIDENCE | 0 | 0.00% |
| NO_COUNTERPART | 0 | 0.00% |
| OTHER | 0 | 0.00% |

Model-origin: **3**. Validator-origin: **186**.

`uncertain_reason` — первопричина, а origin — отдельная ось. Поэтому три model-origin кейса находятся в OCR_NOISE, а строка MODEL_UNCERTAIN в таблице первопричин равна нулю.

Из 140 MOVED-page cases 103 совпадают дословно и 115 совпадают после canonicalization. Во всех случаях правый fragment остаётся внутри принятой связи листов; абсолютный номер PDF не является доказательством MOVED.

### Validator/policy reasons

| Причина | Количество |
|---|---:|
| moved_requires_source_outside_linked_pages | 125 |
| unsupported_model_summary | 53 |
| same_conflicts_with_deterministic_change | 4 |
| unsupported_model_reason | 3 |
| unsupported_model_summary_and_reason | 1 |

1→N: 0; N→1: 0; N→M: 0; вероятный chunk boundary: 0. Отдельно найдены 3 one-to-one кейса с несколькими кандидатами: соответствие linked sheet находилось в том же model call, но было занято дубликатом с другой страницы. Это assignment problem, не 1→N и не chunk boundary.

Проверка контекста сохраняет по каждому sample-case соседние строки, заголовки и local_context. Ни один из 50 не перешёл в NEED_MORE_CONTEXT: 45 решаются по уже доступному тексту/структуре группы, пяти нужен raster из-за неверного OCR. Формульный кейс оказался response leakage, а не нехваткой формульного контекста.

## Human-review sample

Seeded stratified sample: 50 cases; manually annotated: 50. Complete: `true`.

- HUMAN_RESOLVABLE: 45
- GENUINELY_UNCERTAIN: 0
- NEED_MORE_CONTEXT: 0
- BAD_INPUT: 5

Coverage: 5 sheet groups; model/validator origin 3/47; formula-like cases 7; short fragment sides ≤20 chars 7; long fragment sides ≥400 chars 2. Таблицы, обычные абзацы, OCR и обозначения представлены; 1→N/N→1 не включались искусственно, потому что во всех 189 таких случаев нет.

Стратифицированная point estimate: потенциально автоматизируемы **184/189**, для человека или исправления OCR следует оставить **5/189**. Из первых 184 только 140 адресуются рекомендуемым первым изменением; остальные требуют отдельных проверок. Это оценка возможностей, не разрешение автоматически переписать production statuses.

The sample is deliberately stratified and oversamples minority failure modes; raw sample percentages must not be projected to all 189 without stratum weighting. It remains a diagnostic artifact and is not production ground truth.

## Performance

- Groups: 11; model calls/chunks: 21/21.
- Model time: 1039.385 s; average 94.490 s/group; median 29.016 s/group.
- Fastest: `link_93f2698d3cc6` — 12.582 s; slowest: `link_5bac0a5098c7` — 320.242 s.
- Chunks/group average 1.909, median 1, max 5.
- Tokens: input 414316, output 49977, cached 6912 (1.668% input).
- Model share of reconstructed run: 99.9458%; local replay total 563.8410 ms.

- Correlation duration↔output tokens: 0.998891; duration↔input tokens: 0.89751.
- Explicit chunk records: 4 groups; legacy group-level usage: 7; one legacy group would be split into three by the current policy.

| Sheet group | Sources | Items | Calls | Input chars | Input tok | Output tok | Cached | Model s | UNCERTAIN |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `link_5bac0a5098c7` | 290 | 171 | 5 | 153543 | 110103 | 15980 | 0 | 320.242 | 52 |
| `link_8b7b6a4bcb55` | 105 | 63 | 2 | 54824 | 40686 | 5651 | 0 | 113.800 | 2 |
| `link_6976a3842ce3` | 156 | 109 | 1 | 75382 | 37909 | 5327 | 0 | 102.070 | 3 |
| `link_da35e872b7f4` | 87 | 45 | 2 | 42939 | 35344 | 4479 | 0 | 95.321 | 41 |
| `link_93f2698d3cc6` | 2 | 2 | 1 | 3851 | 11507 | 232 | 0 | 12.582 | 0 |
| `link_b02dda785776` | 20 | 10 | 1 | 10821 | 14131 | 851 | 0 | 22.188 | 0 |
| `link_01bcbc23fa46` | 24 | 12 | 1 | 12013 | 14535 | 917 | 0 | 21.438 | 0 |
| `link_ccb502cd6744` | 18 | 9 | 1 | 10552 | 14261 | 728 | 0 | 21.858 | 0 |
| `link_8d695a3d3dee` | 30 | 15 | 1 | 14104 | 15195 | 1188 | 0 | 26.885 | 0 |
| `link_ba08ed80436a` | 266 | 177 | 5 | 150293 | 104627 | 13442 | 6912 | 273.985 | 91 |
| `link_6d1433843c45` | 26 | 13 | 1 | 15009 | 16018 | 1182 | 0 | 29.016 | 0 |

### Exact accepted model calls

| Call/chunk | Input chars | Input tok | Output tok | Cached | Model s |
|---|---:|---:|---:|---:|---:|
| `link_5bac0a5098c7::chunk_1` | 33649 | 23311 | 3818 | 0 | 75.773 |
| `link_5bac0a5098c7::chunk_2` | 34052 | 23179 | 4062 | 0 | 81.203 |
| `link_5bac0a5098c7::chunk_3` | 33286 | 23213 | 3775 | 0 | 73.256 |
| `link_5bac0a5098c7::chunk_4` | 39481 | 25372 | 3244 | 0 | 65.499 |
| `link_5bac0a5098c7::chunk_5` | 13075 | 15028 | 1081 | 0 | 24.511 |
| `link_8b7b6a4bcb55::chunk_1` | 29994 | 21707 | 3419 | 0 | 68.928 |
| `link_8b7b6a4bcb55::chunk_2` | 24830 | 18979 | 2232 | 0 | 44.872 |
| `link_6976a3842ce3` | 75382 | 37909 | 5327 | 0 | 102.070 |
| `link_da35e872b7f4::chunk_1` | 34946 | 22153 | 3926 | 0 | 76.474 |
| `link_da35e872b7f4::chunk_2` | 7993 | 13191 | 553 | 0 | 18.847 |
| `link_93f2698d3cc6` | 3851 | 11507 | 232 | 0 | 12.582 |
| `link_b02dda785776` | 10821 | 14131 | 851 | 0 | 22.188 |
| `link_01bcbc23fa46` | 12013 | 14535 | 917 | 0 | 21.438 |
| `link_ccb502cd6744` | 10552 | 14261 | 728 | 0 | 21.858 |
| `link_8d695a3d3dee` | 14104 | 15195 | 1188 | 0 | 26.885 |
| `link_ba08ed80436a::chunk_1` | 26761 | 20023 | 712 | 6912 | 20.996 |
| `link_ba08ed80436a::chunk_2` | 33381 | 22240 | 3687 | 0 | 71.585 |
| `link_ba08ed80436a::chunk_3` | 31791 | 21837 | 3375 | 0 | 67.393 |
| `link_ba08ed80436a::chunk_4` | 36934 | 23121 | 3557 | 0 | 70.249 |
| `link_ba08ed80436a::chunk_5` | 21426 | 17406 | 2111 | 0 | 43.762 |
| `link_6d1433843c45` | 15009 | 16018 | 1182 | 0 | 29.016 |

### Local phases by group (diagnostic replay, ms)

| Sheet group | Preprocess | Load alloc. | Prompt | Validator | Aggregation | File IO | Local total | Reconstructed total |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| `link_5bac0a5098c7` | 128.3720 | 3.0881 | 0.7626 | 8.0891 | 0.2992 | 5.9839 | 147.8797 | 320389.8797 |
| `link_8b7b6a4bcb55` | 26.0794 | 3.0881 | 0.2289 | 3.0687 | 0.1329 | 2.1502 | 35.1450 | 113835.1450 |
| `link_6976a3842ce3` | 51.5073 | 3.0881 | 0.3271 | 4.1700 | 0.1576 | 2.3464 | 62.2123 | 102132.2123 |
| `link_da35e872b7f4` | 4.8766 | 3.0881 | 0.1736 | 2.2757 | 0.0377 | 1.5609 | 12.3124 | 95333.3124 |
| `link_93f2698d3cc6` | 0.3147 | 3.0881 | 0.0115 | 0.0709 | 0.0084 | 0.1065 | 3.6003 | 12585.6003 |
| `link_b02dda785776` | 0.5623 | 3.0881 | 0.0503 | 0.2793 | 0.0365 | 0.4139 | 4.4306 | 22192.4306 |
| `link_01bcbc23fa46` | 0.6066 | 3.0881 | 0.0544 | 0.3233 | 0.0449 | 0.4502 | 4.5677 | 21442.5677 |
| `link_ccb502cd6744` | 0.5625 | 3.0881 | 0.0471 | 0.2688 | 0.0371 | 0.3755 | 4.3793 | 21862.3793 |
| `link_8d695a3d3dee` | 0.7313 | 3.0881 | 0.0647 | 0.4046 | 0.0530 | 0.5406 | 4.8825 | 26889.8825 |
| `link_ba08ed80436a` | 249.9408 | 3.0881 | 0.5837 | 8.2087 | 0.1633 | 5.0948 | 268.1342 | 274253.1342 |
| `link_6d1433843c45` | 12.0050 | 3.0881 | 0.0444 | 0.7044 | 0.0255 | 0.4289 | 16.2965 | 29032.2965 |

Stage 4 did not persist historical local-phase timestamps. These local values are medians of 31 read-only replays; model durations and token usage above are exact persisted data.

## Prompt and CLI diagnosis

| Submitted context component | Characters |
|---|---:|
| Source text | 60953 |
| Neighbor/local context | 27289 |
| Source bbox | 96155 |
| Other source metadata/JSON | 216162 |
| Deterministic SAME evidence (estimate) | 44680 |
| Deterministic MOVED evidence (estimate) | 3764 |
| Differences/other preliminary evidence (estimate) | 31910 |
| Fixed instruction | 28665 |
| Schema embedded in prompt | 18291 |
| Wrapper | 12558 |
| Native schema file (also passed per call) | 19950 |

Source-text duplication factor: **1.0x**; whole-context estimate: **1.1165x**. Chunking does not repeatedly send the same source fragments; repeated fixed CLI/system context is the material duplication.

Largest group example `link_5bac0a5098c7`: unique source text 10813 chars, submitted source text 10813 chars, factor 1.0× across 5 calls. With repeated fixed application context its whole-context estimate is 1.0798×.

Regression over 21 actual calls estimates 10160.35 fixed input tokens per fresh call (R²=0.994168); about 51.5% of production input. Every call starts a new `codex exec --ephemeral` process/session, writes the native schema again, and runs sequentially. There is no per-call model discovery. Successful transient retry count cannot be recovered because the current artifact does not persist it; no rate-limit evidence remains in the artifact.

The 6,912 cached tokens came from one accepted chunk only. That is 1.669% of input and does not represent reusable context across ephemeral chunk sessions. The reported ~0.055 s repeat run is artifact reuse: it performs no new model review and therefore is not a latency comparison with the initial run.

## Maximum three evidence-backed next changes

1. **Stage 4.2 candidate:** clarify in the prompt that corresponding П/РД PDF page numbers may differ inside an accepted sheet link and that this is SAME, not MOVED. Test this as one isolated change on the existing Stage 4 benchmark and a production UNCERTAIN sample.
2. In a separate experiment, generate source-exact backend summaries for right-only ADDED after validating the status and ids; keep failing closed on unrelated response leakage. This targets 38 designation-normalization rejects.
3. In a separate experiment, rank a candidate on the accepted linked sheet ahead of a duplicate outside that link before enforcing one-use coverage. This targets the three MULTIPLE_CANDIDATES cases. No latency optimization is recommended now: production is already faster per decision than benchmark, and batching changes the safety surface.

## Recommendation

**B — выполнить одну небольшую Stage 4.2 доработку.** Start only with the MOVED page semantics clarification: it explains 140/189 UNCERTAIN. Do not begin vector graphics before that controlled experiment, and do not combine it with validator or batching changes. Accepted False SAME and False MOVED must remain zero.

## Integrity

This diagnostic run made no production changes, did not call a model, and did not alter prompt, model, chunk size, validator, preprocessing, statuses, UI or sheet links. Exact model-call durations come from the production artifact; local phase timings are labeled read-only diagnostic replays because Stage 4 did not historically persist those timers. The exact post-schema model proposal, validator reason and final status are retained per inventory case. The provider stdout envelope was never persisted by Stage 4, so it cannot be reconstructed; this run deleted no raw data.
