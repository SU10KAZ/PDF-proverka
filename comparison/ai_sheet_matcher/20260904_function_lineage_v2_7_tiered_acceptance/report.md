# Function Lineage v2.7 — tiered acceptance holdout (prepared, not run)

**AWAITING EXTERNAL AI CONSENT.** No model call has been made.

Excluded as already seen: the seven v2.4.2 controls, the v2.5 diagnostic 36 and the v2.6 holdout 36.

## Tiers

| Tier | Decides GO | Eligible | Selected | Sampling |
|---|---|---:|---:|---|
| `AUTO_ONE_TO_ONE` | YES | 33 | 33 | whole tier |
| `AUTO_MERGED` | YES | 54 | 54 | whole tier |
| `HARD_DIAGNOSTIC` | no | 47 | 12 | sampled |

Tier follows only from the relation types a task's own candidates carry, so membership is fixed before inference and cannot be revised after an output is seen. Both GO-deciding tiers are taken whole: there is no sampling freedom in the sets that decide the product question.

## Acceptance gates (fixed before inference)

| Gate | Value |
|---|---|
| `stable_3_of_3_min` | `0.9` |
| `cross_cold_exact_consistency_min` | `0.85` |
| `unsupported_accepted_max` | `0` |
| `false_capacity_conflicts_max` | `0` |
| `right_map_conflict_max` | `0` |
| `technical_failures_max` | `0` |
| `sentinel_regression_max` | `0` |
| `batch_permutation_changes_max` | `0` |
| `accepted_match_requires` | `['PASS_A_EQUALS_PASS_B', 'PARSER_PASS', 'VERIFIER_PASS', 'CAPACITY_PASS']` |
| `majority_override` | `False` |
| `threshold_source` | `pre-existing v2.5 verdict-A thresholds` |

## Preflight

OK `True`; failures `none`; sentinel prompts identical to v2.5 `True`.

Shards by set: `{'AUTO_ONE_TO_ONE': 11, 'AUTO_MERGED': 12, 'HARD_DIAGNOSTIC': 8, 'SENTINEL': 4}`; tasks by set: `{'AUTO_MERGED': 54, 'AUTO_ONE_TO_ONE': 33, 'HARD_DIAGNOSTIC': 12, 'SENTINEL': 7}`.

## External model data gate

Provider `OpenAI via Codex CLI subscription transport`; model `gpt-5.6-sol` / effort `low`; vision `False`.
Planned requests **`194`** ({'tiered_shards': 31, 'tiered_cold_repeats': 3, 'sentinel_shards': 4, 'sentinel_cold_repeats': 1, 'passes_per_repeat': 2, 'shards_by_set': {'AUTO_ONE_TO_ONE': 11, 'AUTO_MERGED': 12, 'HARD_DIAGNOSTIC': 8, 'SENTINEL': 4}}).
Prompt characters median/p95/max `205701` / `244831` / `245055`.

### Transmitted data classes

* FunctionScope core facts derived from project Markdown/OCR text (function class, role, serviced object, zone, building, floors, systems, consumers, equipment roles, upstream/downstream text)
* deterministic functional candidate metadata (candidate_id, relation_type, exact LEFT/RIGHT fragment and function identifiers, capacity keys, component_map, matched evidence channels, deterministic scores and ranks)
* task-local evidence records (evidence_id, field name, normalized textual value, owner fragment/function id, physical page, provenance type)
* RIGHT physical page numbers of the compared documents
* synthetic research identifiers (task_id, scope_id, shard_id, payload_signature)

### Never transmitted

* page images, crops, or any raster/vector drawing content
* human engineer decisions, verdicts, or comparison results
* customer or personal data fields
* credentials, tokens, or infrastructure identifiers
* production database rows or live comparison state

### Hashes

* `model_inputs.jsonl` — `173d2be7a3b2fe291263edc8bc786d89887a39fd92ac156b4d45d1241243dfd9`
* `acceptance_population.json` — `340da5a123db4d74df6b37b05b3b66931df90afea3268b658cc1c704a005615e`
* `acceptance_sample.json` — `f1e900b6068337196e632b018287e6495b429879e6faf840aa27bf05dce0d23f`

Explicit user consent is required before any request is sent.
