# Function Lineage v2.6 — independent holdout sample (prepared, not run)

**AWAITING EXTERNAL AI CONSENT.** No model call has been made. `model_calls = 0`.

The v2.5 36-task sample is now a diagnostic set and is excluded, together with the seven v2.4.2 controls. The controls are carried separately with prompts byte-identical to the frozen v2.5 sentinel shards.

## Eligible population

Scoped population `213`; holdout eligible `170`; excluded `43` (`36` diagnostic + `7` sentinel).

| Corpus | Holdout eligible | Selected |
|---|---:|---:|
| IOS1.1 | 82 | 12 |
| IOS2.1 | 62 | 12 |
| IOS3.1 | 26 | 12 |

## Stratum coverage

| Stratum | Description | Eligible | Selected | Covered |
|---|---|---:|---:|---|
| A | simple same-scope CONTINUED_1_TO_1 with one score-plausible candidate | 28 | 7 | YES |
| B | same-scope ambiguity with at least two score-plausible eligible candidates | 38 | 10 | YES |
| C | eligible SPLIT_1_TO_N candidate | 68 | 21 | YES |
| D | eligible MERGED_N_TO_1 candidate | 64 | 10 | YES |
| E | eligible FUNCTION_DISTRIBUTED candidate | 0 | 0 | NO |
| F | eligible EXACT_CHILD_UNION group candidate | 60 | 10 | YES |
| G | eligible NON_DECOMPOSABLE_GROUP candidate | 68 | 21 | YES |
| H | low evidence: top candidate has at most three matched functional channels | 18 | 14 | YES |
| I | one RIGHT physical page appears with distinct exact fragment IDs in the task inventory | 18 | 8 | YES |
| J | research-reference target candidate absent from old Sheet Matcher edges | 29 | 11 | YES |
| K | large candidate inventory (at least ten) | 54 | 21 | YES |
| L | adjacent deterministic source scores differ by at most 0.005 | 103 | 25 | YES |

Strata with no eligible holdout task: `['E']`. Stratum E stays empty because every frozen FUNCTION_DISTRIBUTED candidate belongs to the excluded LEFT20 PARENT control; it is reported through the sentinel only.

## Preflight

OK: `True`; failures `none`.
Holdout shards `17`; sentinel shards `4`; sentinel prompts identical to v2.5: `True`.

## External model data gate

Provider: `OpenAI via Codex CLI subscription transport`; model `gpt-5.6-sol` / effort `low`; vision `False`.
Planned requests: `110` ({'holdout_shards': 17, 'holdout_cold_repeats': 3, 'sentinel_shards': 4, 'sentinel_cold_repeats': 1, 'passes_per_repeat': 2}).
Prompt characters median/p95/max: `199343` / `232399` / `240363`.

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

* `model_inputs.jsonl` — `c92b3fcc23dda3245a0f720127c26c9a05d870bb9fe5c38b0d3330a40201026d`
* `holdout_population.json` — `95e640ed701acd564064d2310bbf6f3778eeff7f0a3dee6c2e8b6589a531754c`
* `holdout_sample.json` — `ab9cc617e9f2c8b38e632a3f72cff487c4060897ab50c8f1b1a22867bed6e589`

Explicit user consent is required before any request is sent.
