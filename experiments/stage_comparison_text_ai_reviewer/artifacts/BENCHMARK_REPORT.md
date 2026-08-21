# AI Reviewer benchmark П ↔ РД

Dataset `19cc6c83ea1e69b6a94713468d511f5a2c57546e0910b1c71d789901d75abe9d`: **27** groups, 13 real-project and 14 controlled adversarial.

Independent ground truth `17d1fcb5970fdbcd1a38987bf9fb428f97db610de7b3e7d131d49844c8136814`; it is not used by production.

Schema `a58a6c2cc337cdf1150149402f7785ab7ee1852dde478d5b423882d1edd84e58`; validator `stage4_validator_v1_2_page_membership`.

All model runs used identical fragments, ordering, schema, batches and `medium` effort. The same native structured-output schema was enforced by both CLIs. `with_hint` includes deterministic preliminary decisions; `without_hint` hides them. `final/raw` means the accepted validator result versus the model proposal before the SAME/MOVED safety gate.

| Provider | Model | Mode | Accuracy final/raw | False SAME final/raw | False MOVED final/raw | Corrected | Harmful final/raw | Prov. fail | Value fail | Halluc. | JSON fail | Avg group, s | Input/output |
|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| deterministic | DETERMINISTIC ONLY | baseline | 0.718/0.718 | 1/1 | 1/1 | 0/11 | 0/0 | 0 | 0 | 0 | 0 | 0.000 | n/a/n/a |
| codex | gpt-5.6-luna | with_hint | 0.872/0.897 | 0/1 | 0/0 | 8/11 | 2/1 | 0 | 1 | 1 | 0 | 3.785 | 48974/4511 |

## Production selection

Selected: **codex / gpt-5.6-luna / with_hint / medium**.

Selection first minimizes accepted false SAME/MOVED, then maximizes factual accuracy and minimizes harmful reclassification, value/hallucination and JSON failures. Raw unsafe proposals remain a reported tie-breaker before latency and tokens. A candidate must first improve on deterministic accuracy, have zero JSON failures, and correct more preliminary errors than it harms.

### Representative cases

- **SAME paraphrase** (`semantic_paraphrase`): deterministic `REMOVED_ADDED` → final `SAME`; ground truth `SAME`; validation `ok`.
- **Semantic CHANGED** (`calculation_method`): deterministic `REMOVED_ADDED` → final `CHANGED`; ground truth `CHANGED`; validation `ok`.
- **Numerical CHANGED** (`calculation_input`): deterministic `CHANGED` → final `CHANGED`; ground truth `CHANGED`; validation `ok`.
- **MOVED** (`real_moved_other_sheet`): deterministic `MOVED` → final `MOVED`; ground truth `MOVED`; validation `ok`.
- **False MOVED guard** (`false_moved`): deterministic `MOVED` → final `REMOVED_ADDED`; ground truth `UNCERTAIN`; validation `ok`.
- **UNCERTAIN context** (`same_words_different_context`): deterministic `SAME` → final `UNCERTAIN`; ground truth `UNCERTAIN`; validation `ok`.
- **Equivalent formula** (`formula_equivalent`): deterministic `REMOVED_ADDED` → final `SAME`; ground truth `SAME`; validation `ok`.
- **Changed formula** (`formula_changed`): deterministic `CHANGED` → final `CHANGED`; ground truth `CHANGED`; validation `ok`.


## Confusion matrices and errors

### deterministic / DETERMINISTIC ONLY / baseline

```json
{
  "CHANGED": {
    "CHANGED": 17,
    "REMOVED_ADDED": 3
  },
  "ADDED": {
    "ADDED": 6
  },
  "SAME": {
    "SAME": 2,
    "REMOVED_ADDED": 4
  },
  "REMOVED": {
    "REMOVED": 2,
    "MIXED": 1
  },
  "UNCERTAIN": {
    "REMOVED_ADDED": 1,
    "MOVED": 1,
    "SAME": 1
  },
  "MOVED": {
    "MOVED": 1
  }
}
```

- `real_removed_note`: 0/1; false_same=0; false_moved=0; harmful=0; validation=ok.
- `real_contents_with_ambiguity`: 0/2; false_same=0; false_moved=0; harmful=0; validation=ok.
- `semantic_paraphrase`: 0/1; false_same=0; false_moved=0; harmful=0; validation=ok.
- `quantity_words`: 0/1; false_same=0; false_moved=0; harmful=0; validation=ok.
- `formula_equivalent`: 0/1; false_same=0; false_moved=0; harmful=0; validation=ok.
- `calculation_method`: 0/1; false_same=0; false_moved=0; harmful=0; validation=ok.
- `one_to_many`: 0/1; false_same=0; false_moved=0; harmful=0; validation=ok.
- `many_to_one`: 0/1; false_same=0; false_moved=0; harmful=0; validation=ok.
- `false_moved`: 0/1; false_same=0; false_moved=1; harmful=0; validation=ok.
- `same_words_different_context`: 0/1; false_same=1; false_moved=0; harmful=0; validation=ok.

### codex / gpt-5.6-luna / with_hint

```json
{
  "CHANGED": {
    "CHANGED": 18,
    "UNCERTAIN": 2
  },
  "ADDED": {
    "ADDED": 6
  },
  "SAME": {
    "SAME": 6
  },
  "REMOVED": {
    "REMOVED": 2,
    "MIXED": 1
  },
  "UNCERTAIN": {
    "MIXED": 1,
    "REMOVED_ADDED": 1,
    "UNCERTAIN": 1
  },
  "MOVED": {
    "MOVED": 1
  }
}
```

- `real_equipment_marks`: 2/3; false_same=0; false_moved=0; harmful=1; validation=ok.
- `real_removed_note`: 0/1; false_same=0; false_moved=0; harmful=0; validation=ok.
- `real_contents_with_ambiguity`: 1/2; false_same=0; false_moved=0; harmful=0; validation=ok.
- `unit_magnitude`: 0/1; false_same=0; false_moved=0; harmful=1; validation=['policy:unsupported_model_summary'].
- `false_moved`: 0/1; false_same=0; false_moved=0; harmful=0; validation=ok.
