# AI Reviewer benchmark П ↔ РД

Dataset `e4f790e5352056a7186c7a8aa8aca7b98c5e62e0990537c8573d2f84843a0d29`: **27** groups, 13 real-project and 14 controlled adversarial.

Independent ground truth `f923379270430633fe7e52865257703b1878f0b596bb08f95f4a05e6e7cb4b29`; it is not used by production.

Schema `a58a6c2cc337cdf1150149402f7785ab7ee1852dde478d5b423882d1edd84e58`; validator `stage4_validator_v1_1`.

All model runs used identical fragments, ordering, schema, batches and `medium` effort. The same native structured-output schema was enforced by both CLIs. `with_hint` includes deterministic preliminary decisions; `without_hint` hides them. `final/raw` means the accepted validator result versus the model proposal before the SAME/MOVED safety gate.

| Provider | Model | Mode | Accuracy final/raw | False SAME final/raw | False MOVED final/raw | Corrected | Harmful final/raw | Prov. fail | Value fail | Halluc. | JSON fail | Avg group, s | Input/output |
|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| deterministic | DETERMINISTIC ONLY | baseline | 0.718/0.718 | 1/1 | 1/1 | 0/11 | 0/0 | 0 | 0 | 0 | 0 | 0.000 | n/a/n/a |
| claude | claude-fable-5 | with_hint | 0.769/0.872 | 0/0 | 0/0 | 7/11 | 5/1 | 0 | 4 | 4 | 0 | 4.984 | 28204/12573 |
| claude | claude-fable-5 | without_hint | 0.795/0.897 | 0/0 | 0/0 | 7/11 | 4/1 | 0 | 4 | 4 | 0 | 4.784 | 24567/12460 |
| claude | claude-opus-5 | with_hint | 0.795/0.872 | 0/0 | 0/0 | 7/11 | 4/1 | 0 | 3 | 3 | 0 | 4.222 | 27250/9177 |
| claude | claude-opus-5 | without_hint | 0.641/0.744 | 0/1 | 0/0 | 8/11 | 11/8 | 0 | 4 | 4 | 0 | 4.038 | 23613/8835 |
| claude | claude-sonnet-5 | with_hint | 0.795/0.872 | 0/1 | 0/0 | 7/11 | 4/1 | 0 | 4 | 4 | 0 | 9.304 | 92406/27451 |
| claude | claude-sonnet-5 | without_hint | 0.821/0.897 | 0/1 | 0/0 | 7/11 | 3/1 | 0 | 4 | 4 | 0 | 7.182 | 90427/20533 |
| codex | gpt-5.6-luna | with_hint | 0.872/0.897 | 0/1 | 0/0 | 8/11 | 2/1 | 0 | 1 | 1 | 0 | 3.485 | 46900/4312 |
| codex | gpt-5.6-luna | without_hint | 0.821/0.897 | 0/0 | 0/1 | 8/11 | 4/1 | 0 | 3 | 3 | 0 | 3.763 | 46413/4444 |
| codex | gpt-5.6-sol | with_hint | 0.821/0.846 | 0/1 | 0/2 | 6/11 | 2/1 | 0 | 1 | 1 | 0 | 3.903 | 53030/4428 |
| codex | gpt-5.6-sol | without_hint | 0.692/0.718 | 0/1 | 0/2 | 6/11 | 7/6 | 0 | 1 | 1 | 0 | 3.811 | 51087/4630 |
| codex | gpt-5.6-terra | with_hint | 0.744/0.795 | 0/1 | 0/4 | 3/11 | 2/1 | 0 | 2 | 2 | 0 | 3.488 | 52740/4202 |
| codex | gpt-5.6-terra | without_hint | 0.744/0.769 | 0/0 | 0/6 | 4/11 | 3/2 | 0 | 1 | 1 | 0 | 4.006 | 50797/4833 |

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

### claude / claude-fable-5 / with_hint

```json
{
  "CHANGED": {
    "CHANGED": 15,
    "UNCERTAIN": 4,
    "REMOVED_ADDED": 1
  },
  "ADDED": {
    "ADDED": 6
  },
  "SAME": {
    "SAME": 6
  },
  "REMOVED": {
    "REMOVED": 1,
    "UNCERTAIN": 1,
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
- `real_wastewater_values`: 0/1; false_same=0; false_moved=0; harmful=1; validation=['policy:unsupported_model_summary'].
- `real_unrelated_corrections`: 3/4; false_same=0; false_moved=0; harmful=1; validation=['policy:unsupported_model_reason'].
- `real_title_metadata`: 2/3; false_same=0; false_moved=0; harmful=1; validation=['policy:unsupported_model_summary'].
- `real_removed_note`: 0/1; false_same=0; false_moved=0; harmful=0; validation=ok.
- `real_contents_with_ambiguity`: 0/2; false_same=0; false_moved=0; harmful=0; validation=ok.
- `unit_magnitude`: 0/1; false_same=0; false_moved=0; harmful=1; validation=['policy:unsupported_model_summary_and_reason'].
- `false_moved`: 0/1; false_same=0; false_moved=0; harmful=0; validation=ok.

### claude / claude-fable-5 / without_hint

```json
{
  "CHANGED": {
    "CHANGED": 15,
    "UNCERTAIN": 5
  },
  "ADDED": {
    "ADDED": 6
  },
  "SAME": {
    "SAME": 6
  },
  "REMOVED": {
    "REMOVED": 2,
    "UNCERTAIN": 1
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
- `real_wastewater_values`: 0/1; false_same=0; false_moved=0; harmful=1; validation=['policy:unsupported_model_summary'].
- `real_title_metadata`: 2/3; false_same=0; false_moved=0; harmful=1; validation=['policy:unsupported_model_summary'].
- `real_removed_note`: 0/1; false_same=0; false_moved=0; harmful=0; validation=['policy:unsupported_model_summary'].
- `real_contents_with_ambiguity`: 0/2; false_same=0; false_moved=0; harmful=0; validation=ok.
- `unit_magnitude`: 0/1; false_same=0; false_moved=0; harmful=1; validation=['policy:unsupported_model_summary_and_reason'].
- `false_moved`: 0/1; false_same=0; false_moved=0; harmful=0; validation=ok.

### claude / claude-opus-5 / with_hint

```json
{
  "CHANGED": {
    "CHANGED": 15,
    "UNCERTAIN": 5
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
- `real_wastewater_values`: 0/1; false_same=0; false_moved=0; harmful=1; validation=['policy:unsupported_model_summary'].
- `real_title_metadata`: 2/3; false_same=0; false_moved=0; harmful=1; validation=['policy:unsupported_model_summary'].
- `real_removed_note`: 0/1; false_same=0; false_moved=0; harmful=0; validation=ok.
- `real_contents_with_ambiguity`: 0/2; false_same=0; false_moved=0; harmful=0; validation=ok.
- `unit_magnitude`: 0/1; false_same=0; false_moved=0; harmful=1; validation=['policy:unsupported_model_summary_and_reason'].
- `false_moved`: 0/1; false_same=0; false_moved=0; harmful=0; validation=ok.

### claude / claude-opus-5 / without_hint

```json
{
  "CHANGED": {
    "CHANGED": 16,
    "UNCERTAIN": 4
  },
  "ADDED": {
    "ADDED": 1,
    "MIXED": 5
  },
  "SAME": {
    "SAME": 6
  },
  "REMOVED": {
    "MIXED": 2,
    "UNCERTAIN": 1
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
- `real_wastewater_values`: 0/1; false_same=0; false_moved=0; harmful=1; validation=['policy:unsupported_model_summary'].
- `real_unrelated_corrections`: 0/4; false_same=0; false_moved=0; harmful=4; validation=ok.
- `real_title_metadata`: 2/3; false_same=0; false_moved=0; harmful=1; validation=['policy:unsupported_model_summary'].
- `real_removed_note`: 0/1; false_same=0; false_moved=0; harmful=0; validation=['policy:unsupported_model_summary'].
- `real_added_drawing_rows`: 0/3; false_same=0; false_moved=0; harmful=3; validation=ok.
- `real_contents_with_ambiguity`: 1/2; false_same=0; false_moved=0; harmful=0; validation=ok.
- `unit_magnitude`: 0/1; false_same=0; false_moved=0; harmful=1; validation=['policy:unsupported_model_reason'].
- `false_moved`: 0/1; false_same=0; false_moved=0; harmful=0; validation=ok.

### claude / claude-sonnet-5 / with_hint

```json
{
  "CHANGED": {
    "CHANGED": 15,
    "UNCERTAIN": 4,
    "REMOVED_ADDED": 1
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
- `real_wastewater_values`: 0/1; false_same=0; false_moved=0; harmful=1; validation=['policy:unsupported_model_reason'].
- `real_title_metadata`: 2/3; false_same=0; false_moved=0; harmful=1; validation=['policy:unsupported_model_reason'].
- `real_removed_note`: 0/1; false_same=0; false_moved=0; harmful=0; validation=ok.
- `real_contents_with_ambiguity`: 0/2; false_same=0; false_moved=0; harmful=0; validation=['policy:unsupported_model_summary_and_reason'].
- `unit_magnitude`: 0/1; false_same=0; false_moved=0; harmful=1; validation=['policy:unsupported_model_summary_and_reason'].
- `false_moved`: 0/1; false_same=0; false_moved=0; harmful=0; validation=ok.

### claude / claude-sonnet-5 / without_hint

```json
{
  "CHANGED": {
    "CHANGED": 16,
    "UNCERTAIN": 4
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
    "UNCERTAIN": 1,
    "REMOVED_ADDED": 2
  },
  "MOVED": {
    "MOVED": 1
  }
}
```

- `real_equipment_marks`: 2/3; false_same=0; false_moved=0; harmful=1; validation=ok.
- `real_title_metadata`: 2/3; false_same=0; false_moved=0; harmful=1; validation=['policy:unsupported_model_reason'].
- `real_removed_note`: 0/1; false_same=0; false_moved=0; harmful=0; validation=ok.
- `real_contents_with_ambiguity`: 2/2; false_same=0; false_moved=0; harmful=0; validation=['policy:unsupported_model_summary'].
- `unit_magnitude`: 0/1; false_same=0; false_moved=0; harmful=1; validation=['policy:unsupported_model_summary_and_reason'].
- `calculation_method`: 0/1; false_same=0; false_moved=0; harmful=0; validation=['policy:unsupported_model_reason'].
- `false_moved`: 0/1; false_same=0; false_moved=0; harmful=0; validation=ok.
- `same_words_different_context`: 0/1; false_same=0; false_moved=0; harmful=0; validation=ok.

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

### codex / gpt-5.6-luna / without_hint

```json
{
  "CHANGED": {
    "CHANGED": 18,
    "UNCERTAIN": 2
  },
  "ADDED": {
    "ADDED": 5,
    "UNCERTAIN": 1
  },
  "SAME": {
    "UNCERTAIN": 1,
    "SAME": 5
  },
  "REMOVED": {
    "REMOVED": 3
  },
  "UNCERTAIN": {
    "MIXED": 1,
    "REMOVED_ADDED": 1,
    "CHANGED": 1
  },
  "MOVED": {
    "MOVED": 1
  }
}
```

- `real_formatting_only`: 0/1; false_same=0; false_moved=0; harmful=1; validation=ok.
- `real_wastewater_values`: 0/1; false_same=0; false_moved=0; harmful=1; validation=['policy:unsupported_model_summary'].
- `real_unrelated_corrections`: 3/4; false_same=0; false_moved=0; harmful=1; validation=['policy:unsupported_model_summary'].
- `real_contents_with_ambiguity`: 1/2; false_same=0; false_moved=0; harmful=0; validation=ok.
- `unit_magnitude`: 0/1; false_same=0; false_moved=0; harmful=1; validation=['policy:unsupported_model_summary'].
- `false_moved`: 0/1; false_same=0; false_moved=0; harmful=0; validation=ok.
- `same_words_different_context`: 0/1; false_same=0; false_moved=0; harmful=0; validation=ok.

### codex / gpt-5.6-sol / with_hint

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
    "SAME": 4,
    "UNCERTAIN": 2
  },
  "REMOVED": {
    "REMOVED": 3
  },
  "UNCERTAIN": {
    "MIXED": 1,
    "REMOVED_ADDED": 2
  },
  "MOVED": {
    "MOVED": 1
  }
}
```

- `real_equipment_marks`: 2/3; false_same=0; false_moved=0; harmful=1; validation=ok.
- `real_contents_with_ambiguity`: 1/2; false_same=0; false_moved=0; harmful=0; validation=ok.
- `semantic_paraphrase`: 0/1; false_same=0; false_moved=0; harmful=0; validation=ok.
- `quantity_words`: 0/1; false_same=0; false_moved=0; harmful=0; validation=ok.
- `unit_magnitude`: 0/1; false_same=0; false_moved=0; harmful=1; validation=['policy:unsupported_model_summary'].
- `false_moved`: 0/1; false_same=0; false_moved=0; harmful=0; validation=ok.
- `same_words_different_context`: 0/1; false_same=0; false_moved=0; harmful=0; validation=ok.

### codex / gpt-5.6-sol / without_hint

```json
{
  "CHANGED": {
    "CHANGED": 18,
    "UNCERTAIN": 2
  },
  "ADDED": {
    "ADDED": 2,
    "MIXED": 4
  },
  "SAME": {
    "SAME": 4,
    "UNCERTAIN": 2
  },
  "REMOVED": {
    "REMOVED": 2,
    "MIXED": 1
  },
  "UNCERTAIN": {
    "MIXED": 1,
    "REMOVED_ADDED": 2
  },
  "MOVED": {
    "MOVED": 1
  }
}
```

- `real_equipment_marks`: 2/3; false_same=0; false_moved=0; harmful=1; validation=ok.
- `real_unrelated_corrections`: 2/4; false_same=0; false_moved=0; harmful=2; validation=ok.
- `real_added_drawing_rows`: 0/3; false_same=0; false_moved=0; harmful=3; validation=ok.
- `real_contents_with_ambiguity`: 1/2; false_same=0; false_moved=0; harmful=0; validation=ok.
- `semantic_paraphrase`: 0/1; false_same=0; false_moved=0; harmful=0; validation=ok.
- `quantity_words`: 0/1; false_same=0; false_moved=0; harmful=0; validation=ok.
- `unit_magnitude`: 0/1; false_same=0; false_moved=0; harmful=1; validation=['policy:unsupported_model_summary_and_reason'].
- `false_moved`: 0/1; false_same=0; false_moved=0; harmful=0; validation=ok.
- `same_words_different_context`: 0/1; false_same=0; false_moved=0; harmful=0; validation=ok.

### codex / gpt-5.6-terra / with_hint

```json
{
  "CHANGED": {
    "CHANGED": 17,
    "UNCERTAIN": 3
  },
  "ADDED": {
    "ADDED": 6
  },
  "SAME": {
    "SAME": 2,
    "UNCERTAIN": 4
  },
  "REMOVED": {
    "REMOVED": 3
  },
  "UNCERTAIN": {
    "MIXED": 1,
    "CHANGED": 2
  },
  "MOVED": {
    "MOVED": 1
  }
}
```

- `real_equipment_marks`: 2/3; false_same=0; false_moved=0; harmful=1; validation=ok.
- `real_contents_with_ambiguity`: 1/2; false_same=0; false_moved=0; harmful=0; validation=ok.
- `semantic_paraphrase`: 0/1; false_same=0; false_moved=0; harmful=0; validation=ok.
- `quantity_words`: 0/1; false_same=0; false_moved=0; harmful=0; validation=ok.
- `unit_magnitude`: 0/1; false_same=0; false_moved=0; harmful=1; validation=['policy:unsupported_model_summary'].
- `formula_equivalent`: 0/1; false_same=0; false_moved=0; harmful=0; validation=ok.
- `calculation_method`: 0/1; false_same=0; false_moved=0; harmful=0; validation=['policy:unsupported_model_reason'].
- `one_to_many`: 0/1; false_same=0; false_moved=0; harmful=0; validation=ok.
- `false_moved`: 0/1; false_same=0; false_moved=0; harmful=0; validation=ok.
- `same_words_different_context`: 0/1; false_same=0; false_moved=0; harmful=0; validation=ok.

### codex / gpt-5.6-terra / without_hint

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
    "UNCERTAIN": 5,
    "SAME": 1
  },
  "REMOVED": {
    "REMOVED": 3
  },
  "UNCERTAIN": {
    "MIXED": 1,
    "CHANGED": 2
  },
  "MOVED": {
    "MOVED": 1
  }
}
```

- `real_equipment_marks`: 2/3; false_same=0; false_moved=0; harmful=1; validation=ok.
- `real_formatting_only`: 0/1; false_same=0; false_moved=0; harmful=1; validation=ok.
- `real_contents_with_ambiguity`: 1/2; false_same=0; false_moved=0; harmful=0; validation=ok.
- `semantic_paraphrase`: 0/1; false_same=0; false_moved=0; harmful=0; validation=ok.
- `quantity_words`: 0/1; false_same=0; false_moved=0; harmful=0; validation=ok.
- `unit_magnitude`: 0/1; false_same=0; false_moved=0; harmful=1; validation=['policy:unsupported_model_summary'].
- `formula_equivalent`: 0/1; false_same=0; false_moved=0; harmful=0; validation=ok.
- `one_to_many`: 0/1; false_same=0; false_moved=0; harmful=0; validation=ok.
- `false_moved`: 0/1; false_same=0; false_moved=0; harmful=0; validation=ok.
- `same_words_different_context`: 0/1; false_same=0; false_moved=0; harmful=0; validation=ok.
