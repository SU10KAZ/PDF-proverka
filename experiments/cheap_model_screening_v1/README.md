# Cheap model screening V1

Eight frozen Change Miner groups (four Pair A and four Pair B) are selected by
structural input properties before any truth or Astra result is opened.  The
candidate is `gpt-5.6-terra / high`, run in fresh tools-disabled Codex/ChatGPT
contexts with the exact saved prompts, schemas, and images from the successful
AI mapping + Change Miner experiments.

```sh
python -m experiments.cheap_model_screening_v1.run prepare
python -m experiments.cheap_model_screening_v1.run run
python -m experiments.cheap_model_screening_v1.evaluate
```

There are no retries, Astra calls, OpenRouter calls, Claude calls, or automatic
full-run continuation.  Evaluation is permitted only after the candidate result
freeze has been verified.
