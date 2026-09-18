# Cheap model screening V2 — Sol/high

Runs `gpt-5.6-sol / high` on the byte-identical frozen eight-group sample from
`cheap_model_screening_v1`. Preparation copies and rechecks every prompt,
schema, and image hash. Each group receives a fresh tools-disabled Codex/ChatGPT
context. No reference, Astra, Terra, truth, or prior verdict is available to the
pre-freeze runner.

```sh
python -m experiments.cheap_model_screening_v2_sol_high.run prepare
python -m experiments.cheap_model_screening_v2_sol_high.run run
python -m experiments.cheap_model_screening_v2_sol_high.evaluate
```

Exactly eight calls, no retries, no Astra/Terra/OpenRouter/Claude calls, and no
automatic full-run continuation.
