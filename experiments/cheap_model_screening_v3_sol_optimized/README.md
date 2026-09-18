# Cheap model screening V3 — Sol/high optimized prompt

Runs `gpt-5.6-sol / high` on the byte-identical frozen eight-group source
sample. Only `prompt.txt` changes; every schema and image remains identical to
the V1 frozen candidate input. Each group receives a fresh tools-disabled
Codex/ChatGPT context. No truth or previous model result is available to the
pre-freeze runner.

```sh
python -m experiments.cheap_model_screening_v3_sol_optimized.run prepare
python -m experiments.cheap_model_screening_v3_sol_optimized.run run
```

Exactly eight calls, no retries, no Astra/Terra/OpenRouter/Claude calls, and no
automatic evaluation or continuation.
