# Winner Recommendation

## Verdict

Recommended first candidate for this task: `qwen/qwen3.6-35b-a3b`.

Why: it was the only model with `12/12` pass on the 1024px audit set, it was the fastest, and its answers usually preserved the drawing type and concrete readable details. This is not yet a production-quality defect detector; it is a successful technical/semantic smoke for Chandra vision input.

## Model Comparison

| Model | Image pass | Weak | Fail | Avg s/block | Semantic note |
|---|---:|---:|---:|---:|---|
| `google/gemma-4-31b` | 10/12 | 2 | 0 | 6.25 | Stable, but weaker on two blocks and slower; sometimes too generic. |
| `qwen/qwen3.6-27b` | 11/12 | 1 | 0 | 5.32 | More verbose, mostly good, but one weak table/detail block and slower. |
| `qwen/qwen3.6-35b-a3b` | 12/12 | 0 | 0 | 2.68 | Best balance: fast, concrete, all audit blocks passed; still needs defect-finding validation. |

## Important Fixes That Made It Work

- Use native LM Studio `/api/v1/chat`, not OpenAI-compatible `/v1/chat/completions`, for these vision tests.
- Set `reasoning: "off"`; otherwise small completions are consumed by reasoning and final content can be empty.
- Use image input items as `{"type":"text","content":...}` and `{"type":"image","data_url":...}`.
- Load exactly one model at a time and unload it after the test.

## Current Limit

The test used 1024px capped crop images. This proves Chandra vision can run on the big KJ audit set, but it does not prove original-resolution quality or production defect-detection quality yet. Next step should compare the best candidate against Gemini Pro single-block reference on the same 12 blocks.
