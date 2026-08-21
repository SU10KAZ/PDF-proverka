# Stage 4 semantic reviewer benchmark

This experiment answers a different question from the Stage 3 benchmark: can
a text-only model inspect exact П/РД source fragments plus a deterministic
preliminary classification and safely correct semantic mistakes?

The committed dataset has 27 sheet-sized groups (13 extracted from the current
Balchug Estate project and 14 controlled adversarial groups), independent
hand-authored ground truth and no production dependency on that truth. Each
model receives identical group order, batching, prompt, native JSON Schema and
`medium` effort in both `with_hint` and `without_hint` modes.

Run from the repository root:

```bash
python experiments/stage_comparison_text_ai_reviewer/benchmark.py build
python experiments/stage_comparison_text_ai_reviewer/benchmark.py run
python experiments/stage_comparison_text_ai_reviewer/benchmark.py summarize
```

`run` resumes only completed batches with the current dataset and structured
output contract. Use `--no-resume` to replace a model/mode RAW run. Results are
stored under `artifacts/runs/`; `benchmark_summary.json` and
`BENCHMARK_REPORT.md` are derived and reproducible.

Selection first requires a useful reviewer (accuracy above deterministic,
zero JSON failures, more corrections than harm). It then follows the safety
priority: accepted false SAME/MOVED, factual accuracy, harmful reclassification,
value/provenance/hallucination and JSON failures, then raw unsafe proposals,
latency and tokens. Accepted and raw proposal metrics are both reported because the
production validator can prevent a model proposal from becoming a grey mask.
