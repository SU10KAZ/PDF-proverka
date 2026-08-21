# Stage 4.1 diagnostics

Read-only audit of Stage 4 session `121d764109184c13`, pair `p570d156f57` for
project `272_Sadovnicheskaya_76_Balchug_Esteyt` (`13АВ-РД-АР0.1-ПА_V2.pdf` ↔
`13АВ-РД-АР0.1-ПА_V3.pdf`). The experiment reconstructs reviewer inputs from
the deterministic artifacts and joins them to the stored model proposal,
validator policy and final verdict. It never calls a model, never rewrites the
production result, and writes only below this directory.

Run from the repository root:

```bash
python experiments/stage_comparison_text_ai_reviewer_diagnostics/analyze.py run
python experiments/stage_comparison_text_ai_reviewer_diagnostics/analyze.py verify
```

`human_review_annotations.json` contains diagnostic manual annotations only.
They are not production ground truth and are not imported by application code.
Historical model durations/tokens are exact persisted values. Python/IO phase
timings are explicitly labeled median read-only replays because Stage 4 did
not persist those timestamps.
