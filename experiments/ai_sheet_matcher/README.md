# AI Sheet Matcher research harness

This package is deliberately outside the production Stage Comparison service.
It reads frozen pair artifacts and source PDFs/markdown, creates a bounded
selector contract, runs two identical independent passes over three cold runs,
and verifies every output fail-closed.

Run candidate recall first:

```bash
python -m experiments.ai_sheet_matcher.run candidate-audit
```

Only after inspecting that artifact, run the two model arms:

```bash
python -m experiments.ai_sheet_matcher.run experiment \
  --model gpt-5.6-sol --effort medium --workers 2
```

Derived metrics/reports can be rebuilt from the saved bounded outputs without
another model call:

```bash
python -m experiments.ai_sheet_matcher.run finalize
```

The experiment never writes to `comparison/sessions`, does not import into the
web application, and has no deployment command.
