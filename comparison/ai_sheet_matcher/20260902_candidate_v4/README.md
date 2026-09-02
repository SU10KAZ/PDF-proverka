# Candidate Generator v4 research artifacts

Offline deterministic benchmark for `research-candidate-generator.v4` over the frozen IOS 1.1,
IOS 3.1 and IOS 2.1 corpus from research commit `41d43625`, with forensic audit cases
from `dbddb691`.

Regenerate without model calls:

```bash
python -m experiments.candidate_v4.run
```

The command writes only this research directory. It does not import into production,
change mappings, call a model, deploy, or push.
