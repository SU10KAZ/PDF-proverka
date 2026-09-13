# TEXT comparison V1 (offline research)

Narrative-only vertical pipeline over the unchanged Semantic Foundation V3
LineLedger, PageModel, HeadingModel, FurnitureModel, CaptionModel and
BoundaryDecision. Nothing imports this experiment from the production pipeline.

`sections.py` owns narrative lines, retains local external references, and keeps
weak-heading ownership uncertainty visible. Section keys use normalized semantic
heading paths. Document versions and page references are separate provenance.
Repeated ancestors resume their direct-text section; children remain separate
owners without duplicating their content into parents. Duplicate semantic keys
are REVIEW. No implicit parent aggregation feeds the fact comparator.

`relations.py` supports unique deterministic matches, exact-clause split/merge,
explicit absence only with complete opposite coverage, and unresolved components.
An optional local AI contract validates quotes, references and hashes but never
calls a provider or promotes model adjudication to human truth.

`facts.py` follows the existing Blueprint A / Text Fact Owner discipline of
explicit fields, local owners and source witnesses. Its production table producer
is incompatible with this narrative route and is not called. Numeric values,
equipment counts, fan-coil type, material, fire rating, modes and requirements
are extracted conservatively. Fuzzy fact matches and unexplained wording changes
remain REVIEW. `native.py` retrieves targeted numeric/symbol witnesses; it never
replaces Markdown or accepts native witnesses as automatic facts.

Run checks:

```sh
python -m unittest experiments.text_comparison_v1.test_pipeline experiments.semantic_foundation_v3.test_foundation
```

The CLI `python -m experiments.text_comparison_v1.run` has `prepare`, `run`, and
`replay` commands. Outputs must use a new directory. `dev_score` accepts only the
pinned final DEV truth and filters SECTION/OWNER before scoring. It never reads
old evaluation answers. Wall time/RSS live outside deterministic artifacts.
Context figures use UTF-8 bytes / 4 as an explicitly labelled estimate, not an
exact tokenizer or billed tokens. Model calls and billed tokens are zero.

Artifacts and final limitations:
`/home/coder/auditmanager/corpus-audits/20260913_text_comparison_v1/reports/`.
The final source-independent relation holdout is selected only after code freeze;
no annotations or tuning on its results are part of this task.
