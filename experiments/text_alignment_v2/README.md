# TEXT Alignment V2 — offline research

Content-first paragraph/list retrieval over the existing Foundation routing and
TextSection metadata. TextSection and SectionRelation never gate local matching.
Tables, graphics and furniture use the unchanged source exclusions. Narrative
units retain document/page/block/line hashes and paragraph character spans.

Three retrieval experiments share a conservative, separate fact comparator:
sequence matching, engineering anchor postings, and hybrid lexical/template
retrieval with local uniqueness certificates and adjacent split/merge spans.
Hybrid wins the DEV source-derived contract comparison (not human alignment
truth): 1811/1881 relations, no false contract alignments/engineering changes.
Sequence: 1166/1881; engineering anchors: 375/1881. All three recognize 19/69
constructed numeric interventions; scope/grammar uncertainty stays REVIEW.

No provider calls or model-generated alignment labels. Lexical retrieval alone
never certifies semantic identity. Unmatched facts never imply addition/removal.
Full assertion templates and local scope evidence are required for automatic
engineering output. Unsupported rewordings, ambiguous quantities and changed
entity identifiers remain REVIEW. Existing engineering lexicons are reused,
including their limitations; no diagnostic values are used in development.

```sh
python -m unittest experiments.text_alignment_v2.test_alignment
python -m experiments.text_alignment_v2.experiment --root OUTPUT --foundation-root BASELINE
python -m experiments.text_alignment_v2.run freeze --root OUTPUT
python -m experiments.text_alignment_v2.run run --root OUTPUT --name run1
python -m experiments.text_alignment_v2.run run --root OUTPUT --name run2
python -m experiments.text_alignment_v2.run replay --root OUTPUT
```

Freeze checks every imported research/Foundation Python file and the project
input manifest. Performance telemetry is excluded from byte-identical replay.
Artifacts and checkpoint:
`/home/coder/auditmanager/corpus-audits/20260913_text_alignment_v2/reports/`.
No production imports, flags, Table/Sheet changes, truth edits, push, or deploy.
