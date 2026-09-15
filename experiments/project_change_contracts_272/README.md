# Two-pair F1 / F4 / F2 implementation

Object 272, stage_1 → stage_2, logical v002; only the two selected DEV pairs.
The source files are shared read-only from the original checkout. All code changes
belong to the clean research worktree. No provider, inference, push, or deploy
command is included in this program.

`evidence.py` measures delivery per EvidenceRequirement. `packages.py` is a new,
versioned requirement-driven builder; frozen V1/V2 packages and assembly decisions
are not rewritten. Every new package embeds and writes `EVIDENCE_COVERAGE.json`.
Consumers must read that receipt; page coverage is insufficient. A COMPLETE
receipt certifies delivery of an explicitly source-audited page region, including
its full raster, not the truth of a proposed event. Native text alone cannot
certify diagram/table/footnote completeness. The builder preserves whole sections
under budget and explicitly reports omitted units and images.

The two-pair fixtures are a source-audit regression allowlist, not rules in the
retriever. This establishes bounded retrieval from known source requirements;
autonomous semantic scope discovery remains unproven. Each case's missing OLD or
NEW counterpart blocks direct-comparison readiness. Counter-evidence stays inside
the subject, document, and OLD physical version. Unadmitted linked pages remain
MISSING, including the linked questionnaire pages in C22.

Run locally, without model calls:

```sh
python -m unittest discover -s experiments/project_change_contracts_272 -t .
python -m experiments.project_change_contracts_272.regression \
  --phase f1 --source-checkout /home/coder/projects/PDF-proverka \
  --output /home/coder/auditmanager/corpus-audits/20260914_project_change_272/two_pair_f1_f4_f2/f1
```

F1 baseline: [F1_REGRESSION.md](F1_REGRESSION.md). Full JSON and per-package
receipts are in the output directory. PASS means no incomplete requirement was
certified COMPLETE; it does not require all retrieval to succeed. R25 and the
post-output corrections retain their original exposure labels. Source truth is
hash-checked before and after the replay.
