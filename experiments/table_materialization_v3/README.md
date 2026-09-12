# Table materialization V3

Offline research only. The Foundation ledger, semantic sections, page routing,
SHEET payloads and Foundation decisions are preserved byte for byte. The new
`tables.json` artifact contains logical table components, retained row references,
all table boundary evidence and unresolved candidate relations. No backend imports
this package.

The sole tuning truth is the 44-case TABLE slice of QA-final human DEV truth,
SHA-256 `71ed12f693442665a64c073a56f3d13df321bfdd0303f4a378286437616906d7`.
The immutable slice, source hashes, baseline and complete iteration records live at
`/home/coder/auditmanager/corpus-audits/20260913_table_materialization_v3/`.
Old evaluation answers are not loaded. Source-only conservation controls are
separate from human calibration.

Every boundary collects all predicates. Conflicting candidate evidence always
abstains, including when one side lacks enough DEV support. A predicate can
produce PROVEN only in an explicitly promoted structural stratum: at least five
human cases from two documents and at least 90% precision. The checked-in policy
currently promotes ordinal continuation with a semantic header across adjacent
pages (5/5) and a changed semantic contract across adjacent pages (8/8). Other
rules remain observable, including specification groups and non-adjacent explicit
table identities, but cannot silently inherit another stratum's gate.

Use the frozen policy by default:

```sh
python -m experiments.table_materialization_v3.run --documents DOCUMENTS.json --output NEW_DIRECTORY
python -m unittest experiments.table_materialization_v3.test_tables
```

`--observe` retains candidates without promotion. `--policy` is an explicit DEV
experiment override; it is not a production flag. `prepare` verifies Foundation
before DEV extraction. `evaluate` reads only the pinned DEV slice. `verify` runs
fresh-process benchmarks, byte replay and independent persisted row checks.
The experiment's `freeze` and `holdout` modules enforce candidate freeze before
structural holdout preparation. Never tune this candidate after holdout selection.

Row values are materialized with `model.row_values`, resolving raw cells through
the pinned source and ledger line. This preserves every cell and every DATA_LIKE
first row without duplicating the source text. Source segment ownership stays in
the ledger; logical tables reference segments rather than claiming lines again.
An unresolved component's key is deterministic, not a claim that its external
boundaries have been proved complete.
