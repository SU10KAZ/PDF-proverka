# 12F.1 migration plan

There is no existing production relational DB to alter. Enablement adds one
separate `workers.db`; all project, audit, normative and comparison data remain
untouched.

For a new file, canonical migrations 1–11 run in order. Each version is one
explicit transaction together with its `schema_migrations` row. Migration 3
contains legacy-table restructuring, but a new production DB has no legacy
rows, so it cannot destroy production history. Reference initialization took
`0.006007 s` and ended at schema 11, WAL and integrity `ok`.

Code rollback does not reverse the DB. The old production release has no
distributed router and ignores the retained file. Any future DB containing
real history must be backed up consistently before later migrations.

No production migration has run in Phase A.
