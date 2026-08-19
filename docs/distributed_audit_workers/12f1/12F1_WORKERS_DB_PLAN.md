# 12F.1 workers.db plan

Production persistence is a new, separate SQLite database at
`/home/coder/projects/PDF-proverka/backend/app/data/distributed_workers/workers.db`.
It is not part of project/version trees and receives no 12B–12E test rows.

Creation must use `database.ensure_ready()` from the exact candidate with
`DISTRIBUTED_WORKERS_ENABLED=true`. No `touch`, raw DDL, copied test DB or
manual row import is permitted. The canonical path creates the directory set,
applies migrations 0→11 transactionally and opens WAL with foreign keys,
`synchronous=NORMAL` and a 5-second busy timeout.

The production process currently has umask `0002`, which would produce overly
broad permissions. Authorized Phase B must first run the canonical initializer
under umask `0077`, verify directory `0700`, DB/WAL/SHM `0600`, schema version
11 and `PRAGMA integrity_check=ok`, then start the same candidate under the
same restrictive umask.

Rollback retains this separate DB; old code ignores it. It is not deleted or
downgraded automatically. If it ever contains production rows, backups use the
SQLite backup API or `VACUUM INTO`, never a blind copy of a live WAL file.
