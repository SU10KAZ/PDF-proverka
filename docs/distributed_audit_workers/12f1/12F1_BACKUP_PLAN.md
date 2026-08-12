# 12F.1 backup plan

Phase A performed no production backup write because no production mutation is
authorized. Phase B may proceed only after the baseline/candidate blockers are
resolved.

Required final pre-restart backups:

1. **Previous code release.** Freeze a clean, reviewed immutable release that
   reproduces the supported production behavior. The current commit plus disk
   state is insufficient because runtime modules changed after process start.
   Record release archive SHA-256 and restore launcher. This item is currently
   **not ready**.
2. **Configuration.** Take a mode-0600 byte-for-byte backup of `.env`, record
   size and SHA-256, and verify it before changing permissions/adding only the
   planned distributed fields. Restore is atomic replacement followed by the
   previous launcher.
3. **workers.db.** It is currently absent, so there is no pre-enable DB to
   copy. After canonical initialization, verify schema/integrity and take a
   SQLite backup/VACUUM snapshot before any later migration. Never copy a live
   DB without coordinating WAL.
4. **Existing AuditManager data.** No schema or content mutation is planned.
   Record the existing root/path inventory and verify representative hashes;
   do not pause or copy multi-hundred-GB project trees merely for a code-only
   restart.

Backup plan readiness is `NO` until item 1 has an exact immutable rollback
release and launcher.
