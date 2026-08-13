# Transaction boundary

Completion runs under the canonical `database.write_txn` (`BEGIN IMMEDIATE` and
the process writer lock). In that single transaction it:

1. loads the authorization;
2. compares token digest and validates typed state/TTL;
3. verifies the exact stored/request pair;
4. rejects Worker/instance conflicts;
5. inserts the exact approved Worker with intake still disabled and a
   conservative one-slot setting;
6. inserts polling transport ownership;
7. generates a new runtime token and inserts only its digest;
8. atomically marks the authorization consumed with request fingerprint and
   token ID;
9. appends the completion security event.

Three fault seams were injected before Worker insert, after Worker insert, and
after runtime-token insert. Every case left zero Worker rows, zero token rows
and a reusable pending authorization. Thus none of the prohibited half-states
survives rollback.

Security-significant rejections are appended in a separate transaction after
the failed completion has rolled back. Expiry is materialized there without
partially committing enrollment.
