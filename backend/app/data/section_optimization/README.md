# Section optimization data

This directory contains persistent runtime data for section-level optimization.

For every object and section the application stores:

- `snapshot.json` — the latest successfully prepared section optimization;
- `pipeline.json` — recalculation state;
- `history/*.snapshot.json` — immutable successful snapshot history;
- `replications/*.json` — persistent decision-replication processes and dossiers.

Runtime JSON files are intentionally excluded from Git. They remain on the
application server and are replaced atomically by the backend.
