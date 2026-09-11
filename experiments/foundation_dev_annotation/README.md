# Simple first-wave DEV annotation

Run from the repository root:

```sh
python -m experiments.foundation_dev_annotation.server
```

Open **http://127.0.0.1:8768/**. The service binds only to this loopback address;
the CLI has no host or port override. Ports 8766 and 8767 are not used.
The runtime requires the existing PyMuPDF dependency. No frontend build is needed.

This package only reads the frozen packet and verified source snapshots. It does
not import Foundation, the scorer, EVAL registries, or production HumanDecision.
The original Foundation UI and its artifacts remain untouched.

## Human questions and mapping

Only 104 human cases appear. The 22 automatic controls are included in a separate
export collection and are never accepted by the answer endpoint.

| Question | Да | Нет | Не могу определить |
| --- | --- | --- | --- |
| Same semantic section? | SAME | NEW | UNSURE |
| Same continuing table? | SAME | NEW | UNSURE |
| Fragment belongs to the shown owner? | OWNER_FOLLOWING_HEADING | OWNER_PRECEDING_SECTION | UNSURE |

For all eight ownership cases, the second **unchanged frozen anchor** is the
following heading. It is explicitly shown on the right as «Показанный раздел».
The preceding heading is additional source context on the left. The NO mapping
preserves the original packet's two-candidate ownership schema; uncertainty is a
separate valid answer. Mapping uses no Foundation predictions or confidence.
The export retains the simple human answer as well as its derived schema value.

«Проблема с примером» stores `case_state=BROKEN_CASE`,
`review_state=NEEDS_REVIEW`, a reason and an optional note. Its `human_answer` and
`mapped_answer` are null; it is separately included in `problem_cases`. It never
becomes a YES/NO label. UNSURE retains its own human and mapped answer and count.

The primary UI uses plain Russian questions. Internal IDs, anchors, schema and
saved revision metadata are confined to collapsed «Диагностика». Predictions are
not sent to the browser. Exact source focus and surrounding text are displayed,
with expanded page text and PDF image available. PDF links use approved source
IDs and physical `#page=N` jumps. File paths are never accepted in URLs.

## Persistence and recovery

The independent state directory is:

```
/home/coder/auditmanager/corpus-audits/20260911_foundation_v3_simple_annotation/FOUNDATION_V3_DEV_WAVE1/
```

`local-session.json` fixes the server-side annotator to the local OS account on
first startup (on this workstation, `local:coder`). The UI cannot supply or change
the author. Once a registry exists, changing its configured author, namespace,
schema or packet hash prevents startup. This is one fixed local annotation
session, not a shared multi-user login service.

`wave1.sqlite3` is authoritative. SQLite WAL with `synchronous=FULL` commits each
answer in one transaction. Revision rows are append-only with database triggers
rejecting UPDATE/DELETE. Every save has a unique submission UUID and the previous
case revision. A replay of the same request returns its original receipt; reuse
with different content and concurrent stale edits are rejected. Server restart
retains answers, receipts and history.

Browser localStorage contains only drafts, keyed by namespace, packet hash and
case. Save becomes pending before the request is sent. A failed request retains
the selection, disables repeated manual submission, and checks the server's
receipt before replaying exactly the same request. Reconciliation refreshes the
server token after restart. Requests time out after eight seconds; automatic
recovery makes at most three attempts, followed by «Проверить сохранение».
Drafts are removed only after confirmation and an authoritative state reload.
If localStorage is unavailable, saving still works; a failed request explicitly
tells the user to keep that window open because the draft cannot survive closure.

`GET /api/export` downloads `DEV_HUMAN_TRUTH_WAVE1.json` at any time. It always
contains 104 case records (unanswered records have null values and revision 0),
source anchors, packet hash, schema version, author, current revisions, history,
separate boundary/ownership maps, and separate automatic controls. The annotation
namespace is `FOUNDATION_V3_DEV_WAVE1`; the packet's original namespace remains
recorded separately. This export is deliberately not fed into the frozen scorer.

After the 104th answer, the same database transaction stores an immutable final
export. The service publishes the JSON and SHA-256 receipt using fsync and atomic
replacement, and the browser starts a download. A restart repairs publication if
the process stopped after the transaction committed. The UI then stops and the
server rejects further new revisions. No scoring, tuning or Table V3 work runs.

## Local service

`foundation-dev-wave1.service` can be installed as a separate **user** systemd
service; it starts this package from the checkout and only listens on 8768:

```sh
install -m 644 experiments/foundation_dev_annotation/foundation-dev-wave1.service \
  /home/coder/.config/systemd/user/foundation-dev-wave1.service
systemctl --user daemon-reload
systemctl --user enable --now foundation-dev-wave1.service
```

This does not deploy or restart the production portal or the existing annotation
services. State and reports live outside the Git checkout. No push is required.

## Verification

```sh
python -m unittest experiments.foundation_dev_annotation.test_annotation -v
python -m unittest experiments.foundation_dev_annotation.test_browser -v
```

Browser tests require Playwright and Chromium in a test environment; set
`WAVE1_CHROMIUM` to an existing Chromium executable if needed. Optional
`WAVE1_SCREENSHOTS` saves visual checks. Browser tests skip explicitly if Playwright
is unavailable; a skip is not evidence that browser checks passed.

All test answers use disposable registries outside the live state directory.
Tests cover all nine semantic answers, all broken-case reasons, draft reload,
browser close with empty browser storage, service restart, duplicate submissions,
lost requests/acknowledgments, concurrent edits, author/namespace isolation,
PDF allowlisting/ranges/page jumps, final atomic freeze and download. Frozen packet
hash and DEV/EVAL separation proof inputs are checked read-only. The accompanying
readiness audit compares before/after hashes of protected Foundation files,
predictions, packet, old registries and frozen truth manifests without reading
their answer contents.
