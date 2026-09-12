# Human DEV truth QA before Table V3

Independent human annotation QA for the frozen 104-case export. It performs no
Foundation/Section/Table inference, prediction comparison, scoring, or tuning.
The existing export and annotation registry are read-only inputs.

Run from the repository root:

```sh
PYTHONDONTWRITEBYTECODE=1 /opt/py312/bin/python -m experiments.human_dev_truth_qa.server
```

Open **http://127.0.0.1:8769/**. This is a separate loopback-only service; the
original annotation service remains on 8768. The included user systemd unit
provides persistence across terminal closure. It does not deploy the portal.

## Integrity and selection

Startup verifies the exact expected packet and original export SHA-256, all 104
case IDs and answers, distinct submission IDs, revision continuity, latest
records, derived answer maps, the read-only SQLite freeze and original requests.
It verifies every receipt in the 21-document DEV manifest, the 17 referenced PDF
snapshots, source document identities, all 252 packet anchors and 126 page hash
pairs. Missing or mismatched inputs stop startup. Source parsing here reconstructs
only frozen coordinates and content hashes; no semantic algorithm is executed.

Ten cases are fixed in `packet.py`, selected from source content: 4 SECTION,
5 TABLE, 1 OWNER. Both original YES and NO are represented for SECTION and TABLE.
Selection includes title/front-matter transitions, long page spans and repeated
notes, equipment groups, changed table headers, different fragments on one page,
and an ownership question between equally numbered headings. Human labels were
used only for coverage and analogous cases answered differently. No prediction or
model-agreement field was used. The private selection rationale is never served
by HTTP. Cases use separate QA IDs and a fixed reordered presentation.

The browser receives an explicit source-only allowlist. There are no original
answers, predictions, strata, mapped labels, diagnostics or expected answers in
the blind packet. Source fragments retain their exact focus and surrounding
text; the full page text, PDF, rendered pages, previous/next and physical page
input are available. All answer buttons are exactly: «Да», «Нет», «Не могу
определить», «Проблема с примером».

## State, comparison and final human decision

Live artifacts are separate from the original truth, under:

```text
/home/coder/auditmanager/corpus-audits/20260913_human_dev_truth_qa/
```

| Artifact | Purpose |
| --- | --- |
| `EXPORT_VERIFICATION.json` | Initial integrity audit and source hash receipts |
| `BLIND_QA_PACKET.json` | Immutable source-only UI packet |
| `QA_SELECTION_PRIVATE.json` | Immutable original-to-QA mapping and selection rationale; not served |
| `qa.sqlite3` | Authoritative append-only event log and immutable freezes |
| `QA_PROVENANCE.jsonl` | Append-only, SHA-256 chained event export |
| `DEV_HUMAN_TRUTH_WAVE1_QA.json` | Recoverable partial QA view, then immutable after all 10 responses |
| `QA_COMPARISON.json` | Comparison and decision view, created only after all 10 responses |
| `DEV_HUMAN_TRUTH_WAVE1_FINAL.json` | Created only after human resolution and explicit human freeze |

Each blind answer is committed once. UUID replay is idempotent; UUID reuse with
changed content and stale/concurrent writes are rejected. Client drafts and the
pending UUID survive reload. Lost acknowledgements safely replay the same
request. No synthetic responses are ever submitted to the live registry.

Before all ten responses, match counts and original answers are absent from every
available HTTP response. The comparison route and final export return 409. After
completion, only disagreements appear as human decision tasks. Equal answers
are `CONSISTENT`; different answers, including UNSURE/BROKEN, are
`NEEDS_HUMAN_REVIEW`. UNSURE/BROKEN are also reported as separate counts, so those
counts overlap the disagreement count. Nothing replaces the original truth.

Final human decisions have append-only revisions and refer to the immutable QA
SHA-256. UNSURE/BROKEN decisions remain unresolved and block final freeze. Even
if all blind answers match, a human must explicitly click the final freeze
button. The final export contains all 104 case records: the original answer,
optional blind answer, final answer, and provenance for every actual human
change. The 94 unselected answers are carried forward from the frozen original.
The final `answers` map uses YES/NO; this is a new QA schema, not an automatic
scorer import. The full chained history links the original frozen SHA-256, blind
packet/selection, blind responses, QA freeze, human resolutions and explicit
human finalization. Final and QA freezes receive SHA-256 sidecars. No algorithm
development is launched by finalization.

SQLite transactions use synchronous FULL and reject UPDATE/DELETE of event,
identity and freeze rows. Export publication is recoverable after restart.
Frozen files and provenance prefixes are checked before publication; a detected
modification stops publication. Original truth SHA-256 is checked on every save.

## Verification

```sh
PYTHONDONTWRITEBYTECODE=1 /opt/py312/bin/python -m unittest experiments.human_dev_truth_qa.test_qa -v
PYTHONDONTWRITEBYTECODE=1 /path/to/playwright/python -m unittest experiments.human_dev_truth_qa.test_browser -v
node --check experiments/human_dev_truth_qa/app.js
```

Tests use disposable directories, including all simulated human responses and
finalization. Browser tests require Playwright and Chromium; `QA_CHROMIUM`
defaults to `/opt/google/chrome/chrome`. Optional `QA_SCREENSHOTS` writes test
screenshots outside the checkout. Skipped browser tests are not a browser pass.
