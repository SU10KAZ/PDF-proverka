# ProjectChange preview UI V2 — pair-scoped table

STATUS: Implemented; local review candidate. Production deployment requires a separate instruction.

BASE COMMIT: `03774dff2a5ec9a87b1ded699bf09659bf14eeac` (verified against actual `origin/main` on 2026-09-14).

Development uses an isolated clean clone because the shared checkout contains unrelated local work. This follows the task-specific clean-clone instruction; the shared checkout and active processes are untouched. The candidate is committed on the clone's `main`, with no push or release.

## Changed files

- `frontend/index.html`: pass canonical pair state and PDF-return target to the list.
- `frontend/static/js/app.js`: select from the existing pair registry through `scOpenPair`, returning to Page 3; surface load errors and reject stale object responses.
- `frontend/static/js/project-change-view.js`: validate complete evidence-to-pair bindings, derive presentation IDs, scope rows, normalize display labels and shorten text without inferring states.
- `frontend/static/js/project-change-ui.js`: compact table, scope switch, pair picker, scoped counts/filters and expandable details.
- `frontend/static/css/project-change-ui.css`: desktop density, sticky table header, bounded table scrolling, evidence layout and responsive overflow.
- `frontend/tests/project_change_table.test.js`: component/state, binding and navigation tests using only the release-bundled presentation.
- `frontend/tests/project_change_table.browser.cjs`: local browser acceptance and screenshots.
- `scripts/project_change_ui_smoke_server.py`: isolated test shell with the real read-only preview router and frozen PDF rendering.
- `docs/project_change_ui_v2.md`: this report and reproduction instructions.

## Pair binding approach

The production API already supplies `viewer_session.pairs` and `evidence[].pair_id`. Page 1, Page 2 and Page 3 use `scSession.pairs` / `scActivePair.id`; there is no separate selected-pair state or filename matching.

For each ProjectChange, the view adapter examines **every** evidence entry, resolves its explicit pair ID, checks its OLD/NEW side, and compares its exact PDF path and version against that side in the pair registry. It derives the complete set of pair IDs without changing any source item, identity, binding signature or decision key. Invalid or incomplete bindings fail closed for pair filtering and remain visibly marked in the all-pairs view. A multi-pair event would appear once in each applicable pair scope and list all its document pairs in the overview.

The pre-implementation audit covered all 73 events and 283 evidence entries: 73 valid single-pair bindings, zero invalid or multi-pair events. The API exposes 13 selectable pairs; 9 contain changes, 4 have no changes in this snapshot. Absence of OLD evidence in 59 events does not prevent document-pair binding: the pair's OLD document is known, but an OLD fact/evidence binding has not been established.

| Cipher | Changes |
| --- | ---: |
| ИОС2.1 | 10 |
| ИОС3.1 | 10 |
| ИОС4.1 | 15 |
| ИОС4.2 | 16 |
| ИОС4.3 | 15 |
| ИОС1.1 | 1 |
| ОДИ | 2 |
| ООС1 | 3 |
| ПЗУ | 1 |

The full 73-row audit, including both documents and all evidence identities, is saved at `/home/coder/auditmanager/preview-preparation/20260914-project-change-ui-v2/pair-binding-audit.json`.

## Page 3 table design

Default scope is **Текущая пара**. Direct entry without a current pair displays **Выберите пару документов**; selecting an option opens the same canonical pair used by the existing sheet viewer. Selecting a known pair with no changes displays an explicit empty state. **Все пары объекта** is a secondary scope with a document-pair column.

Columns: ID, importance, cipher/discipline, system, type, change summary, OLD → NEW, sources, human workflow status, expand. One row represents one ProjectChange. `PC-001`–`PC-073` are display numbers from immutable envelope order; backend IDs remain row keys and PDF-return anchors. Display numbers are never recomputed after filtering.

Rows use the supplied human summary. Generic summaries and long state descriptions already present in the frozen contract are retained; the UI does not generate new engineering interpretations. OLD and NEW have separately truncated areas so a long OLD cannot hide NEW. Full states are available in details and tooltips. Importance remains the real HIGH/NORMAL contract, with no invented medium tier.

## Detail view design

Click a row or use the keyboard-accessible expand button. A single detail row holds the existing full card: states, review question and rationale, conflicts, evidence, changed characteristics, and disabled decisions. Crops are mounted only while the event is expanded. OLD evidence stays left and NEW right. Missing OLD evidence has an explicit unestablished-binding message. PAGE_LEVEL images explicitly identify the whole-page fallback. Enlargement, Escape dismissal, PDF document/version/page navigation, exact region overlay and return to the same expanded event are preserved.

Human status remains **Нужно проверить** or **Конфликт источников** in the read-only preview. Research confidence appears inside details with explicit separation from engineer confirmation. The 14 research PROVEN events do not populate Page 4.

## Filters

Cipher/discipline, engineering system, human-readable change type, human workflow status and source. Filters operate after scoping to the current pair; options and denominators are also scoped. For example, the SYSTEM filter in ИОС4.1 shows **1 из 15**, not a count out of 73. Changing pair or scope clears stale filters and expansion. UI state is in memory; no decision persistence is added.

## Tests, build and browser smoke

- Frontend: **89 tests passed** across `project_change_view`, `project_change_bridge`, `project_change_autosave`, and `project_change_table`.
- Backend: **23 tests passed** in `tests/test_project_change_bridge.py`, covering the gate, exact immutable envelope, all read-only decision methods, all 55 unique crops, source drift and isolation. Backend implementation is unchanged.
- Build: `npm --prefix frontend run build` passed.
- Isolated Chromium: **16 scenarios passed**, including pair navigation, all 73 rows, scoped filters/counts, lazy evidence, keyboard expansion, all evidence source badges, enlargement, PDF overlay and return, missing OLD, empty Page 4, direct entry and legacy gates.
- Browser observed zero preview mutation requests, zero decision-storage keys, zero legacy stage-session requests in preview, zero failed preview responses and zero JavaScript runtime errors.
- Smoke runs against localhost with the real production preview router and frozen PDF assets; the surrounding portal shell is a minimal local fixture. External browser requests are blocked. Production is not exercised.

Reproduce from the candidate clone (using installed frontend dependencies and Playwright):

```sh
npm --prefix frontend test -- --maxWorkers=2 --minWorkers=1 tests/project_change_view.test.js tests/project_change_bridge.test.js tests/project_change_autosave.test.js tests/project_change_table.test.js
python -m pytest tests/test_project_change_bridge.py -q
npm --prefix frontend run build
python scripts/project_change_ui_smoke_server.py --port 8991
# Separate terminal, NODE_PATH pointing at an existing Playwright install:
NODE_PATH=/tmp/project-change-ui-tools/node_modules SMOKE_OUTPUT=/tmp/project-change-table-smoke node frontend/tests/project_change_table.browser.cjs
```

## Screenshot paths

All artifacts are in `/home/coder/auditmanager/preview-preparation/20260914-project-change-ui-v2/`:

- A: `A-pair-table-collapsed.png`
- B: `B-expanded-old-new.png`
- C: `C-all-pairs.png`
- D: `D-filtering.png`
- E: `E-missing-old-binding.png`
- Additional: `enlarge-exact-crop.png`, `pdf-deep-link-overlay.png`, `page4-empty.png`, `direct-entry-selected.png`.
- Logs: `frontend-tests.log`, `backend-tests.log`, `build.log`, `browser.log`, `browser-results.json`.

## Scope integrity

The snapshot manifest SHA-256 remains `37a76c86508aca5b2dbbffca58aac6a23e88bc723c031fd78ad1cfd58df87dd7`. Every bundled snapshot file is compared before/after. No research JSON, validation, final holdout or external AI endpoint is used.

ALGORITHMS CHANGED: NO

SNAPSHOT CHANGED: NO

RESEARCH TOUCHED: NO

PRODUCTION DEPLOYED: NO
