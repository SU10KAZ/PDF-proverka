# ProjectChange UI V2.1 — simplify workflow

STATUS: READY FOR VISUAL REVIEW. Production deployment is not part of this change.

BASE COMMIT: `9da04632b18d3c974cb0e984052c2a720786830e` (published V2 plus smoke harness follow-up).

The candidate is committed on `main` in the isolated clone `/tmp/project-change-ui-v2-1`. The task-specific clean-clone instruction takes precedence over the repository's usual shared-main workflow. The shared checkout contains unrelated work and was not edited. The exact new commit is recorded in the accompanying `REPORT.md` and `candidate.json` after commit creation.

Review artifacts: `/home/coder/auditmanager/preview-preparation/20260914-project-change-ui-v2-1/`.

## Changed files

| File | Change |
| --- | --- |
| `frontend/index.html` | Remove preview service text on Page 1, use a directional arrow under the feature flag, wire the Page 3 empty-state button to Page 1. |
| `frontend/static/css/project-change-ui.css` | Compact paired documents and Page 3 summary; remove obsolete filter/selector styles. |
| `frontend/static/js/app.js` | Remove Page 3 pair-selection handler; reject PDF navigation to another pair. |
| `frontend/static/js/project-change-ui.js` | Keep the current-pair table, four counters and details; remove filters, pair selector and all-pairs scope; add no-pair empty state. |
| `frontend/tests/project_change_table.test.js` | Update state/component tests for the required workflow and cross-pair navigation guard. |
| `frontend/tests/project_change_table.browser.cjs` | Exercise Page 1 selection, Page 2/3 continuity, absent controls, all empty states and retained evidence actions; capture review screenshots. |
| `docs/project_change_ui_v2_1.md` | This implementation and verification report. |

## Page 1 changes

The entire `RESEARCH / PREVIEW … Доступен просмотр исходных PDF.` status line is absent from read-only preview DOM. It has no replacement notice. The existing common preview status above the tabs remains.

OLD → NEW documents form a compact group with controls on the right. At the 1600 × 1100 desktop viewport, measured document separation changes from 20 to 16 px; document widths change from 510 to 418 px; row height changes from 53 to 48 px. The document columns are capped at 420 px to bring filenames closer. Padding and grip spacing are reduced. All 26 real filenames remain fully visible at this viewport. At 1280 px the layout retains both sides, full filename tooltips and bounded row height without page overflow.

## Current pair state approach

The existing `scActivePair.id` is the single current-pair identity for Pages 2 and 3. Page 1 continues using the existing pair-open path, canonical registry and exact document paths; no new filename heuristic or duplicate selection store is introduced. Existing saved-pair restoration survives reload and restores the same pair ID.

Page 3 receives `scActivePair?.id || ''` and always uses the existing `ProjectChangeView.inPair` binding validation. Unknown, stale or absent selection produces no rows, never a first-pair fallback. The empty state reads:

> Сначала откройте пару документов на вкладке «Загрузка документации».

Its `Перейти к загрузке документации` button only sets the active tab to Page 1. It makes no pair request or selection. A selected pair with zero changes retains four zero counters and the existing pair-specific empty message.

The Page 3 `pcSelectPair` handler and its event are removed. Evidence navigation checks that its explicit target pair is already active; it cannot automatically open another pair. Exact PDF path/version/page checks and the return anchor remain intact.

## Page 3 changes

Normal read-only presentation consists of the heading, four current-pair counters and the V2 table. The counters are **Всего изменений**, **Нужно проверить**, **Конфликты**, **Высокая важность**. The table starts less than 75 px below the heading in the browser check.

All five filter controls, reset action, pair dropdown, document-pair toolbar and all-pairs switch are removed from the component presentation and state. No 73-event aggregate is rendered. Semantic fields and filtering/binding helpers in `project-change-view.js` are unchanged.

The ten table columns, stable IDs, single-row expansion, full states, evidence, crops, enlarge dialog, PDF links, review questions, characteristics and conflicts remain. The expanded `<article>` template is byte-identical to the base commit. Page 4 remains empty with disabled export actions and no persisted human decisions.

## Tests and build

- Frontend: **85 tests passed** across the four ProjectChange suites, including 19 component/navigation tests.
- Backend regression: **23 tests passed** in `tests/test_project_change_bridge.py`; the backend implementation is unchanged.
- Browser smoke harness regression: **1 passed**, including mutation classification, legacy requests and observed browser errors.
- Build: **PASS**, `npm --prefix frontend run build`, after installing the clone's locked dependencies with `npm ci`.
- Additional distributed typecheck: **pre-existing failure** at `frontend/static/js/distributed-feature.js:511:54`, TS2339 (`slice` on `() => any`). Reproduced using the unmodified files and config extracted from base commit `9da04632`; it is outside the changed ProjectChange UI. Both logs are included.
- `git diff --check`: **PASS**.

## Browser smoke

**23 scenarios passed** in isolated Chromium. The server bound only to `127.0.0.1:8993` and used the unchanged real read-only preview router with bundled snapshot assets. Surrounding portal metadata and legacy session creation use the existing local smoke fixture. Browser requests to other origins are blocked by the existing harness. Production was not exercised.

Coverage includes both desktop widths; selection through Page 1 for 1-, 10- and 15-change pairs; exact row IDs and four pair counters; Page 2 continuity; reload restoration; absent Page 3 controls; row/keyboard expansion; characteristics; raster decode; enlargement and Escape; PDF page/version/overlay and return; missing OLD; empty Page 4; absent and zero-change pair states; object/flag legacy controls.

Observed preview requests: **POST=0, PUT=0, PATCH=0, DELETE=0**. There are zero preview calls to legacy session APIs, zero persisted decision keys, zero failed preview API responses, zero application JavaScript errors and zero harness errors. Legacy session POST is separately observed and allowed only outside the preview gate. The preview envelope before and after the workflow is identical.

## Screenshots

All paths are relative to the review artifact directory above:

- A: `A-page1-compact-pairs.png` — Page 1, compact OLD → NEW, service line removed.
- B: `B-page3-one-change.png` — Page 3, ИОС1.1, one ProjectChange.
- C: `C-page3-multiple-changes.png` — Page 3, ИОС2.1, ten ProjectChanges.
- D: `D-expanded-old-new.png` — expanded PC-004 with both source documents.
- E: `E-page3-no-current-pair.png` — Page 3 without selection.
- Additional: `enlarge-exact-crop.png`, `pdf-deep-link-overlay.png`, `missing-old-binding.png`, `page4-empty.png`, `page2.png`, and legacy controls.

Screenshots A–E were visually inspected. Capture disables transient CSS animations so the active tab is shown correctly. Logs, `browser-results.json`, `page1-layout.json`, the baseline layout and `scope-integrity.json` accompany the images.

## Reproduce locally

```sh
npm --prefix frontend ci --no-audit --no-fund
npm --prefix frontend test -- --maxWorkers=2 --minWorkers=1 tests/project_change_view.test.js tests/project_change_bridge.test.js tests/project_change_autosave.test.js tests/project_change_table.test.js
python -m pytest tests/test_project_change_bridge.py -q
npm --prefix frontend run build
python scripts/project_change_ui_smoke_server.py --port 8993
# Separate terminal, using the existing local Playwright installation:
NODE_PATH=/tmp/project-change-ui-tools/node_modules CHROME_BIN=/home/coder/.cache/ms-playwright/chromium-1228/chrome-linux64/chrome SMOKE_BASE=http://127.0.0.1:8993 node frontend/tests/project_change_table.browser.cjs
NODE_PATH=/tmp/project-change-ui-tools/node_modules CHROME_BIN=/home/coder/.cache/ms-playwright/chromium-1228/chrome-linux64/chrome node --test frontend/tests/project_change_smoke_harness.node.cjs
```

## Scope integrity

All 29 manifest-listed snapshot files match their expected SHA-256. Manifest SHA-256 remains `37a76c86508aca5b2dbbffca58aac6a23e88bc723c031fd78ad1cfd58df87dd7`. The backend, algorithms, research files, presentation contract and stable IDs have no diff from the base commit. No research services, VALIDATION, FINAL HOLDOUT or OpenRouter requests were used.

| Required flag | Value |
| --- | --- |
| UI CODE CHANGED | YES |
| BACKEND PREVIEW CHANGED | NO |
| SNAPSHOT CHANGED | NO |
| ALGORITHMS CHANGED | NO |
| RESEARCH TOUCHED | NO |
| PRODUCTION DEPLOYED | NO |
