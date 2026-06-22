# Реестр semantic conflicts: fix/audit-queue-rate-limit-md-resolution ↔ deploy/main-live

Дата: 2026-06-23 01:15 MSK  
Репо: `/home/coder/projects/PDF-proverka`  
Fix HEAD: `4b300dd` (`Покрыть v2-primary active version binding`)  
Deploy target: `deploy/main-live` = `cba2bcc`  
Integ ref: `integ/v2-cutover-on-deploy` = `5b0cca8`  
Merge-base fix↔deploy/integ: `91d60d3`

## Метод

Read-only сравнение в main-репо, без deploy/integ worktree, без merge/cherry-pick, без push:

- `git merge-base HEAD deploy/main-live`
- `git diff --name-status deploy/main-live..HEAD`
- `git diff --name-status integ/v2-cutover-on-deploy..HEAD -- <v2-files>`
- `git grep` ключевых symbols на `HEAD`, `deploy/main-live`, `integ/v2-cutover-on-deploy`

## Executive Summary

**Нельзя мержить fix-ветку целиком в deploy.** Fix branch всё ещё основана на `91d60d3`, а deploy/main-live уже содержит merge `cba2bcc` с большим reserc/deploy срезом. Wholesale merge/rebase в неправильную сторону несёт риск удалить deploy-фичи и файлы: `upload-folder` endpoints, read-canary, большой блок stage_comparison pipeline_v2, frontend tests/docs, deploy start script и др.

**Рекомендованный способ интеграции:** не брать ветку целиком; cherry-pick/rebase только новых commits после `f688422` поверх актуальной deploy/integ линии:

- `5af0cb5` — v2-primary audit entrypoints/output paths
- `c0a210d` — v2-primary version upload/create-version path
- `4b300dd` — v2-primary active version binding test

При cherry-pick этих трёх commits ожидаемые конфликтные области ограничены `audit.py`, `projects.py`, `manager.py`, `claude_runner.py`, `version_service.py`, `storage_write_facade.py`, stage runners. При wholesale merge список конфликтов/удалений огромный и небезопасный.

## Реестр конфликтов/рисков

| # | Символ / файл | Deploy/resecr состояние | Fix состояние | Риск | Рекомендация |
|---|---|---|---|---|---|
| 1 | Branch topology | `deploy/main-live` = `cba2bcc`, уже содержит интеграцию d60/a1/f688 + reserc | fix HEAD основан на `91d60d3` и не содержит deploy +208 как first-class history | Whole-branch merge может удалить/откатить deploy-only код и frontend/stage_comparison reserc-срез | Интегрировать только cherry-pick новых commits `5af0cb5 c0a210d 4b300dd` поверх deploy/integ; либо rebase fix на `cba2bcc` в отдельном scratch, не deploy worktree |
| 2 | `backend/app/api/routers/projects.py` — `/upload-folder`, `/upload-folder/precheck` | Есть в deploy; используются UI upload folder flow | В fix branch отсутствуют (видны как deletion в `deploy..HEAD`) | Whole merge сломает загрузку папок/новых проектов из UI | При cherry-pick `c0a210d` руками сохранить deploy upload-folder endpoints; применить только `_resolve_project_dir_for_version_api`, v2-only version endpoints и upload layout fixes |
| 3 | `backend/app/api/routers/projects.py` — read canary `list_projects(request)` | Deploy имеет opt-in read-canary через `read_canary.resolve_read_backend(request)` | Fix branch вернула простую `list_projects()` | Whole merge удалит canary/debug path | Сохранить deploy canary, если он ещё нужен. Новые v2-primary fixes не требуют удаления canary |
| 4 | `backend/app/services/storage/read_canary.py`, docs/scripts read canary | Deploy содержит read-canary модуль/docs/scripts | Fix branch показывает deletion относительно deploy | Потеря операционного canary tooling | Не мержить wholesale; если rebase, keep deploy version |
| 5 | `partial_gemma_allowed` (`gemma_gate.py`) | Deploy/integ reserc #17 удалил мёртвый partial gate; integ имеет fixup `5b0cca8` | Fix HEAD снова содержит функцию и вызов | При повторном wholesale merge может снова появиться semantic conflict/NameError или откат reserc #17 | Для cherry-pick новых commits риска нет (они не трогают `gemma_gate.py`). При full rebase keep deploy/integ state: dead partial call removed |
| 6 | `version_service.resolve_active_output_dir` | Deploy/integ содержит helper `resolve_active_output_dir(project_id)` | Fix HEAD не содержит его; `_version_output_dir()` использует `resolve_version_output_dir()` | При full merge может откатить deploy helper или изменить helper contract для reserc code | При cherry-pick `5af0cb5`, если конфликт в `findings_merge/runner.py`, сохранить deploy helper или убедиться, что replacement still v2-aware and tests cover it |
| 7 | `PipelineStageContext.run_subprocess` / `manager._make_stage_context` | Integ has d60/a1/f688 context, but not Workstream 1 subprocess delegation fix | Fix `5af0cb5` меняет `ctx.run_subprocess` на `_run_script_for_job()` | Если конфликт resolved старой deploy логикой, subprocess stages again miss `AUDIT_VERSION_DIR/AUDIT_OUTPUT_DIR` | Обязательно keep fix side for `ctx.run_subprocess`; verify `test_stage_context_subprocess_uses_job_scoped_runner` |
| 8 | `audit.py` batch retry/resume | Deploy ignores body `version_id` for `add-retry/add-resume`; retry/{stage} lacks query version for some paths | Fix passes `version_id` through router and manager | Если deploy side chosen, retry/resume under v2-primary may write latest/wrong version | Keep fix additions; verify `test_batch_retry_and_resume_preserve_version_id` |
| 9 | `claude_runner` stage signatures | Deploy stage runners call older signatures for findings/norms/optimization | Fix adds optional `output_dir/version_dir/version_id` kwargs to many runner functions | Conflict resolution must update both signatures and all callsites; missed callsite can silently fall back to latest/legacy `_output` | Keep signature extensions and stage-runner kwargs; run `tests/test_v2_primary_audit_entrypoints.py` |
| 10 | `findings_merge/runner._version_output_dir` | Deploy/integ comment says reserc #97 `resolve_active_output_dir` v2-aware | Fix fallback changed to `resolve_version_output_dir()` | Low/medium: both are intended v2-aware, but losing deploy helper could affect other reserc code | Prefer deploy helper if available, but ensure `AUDIT_OUTPUT_DIR` override remains first. Run findings merge tests after integration |
| 11 | `version_service.create_next_version` | Deploy/integ has v2 context from f688 but no Workstream 2 v2 create-version helper | Fix adds `_create_next_projects_v2_version()` and physical `vNNN` create path | Conflict likely around function line numbers; dropping fix means v2-only create-version endpoint 404/falls legacy | Keep fix v2 branch before legacy `project_dir.exists()` check; run `tests/test_v2_primary_version_upload.py` |
| 12 | `version_service.save_files_to_version` | Deploy writes upload target as `version_dir / filename` | Fix writes `01_input/<original>` + syncs `02_work/document.*` in `storage_layout=projects_v2` | Dropping fix puts uploads in wrong v2 root and prepare cannot find source | Keep fix. Verify no regression with `tests/test_version_file_upload.py` |
| 13 | `storage_write_facade._ensure_document_scaffold` | Deploy scaffold can create document without visible `versions/version_ids` | Fix fills `document.json.versions`, `version_ids`, standard dirs | Dropping fix makes newly written v2 documents partially invisible to read adapter | Keep fix; verify `test_storage_write_facade_scaffold_is_visible_to_v2_adapter` |
| 14 | `project_service.py` huge diff | Deploy has many reserc/upload/delete/root guard changes not present in fix branch | Fix branch older copy plus v2 source resolver changes | Whole merge is high risk: can remove deploy operational safeguards | Do not merge whole branch. Cherry-pick new commits avoids touching `project_service.py` except already integrated content |
| 15 | Stage comparison pipeline_v2 files/frontend tests/docs | Deploy contains/removes a large reserc shape different from fix; `deploy..HEAD` shows many deletes from fix perspective | Fix branch lacks many deploy files | Whole merge can resurrect/delete large unrelated stage-comparison surface | Treat as out-of-scope; preserve deploy state entirely |
| 16 | `frontend/static/js/app.js`, `frontend/index.html` | Deploy has reserc UI changes and known stage-comparison expectations | Fix branch older UI | Whole merge can regress UI unrelated to v2 cutover | No frontend changes from new commits; preserve deploy UI |

## Интеграционный чеклист для человека

1. Создать/обновить отдельный integration worktree от `deploy/main-live`/актуального deploy HEAD.
2. Cherry-pick only:
   - `5af0cb5`
   - `c0a210d`
   - `4b300dd`
3. При конфликтах сохранять deploy business/resecr logic, добавляя v2 behavior только за флагами.
4. Особо проверить файлы:
   - `backend/app/api/routers/audit.py`
   - `backend/app/api/routers/projects.py`
   - `backend/app/pipeline/manager.py`
   - `backend/app/pipeline/stages/findings_merge/runner.py`
   - `backend/app/pipeline/stages/findings_review/runner.py`
   - `backend/app/pipeline/stages/norms/runner.py`
   - `backend/app/pipeline/stages/optimization/runner.py`
   - `backend/app/services/llm/claude_runner.py`
   - `backend/app/services/common/version_service.py`
   - `backend/app/services/storage/storage_write_facade.py`
5. Минимальный post-integration test set:
   - `python3 -m pytest tests/test_v2_primary_audit_entrypoints.py tests/test_v2_primary_version_upload.py tests/test_resolve_active_project_dir.py -q`
   - `python3 -m pytest tests/test_version_file_upload.py tests/test_projects_v2_write_facade.py tests/test_projects_v2_primary_job_paths.py -q`
   - existing deploy/resecr stage-comparison/UI smoke relevant to `upload-folder` and comparison panel.

## Безопасность

Это только read-only registry. Deploy worktree не трогался, backend/:8082 не трогались, `.env` не менялся, push не выполнялся.
