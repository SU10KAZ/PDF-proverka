# projects_v2 — write-cutover playbook (controlled)

**Дата:** 2026-06-18
**Статус:** ПОДГОТОВКА. Ничего из этого НЕ включено в production. Документ
описывает будущий controlled cutover на v2-primary. Выполнять только по явному
человеческому решению, не автономно.

## Текущее состояние (на момент написания)

| Флаг | Текущее prod-значение | Назначение |
|---|---|---|
| `AUDIT_STORAGE_BACKEND` | `legacy` | источник ЧТЕНИЯ (read) |
| `AUDIT_PROJECTS_V2_WRITE_MODE` | `dual_write_shadow` | режим ЗАПИСИ |
| `AUDIT_PROJECTS_V2_READ_DEFAULT_ENABLED` | `true` | v2-форма ответов на части endpoint'ов |

Что уже подключено в коде (за флагом `projects_v2_primary`, **в проде не активно**):

- `save_project_info` → v2-primary метаданные (Шаг 6A);
- `manager._resolve_job_paths` → v2-primary пути `03_analysis/runs/<run_id>` (Шаг 6B);
- `clean_project_data` / `rename_project` → блокируются в v2-primary до контракта (Шаг 6C);
- late audit mirror + KB expert_review mirror (Шаг 2b, активны в dual_write_shadow).

## Открытые блокеры (ДОЛЖНЫ быть закрыты до cutover)

1. **Source-reading стадий из v2.** Pipeline читает PDF/MD из `version_dir` root
   с legacy-именами (`*_document.md`); v2 хранит в `01_input/`/`02_work/` с
   нормализованными именами (`document.md`). MD-gate (`_require_project_md`) под
   v2-primary не найдёт источник без адаптации.
2. **Promotion `03_analysis/runs/<run_id>` → `latest`.** После прогона ключевые
   артефакты надо продвинуть в `latest` (сейчас прямую запись в latest делает
   только `write_completed_audit_artifacts_v2`).
3. **Export ZIP.** `export.py` использует `resolve_project_dir` (legacy) и ищет
   PDF в `version_dir` root, не в `01_input/`. Нужен v2-aware export (helper
   `v2_source_pdf` уже есть как основа).
4. **Destructive контракт.** clean/rename/delete в v2-primary заблокированы; для
   разблокировки нужен backup+confirmation контракт.
5. **prepare / batch queue.** Не переведены на v2-primary.
6. **`production_uses_v2()==False`.** Read-path для write-backed данных всё ещё
   обслуживается legacy-сервисами даже при `AUDIT_STORAGE_BACKEND=projects_v2`.

## 1. Pre-cutover gates (все должны быть TRUE)

- [ ] нет running audit (`ps`, batch_queue.json idle, нет active job-файлов);
- [ ] нет активного batch (`batch_queue.json` отсутствует/completed);
- [ ] нет prepare jobs (`prepare_queue.json` без running/queued);
- [ ] нет live subprocess (`claude -p` / `blocks.py` / `process_project.py` / refresh / migrate);
- [ ] нет running comparison/qwen job;
- [ ] свежий backup существует (см. раздел 3);
- [ ] maintenance-окно согласовано (cutover требует рестарта backend).

## 2. Required tests (все зелёные перед cutover)

```bash
pytest tests -q -k "projects_v2 or v2_primary or write_facade or migration_coverage or audit_shadow_mirror or storage_read_facade"
pytest tests/test_projects_v2_only_harness.py -q          # v2-only harness
pytest tests/test_projects_v2_only_compat.py -q           # read/export/destructive guards
pytest tests/test_projects_v2_primary_wiring.py -q        # save_project_info v2-primary
pytest tests/test_projects_v2_primary_job_paths.py -q     # pipeline paths v2-primary
```
Плюс ручная проверка закрытия блокеров 1–6 выше (source-reading, promotion,
export, destructive contract, prepare/batch, read-path).

## 3. Required backups (перед любым переключением)

```bash
TS=$(date +%Y%m%d-%H%M%S)
# legacy projects/ (источник истины до cutover)
tar -czf /backup/projects_legacy_$TS.tar.gz -C /home/coder/projects/PDF-proverka projects
# projects_v2/ (новый primary)
tar -czf /backup/projects_v2_$TS.tar.gz -C /home/coder/projects/PDF-proverka projects_v2
# app data json stores (decisions_log, objects, project_groups, prepare/batch queue)
tar -czf /backup/app_data_$TS.tar.gz -C /home/coder/projects/PDF-proverka backend/app/data knowledge_base
# ledger
cp projects_v2/_system/old_to_new_map.json /backup/old_to_new_map.$TS.json
```
Backups должны лежать на ВНЕШНЕМ хранилище (не на том же диске).

## 4. Env flags (cutover)

```env
AUDIT_STORAGE_BACKEND=projects_v2
AUDIT_PROJECTS_V2_WRITE_MODE=projects_v2_primary
AUDIT_PROJECTS_V2_READ_DEFAULT_ENABLED=true
```
Менять в prod `.env` ТОЛЬКО в maintenance-окне, после gates + tests + backups.

## 5. Smoke checklist (после cutover, на canary-проекте)

- [ ] upload нового проекта → v2 document/version создаётся;
- [ ] audit (полный) → артефакты в `03_analysis/runs/<run_id>` + promotion в `latest`;
- [ ] findings endpoint → корректное число замечаний из v2;
- [ ] optimization → читается из v2;
- [ ] export ZIP → PDF из v2 `01_input`, отчёты из v2;
- [ ] clean → работает по новому контракту (или явно заблокирован);
- [ ] rename → работает по новому контракту (или явно заблокирован);
- [ ] batch / resume → пишет в v2, переживает рестарт;
- [ ] prepare → пишет в v2.

## 6. Rollback

```env
# вернуть в .env:
AUDIT_STORAGE_BACKEND=legacy
AUDIT_PROJECTS_V2_WRITE_MODE=dual_write_shadow
```
Затем:
```bash
pkill -f "uvicorn backend.app.main"; <перезапуск backend>
```
Если v2 успел записать что-то некорректное — restore из backup раздела 3.
Legacy остаётся нетронутым во время dual_write_shadow, поэтому rollback к
legacy-read безопасен.

## 7. Stop conditions (немедленный rollback)

- любой HTTP 500 на read/audit/export;
- пропавшие findings (число не совпадает с legacy);
- сломанный export (нет PDF / битый ZIP);
- ошибки v2-записи (`dual_write_shadow_errors.jsonl` растёт / v2_primary raise);
- неожиданно активный legacy fallback (данные читаются из legacy там, где
  ожидался v2);
- рост `failed_interrupted` job'ов после рестарта.

## Принцип

Legacy `projects/` НЕ удаляется на cutover. Cutover лишь переключает primary на
v2; legacy остаётся как авторитетный архив до отдельного quarantine-периода
(см. `projects_v2_legacy_quarantine_plan` и `projects_v2_legacy_deletion_checklist`).
