# Stage Comparison — Pipeline V2 runtime artifact roots (guardrail)

**Дата:** 2026-06-12
**Статус:** guardrail / диагностика (read-only). Runtime не меняется.
**Модуль:** [backend/app/services/stage_comparison/pipeline_v2_runtime_root_audit.py](../backend/app/services/stage_comparison/pipeline_v2_runtime_root_audit.py)

## Зачем этот документ

В задаче skip-readiness обнаружилось, что **production backend читает артефакты
не из того worktree, куда их обычно пишут**. Отчёт `skip_readiness_report.json`
записали в main worktree, а endpoint вернул `not_found` — пришлось зеркалировать
файл в deploy worktree. Перед любым controlled enforce/skip это нужно понимать
точно, иначе отчёт уйдёт «не туда» и backend его не увидит (или, хуже, увидит
устаревшую копию).

## Какие worktree существуют

| worktree | путь | роль |
|---|---|---|
| **main** | `/home/coder/projects/PDF-proverka` | основная рабочая копия + полный `comparison/` (данные) |
| **deploy** | `/home/coder/projects/PDF-proverka-deploy` | код production-сервера (ветка `deploy/main-live`); **отсюда запущен uvicorn** |
| pv2 | `/home/coder/projects/PDF-proverka-pv2` | worktree разработки Pipeline V2 |

## Какой `comparison/` root читает production

`comparison/` резолвится так
([paths.comparison_root_path](../backend/app/services/stage_comparison/paths.py) →
[config.ROOT_DIR](../backend/app/core/config.py)):

```text
COMPARISON_ROOT (env)                       — если задан, побеждает
  └ иначе ROOT_DIR / "comparison"
       ROOT_DIR = AUDIT_ROOT_DIR / AUDIT_BASE_DIR (env)
                  └ иначе autodetect backend/../../  (worktree запущенного кода)
```

В production **ни одна из этих env не задана** (проверено в process env
запущенного uvicorn и в `PDF-proverka-deploy/.env`). Значит:

```text
ROOT_DIR        = /home/coder/projects/PDF-proverka-deploy
comparison root = /home/coder/projects/PDF-proverka-deploy/comparison   ← АКТИВНЫЙ
```

Это подтверждается `GET /api/info` → `base_dir = /home/coder/projects/PDF-proverka-deploy`.

> ⚠️ `AUDIT_PROJECTS_DIR` / `AUDIT_STAGE_COMPARISON_ROOTS` в `.env` указывают на
> main worktree, НО они влияют только на `projects/` (аудит) и на
> **comparison_sources** (вход сканера), а **не** на runtime `comparison/`
> (сессии/пары/артефакты Pipeline V2). Runtime `comparison/` идёт от `ROOT_DIR`.

**Вывод:** source-of-truth для runtime-write Pipeline V2 = **deploy worktree's
`comparison/`**, пока сервер запущен из deploy worktree без env-override.

## Чем опасен рассинхрон

* запись только в main worktree → backend (читает deploy) её **не видит** →
  endpoint `not_found`, UI пустой;
* запись только в один root при разном содержимом → backend и оператор видят
  **разные данные** (hash mismatch). Перед enforce это критично: enforce может
  опираться на устаревшую версию отчёта;
* «тихое» зеркалирование без отчёта → теряется история, какой файл канонический.

Реальный снимок ИОС 1.1 (`ba413a93c5754f6c` / `pf06effb7`) на 2026-06-12:

| | main worktree | deploy worktree (АКТИВНЫЙ) |
|---|---|---|
| pipeline_v2 артефактов | **17 / 17** | **1 / 17** (только `skip_readiness_report.json`) |
| `skip-readiness` endpoint | — | `ok` (файл зеркалирован) |
| `ui-payload` endpoint | — | `not_found` |
| `exclusion-preview-v2` endpoint | — | `not_found` |
| `link-validation` endpoint | — | `not_found` |

`same_hashes=True` (единственный общий файл идентичен), но `same_file_set=False`
(16 артефактов есть только в main). Endpoint-correlation прямо показывает: всё,
что не зеркалировано в активный deploy root, backend отдаёт как `not_found`.

## Диагностика (как проверить перед runtime-write)

```python
from backend.app.services.stage_comparison.pipeline_v2_runtime_root_audit import (
    build_runtime_root_audit,
)
# api_info = {"base_dir": "..."} от GET /api/info → detect active root (high conf)
report = build_runtime_root_audit("ba413a93c5754f6c", "pf06effb7", api_info=api_info)
report["active_runtime_root"]["detected"]   # активный comparison root
report["comparison"]["same_file_set"]        # одинаковый набор файлов?
report["comparison"]["same_hashes"]          # одинаковые хэши?
report["comparison"]["differences"]          # список расхождений
```

Helper-скрипт прогона (read-only, пишет только в `diagnostics_pipeline_v2/`):
`diagnostics_pipeline_v2/runtime_root_audit_ios11_*/helper_script.py`.

Схема отчёта — `kind = stage_comparison_pipeline_v2_runtime_root_audit`:
`roots[]` (per-root: `comparison_root`, `pair_pipeline_v2_path`, `exists`,
`artifact_count`, `artifacts[]` с `size/mtime/sha256`), `comparison`
(`same_file_set`, `same_hashes`, `differences[]`: `missing_in_root` /
`hash_mismatch`), `active_runtime_root` (`detected`, `confidence`, `evidence[]`),
`recommendations[]`.

## Mandatory checklist перед runtime-write Pipeline V2

1. **Active root.** Определить активный `comparison/` через `GET /api/info`
   (`base_dir`) + `build_runtime_root_audit`. Не угадывать.
2. **Backup.** Перед записью сделать timestamp-бэкап целевого файла/каталога
   пары в активном root.
3. **Protected hashes.** Зафиксировать sha256 защищённых артефактов
   (`exclusion_preview_v2_report.json`, `exclusion_review_overrides.json`,
   `link_validation_report.json`, старые comparison results/findings) ДО и ПОСЛЕ
   — они должны совпасть (запись не должна их трогать).
4. **Endpoint smoke.** После записи — read-only GET соответствующего
   pipeline-v2 endpoint, убедиться, что backend видит новый артефакт
   (`status=ok`, корректный hash/mtime).
5. **No writes to inactive root.** В неактивный worktree не писать, **если только
   зеркалирование не выполняется ЯВНО и не зафиксировано в отчёте** (что
   зеркалировали, откуда, с каким hash).

## Чего нельзя делать

* ❌ писать только в main worktree, когда production читает deploy — backend это
  не увидит;
* ❌ зеркалировать между worktree'ами без отчёта (какой файл канонический,
  откуда копия, hash) — теряется аудируемость;
* ❌ менять `.env` (в т.ч. добавлять `COMPARISON_ROOT`/`AUDIT_ROOT_DIR`) без
  отдельного задания — это меняет активный root для всего сервера и требует
  рестарта/перепроверки всех пар;
* ❌ включать skip/enforce до подтверждённого active root + согласованных
  артефактов в нём.

## Использование в controlled enforce preflight

Runtime-root guard этого аудита переиспользуется слоем
**controlled_enforce_preflight**: его `detect_active_runtime_root` подтверждает
active root, и при `runtime_root_unconfirmed` preflight даёт fatal-блок (enforce
запрещён). Это первая точка, где active-root проверка реально влияет на
решение о enforce. См.
[stage_comparison_pipeline_v2_controlled_enforce.md](stage_comparison_pipeline_v2_controlled_enforce.md).

## Связанные документы

* [stage_comparison_pipeline_v2_dry_run.md](stage_comparison_pipeline_v2_dry_run.md)
* [stage_comparison_pipeline_v2_skip_readiness.md](stage_comparison_pipeline_v2_skip_readiness.md)
* [stage_comparison_pipeline_v2_exclusion_preview.md](stage_comparison_pipeline_v2_exclusion_preview.md)
* [stage_comparison_pipeline_v2_controlled_enforce.md](stage_comparison_pipeline_v2_controlled_enforce.md)
* [stage_comparison_pipeline_v2_readonly_endpoint.md](stage_comparison_pipeline_v2_readonly_endpoint.md)

## Тесты

* [tests/test_stage_comparison_pipeline_v2_runtime_root_audit.py](../tests/test_stage_comparison_pipeline_v2_runtime_root_audit.py)
  — missing root / missing pair dir / sha256 / same-hashes / missing-in-root /
  hash-mismatch / active-root-from-/api/info / no-writes / no model-job imports /
  path-traversal.
