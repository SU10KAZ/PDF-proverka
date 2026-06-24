# Production data-root guardrails

**Дата:** 2026-06-14
**Статус:** guardrail / диагностика (read-only). Runtime не меняется.
**Модули:**
[backend/app/services/stage_comparison/production_root_health.py](../backend/app/services/stage_comparison/production_root_health.py),
[backend/app/services/stage_comparison/pipeline_v2_runtime_root_audit.py](../backend/app/services/stage_comparison/pipeline_v2_runtime_root_audit.py),
[scripts/check_production_data_roots.sh](../scripts/check_production_data_roots.sh)

## Источник истины

```text
MAIN   = /home/coder/projects/PDF-proverka          ← ДАННЫЕ (source of truth)
DEPLOY = /home/coder/projects/PDF-proverka-deploy    ← КОД production (worktree)
```

MAIN содержит: **3 объекта** (213 / 214 Alia / 272 Балчуг), **~109 проектов**,
полный `comparison/` + Pipeline V2, `knowledge_base`, `decisions_log`. Deploy
worktree держит **неполные** данные (1 объект, пустые projects) и **не должен**
быть data source.

Production backend обязан читать данные из MAIN:

```text
AUDIT_DATA_DIR=/home/coder/projects/PDF-proverka
AUDIT_APP_DATA_DIR=/home/coder/projects/PDF-proverka/backend/app/data
COMPARISON_ROOT=/home/coder/projects/PDF-proverka/comparison
```

Это выставляет канонический launcher
[webapp/start_server_deploy.sh](../webapp/start_server_deploy.sh) (код из deploy,
данные из MAIN). Запуск без этих env (raw `uvicorn` из deploy worktree) =
autodetect на deploy = неполный data root.

## Почему `/api/info base_dir` НЕДОСТАТОЧЕН

`base_dir` = worktree **кода** (`ROOT_DIR`), а не data root. При канонической
раскладке `base_dir = DEPLOY`, но данные читаются из MAIN через env. Поэтому:

* **`/api/info == 200` ≠ здоровый backend.** Сервер отвечает 200, даже если
  читает пустой deploy root.
* Активный data root определяется по **`COMPARISON_ROOT` / `AUDIT_DATA_DIR`**, а
  НЕ по `base_dir`. Старый `runtime_root_audit` определял root по `base_dir` →
  давал неверный ответ (инцидент 2026-06-14: после рестарта объекты Alia/Балчуг
  и все projects «исчезли», потому что флипнули data root на deploy).

`/api/info` теперь явно отдаёт `data_roots` (только пути, без секретов):

```json
{
  "base_dir": "/home/coder/projects/PDF-proverka-deploy",
  "data_roots": {
    "audit_data_dir": "/home/coder/projects/PDF-proverka",
    "audit_app_data_dir": "/home/coder/projects/PDF-proverka/backend/app/data",
    "projects_dir": "/home/coder/projects/PDF-proverka/projects",
    "comparison_root": "/home/coder/projects/PDF-proverka/comparison"
  }
}
```

## Runtime root detector (исправленный приоритет)

`detect_active_runtime_root` (в
[pipeline_v2_runtime_root_audit.py](../backend/app/services/stage_comparison/pipeline_v2_runtime_root_audit.py))
теперь выбирает active comparison root по убыванию надёжности:

1. `api_info.data_roots.comparison_root` — явный runtime-root (high);
2. env `COMPARISON_ROOT` (high);
3. env `AUDIT_DATA_DIR` → `.../comparison` (medium);
4. env `AUDIT_ROOT_DIR` / `AUDIT_BASE_DIR` → `.../comparison` (medium);
5. backend `comparison_root_path()` текущего процесса (medium);
6. `api_info.base_dir` → `base_dir/comparison` — **ТОЛЬКО последний fallback** (low).

Возвращает также `base_dir_implied_comparison_root` и `drift_from_base_dir`
(bool): если выбранный root ≠ `base_dir/comparison` — это active-root drift
(код в deploy, данные в MAIN). Именно тут старая «по base_dir» логика ошибалась.

## Health / sanity check

`evaluate_production_data_roots` (в
[production_root_health.py](../backend/app/services/stage_comparison/production_root_health.py))
— детерминированный вердикт `ok | warning | dangerous`:

| Сигнал | Статус |
|---|---|
| `objects_count <= 1` | **dangerous** (deploy-root симптом: объекты исчезли) |
| `projects_count == 0` | **dangerous** (пустой data root) |
| `comparison_root` не существует | **dangerous** |
| `objects_count < 3` / `projects_count < 100` | warning |
| Pipeline V2 артефакты эталонной пары отсутствуют | warning |
| всё в норме | ok |

CLI-обёртка [scripts/check_production_data_roots.sh](../scripts/check_production_data_roots.sh)
собирает live `/api/info` + `/api/objects` + `/api/projects` + Pipeline V2
ui-payload и печатает вердикт. Exit code: `0` ok / `1` warning / `2` dangerous /
`3` unreachable.

```bash
PORTAL_TOKEN=<token> ./scripts/check_production_data_roots.sh
```

> ⚠️ **Watchdog.** Cron-watchdog (`~/bin/webapp-watchdog.sh`) сейчас считает
> бэкенд живым по `curl -fsS /api/info` (200). Этого НЕДОСТАТОЧНО — он не ловит
> неверный data root. Watchdog намеренно НЕ менялся в этой задаче (он fragile —
> агрессивный рестарт ломал прод). Если будете усиливать healthcheck — добавьте
> проверку `objects>=3` / `comparison_root exists` через этот скрипт, но
> осторожно (рестарт прерывает live-джобы → `failed_interrupted`).

## Симптомы неверного root (что увидит пользователь)

* объекты **214 Alia / 272 Балчуг исчезают** из портала;
* `/api/projects` → **0** проектов;
* загрузка Excel-решений сохраняется в **deploy tree** (не в
  `MAIN/knowledge_base`) → решения «пропадают»;
* Pipeline V2 панели (skip-readiness / controlled-enforce-*) → `not_found`;
* `/api/info` при этом отвечает **200** (вот почему он один не годится).

## Правила

1. **Запуск только через канонический launcher** `start_server_deploy.sh`
   (выставляет MAIN data roots). Не запускать raw `uvicorn` из deploy без env.
2. **После любого рестарта** сверять `data_roots.comparison_root == MAIN/comparison`
   и `objects==3` (`check_production_data_roots.sh`), не только `/api/info=200`.
3. **`base_dir` != data root** — всегда смотреть `data_roots`, не `base_dir`.
4. **Не менять `.env`** для смены root без отдельной задачи — это глобально
   меняет data source для всего backend.

## Связанные документы

* [stage_comparison_pipeline_v2_runtime_artifact_roots.md](stage_comparison_pipeline_v2_runtime_artifact_roots.md)
* [stage_comparison_pipeline_v2_controlled_enforce.md](stage_comparison_pipeline_v2_controlled_enforce.md)
* [stage_comparison_pipeline_v2_skip_readiness.md](stage_comparison_pipeline_v2_skip_readiness.md)

## Тесты

* [tests/test_production_data_root_guardrails.py](../tests/test_production_data_root_guardrails.py)
* [tests/test_stage_comparison_pipeline_v2_runtime_root_audit.py](../tests/test_stage_comparison_pipeline_v2_runtime_root_audit.py)
  — detector priority (COMPARISON_ROOT > base_dir), fallbacks, drift detection.
