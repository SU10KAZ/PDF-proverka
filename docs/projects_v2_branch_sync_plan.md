# projects_v2 branch sync plan

Дата: 2026-06-23
Репозиторий: `/home/coder/projects/PDF-proverka`
Рабочая ветка анализа: `fix/audit-queue-rate-limit-md-resolution`

## Ограничения

Это read-only план. Во время подготовки отчёта не выполнялись `push`, `merge`, `rebase`, `reset`, не трогались `/home/coder/projects/PDF-proverka-deploy`, `.env`, backend и live-процессы.

## Снимок refs

- `fix/audit-queue-rate-limit-md-resolution`: `c7a2ec24c6feda5796ad1f551de012f65cfbf544` до baseline-test коммита этой задачи.
- `origin/fix/audit-queue-rate-limit-md-resolution`: `b70c613065be02cce2637007c9e8237fbbe70702`.
- `origin/main`: `7cc98c0e30db1cec6bee3f7e3441d14bb5ec415e`.
- `deploy/main-live`: `149f5f2c157c3466a857e5d97fa68545aa3fef95`.

## Что есть на fix сверх origin/fix

Локально на `fix` было 22 коммита сверх `origin/fix`:

```text
b5b3676 fix(projects-v2): читать исходники из v2-primary
ce03291 fix(audit,comparison): честное покрытие кропа + жёсткий гейт large-sheet (reserc.md #10/#49)
6644b08 fix(projects-v2): направить prepare в v2 latest
7a991c7 fix(projects-v2): продвигать run-артефакты в latest
3dbc1b5 fix(projects-v2): включить read-path за флагом
3eb5847 fix(projects-v2): экспортировать audit package из v2
835d1b1 fix(projects-v2): добавить безопасный clean и restore
89a675c fix(projects-v2): обогащать v2 findings листами
91d60d3 fix(projects-v2): читать недостающие latest файлы из runs
d60f5f2 Исправить v2-резолвинг prepare и триажа
a1a1201 Закрыть v2-primary резолвинг источников
f688422 Исправить v2-primary контекст версий
5af0cb5 Укрепить v2-primary entrypoints аудита
c0a210d Исправить v2-primary загрузку версий
4b300dd Покрыть v2-primary active version binding
69c1130 Зафиксировать риски интеграции fix и deploy
5ddbb8a Добавить матрицу проверки v2-аудита
ab5a94e Документировать v2-primary путь аудита
dff168c Проверить когерентность v2 read write
d98dea3 Выровнять version-files тест под read_canary
2458b75 Сделать смену раздела v2-aware (физический перенос документа)
c7a2ec2 Исправить дубль при смене раздела под v2-primary
```

После задачи baseline к этому списку добавляется отдельный тестовый коммит с изоляцией storage `.env` для CI.

## Представленность fix в deploy/main-live

Вывод: содержательная v2-cutover линия в основном уже представлена в `deploy/main-live`, но частью как cherry-pick с другими hash и частью как deploy-adapted conflict resolution. Поэтому hash-сравнение недостаточно; нужно ориентироваться на subject/patch intent и на deploy как источник истины.

Соответствия по subject:

```text
d60f5f2 -> b70f7b8 Исправить v2-резолвинг prepare и триажа
a1a1201 -> a3558ec Закрыть v2-primary резолвинг источников
f688422 -> 5a8c175 Исправить v2-primary контекст версий
5af0cb5 -> 1200582 Укрепить v2-primary entrypoints аудита
c0a210d -> 6c7d573 Исправить v2-primary загрузку версий
4b300dd -> 33543f1 Покрыть v2-primary active version binding
69c1130 -> 129238e Зафиксировать риски интеграции fix и deploy
5ddbb8a -> 634ee56 Добавить матрицу проверки v2-аудита
ab5a94e -> 0f0231c Документировать v2-primary путь аудита
dff168c -> edbd603 Проверить когерентность v2 read write
d98dea3 -> c697d44 Выровнять version-files тест под read_canary
2458b75 -> 2079d14 Сделать смену раздела v2-aware (физический перенос документа)
c7a2ec2 -> 149f5f2 Исправить дубль при смене раздела под v2-primary
```

Также `deploy/main-live` содержит `cba2bcc merge: интегрировать v2-cutover поверх deploy`, который внес ранние B1-B6 изменения и разрешил конфликты с deploy-логикой.

`git log --cherry-pick deploy/main-live...HEAD` показывает, что часть коммитов с теми же subject не patch-id идентична. Это ожидаемо: deploy содержит адаптированные версии после конфликтов и reserc-фиксов.

## Что есть в deploy, чего нет в fix

`deploy/main-live` существенно богаче текущей fix-ветки. Среди deploy-only направлений:

- `backend/app/services/storage/read_canary.py` и read-canary тесты/доки.
- Расширенный `projects_v2` read-cutover слой и upload-folder/read-default work.
- Большой блок `stage_comparison/pipeline_v2_*` модулей, UI и тестов.
- Upload-folder функциональность, version deletion, project delete, production data-root guardrails.
- Production schedule UI, knowledge-base UI, dashboard/findings UI изменения.
- reserc backend cluster: `b39cea8`, `18a7ccc`, `36015f1`, `ff3f71e`, `6e2affa`, `f25c145`.

Риск подтвержден diff-ом `deploy/main-live..HEAD`: если механически мержить fix поверх deploy или делать main из fix, будут удаления deploy-only файлов, включая `read_canary.py`, `pipeline_v2_*`, upload-folder тесты и production guardrails. Это нельзя делать без отдельного ручного переноса.

## Рекомендованный безопасный путь

### Цель

Сделать `main/origin` отражением живой production-линии, не потеряв deploy-only код и добавив только отсутствующие fix/test дельты.

### Вариант A, предпочтительный: main от deploy/main-live

Идея: считать `deploy/main-live` источником истины, создать sync-ветку от deploy и перенести туда только отсутствующие коммиты fix. На момент этого отчёта содержательная v2-функциональность уже есть в deploy; новый отсутствующий кусок - baseline-test коммит этой задачи.

Команды для человека, не выполнялись:

```bash
git fetch origin
git switch -c sync/main-from-deploy-20260623 deploy/main-live
# После появления baseline-test коммита на fix:
git cherry-pick <baseline-test-commit>
AUDIT_STORAGE_BACKEND=legacy AUDIT_PROJECTS_V2_WRITE_MODE=dual_write_shadow AUDIT_PROJECTS_V2_READ_DEFAULT_ENABLED=false python3 scripts/ci_regression_gate.py
AUDIT_STORAGE_BACKEND=projects_v2 AUDIT_PROJECTS_V2_WRITE_MODE=projects_v2_primary AUDIT_PROJECTS_V2_READ_DEFAULT_ENABLED=true python3 scripts/ci_regression_gate.py
git push origin sync/main-from-deploy-20260623
# Далее PR sync/main-from-deploy-20260623 -> main с явной проверкой, что read_canary и deploy-only файлы сохраняются.
```

### Вариант B: обновить fix как integration ветку от deploy

Идея: не мерджить fix целиком, а создать новую integration-ветку от deploy и cherry-pick только отсутствующие fix-коммиты.

Команды для человека, не выполнялись:

```bash
git fetch origin
git switch -c integ/fix-missing-on-deploy-20260623 deploy/main-live
git cherry-pick <baseline-test-commit>
# Если появятся новые fix-коммиты после c7a2ec2, добавлять их по одному, с тестами после каждого.
python3 -m pytest tests -q -k "projects_v2 or v2_primary or write_facade or storage_read_facade or findings or export or promotion_latest"
python3 scripts/ci_regression_gate.py
```

### Чего не делать

- Не делать `git merge fix/audit-queue-rate-limit-md-resolution` прямо в `deploy/main-live`.
- Не делать `main = fix` и не fast-forward main на fix: это потеряет deploy-only историю и файлы.
- Не удалять deploy-only файлы из-за того, что их нет на fix.
- Не переписывать `ci_known_failures.txt` ради прохождения baseline под v2-primary.

## Минимальные проверки перед PR

```bash
git diff --name-status deploy/main-live..HEAD
AUDIT_STORAGE_BACKEND=legacy AUDIT_PROJECTS_V2_WRITE_MODE=dual_write_shadow AUDIT_PROJECTS_V2_READ_DEFAULT_ENABLED=false python3 scripts/ci_regression_gate.py
AUDIT_STORAGE_BACKEND=projects_v2 AUDIT_PROJECTS_V2_WRITE_MODE=projects_v2_primary AUDIT_PROJECTS_V2_READ_DEFAULT_ENABLED=true python3 scripts/ci_regression_gate.py
python3 -m pytest tests -q -k "projects_v2 or v2_primary or write_facade or storage_read_facade or read_canary or promotion_latest"
```

## Итог

Безопасная стратегия: deploy-first. `deploy/main-live` уже содержит production-адаптированный v2-cutover с read_canary и reserc-фикками. В `origin/main` нужно заводить именно deploy-срез плюс минимальные недостающие test/fix дельты, а не вливать старую fix-ветку целиком.
