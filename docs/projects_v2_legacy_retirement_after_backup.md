# Ускоренное удаление legacy `projects/` после внешнего бэкапа

**Дата подготовки:** 2026-07-13
**Legacy:** `projects/`
**Canonical storage:** `projects_v2/`

Этот сценарий предназначен только для осознанного досрочного завершения
карантина. Он не делает прямой `rm projects`: защищённый скрипт повторно
проверяет внешний бэкап и исходное дерево, переименовывает `projects/` в
карантин, запускает v2-only smoke и удаляет карантин только после успеха. При
ошибке smoke исходное имя восстанавливается автоматически.

## Что уже подготовлено

- `AUDIT_STORAGE_BACKEND=projects_v2`;
- `AUDIT_PROJECTS_V2_WRITE_MODE=projects_v2_primary`;
- реальный migration backlog: `0`;
- ledger validation: `95/95 PASS`;
- migrated drift: `0`;
- migration/v2-primary suite: `564 passed`;
- pytest изолирован от рабочего `projects_v2`;
- создан исходный манифест:
  `projects_v2/_system/legacy_projects_source_manifest.json`;
- текущий манифест: 74 740 файлов, 10 778 235 647 байт,
  content id `488ace33d1a42ba4974934552b9a77a16fc972d13d977e6b32d4579327895667`.

Если `projects/` изменится до копирования, скрипт потребует новый манифест и
новый бэкап — старый receipt не позволит удалить папку.

## 1. Сделать копию на внешнее устройство

Пример для подключённого диска `/media/EXTERNAL_BACKUP`:

```bash
cd /home/coder/projects/PDF-proverka
rsync -aHAX --info=progress2 projects/ /media/EXTERNAL_BACKUP/projects/
cp projects_v2/_system/legacy_projects_source_manifest.json \
  /media/EXTERNAL_BACKUP/legacy_projects_source_manifest.json
```

Вместо каталога можно создать `.tar`, `.tar.gz` или `.tgz`. Символические
ссылки должны сохраняться как ссылки. Копия на том же filesystem/device не
принимается как внешний бэкап.

## 2. Побайтово проверить внешний бэкап

```bash
cd /home/coder/projects/PDF-proverka
python3 scripts/projects_v2/retire_legacy_projects.py verify-backup \
  --manifest projects_v2/_system/legacy_projects_source_manifest.json \
  --backup /media/EXTERNAL_BACKUP/projects \
  --receipt /media/EXTERNAL_BACKUP/projects-backup-receipt.json
```

Проверяются размер и SHA-256 каждого файла, а также все символические ссылки.
Receipt с `external_device_verified=false` не может разрешить удаление.

## 3. Остановить сервисы и выполнить preflight

Остановить backend и все audit/batch/prepare workers штатным способом, затем:

```bash
python3 scripts/projects_v2/retire_legacy_projects.py preflight \
  --manifest projects_v2/_system/legacy_projects_source_manifest.json \
  --receipt /media/EXTERNAL_BACKUP/projects-backup-receipt.json
```

Preflight заново читает каждый байт исходника и бэкапа, запускает coverage,
ledger validation, drift scan, v2-only тесты, полный parity diagnostic и ищет
активные процессы/открытые файлы под `projects/`.

## 4. Отдельное решение по parity

На 2026-07-13 literal parity не зелёный: 26 документов имеют больше findings в
legacy-current snapshot, 24 отличаются числом версий, один документ имеет
ошибку current-version metadata. При этом `projects_v2` уже содержит много
новых результатов, которых нет в legacy. Полный внешний бэкап сохраняет эти
старые snapshots без потерь, но ускоренное удаление требует отдельного явного
подтверждения этого риска.

Отчёт:
`projects_v2/_system/legacy_projects_retirement_preflight.json`.

## 5. Удаление в maintenance-окне

Только после `[OK]`/`[REVIEW]` preflight, проверки отчёта и остановки backend:

```bash
python3 scripts/projects_v2/retire_legacy_projects.py execute \
  --manifest projects_v2/_system/legacy_projects_source_manifest.json \
  --receipt /media/EXTERNAL_BACKUP/projects-backup-receipt.json \
  --confirm DELETE_LEGACY_PROJECTS_AFTER_VERIFIED_BACKUP \
  --acknowledge-parity-risk ACKNOWLEDGE_LEGACY_ONLY_DATA_REMAINS_IN_VERIFIED_BACKUP
```

Последний флаг нужен только пока parity diagnostic содержит legacy-only данные.
Если эти расхождения будут устранены, подтверждение риска не потребуется.

После успеха результат записывается в
`projects_v2/_system/legacy_projects_retirement_result.json`. Внешний бэкап и
его receipt удалять нельзя до отдельного решения.

## Восстановление

При проблеме после удаления остановить backend, восстановить `projects/` из
проверенного внешнего бэкапа с сохранением ссылок/атрибутов и при необходимости
временно вернуть:

```dotenv
AUDIT_STORAGE_BACKEND=legacy
AUDIT_PROJECTS_V2_WRITE_MODE=dual_write_shadow
```

После изменения `.env` backend должен быть перезапущен.
