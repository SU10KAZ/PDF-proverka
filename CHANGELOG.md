# Changelog — Аудит проектной документации МКД

Журнал значимых изменений production. Записи — сверху новые. Только
документация; не путать с git-историей (детали — в коммитах).

---

## 2026-06-17 — Загрузка папки с ПК, версии без orphan, удаление проекта

Релиз закрывает цикл «инженер загружает проект через сайт» и чинит побочные
проблемы версионирования/удаления в раскладке `projects_v2`.

### Финальное production-состояние

| Параметр | Значение |
|---|---|
| prod code HEAD (`deploy/main-live`) | **`a17682b`** |
| backend PID | **`1800673`** |
| `AUDIT_STORAGE_BACKEND` | **`legacy`** (авторитетно) |
| `AUDIT_PROJECTS_V2_WRITE_MODE` | **`dual_write_shadow`** |
| read-default из `projects_v2` | **включён** |
| full write-cutover в `projects_v2_primary` | **НЕ выполнялся** |
| validate / parity | **PASS / PASS** |
| `dual_write_shadow_errors` | **0** |

> Код берётся из worktree `PDF-proverka-deploy` (`deploy/main-live`), данные — из
> MAIN (`AUDIT_DATA_DIR=/home/coder/projects/PDF-proverka`). uvicorn без
> `--reload`.

### Что теперь работает

- **«Добавить проект → Из папки на компьютере»** — загрузка папки проекта через
  браузер (`POST /api/projects/upload-folder`, multipart). Браузер не отдаёт
  абсолютный путь — это нормально.
- Сохранение комплекта: **PDF** (обязателен, ровно один) + опционально
  **`*_document.md`**, **`*_result.json`**, **`*_ocr.html`**.
- **Двойная запись:** legacy `projects/` (авторитетно, первым) → затем
  `projects_v2` shadow-зеркало (fail-soft).
- **Привязка проекта как версии** существующего — **без orphan-карточек**.
- **Удаление проекта** кнопкой **«🗑 Удалить»** в окне «Изменить выбранные
  проекты» (жёсткое удаление: legacy-папка + документ в `projects_v2`, гард на
  идущий аудит, подтверждение в UI).

### Что было исправлено

- `register_external_project` теперь копирует **`*_ocr.html`** (раньше терялся —
  нужен для text_evidence).
- `merge_project_as_version` больше **не оставляет v2-orphan**: при привязке
  версии удаляется v2-документ source.
- После привязки версии **target-контейнер пере-зеркаливается** в `projects_v2`
  (иначе `validate` видел «LEGACY CHANGED since migration» по `project_info.json`
  новой версии).
- `DELETE /api/projects/{project_id}`: несуществующий проект возвращает **404**,
  а не 500 (`ProjectNotResolvedError` маппится в `ValueError`).

Коммиты: `e77d9e4` (upload-folder + ocr.html), `2e83f00` (удаление + фикс
orphan/re-mirror), `a17682b` (404 hotfix). Деплой — ff-merge в `deploy/main-live`
+ controlled restart (PID `1776527 → 1790867 → 1800026 → 1800673`).

### Эксплуатационная инструкция (для инженеров)

1. **Hard-refresh** браузера (Ctrl+F5 / Cmd+Shift+R) — подтянуть новый фронтенд.
2. **«Добавить проект → Из папки на компьютере»** → выбрать объект, дисциплину,
   ввести название (без префикса `_`).
3. В папке должен быть **ровно один PDF**; желательно рядом `*_document.md`,
   `*_result.json`, `*_ocr.html`.
4. Если в папке **несколько PDF** — выбрать отдельную папку одного проекта (UI
   заблокирует с подсказкой).
5. Проверить превью (найденные файлы + предупреждения) → «Загрузить».
6. Первые **1–2 дня** загрузки держать под мониторингом (см. ниже).

### Monitoring (после партий загрузок)

```bash
python scripts/projects_v2/monitor_dual_write_uploads.py \
  --v2-root /home/coder/projects/PDF-proverka/projects_v2 \
  --legacy-root /home/coder/projects/PDF-proverka/projects \
  --phase post_batch --baseline-docs <docs_before> --baseline-map <map_before> --write-report
```

`PASS` — только если новые документы появились и в legacy, и в `projects_v2`,
все проверки зелёные, `shadow_write_errors=0`. Гейт остановки: validate FAIL,
MISMATCH>0, findings/version loss, shadow-errors>0, drift unstable>0.

### Backlog (НЕ делать сейчас)

- Full write-cutover в `projects_v2_primary`.
- Гонка `old_to_new_map.json` при массовых ОДНОВРЕМЕННЫХ загрузках
  (shadow-hook load→upsert→save без лока) → сериализовать/локать.
- Оптимизация синхронной латентности `migrate_project` в request-обработчике
  (для mass uploads вынести v2-mirror в background).
- Общий bundle-helper для `register-external` и `upload-folder` (сейчас
  `*_ocr.html` реализован в обоих, но единый хелпер не вынесен).

### Rollback

- Код: `git -C /home/coder/projects/PDF-proverka-deploy reset --hard e77d9e4` +
  targeted restart.
- v2-запись: `AUDIT_PROJECTS_V2_WRITE_MODE=legacy` + restart (legacy остаётся
  авторитетным).
- read-default: `AUDIT_PROJECTS_V2_READ_DEFAULT_ENABLED=false` + restart.
