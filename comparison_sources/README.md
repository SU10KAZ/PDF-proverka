# comparison_sources/ — исходные документы для раздела «Сравнение стадий»

Это **папка с исходными данными** для сравнения стадий проектной документации.

## Что куда класть

1. **Исходные проекты класть сюда (в `comparison_sources/`), а НЕ в `comparison/`.**
   Папка `comparison/` в корне проекта — runtime-данные системы (сессии,
   page_alignment, links, crops, findings, jobs, reports). Класть туда
   исходные PDF / MD / result.json **нельзя**.
2. В `stage_1/` кладётся **первая** версия / стадия документации (например,
   старая редакция, П-стадия, версия N−1).
3. В `stage_2/` кладётся **вторая** версия / стадия документации (новая
   редакция, РД-стадия, версия N).
4. **PDF, MD и `result.json` лучше класть рядом друг с другом и называть
   одинаково** — тогда сканер раздела «Сравнение стадий» сразу подцепит
   соседние Markdown и result.json к PDF.

## Пример раскладки

```
comparison_sources/object_01/stage_1/GP/project_gp.pdf
comparison_sources/object_01/stage_1/GP/project_gp.md
comparison_sources/object_01/stage_1/GP/project_gp_result.json

comparison_sources/object_01/stage_2/GP/project_gp.pdf
comparison_sources/object_01/stage_2/GP/project_gp.md
comparison_sources/object_01/stage_2/GP/project_gp_result.json
```

Объектов может быть несколько — например `object_02/stage_1/...`,
`object_03/stage_1/...` и т.д. Каждая папка `object_NN` — это один
объект (одно сравнение между двумя стадиями).

## Что указывать в UI «Сравнение стадий»

При создании сессии:

- **Папка первой стадии:**
  `/home/coder/projects/PDF-proverka/comparison_sources/object_01/stage_1`
- **Папка второй стадии:**
  `/home/coder/projects/PDF-proverka/comparison_sources/object_01/stage_2`

## Безопасность: allowlist

В проекте уже есть allowlist-проверка путей при создании сессии и при
ручном сопоставлении PDF-пар. Управляется переменной окружения
`AUDIT_STAGE_COMPARISON_ROOTS` — список папок через `:` (как `PATH`).
Если переменная не задана — backend пускает любые пути (для разработки).

Рекомендуемое значение для этой машины:

```
AUDIT_STAGE_COMPARISON_ROOTS=/home/coder/projects/PDF-proverka/comparison_sources
```

Тогда backend разрешит читать **только** содержимое
`comparison_sources/`, и случайный путь типа `/etc/...` или
`/home/coder/.ssh/...` система отклонит с 403.

Установить можно либо в `.env` в корне проекта, либо экспортировать в
shell перед запуском backend.

## Git

`.gitignore` настроен так, что в git попадают только `README.md` и
`.gitkeep`. Все реальные PDF / MD / JSON в `comparison_sources/`
**игнорируются** — их можно безопасно складывать сюда, ничего не
утечёт в репозиторий.
