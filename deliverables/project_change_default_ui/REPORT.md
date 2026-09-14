# ProjectChange UI — общий интерфейс Stage Comparison

Локальная приёмка **PASS**. Подготовлен кандидат **21e0b8da**, основанный непосредственно на текущем production-коммите **1e9ca7ee**. Изменения в production не установлены; push и deploy не выполнялись.

Обычный маршрут `https://auditmanager.app/#/stage-comparison` в кандидате открывает одинаковые четыре вкладки для любого объекта. Параметр `projectChangeUi` не участвует ни в выборе интерфейса, ни в доступе к snapshot. Пользовательские названия Research Preview / предпросмотр удалены из основной оболочки.

## Результаты по требованиям

| Проверка | Результат и подтверждение |
|---|---|
| A. Объект 272, обычный URL | PASS — новая оболочка, 13 исходных пар |
| B. Объект 272, старый URL с флагом | PASS — те же вкладки и тот же dataset |
| C. Другой объект, обычный URL | PASS — та же оболочка, собственные документы |
| D. Другой объект без ProjectChange | PASS — Page 1, Page 2, пустые Page 3 и Page 4 |
| E. Изоляция snapshot endpoint | PASS — чужие ID получают 404 для presentation, manifest, report, pair и crop |
| F. Отсутствие переноса 73 изменений | PASS — контракт другого объекта возвращает его ID и пустой `items`; неверный object_id отбрасывается frontend |
| G. Current pair: Page 1 → 2 → 3 | PASS — проверены пары 272, смена пары через Page 1, обычная пара и переключение объектов |
| H. Загрузка и сопоставление | PASS — реальные POST upload-folder/session/pair, автоматическое сопоставление документов, PUT document-pairing и sheet-links, ответы 200 |
| I. Ошибки JavaScript | PASS — 0 page errors, 0 ошибок приложения в консоли, 0 ошибочных HTTP-ответов Stage Comparison в рабочем сценарии |

Page 3 показывает «Для выбранной пары пока нет результатов анализа изменений.». Page 4 показывает «Подтверждённых изменений пока нет.». Для входа без выбранной пары остаётся переход на Page 1. На Page 3 нет выбора пары, режима всех пар или фильтров.

Общий endpoint `GET /api/stage-comparison/objects/{object_id}/project-changes` сообщает о доступности явного ProjectChangeView. Только зарегистрированный dataset объекта 272 использует замороженный snapshot. Для остальных объектов возвращается `availability: UNAVAILABLE`, после чего продолжается обычный workflow документов. Явный object-scoped ProjectChangeView из session/pair также принимается; discrepancies, questions и sheet differences не преобразуются в ProjectChange.

Загрузка документов не блокируется из-за отсутствия или ошибки ProjectChange dataset. Устаревшие ответы при переключении объекта не могут восстановить чужую пару или заменить текущую сессию. Запрет mutations ограничен решениями read-only snapshot; обычные изменения документов разрешены.

## Snapshot и границы изменений

SHA-256 `MANIFEST.json` сохранился:

```
37a76c86508aca5b2dbbffca58aac6a23e88bc723c031fd78ad1cfd58df87dd7
```

Все 73 ProjectChange сохранены. Проверены crop, увеличение, точные PDF deep-links, возврат к строке, пустая OLD-сторона при отсутствии привязки и запрет решений. 14 research PROVEN не включаются в отчёт автоматически. Snapshot-файлы, service адаптера snapshot, алгоритмы и semantic pipeline не входят в diff кандидата.

Приёмка выполнена в чистом clone `/tmp/project-change-default-acceptance`, ветка `main`. Для 272 использован настоящий release snapshot. Другой объект — явно обозначенная UI fixture с отдельными сгенерированными PDF и временным хранилищем. Upload/session/pair/document matching/sheet linking/viewer обслуживались настоящими backend-маршрутами; реестр объектов и служебная оболочка портала изолированы. Это локальная проверка совместимости контрактов, а не live smoke всех существующих объектов production.

Внешние соединения сервера приёмки запрещены. Запросов к OpenRouter: **0**. Исследовательские алгоритмы и резервные выборки не запускались.

## Проверки

| Набор | Результат |
|---|---|
| Browser acceptance | **34 PASS** |
| Профильные frontend-тесты, включая uploads | **130 PASS** |
| Backend snapshot/isolation/uploads | **32 PASS** |
| Browser mutation harness | **1 PASS** |
| Vite build | **PASS** |
| Полный frontend-набор | **744 PASS**, 6 прежних падений, 0 новых |

Шесть прежних падений воспроизведены до promotion: `md_page_alignment` — 3, `section_optimization_card` — 1, `stage_algorithm_guide` — 2. [Сводка исходной версии](baseline-vitest-summary.txt), [сводка кандидата](vitest-summary.txt). Полные логи сохранены с gzip.

[Машинный отчёт](acceptance.json) содержит commit, SHA-256 рабочих файлов, чистоту clone и счётчики. [Браузерный журнал](browser-results.json) содержит все 34 проверки и HTTP-запросы. Также сохранены [pytest](pytest.log), [build](build.log) и [harness](harness.log).

## Обязательные скриншоты

1. [Объект 272 — обычный URL](screenshots/01-object-272-canonical.png).
2. [Другой объект — обычный URL](screenshots/02-other-canonical.png).
3. [Другой объект — Page 3 без ProjectChange](screenshots/03-other-changes-empty.png).
4. [Обычный объект — Page 1 после загрузки дополнительного PDF](screenshots/04-other-upload-page.png).
5. [Обычный объект — Page 2 с сохранённой связью листов](screenshots/05-other-viewer.png).

Дополнительно: [пустой отчёт другого объекта](screenshots/06-other-report-empty.png), [изменения выбранной пары 272](screenshots/07-object-272-changes.png), [PDF deep-link snapshot](screenshots/08-snapshot-pdf-deep-link.png).

## Кандидат и воспроизведение

Кандидат: `21e0b8daa1e2001389bd0c49d3162b720916bce0`.
База: `1e9ca7ee434f3412f347cfaee1dbd559f1a68ca7`.
Изменение в основном рабочем `main`: `755a405b040b977f3e39d44a558f8e0e16616d81`.

[Patch кандидата](canonical-candidate.patch), [проверенный Git bundle](canonical-candidate.bundle), [список 13 изменённых файлов](changed-files.txt). Bundle требует указанную production-базу; patch и bundle содержат одно и то же изменение и являются альтернативными способами переноса.

Перед правками основной `main` был синхронизирован с семью уже опубликованными коммитами UI/snapshot через merge `15433714`. Для кандидата поверх production перенесён только promotion-коммит: локальные исследовательские изменения в кандидат не попали.

```bash
# В чистом checkout кандидата:
python -m pytest tests/test_project_change_bridge.py \
  tests/test_stage_comparison_stage_upload.py \
  tests/test_stage_comparison_stage_upload_batch.py -q
cd frontend
npm test
npm run build
cd ..
python scripts/project_change_ui_smoke_server.py --port 8995
# В другом терминале; каждый новый browser acceptance требует свежий сервер:
NODE_PATH=/tmp/project-change-ui-tools/node_modules \
CHROME_BIN=/home/coder/.cache/ms-playwright/chromium-1228/chrome-linux64/chrome \
SMOKE_BASE=http://127.0.0.1:8995 SMOKE_OUTPUT=/tmp/project-change-default-browser \
node frontend/tests/project_change_table.browser.cjs
```

Основное рабочее дерево по-прежнему содержит два существовавших до задачи посторонних untracked-файла: `docs/diverse_corpus_restore.md` и `scripts/restore_diverse_corpus.py`. Они не включены в коммиты задачи. Clone кандидата чистый. Релиз не собирался и не публиковался; production остался на прежнем коммите.
