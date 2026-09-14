# ProjectChange backend bridge V1

Локальная интеграция интерфейса `b7243c89` с закреплёнными исследовательскими
ProjectChange объекта 272, Садовническая 76 / Балчуг Эстейт, OLD=stage_1,
NEW=stage_2, logical v002. Никакие алгоритмы сравнения, grouping, truth,
Sheet Matcher, EvidenceScope и EngineeringSubject не изменены и не запускаются.

## Источники и статус

[Манифест источников](../backend/app/data/project_change_bridge_sources.json)
создан до реализации адаптера. Выбран один завершённый замороженный replay:
`runs/dev_frozen_replay_b`, кандидат `within_272_v1`. Его code receipts совпадают
с замороженным candidate manifest; ProjectChanges replay A и B идентичны на
всех 13 допущенных DEV-парах. Обе версии уже DEV-known, новых blind claims нет.

Кандидат имеет статус **REJECT_CANDIDATE / NOT_ADJUDICATED**. Это доступный
замороженный исследовательский снимок, а не принятый результат или текущая
production truth. Более новые незамороженные semantic/typed experiments не
смешиваются с ним. Предыдущий UI fixture `dev_union_v1` не используется bridge.
В манифесте перечислены рассмотренные альтернативы, причина выбора и SHA-256
каждого артефакта. Содержимое VALIDATION и FINAL_HOLDOUT для этой работы не
используется; статус отклонения взят из метаданных исследовательского состояния.

| Данные | Количество |
|---|---:|
| ProjectChanges | 73 |
| Исследовательский PROVEN | 14 |
| Исследовательский REVIEW | 59 |
| TEXT evidence entries | 77 |
| TABLE evidence entries | 198 |
| GRAPHIC evidence entries | 8 |
| Все evidence entries | 283 |
| Уникальные evidence IDs / URL | 213 |
| Разные PDF-изображения (PDF hash + page + region) | 55 |
| Точные области | 8 |
| Полные страницы, PAGE_LEVEL | 47 |

GRAPHIC в выбранном frozen replay уже не пуст: показаны существующие восемь
доказательств двух событий; алгоритм графического сравнения не добавлялся.
Одна страница может доказывать несколько ячеек/событий, поэтому количество
доказательств не равно количеству разных растров.

`FrozenSources` вызывает существующий `admitted_pairs('DEV')`, проверяет
закреплённый whole-cipher split, inventory, overlap audit, манифест кандидата,
replay и артефакты. Каждый PDF обязан принадлежать допущенным object/stage/code/
version. Вспомогательные receipts могут указывать только на ту же исходную
версию или её изолированную DEV-копию. Проверяются PDF, blocks и markdown,
включая изолированные источники, с SHA-256 и повторной проверкой при изменении
stat/identity файла. Изменение источника останавливает выдачу и сохранение
решений; автоматического переключения на свежий run нет.

## API и запуск

Префикс:

```text
/api/project-change-preview/objects/272_Sadovnicheskaya_76_Balchug_Esteyt
```

| Метод / суффикс | Ответ |
|---|---|
| GET (корень) | ProjectChangeView envelope, эффективные решения, summary, read-only viewer session |
| GET `/manifest` | Закреплённый source manifest |
| GET `/report` | Только элементы с эффективным CONFIRMED и без открытого конфликта |
| POST `/decisions` | Append-only решение и обновлённый envelope |
| GET `/decisions/{decision_key}` | История точного ключа и родственных устаревших версий с `same_decision_key` |
| GET `/evidence/{evidence_id}/crop` | PNG по серверной provenance |
| GET `/viewer/pairs/{pair_id}` | Исходная пара, размеры документов, перечень страниц доказательств |
| GET `/viewer/pairs/{pair_id}/page-info` | Размеры, rotation, page count, PDF SHA-256 |
| GET `/viewer/pairs/{pair_id}/page-preview`, `/page-thumb`, `/page-tile` | Растры допущенных PDF-страниц |

Контракт остаётся `schema_version: project-change-view/1`, `origin: RESEARCH`,
добавляется `mode: BACKEND_PREVIEW`. `items` — готовые ProjectChangeView.
Frontend не разбирает исследовательский JSON. Каждый элемент содержит
`source_run_id`, `candidate_version`, `research_status`, `decision_key`,
`binding_signature`, `decision_state`, `effective_decision`. В обычной карточке
нет служебных run/entity/event IDs; они доступны в технических подробностях.
Исходные summaries, OLD/NEW states и факты не переписываются ради демонстрации.

Запись решения требует JSON (не допускаются лишние поля) и заголовок
`X-ProjectChange-Preview: 1`:

```json
{
  "change_id": "pc_current_id",
  "decision_key": "pcbridge1_<64 hex SHA-256>",
  "binding_signature": "<64 hex SHA-256>",
  "action": "CONFIRM",
  "comment": "Необязательный комментарий, до 4000 символов",
  "expected_source_revision": "<64 hex SHA-256>",
  "expected_decision_revision": 0
}
```

Действия: CONFIRM, NOT_A_CHANGE, UNSURE, BROKEN_CASE. Actor нельзя передать в
теле: в portal берётся подписанная серверная сессия; без неё запись 401.
Отдельный loopback launcher задаёт явного локального actor, по умолчанию
`codex-local-preview`. Он не выдаёт себя за сотрудника. Чужой объект и выключенный
feature дают 404, несовпадение версий — 409, недоступные/изменённые источники —
503. При недоступности источников UI убирает устаревшие карточки/отчёт и
предлагает обновление. Поздние ответы другого объекта или более старой ревизии
не перезаписывают текущие данные.

В основном backend флаг выключен по умолчанию. Для намеренного локального
включения необходимы оба параметра `PROJECT_CHANGE_PREVIEW_ENABLED=1` и
`PROJECT_CHANGE_PREVIEW_STATE_DIR=/путь/вне/репозитория/и/auditmanager`.
Они не установлены в production. Импорт research guard отложен до явного
включения preview, основной startup его не импортирует.

Для проверки без запуска production backend:

```bash
python scripts/project_change_bridge_preview.py \
  --port 8974 --state-dir /tmp/project-change-bridge-v1-verified
```

Открыть `http://127.0.0.1:8974/?projectChangeUi=1#/stage-comparison`.
Launcher слушает только loopback, проверяет Host, использует настоящий router,
adapter и SQLite; fixture ProjectChanges не создаёт. Служебные endpoints общей
оболочки возвращают пустые списки, не обращаясь к production. Вне этой оболочки
неподключённые сервисы могут отвечать 404. V1 mock server на 8973 используется
только для регрессионного теста старого режима, не для доказательств bridge.

## Идентичность и повторное применение

В существующем `stage_comparison/domain_keys.py` есть канонизация и SHA-256,
но нет стабильного namespace ProjectChange. `pc_xxx` зависит от event key,
subject/grouping и для TABLE/GRAPHIC от fact IDs; `EngineeringSubject.entity_id`
и `scope_key` также не являются доказанной внешней идентичностью. Ledger
`decision_registry.py` относится к другому production-контракту. Из него
решения не импортируются, существующие данные не мигрируют.

Новый `pcbridge1_<full SHA-256>` использует существующую DomainKey canonical
JSON/signature: object, OLD/NEW stages, document codes, структурированный
subject (mark/class/system/function/room/floor/semantic subject), event type,
stable scope, набор свойств и отсортированные content-bound evidence keys.
Evidence keys включают PDF SHA-256, физическую версию, сторону, страницу,
route, hash цитаты, позиции/хэши строк, locator таблицы/области и source receipts.
Display summary, runtime pc/event/entity/fact IDs и run ID не являются ключом.

Отдельная binding signature включает identity, candidate version, OLD/NEW
состояния, типизированные значения/единицы/цитаты, resolution/identity basis,
research status/reasons и conflicts. Повторное применение разрешено только
при полном совпадении этой подписи и кандидата, EXPLICIT/LOCAL identity,
наличии доказательств и отсутствии неоднозначности/коллизий ключа. Изменение
порядка доказательств, текста заголовка или runtime ID при прежней binding
не теряет решение. Реальный frozen A/B replay подтверждён тестом.

Для неоднозначного события решение применимо только к конкретному исходному
run/pc ID с прежней binding; на новом run оно STALE_DECISION. Новый кандидат,
изменённые доказательства, вывод, статус или конфликт не наследуют одобрение.
Общий `pclineage1_` без evidence служит **только для поиска устаревшей истории**,
никогда для автоматического подтверждения. Если изменился и общий subject/scope,
старое решение не применяется, новая запись остаётся REVIEW; автоматического
поиска всех возможных предшественников/переноса нет.

Берётся последнее решение точного ключа: несовместимое последнее решение не
позволяет восстановить более раннее одобрение. Консервативные ложные stale
допустимы; изменение source serialization/locator/кандидата требует новой
проверки. Межкандидатная миграция решений не реализована.

## Журнал

`<state-dir>/decisions.sqlite3`, отдельная таблица `decisions`:

```text
revision INTEGER PRIMARY KEY AUTOINCREMENT
stable decision_key, lineage_key
project_change_id, source_run_id, candidate_version, binding_signature
decision (CHECK: четыре действия)
actor, timestamp (UTC, server-owned), comment
evidence_snapshot (JSON с content receipts, состояниями и условиями binding)
```

Индексы: decision_key/revision и lineage_key/revision. SQLite triggers запрещают
UPDATE/DELETE. `BEGIN IMMEDIATE` + expected global revision сериализуют запись;
второй клиент со старой ревизией получает 409. История доступна через API и
технические подробности карточки. SessionStorage используется только прежним
V1 demo, не backend bridge. Открытый конфликт сохраняет значения источников;
подтверждение запрещено, алгоритма разрешения конфликтов нет.

## PDF

Координаты берутся только из `locator.bbox_pdf_points`, если его страница
совпадает с evidence page. Проверяются числовая конечность, границы и rotation;
normalized region относится к растру этой страницы. Поиск цитаты ради
придумывания bbox не используется. Нет bbox — полный PDF page и PAGE_LEVEL,
видимая надпись «Открыть страницу». Множественные OLD/NEW evidence сохраняются.

PyMuPDF читает PDF и пишет только PNG cache в `<state-dir>/crops/`. Cache key:
PDF SHA-256, page, source region, rendering options/version. Для переходов
используется read-only viewer adapter, проверяются exact pair, PDF path,
version и page; region подсвечивается только при наличии. Непривязанная сторона
остаётся пустой. Embargo pages недоступны. Параметры path/bbox от клиента API
не принимает; ID разрешается по серверному индексу. Представленные страницы
не выдаются за новое сопоставление листов: карта помечена как инвентарь
доказательств, редактирование отключено.

## Демонстрация и проверки

[Результаты браузера](../deliverables/project_change_backend_bridge_v1/integration-results.json)
содержат источник, счётчики, выбранные реальные pc IDs, серверные решения и
список запросов записи. Демонстрационные решения выполнены автоматически
актором `codex-local-preview` в отдельной БД; это проверка интеграции, не
независимая инженерная adjudication. Источник/research truth не изменён.
[Receipts всех 213 crop URL](../deliverables/project_change_backend_bridge_v1/crop-receipts.json)
включают SHA-256 полученных PNG.

Скриншоты настоящих данных:

- [Страница 3 — REVIEW](../deliverables/project_change_backend_bridge_v1/screenshots/page-3-review.png)
- [Страница 3 — TABLE, несколько доказательств](../deliverables/project_change_backend_bridge_v1/screenshots/page-3-table.png)
- [Страница 3 — PROVEN, TEXT/GRAPHIC](../deliverables/project_change_backend_bridge_v1/screenshots/page-3-proven-multiple.png)
- [Область PDF](../deliverables/project_change_backend_bridge_v1/screenshots/real-evidence-region.png)
- [PDF deep-link](../deliverables/project_change_backend_bridge_v1/screenshots/pdf-provenance.png)
- [Страница 4 — только подтверждённое](../deliverables/project_change_backend_bridge_v1/screenshots/page-4-confirmed-report.png)

Команды:

```bash
python -m pytest tests/test_project_change_bridge.py tests/test_stage_comparison_domain_keys.py -q
cd frontend
npx vitest run
npm run build
# Из корня репозитория, с новым изолированным state-dir для каждого прогона:
PLAYWRIGHT_MODULE=/tmp/project-change-ui-tools/node_modules/playwright \
PC_CHROME=/home/coder/.cache/ms-playwright/chromium-1228/chrome-linux64/chrome \
node frontend/tests/project_change_bridge.browser.cjs
```

Browser script требует пустую новую БД и не сбрасывает журнал. Backend tests
проверяют реальные 73 события, A/B replay, identity changes, ambiguity,
append-only/restart/concurrent writes, старые approvals, report, actor,
feature/object scope, реальные OLD/NEW crops, координаты, fallback и embargo.
Frontend tests проверяют binding/status, report, три evidence route и
поздние/ошибочные ответы. V1 browser regression сохраняется.

Проверено: **57 Python PASS** (31 bridge + 26 DomainKey), **714 Vitest PASS**,
**17 browser bridge PASS**, **20 browser V1 regression PASS**, Vite build PASS.
Всего 808 прошедших проверок. Общий Vitest также сохраняет шесть известных
падений, воспроизведённых до bridge на исходном UI: три `md_page_alignment`,
одно `section_optimization_card`, два `stage_algorithm_guide`. Новых падений нет.
[Машинная сводка](../deliverables/project_change_backend_bridge_v1/validation-results.json).
[Точный список изменённых файлов](../deliverables/project_change_backend_bridge_v1/changed-files.txt).

## Оставшиеся ограничения

- Это read-only frozen research preview отклонённого кандидата; автоматического
  выбора новых результатов/production публикации нет.
- Большинство TEXT/TABLE receipts не содержат точных bbox: 47 разных полных
  страниц вместо придуманных точных фрагментов.
- Перенос решений между различными кандидатами и разрешение конфликтов требуют
  отдельного контракта. SQLite рассчитан на локальный preview; централизованные
  migration/backup/access-management не вводились.
- Whole-project launch, Excel/PDF/HTML export, изменение сопоставления и PDF
  text search в preview отключены. Новый GRAPHIC comparator не реализован.
- Feature выключен по умолчанию; production не активирован, push/deploy: 0/0.
