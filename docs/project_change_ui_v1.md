# ProjectChange UI V1

Локальная реализация под флагом `?projectChangeUi=1#/stage-comparison`.
Включается только для `272_Sadovnicheskaya_76_Balchug_Esteyt`. Без флага
сохраняется действующий интерфейс. Дополнительный `&pcDebug=1` открывает старую
полосу внутренних стадий для диагностики.

## Что изменилось

| Страница | До | После включения флага |
|---|---|---|
| Загрузка документации | Большой блок сохранения и запуска каждой пары | Компактные счётчики, OLD/NEW, существующее автосохранение, открытие и меню ⋯. Ошибка сохранения с повтором. Ответ предыдущей сессии не перезаписывает текущую. |
| Сопоставление листов | «Связь блоков», технический режим в основной полосе | Полноценная вторая вкладка; фильтр «Все / Требуют проверки». Режим перенесён в «Дополнительно». Сохранены viewer, миниатюры, масштаб и навигация. |
| Изменения проекта | Разрозненные блоки расхождений, предварительный отчёт и отдельная проверка | Один список инженерных событий, пять фильтров, компактная сводка, встроенные REVIEW, несколько растровых фрагментов с подписями и увеличением. Характеристики и технические причины раскрываются отдельно. |
| Отчёт | Отдельный этап после предварительного отчёта | Только подтверждённые ProjectChange без открытого конфликта; счётчики по шифру, разделу или системе. Excel/PDF/HTML отключены с объяснением. |

Цвета, sidebar, селектор объекта и PDF viewer взяты из существующего приложения.
Интерфейс использует те же CSS tokens; проверен в desktop viewport 1600 × 1100.

## Контракт

`ProjectChangeView.fromEnvelope(envelope, objectId)` нормализует только явный
presentation-контракт. Сырые findings/diffs и атомарные факты в события не превращаются.

```ts
interface ProjectChangeEnvelope {
  schema_version: 'project-change-view/1';
  object_id: '272_Sadovnicheskaya_76_Balchug_Esteyt';
  origin: 'RESEARCH' | 'PRODUCTION';
  revision: string; // смена набора/версии сбрасывает применение демо-решений
  items: ProjectChangeView[];
}
interface ProjectChangeView {
  id: string;
  summary_ru: string;
  change_type: 'EQUIPMENT' | 'SYSTEM' | 'QUANTITY' | 'PARAMETERS'
    | 'REQUIREMENT' | 'LAYOUT' | 'OTHER';
  status: 'REVIEW' | 'CONFIRMED' | 'REJECTED' | 'UNDETERMINED'
    | 'PROBLEM' | 'CONFLICT';
  importance: 'HIGH' | 'NORMAL';
  cipher: string;
  discipline: string;
  engineering_system: string;
  engineering_subject: string;
  old_state: string;
  new_state: string;
  evidence: EvidenceView[];
  details: {label: string; old: string; new: string}[];
  conflicts: {explanation_ru: string; resolved: boolean;
    values: {source_type: 'TEXT' | 'TABLE' | 'GRAPHIC'; value: string}[]}[];
  review_question: string;
  review_explanation_ru: string;
  technical_provenance: string[];
}
interface EvidenceView {
  id: string;
  source_type: 'TEXT' | 'TABLE' | 'GRAPHIC';
  side: 'OLD' | 'NEW';
  document: {id: string; label: string; version: string; pdf_path: string};
  pair_id: string;
  page: number | null; // номер PDF, начиная с 1
  region: {units: 'normalized'; x: number; y: number;
    width: number; height: number} | null;
  image_url: string; // same-origin /static/... или /api/...; пусто при отсутствии
  short_explanation_ru: string;
  quote: string;
}
```

Нормализатор добавляет `research`, `revision`; применение демо-решения добавляет
`local_decision`. Неизвестный статус становится REVIEW, неизвестный источник — null.
Отсутствующие координаты не выдумываются. Внешние/protocol-relative/script URL
изображений не принимаются. Входные данные не мутируются, повторяющиеся id отсекаются.

`fromResearch(changes, context)` — отдельный offline adapter для уже сгруппированных
ProjectChange. Он допускает только объект 272 / DEV, получает проверенные привязки PDF,
переносит факты в details, сохраняет исходные причины в technical_provenance.
PROVEN и REVIEW исследовательского результата не становятся production truth:
все демо-события начинают с REVIEW, открытый конфликт — с CONFLICT.

Приложение читает `project_change_presentation` из ответа сессии (весь проект),
либо из ответа выбранной пары. При отсутствии поля показывает явное пустое состояние.
Компонент UI не читает research JSON. Выбор маршрута evidence не меняет группировку.

## Навигация и решения

«Открыть в PDF» передаёт `change_id`, `pair_id`, точные evidence OLD/NEW.
Проверяются PDF path, физическая версия и диапазон страниц. Для другого документа
загружается его пара; обе страницы устанавливаются одновременно, чтобы sheet-link
не заменил нужную NEW страницу. Масштаб/центр применяются после получения размеров
страниц, обе области подсвечиваются. Непривязанная сторона остаётся пустой.
Кнопка возврата открывает исходную карточку. Это переход внутри SPA; отдельный
копируемый URL с сериализованными координатами пока не создаётся.

Четыре REVIEW-действия работают в локальном исследовательском preview. Демо-решения
сохраняются в sessionStorage по объекту и revision, переживают reload в той же
вкладке, сбрасываются отдельным действием. Если запись в хранилище не удалась,
статус не меняется и видна ошибка. Открытый конфликт нельзя подтвердить; отклонённые,
неопределённые, проблемные и REVIEW-события в отчёт не попадают.

## Неподключённый backend

1. Публикация стабильного `ProjectChangeEnvelope` на уровне проекта, с обновлением
   revision и полным покрытием выбранных документов. Сейчас существующие production
   API возвращают другие объекты и не получают фиктивный адаптер из atomic findings.
2. Версионное сохранение решений по ProjectChange (id события, автор, revision,
   конфликт параллельных решений), разрешение конфликтов источников. Production
   REVIEW-кнопки отключены; существующие pair/finding decisions не переиспользуются
   для несоответствующего объекта.
3. Выдача crop/image для всех production evidence с проверенной привязкой к версии.
   Готовые `image_url` поддерживаются, отсутствие/ошибка растрового источника видимы.
   Для локального preview реальные PNG создаются из допущенных PDF.
4. Запуск и статус анализа всего проекта. Имеющийся запуск работает с одной парой;
   общая кнопка отключена. Индивидуальный запуск остаётся в меню пары (в preview
   также отключён, потому что сервер preview только читает данные).
5. Excel/PDF/HTML export именно подтверждённых ProjectChange. Все три кнопки
   отключены; подмена скачиванием research JSON не реализована.

В рабочем режиме загрузка документов, автоматическое/ручное сопоставление и
автосохранение используют существующие API. В preview изменение данных отключено.

## Источники preview и воспроизведение

Только `admitted_pairs('DEV')`, logical v002; события взяты из
`runs/dev_union_v1/pairs/{9,6,13}.json` среза 20260914_project_change_272.
Берутся четыре существующих группированных события: контур T12/T22, неопределённый
насос, общий расход водоотведения, площадь из ОДИ. Представительская выборка UI
не является метрикой качества. Для каждого evidence проверен SHA256 исходного PDF;
все использованные страницы проверяются на embargo. VALIDATION/FINAL_HOLDOUT не читались.

Сохранённые пары/карта листов preview — демонстрационное состояние интерфейса по
страницам этих evidence, а не новый результат matcher или ручная truth.
Ни human truth, ни исследовательские результаты не записываются.

```sh
# Из корня checkout; PyMuPDF и Node должны быть доступны.
python scripts/project_change_ui_demo.py --port 8973
# Открыть http://127.0.0.1:8973/?projectChangeUi=1#/stage-comparison

cd frontend
npm test -- --run tests/project_change_view.test.js tests/project_change_autosave.test.js tests/stage_comparison*.test.js
```

Для браузерной проверки (из корня; preview должен работать):

```sh
npm install --prefix /tmp/project-change-ui-tools playwright --no-audit --no-fund
PLAYWRIGHT_MODULE=/tmp/project-change-ui-tools/node_modules/playwright \
PC_CHROME=/home/coder/.cache/ms-playwright/chromium-1228/chrome-linux64/chrome \
node frontend/tests/project_change_ui.browser.cjs
```

`PC_CHROME` можно заменить локальным Chromium либо не задавать при установленном
браузере Playwright. Preview слушает только 127.0.0.1; production не используется.
Растры, presentation.json, вход adapter и source-receipts.json создаются в
`/tmp/project-change-ui-272`. В preview нет WebSocket-сервиса: индикатор WS остаётся offline.

## Снимки

- [Страница 1 — Загрузка документации](../deliverables/project_change_ui_v1/screenshots/page-1-upload.png)
- [Страница 2 — Сопоставление листов](../deliverables/project_change_ui_v1/screenshots/page-2-sheets.png)
- [Страница 3 — Изменения проекта](../deliverables/project_change_ui_v1/screenshots/page-3-changes.png)
- [Страница 4 — Отчёт](../deliverables/project_change_ui_v1/screenshots/page-4-report.png)
- [Увеличенный REVIEW-фрагмент](../deliverables/project_change_ui_v1/screenshots/review-image-expanded.png)

В снимке отчёта показано одно локальное демо-подтверждение, сделанное браузерным
тестом. Это не подтверждение инженерной истины и не запись production.

## Изменённые файлы

- `frontend/index.html` — подключение четырёх страниц под флагом.
- `frontend/static/js/app.js` — presentation state, демо-решения, PDF navigation, защита автосохранения от ответа чужой сессии.
- `frontend/static/js/project-change-view.js` — контракт, адаптеры, фильтры и правила отображения.
- `frontend/static/js/project-change-ui.js` — Vue-компонент изменений/отчёта, REVIEW и изображения.
- `frontend/static/css/project-change-ui.css` — компактная компоновка в существующей палитре.
- `frontend/tests/project_change_view.test.js` — контрактные проверки.
- `frontend/tests/project_change_autosave.test.js` — асинхронные проверки сохранения.
- `frontend/tests/project_change_ui.browser.cjs` — браузерные проверки и снимки.
- `scripts/project_change_ui_demo.py` — read-only loopback preview, admission, source receipts, реальные crops.
- `docs/project_change_ui_v1.md` — этот документ.
- `deliverables/project_change_ui_v1/browser-results.json` — результат браузерных проверок.
- `deliverables/project_change_ui_v1/source-receipts.json` — происхождение исследовательских артефактов.
- Пять PNG из списка «Снимки» выше.

## Проверки

- 348 профильных Vitest-тестов PASS, включая 32 новых теста контракта и 4 теста
  асинхронного автосохранения.
- 20 браузерных сценариев PASS: настоящие Vue-компоненты и существующий viewer;
  все 4 страницы, multiple evidence, увеличение/Escape/focus, детали, фильтры,
  точные страницы и регионы, миниатюры, возврат, четыре решения, reload,
  неполный OLD, исключения из отчёта, конфликт и ошибка изображения, флаг off.
- Общий frontend-прогон: 692 PASS / 6 FAIL. Все шесть падений воспроизведены
  на исходном HEAD: `md_page_alignment.test.js` (3),
  `section_optimization_card.test.js` (1), `stage_algorithm_guide.test.js` (2).
  В связанном с задачей наборе падений нет.
- Vite build в `/tmp/project-change-ui-build`: PASS; syntax checks JS/Python
  и `git diff --check`: PASS. Сборка не является релизом и не копируется в production.

## Границы изменений

Алгоритмы TEXT/TABLE/GRAPHIC, Sheet Matcher, EngineeringSubject, EvidenceScope,
ProjectChange grouping не менялись. Миграций, tuning и human truth edits нет.
Production: NO. Push/deploy: 0/0.
