# Профиль Вектографа «АР. План потолков и освещения» (`ar_ceiling_lighting`)

**Статус:** shadow-пилот, в production-аудит НЕ подключён.
**Ветка:** `feature/block-vector-graphs`.
**Текущая версия профиля:** `2026.08.07-2`.
**Последний коммит профиля:** `87fd9ea6` (audit-контекст).

Детерминированное извлечение семантического графа квартир из **векторного
слоя PDF**: без LLM, без OCR, без растрового распознавания. Символы
классифицируются по условным обозначениям самого листа (при отсутствии
подписи — по кросс-листовому реестру легенд комплекта), номера групп
читаются по координатам отдельных знаков, помещения восстанавливаются
заливкой от марки.

---

## 1. Что уже сделано (история коммитов)

| Коммит | Содержание |
|---|---|
| `5f34691d` | checkpoint пилота: 12 модулей, CLI, тесты; работает на эталонном листе |
| `e621c77d` | переносимость: 8 дефектов приёмки закрыты (см. §6) |
| `6910b8f3` | UI: полное описание в панели «txt» + backfill + endpoint |
| `8deab2dd` | backfill для галерейных проектов (`--corpus-dir`) |
| `33a9513b` | compact-представление (подробное поквартирное) |
| `87fd9ea6` | audit-представление + секционный `audit_context` + переключатель UI |

---

## 2. Карта кода

```
backend/app/pipeline/stages/block_grounding/ar_ceiling_lighting/
  coords.py        канонизация координат: CropBox→MediaBox, две НЕЗАВИСИМЫЕ проверки —
                   coordinate_alignment (fail-closed) и text_decoding_agreement (fail-soft);
                   rotation≠0 → RotationUnsupported (статус no_graph, не error)
  inventory.py     полный инвентарь: texttrace + get_drawings(extended), flatten re/qu/Безье,
                   окружности (full_arc + парные полудуги), цветовые семейства
  spatial.py       SpatialIndex (grid-hash), OccupancyGrid, build_chains, line_intersection
  legend.py        справочная область: ГЕОМЕТРИЧЕСКАЯ сборка строк легенды (дефис отдельным
                   спаном, переносы, мультисекции, многоколоночная нижняя полоса),
                   ведомость помещений, карточки квартир, штамп, примечания
  registry.py      кросс-листовой реестр легенд комплекта (tier 4)
  symbols.py       классификация по эталонам; template-evidence гейт CAD-слоёв при union;
                   снятие кружков-оформления подписей групп; составные потолочные маркеры;
                   раскрой «слипшихся» числовых спанов
  rooms.py         марки помещений, flood-fill областей, watershed для открытых планировок
  dimensions.py    размерные конструкции (засечки+линия+значение), линии центрирования
  graph.py         сборка графа + GEOMETRY_CONFLICT + semantic_ledger + плоская проекция
  render_md.py         ПОЛНЫЙ технический рендер (диагностика/приёмка)
  render_md_compact.py КОМПАКТНОЕ поквартирное описание (режим «Подробно» в UI)
  render_md_audit.py   AUDIT-контекст + секционный build_audit_context
                       (режим «Аудит», UI по умолчанию)
  overlay.py       диагностическая SVG-схема
  runner.py        оркестратор, гейт применимости, статусы, write_artifacts

scripts/
  build_ar_ceiling_lighting_description.py  один PDF → артефакты
  build_ar_ceiling_lighting_corpus.py       корпус: автопоиск по имени + реестр + матрица
  backfill_ar_ceiling_lighting_graphs.py    shadow-пакеты для реальных блоков проекта

tests/test_ar_ceiling_lighting_profile.py        73 теста (синтетика + регресс на эталоне)
tests/test_block_llm_text_shadow_markdown.py     14 тестов endpoint
frontend/tests/block_profile_full_markdown.test.js  15 тестов UI
```

---

## 3. Три представления графа

| Представление | Эталонный лист | Назначение | Где |
|---|---|---|---|
| `markdown` (full) | 86 973 байт / 51 237 симв. / 1287 строк | диагностика и приёмка | артефакт + API, **в UI не показывается** |
| `markdown_compact` | 31 424 байт / 19 105 симв. / 435 строк | подробное поквартирное описание | режим «Подробно» |
| `markdown_audit` | 9 110 байт / 5 827 симв. / 88 строк | высокосигнальный контекст для поиска замечаний | режим **«Аудит» — по умолчанию** |
| `audit_context` | 6 секций | сборка промпта по частям | API |

`audit_context` = `{summary, sheet_rules, ceilings, lighting_control, dimensions, uncertainties}`.

Секционный API (фильтрация **по графу до рендера**, не обрезка текста):

```python
from backend.app.pipeline.stages.block_grounding.ar_ceiling_lighting import (
    build_ar_ceiling_lighting_audit_context, render_markdown_audit)

build_ar_ceiling_lighting_audit_context(graph, sections=["dimensions"])
render_markdown_audit(graph, apartment_id="709")
render_markdown_audit(graph, group_ids=["<кв>:<номер>"])
```

---

## 4. Команды

```bash
# один лист
python scripts/build_ar_ceiling_lighting_description.py \
  --pdf "<вектор-PDF>" --out-dir "<папка>" \
  --legend-registry experiments/vectograf/ar_ceiling_lighting/legend_registry.json

# весь корпус (автопоиск по нормализованному имени + реестр легенд + матрица)
python scripts/build_ar_ceiling_lighting_corpus.py \
  --corpus-dir "experiments/блоки разных дисциплин/АР" \
  --out-dir "experiments/vectograf/ar_ceiling_lighting/corpus"

# backfill shadow-пакета для реального блока проекта (v2-путь ТОЛЬКО через --project-dir)
python scripts/backfill_ar_ceiling_lighting_graphs.py \
  --project-dir "projects_v2/objects/<объект>/disciplines/AR/documents/<док>/versions/v001" \
  --block-id <BLOCK_ID>

# галерейные проекты «ВЕКТОГРАФ — <дисциплина>»: у всех блоков page=1,
# document.pdf — склейка, поэтому источник ищется по block_id в имени файла
python scripts/backfill_ar_ceiling_lighting_graphs.py \
  --project-dir "projects_v2/objects/<объект>/disciplines/POS/documents/ВЕКТОГРАФ — АР/versions/v001" \
  --corpus-dir "experiments/блоки разных дисциплин/АР" --block-id <BLOCK_ID>

# тесты
python -m pytest tests/test_ar_ceiling_lighting_profile.py tests/test_block_llm_text_shadow_markdown.py -q
cd frontend && npx vitest run tests/block_profile_full_markdown.test.js
```

---

## 5. Контракт данных и UI

**Shadow-пакет** пишется в штатный каталог артефактов версии под
**суффиксным** именем:

```
<output_dir>/block_vector_graphs/<block_id>.ar_ceiling_lighting.json
<output_dir>/block_vector_graphs/_graphs/<block_id>.ar_ceiling_lighting.graph.json
```

Суффикс критичен: production-читатель Stage 01/02 (`load_prepared_package`)
ищет строго `<block_id>.json` и shadow **не видит** — аудит, findings и
промпты не меняются (закреплено тестом
`test_stage01_prepared_package_does_not_see_shadow`).

**Endpoint** `GET /api/tiles/{project_id}/blocks/llm-text/{block_id}`
дополнительно возвращает:
`profiled_graph_markdown_full`, `profiled_graph_markdown_compact`,
`profiled_graph_markdown_audit`, `audit_context`, `profile_shadow`.
Отсутствие артефакта → `null`, не 500. Путь к файлу клиенту не отдаётся,
имя строится санитайзером `artifact_filename` (traversal невозможен).

**UI**: существующая пилл-кнопка «txt» в правой панели блока; внутри
панели переключатель «Аудит» (по умолчанию) / «Подробно». Fallback:
audit → compact → full. Панель, кнопка, вкладка источника и кэш не
менялись.

---

## 6. Закрытые дефекты приёмки (не ломать при доработке)

1. Легенда собирается **геометрически** из спанов — `startswith("-")` гейтом не является.
2. Кросс-листовой реестр возвращает виды, которых нет в легенде текущего листа (tier 4).
3. Кружки вокруг цифр групп — оформление подписи, снимаются; истинная окружность переключателя **не** снимается.
4. `coordinate_alignment` ⊥ `text_decoding_agreement`: различие кодировки ≠ ошибка координат.
5. Чужой блок → `no_graph/profile_not_applicable`, не пустой граф.
6. Конкурирующие толкования → `GEOMETRY_CONFLICT`, без молчаливого выбора.
7. Размер tier 3 — только подтверждённая цепочка выносных + масштаб; близость → tier 2 `requires_review`.
8. Всё непривязанное уходит в `semantic_ledger`, а не теряется.
9. Наложенные переносы подписи из параллельных CAD-слоёв дедуплицируются
   геометрически; отдельная завершающая пунктуация не теряется.
10. Геометрически близкие элементы известных несовместимых CAD-слоёв не
    объединяются. Законная межслойная сборка разрешена только по сигнатуре
    доверенного device-template; проверяется вся пара DSU-компонентов, а
    заблокированные пары записываются в `validation.symbol_layer_clustering`.

---

## 7. Результаты на корпусе (10 листов АР)

| Статус | Листы | Причина |
|---|---|---|
| complete | 001 YF7P, 002 67DH, 003 7L6W, 004 4L77 | поквартирные планы |
| partial | 005, 006, 007, 010 | нет квартирных марок / символы не классифицированы |
| no_graph | 008 6NAY | `rotation_unsupported` |
| no_graph | 009 77AW | `profile_not_applicable` (интерьерный план подрядчика) |

Эталон 001: 10 квартир, 60 помещений (60 имён), 48 потолочных марок,
73 световые точки (13 настенных), 60 выключателей, 10 мастер-выключателей,
60 подтверждённых / 13 неполных групп, 59 размерных конструкций
(39 связаны с устройствами), 2 GEOMETRY_CONFLICT, 0 неразрешённых символов.
Два пакетных прогона байт-в-байт идентичны.

Лист 003 после layer-aware кластеризации: 76 световых точек (15 настенных),
64 подтверждённые / 10 неполных групп, 9 неразрешённых символов. Единственная
заблокированная пара слоёв — `09_Освещение` ↔ `14_сантех выводы`; контрольные
счётчики листов 001/002/004 не изменились.

---

## 8. Гочи (сэкономят часы)

1. **Прогон стадии `block_context` удаляет** из `block_vector_graphs/` все
   `*.json`, которых нет в актуальном `index.json` — shadow-файлы после
   него нужно бэкфиллить заново.
2. **Vite dev-сервер (5173) не используется**: он 500-ит на `index.html`
   даже на чистом HEAD. Живой фронт раздаёт сам бэкенд на **8081**.
3. **Кэш браузера**: после правок `app.js`/`index.html` нужен Ctrl+Shift+R,
   иначе панель покажет прежний вариант.
4. **Портал требует авторизации** (`PORTAL_AUTH_ENABLED=true`): для
   curl/Playwright нужен cookie `portal_session`
   (`backend.app.core.portal_auth.issue_token`).
5. **Рестарт бэкенда** — только после проверки очереди
   (`GET /api/audit/batch/status` → `active: false`) и с подтверждением.
6. `resolve_project_version_context` **не резолвит** projects_v2-пути по
   `project_id` — в backfill использовать `--project-dir`.
7. **Различие «лист vs блок»**: граф со стр. 104 полного `document.pdf` и
   граф из вырезанного PDF-блока могут отличаться на единицы (у эталона —
   группа `707:1`: в блоке неполная, в полном листе подтверждена). Причина —
   fail-closed отсечение по границе CropBox. Это не баг рендера.
8. **Мета-тест** `test_no_manual_map_of_this_pdf_in_code` запрещает в
   исходниках профиля шаблоны конкретного эталонного PDF (`YF7P`, `6.70X`,
   `70X:`, «Жилая комната») — в примерах кода использовать плейсхолдеры.

---

## 9. Что НЕ сделано (возможные следующие шаги)

Приоритет не назначен — согласовать с Андреем Ивановичем.

1. **Подключение audit к Stage 01/02 за отдельным флагом.** Сейчас профиль
   полностью shadow. Нужен явный флаг (по образцу существующих
   `*_ENABLED`), инъекция `audit_context` в промпт блока, замер на
   контрольной выборке до включения. Первый canary — только `status=complete`
   с проверкой provenance/freshness shadow-пакета; missing/partial/stale → no-op.
2. **Листы 005–007** — это планы МОП: марки разбиты на три спана
   (`<секция>.` + `МОП` + `.<номер>`), символы импортного CAD лежат на
   других слоях/цветах. Нужен отдельный подпрофиль `common_area`, а не
   расширение квартирного regex/гейта.
3. **Лист 010** — 61 обычная марка находится за исходным CropBox в скрытом
   MediaBox. Расширять fail-closed scope нельзя: нужно заменить/пересобрать
   corpus source либо помечать его preflight-причиной `crop_not_plan`.
4. **Rotation ≠ 0** (лист 008) — нормализация 90/180/270° в `coords.py`
   с сохранением исходного CropBox и provenance. После снятия rotation лист
   всё равно требует МОП-подпрофиль.
5. **Границы потолочных зон** при нескольких марках в одном помещении —
   в вектор-слое не выделены, зоны остаются `unresolved`.
6. **Раскатка на другие профили АР** (`ar_finish_plan`, `ar_wall_elevation`,
   `ar_detail`) по той же схеме: легенда листа → сигнатуры → граф →
   три представления.
7. **Обобщение в `graphic_primitives/`** — см. `docs/graphic_anchors/`
   (утверждённый дизайн, кода нет).

---

## 10. Правила работы с профилем

- Менять **представление** можно свободно; менять **детектор** — только с
  повторным прогоном корпуса и сверкой контрольных чисел (§7).
- `semantic_graph.json`, full и compact — стабильные контракты: не
  сокращать и не переименовывать поля без отдельного согласования.
- Любое изменение проверять командой из §4 (профильные + backend +
  frontend тесты), затем пересобрать корпус и бэкфиллить блоки.
- Production-аудит не трогать: профиль остаётся shadow до отдельного
  решения.
