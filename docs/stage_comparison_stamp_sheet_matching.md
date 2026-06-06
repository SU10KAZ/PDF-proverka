# Stage Comparison — сопоставление листов по штампу (page-alignment)

**Дата:** 2026-06-05 (LLM-доматчинг — 2026-06-06; признаки/hard-gates/mutual-best/adjudicator — 2026-06-06)
**Статус:** always-on (отдельная кнопка в «Связь блоков», ничего не ломает); Haiku-доматчинг — опц. чекбокс, fail-soft
**Модули:**
- [backend/app/services/stage_comparison/stamp_matching.py](../backend/app/services/stage_comparison/stamp_matching.py) — чистый матчер (+ опц. `llm_match_fn`)
- [backend/app/services/stage_comparison/stamp_llm_match.py](../backend/app/services/stage_comparison/stamp_llm_match.py) — Haiku-доматчинг семантически эквивалентных имён
- [backend/app/services/stage_comparison/store.py](../backend/app/services/stage_comparison/store.py) — `suggest_alignment_by_stamp(use_llm=…)`
- [backend/app/api/routers/stage_comparison.py](../backend/app/api/routers/stage_comparison.py) — эндпоинт

## Задача

При сравнении стадий одни и те же листы между старой и новой версией часто
стоят на РАЗНЫХ страницах PDF (схема ГРЩ/ВРУ на стр. 21 старой стадии и на
стр. 56 новой). Существующий `suggest_alignment` матчит страницы по
**fingerprint'у** (соотношение сторон, число блоков, первые 300 символов
текста) с окном `lookahead=4` — то есть предполагает малый сдвиг и НЕ находит
далеко уехавшие листы.

Имя листа в штампе («Наименование листа») — гораздо более устойчивый
идентификатор. `suggest_alignment_by_stamp` матчит листы **глобально по имени**,
поэтому находит совпадения независимо от смещения страниц, и предлагает
поставить их напротив друг друга (в один слот `page_alignment`).

## Источник имени листа

Сырой Chandra MD (`<side>_document.md`, поле `md_path` пары) уже содержит
по-страничную разметку (её же парсит `build_fact_index` в
[evidence_first_fallback.py](../backend/app/services/stage_comparison/evidence_first_fallback.py)):

```
## СТРАНИЦА 21
**Лист:** 21
**Наименование листа:** Однолинейная расчетная схема ВРУ.ИТП
```

- **Основной источник:** `**Наименование листа:**` из MD (доступен до Qwen-обогащения).
- **Фолбэк (офлайн):** для страниц без имени листа подмешивается текст-слой
  блоков из `result.json` (`pdfplumber_text` / `ocr_text`) как слабая
  текст-сигнатура (`extra_text_by_page`). Сети нет, `crop_url` не дёргается.
  `result.json → stamp_data` несёт только шифр/стадию/объект/организацию —
  **имени листа там нет**, поэтому имя берётся из MD/текст-слоя.

## Алгоритм матчинга (`match_sheet_indexes`)

```text
build_sheet_index(md)            # PageRec → SheetRec (page, sheet_no, norm_name, is_graphic)
  ├─ forward-fill: страницы-продолжения многостраничного листа («**Лист:** 2»
  │  без имени) наследуют имя предыдущего именованного листа → многостраничная
  │  «Текстовая часть» матчится по имени в порядке появления, а не рассыпается
  └─ text_layer фолбэк для всё ещё безымянных страниц
match_sheet_indexes(left, right)
  ├─ Pass 1 exact: одинаковое нормализованное имя; дубликаты (повторяющиеся
  │  планы, многостраничные листы) — в порядке появления (1-й↔1-й, 2-й↔2-й)
  ├─ Pass 2 fuzzy: взвешенная косинусная близость токенов c IDF-весами
  │  внутри пары (общий бойлерплейт-префикс «Часть 1. …» обесценивается,
  │  решают редкие токены «ВРУ»/«ГРЩ»/«молниезащита»/номер этажа)
  │  + порог + MARGIN-ГЕЙТ: лучший кандидат должен заметно опережать второго,
  │  иначе слипшийся набор похожих имён → НЕОДНОЗНАЧНО → не предлагаем
  └─ остаток → left_only / right_only (ручной матч)
build items: слоты в порядке левых страниц; сматченные пары — в одном слоте
  (напротив друг друга), right-only вставляются по возрастанию номера
```

**Precision > recall:** инструмент предлагает только уверенные совпадения
(точное имя или fuzzy с явным отрывом). Неоднозначные графические листы с
общим именем (когда per-drawing-заголовок отсутствует в MD) остаются непарными
и матчатся вручную — это честнее, чем выдать скремблированные пары.

### Нормализация имени (`normalize_sheet_name`)

NFKC + ё→е + lower, срезаются `(из N)`, «лист N», «стр. N», «none», пунктуация →
пробел. `«ГРЩ-0,4кВ»` и `«грщ 0 4кв»` дают одну норму.

### Тюнинг (env, безопасные дефолты)

| Переменная | Default | Назначение |
|---|---|---|
| `STAGE_COMPARISON_STAMP_MATCH_MIN_SCORE` | `0.55` | порог fuzzy (взвеш. косинус) |
| `STAGE_COMPARISON_STAMP_FALLBACK_MIN_SCORE` | `0.75` | строже для text_layer-имён |
| `STAGE_COMPARISON_STAMP_MATCH_MIN_MARGIN` | `0.07` | мин. отрыв лучшего от второго |

## API

`POST /api/stage-comparison/sessions/{sid}/pairs/{pid}/page-alignment/suggest-by-stamp`
— возвращает предложение, **ничего не применяет**:

```json
{
  "method": "stamp",
  "confidence": 0.99,
  "matched_count": 19, "left_only_count": 41, "right_only_count": 29,
  "left_page_count": 60, "right_page_count": 48,
  "left_pdf_page_count": 60, "right_pdf_page_count": 48,
  "warnings": [],
  "suggested_items": [
    {"slot": 1, "left_page": 4, "right_page": 4, "mode": "manual",
     "note": "Содержание тома · exact_name 1.00",
     "match": true, "match_type": "exact_name", "score": 1.0,
     "left_sheet_name": "Содержание тома", "right_sheet_name": "Содержание тома",
     "is_graphic": false, "needs_review": false},
    {"slot": 33, "left_page": 51, "right_page": 32, "match": true,
     "match_type": "fuzzy_name", "score": 0.95, "needs_review": true,
     "left_sheet_name": "Однолинейная расчетная схема ВРУ.ИТП", ...}
  ]
}
```

Применение — обычный `PUT .../page-alignment` с очищенными items (только
`slot/left_page/right_page/mode/note`). Сматченные пары встают напротив друг
друга; `save_alignment` сам пересчитывает stale-метки существующих связей
(`_resync_links_after_alignment`).

`match`/`match_type`/`score`/`*_sheet_name`/`needs_review` — display-only поля
для UI; `alignment.validate` их отбрасывает при сохранении.

## UI («Сравнение стадий → 2. Связь блоков»)

- кнопка **«🏷 Сопоставить по штампам»** в тулбаре;
- панель предложений: таблица совпадений (стр. стар→нов, имя листа, тип/уверенность),
  чекбоксы (по умолчанию все отмечены), счётчики left_only/right_only;
- **«Применить — поставить напротив»**: PUT карты. Отклонённые (снятый чекбокс)
  матчи расцепляются на два односторонних слота — их двигают вручную (⊕ / ↑ / ↓).

Перемещение листов «напротив друг друга» = уже существующая механика
`page_alignment` (слоты + `insert_blank_side` + `move_page_side`); stamp-матчинг
лишь автоматически строит эту карту по именам листов.

## Haiku-доматчинг семантически эквивалентных имён (LLM-слой)

**Дата:** 2026-06-06
**Модуль:** [backend/app/services/stage_comparison/stamp_llm_match.py](../backend/app/services/stage_comparison/stamp_llm_match.py)

Детерминированный матчер precision-biased: точное имя + IDF-косинус с
margin-гейтом. Он намеренно **не** сводит листы, у которых имена просто
«похожи по смыслу», но различаются токенами или стоят в неоднозначном наборе.
Классический промах:

```
«Однолинейная расчетная схема ГРЩ»  ==  «Однолинейная схема ГРЩ»
```

(другой набор токенов: левый имеет лишний «расчетная»; OCR штампа дробит имя на
две строки и т.п.). Такие листы остаются `left_only`/`right_only`.

LLM-слой добавляет **третий проход** в `match_sheet_indexes` поверх
детерминированного результата:

```text
Pass 1 exact  →  Pass 2 fuzzy (IDF-косинус + margin)
  →  Pass 3 [опц.] LLM-семантика по ОСТАТКУ:
       rem_left ∩ rem_right (только непарные обе стороны)
         → Haiku (Claude Code subscription, claude -p --model haiku)
         → пары «это один и тот же лист по смыслу»
         → инварианты (page ≤ 1 раза, существует, score∈[0,1]) проверяет Python
         → match_type="llm_semantic", needs_review=true
  →  остаток → left_only / right_only
```

Принципы:
* **только остаток** — детерминированные exact/fuzzy совпадения не трогаются
  (precision сохраняется); LLM не может перебить уже сведённую пару;
* **дёшево и узко** — в промпт идут ТОЛЬКО имена листов (+ номер, тип), не весь
  MD. Haiku хватает, ответ быстрый;
* **advisory** — пары приходят как обычные `suggested_items`
  (`match_type="llm_semantic"`, фиолетовый бейдж «🧠 по смыслу», галочка по
  умолчанию включена), пользователь подтверждает их перед «Применить». LLM
  ничего не применяет сам;
* **fail-soft** — нет CLI / таймаут / мусорный JSON → пустой список пар +
  диагностика; результат деградирует ровно до детерминированного;
* **инварианты в Python** — ответу модели не доверяем: каждый page используется
  не более раза, page обязан существовать, score клампится.

### Триггер и флаги

UI: чекбокс **«🧠 ИИ-доматчинг»** рядом с кнопкой «🏷 Сопоставить по штампам»
(по умолчанию ВКЛ). Фронт шлёт `POST .../suggest-by-stamp` с телом
`{use_llm: true}`. Тяжёлый subprocess-вызов выносится в threadpool, чтобы не
блокировать event loop. Результат содержит `llm_match_count`, `llm_requested` и
блок `llm` (диагностика вызова: status / pairs_added / duration / model).

| Переменная (`.env`) | Default | Назначение |
|---|---|---|
| `STAGE_COMPARISON_STAMP_LLM_ENABLED` | `true` | kill-switch LLM-слоя (false → чекбокс игнорируется, всегда детерминированный результат) |
| `STAGE_COMPARISON_STAMP_LLM_MODEL` | `haiku` | модель для доматчинга |
| `STAGE_COMPARISON_STAMP_LLM_TIMEOUT_SEC` | `90` | таймаут одного вызова |
| `STAGE_COMPARISON_STAMP_LLM_MAX_SHEETS` | `150` | cap листов/сторону в промпте |
| `STAGE_COMPARISON_STAMP_LLM_MIN_CONFIDENCE` | `0.6` | порог приёмки пары |

uvicorn без `--reload` держит модуль в памяти — после правок эвристики/промпта
нужен рестарт backend.

## Усиление матчинга — признаки + hard-gates + mutual-best (2026-06-06)

Детерминированный матчер расширен под precision (лучше непарный лист, чем
неверная пара). Always-on, публичный API совместим, ничего не применяется
автоматически.

### Признаки листа (`SheetFeatures` / `extract_sheet_features`)

Из имени листа извлекаются: `sheet_kind` (план/схема/спецификация/ведомость/
общие_данные/узел/разрез/фасад/текстовая_часть), `system_tokens`
(вру/грщ/що/авр/рп/…), `equipment_ids` (вру-1, що-2, qf-1; номинал «0,4кВ» НЕ
считается номером единицы), `floor_tokens` (этаж:1/этаж:-2/подвал/кровля/…),
`building_tokens` (корпус:1/секция:2/блок:а), `canonical_tokens`,
`text_signature`.

### Каноникализация (`canonicalize_sheet_name`, `SAFE_ALIASES`)

Второй слой нормализации снимает служебные слова (`SAFE_ALIASES`:
«однолинейная расчетная схема»→«однолинейная схема», «план расположения»→«план»,
…). Намеренно НЕ сливает разные виды схем (расчетная/принципиальная/
структурная). Новый match-type **`exact_canonical_name`** — «Однолинейная
расчетная схема ГРЩ» == «Однолинейная схема ГРЩ» без LLM.

### Hard-gates (`get_hard_conflict`)

Жёсткие запреты (применяются в canonical/fuzzy/LLM-проходах): разные единицы
одного типа (ВРУ-1≠ВРУ-2, ЩО-1≠ЩО-2), разные основные системы (ГРЩ≠ВРУ), разные
этажи, разные корпус/секция/блок, несовместимые виды (план↔спецификация/
ведомость; схема↔спецификация без общего оборудования). Отклонённые «соблазны»
пишутся в `result.rejected[]` (`{left_page,right_page,rejected_reason}`).

### Candidate-matrix + mutual-best + составной score

Fuzzy-проход строит матрицу кандидатов (без hard-конфликтов), score =
имя (IDF-косинус по каноническим токенам) + bonus/penalty за вид/систему/
оборудование/этаж/корпус (order/позиция страницы в score НЕ входит — только
tie-break). Пара принимается лишь при **взаимно-лучшем** (best-for-left И
best-for-right) + threshold + margin с обеих сторон. Диагностика в
`item.match_diag` (`score/second_best_score/margin/mutual_best`). Feature-backed
fuzzy помечается `fuzzy_structural`, чисто-именной — `fuzzy_name`.

### LLM как adjudicator (Stage 7)

Вместо «двух плоских списков» LLM получает per-left top-k **безопасных**
кандидатов (`build_candidate_match_prompt` / `llm_adjudicate_candidates`) и
выбирает ОДИН `new_page` из них или null. Инварианты в Python: выбор только
из кандидатов, нет hard-конфликта, page ≤ 1 раза, ≥ confidence-порога, LLM не
перебивает детерминированные пары. Fail-soft сохранён. Старый flat-промпт
(`build_llm_match_prompt`/`llm_match_sheets`) оставлен для backward-compat.

### Display-only поля (для UI, `alignment.validate` их отбрасывает)

`reason`, `positive_evidence[]`, `negative_evidence[]`, `risk_flags[]`
(`low_margin`/`duplicate_sheet_name`/`text_layer_fallback`/`llm_semantic`),
`confidence`, `match_diag`. UI показывает подпись типа, % и risk-бейджи.

### Env (новое; старые сохранены)

| Переменная | Default | Назначение |
|---|---|---|
| `STAGE_COMPARISON_STAMP_CANDIDATE_TOPK` | `3` | top-k безопасных кандидатов на лист (mutual-best/LLM) |
| `STAGE_COMPARISON_STAMP_LLM_CANDIDATE_MIN_SCORE` | `0.20` | нижний порог попадания в кандидаты (ниже auto-accept) |

## Многостраничные листы (multipart) + пакетное авто-сопоставление (2026-06-06)

### Multipart (Pass 1.5 в `match_sheet_indexes`)

Один логический лист может в одной версии занимать 1 страницу, а в другой —
несколько (начало/продолжение/конец). `SheetFeatures` расширен полями
`sheet_group_key` (имя БЕЗ part-маркеров), `multipart_role`
(None/start/continuation/end), `multipart_index`. `extract_multipart(norm)`
распознаёт маркеры `начало/продолжение/прод/продолж/конец/окончание/часть N/
ч N/из N` (числовые — только при наличии числа, поэтому имя «Текстовая часть»
без числа НЕ ломается). normalize уже снимает «лист N»/«стр. N»/«(из N)».

Pass 1.5 (после exact/canonical, до fuzzy/LLM) группирует ОСТАТОК по
`sheet_group_key` и сопоставляет части по ролям (`_align_multipart_parts`):
start↔start, end↔end, продолжения позиционно, лишнее → односторонние. Срабатывает
только при реальном multipart-сигнале (>1 части ИЛИ явная роль); 1↔1 без ролей
уже разобран canonical. hard-gate проверяется на каждой под-паре. Одна страница
используется не более раза.

Новые match types: `exact_multipart_group` (якорь группы), `multipart_group`
(вторичные роль-пары), `multipart_continuation` (односторонняя лишняя часть).
Диагностика результата: `multipart_match_count`, `multipart_continuation_count`.

Раскладки: 1↔N → `(1,10),(None,11),(None,12)`; N↔1 → `(1,10),(2,None),(3,None)`;
N↔M role-aware (start/end якоря, остаток односторонний). Порядок слотов:
одностороннее продолжение ставится рядом с первой сматченной строкой группы (не
обязательно в самом конце) — это валидная эквивалентная раскладка (важны пары и
отсутствие дублей). LLM НЕ создаёт multipart-группы (только deterministic
group_key).

### Пакетное авто-сопоставление по всей сессии

Кнопка **«🏷 Авто сопоставление листов»** в разделе «1. Загрузка документации»
проходит по ВСЕМ парам сессии, применяет безопасные совпадения, рискованные
оставляет на ручную проверку. Прогресс — polling job-эндпоинта.

Reusable core:
- `store.suggest_alignment_by_stamp` (тот же алгоритм, что ручной режим);
- `stamp_auto_apply.should_auto_apply_stamp_match(item) → (bool, reason)` —
  политика; `build_auto_apply_items(suggested_items)` — очищенные items (display-
  поля НЕ копируются в alignment);
- `store.apply_safe_stamp_alignment_for_pair(sid, pid, use_llm, overwrite_existing)`
  — suggest → filter → `save_alignment` (тот же путь, что ручной PUT);
  `has_manual_alignment` — guard от затирания ручной работы (`mode∈{manual,blank}`;
  авто-дефолт `mode=auto` затирать можно);
- `auto_match_jobs.py` — фоновый job (per-pair в `asyncio.to_thread`, fail-soft,
  progress, artifact `page_alignment_auto_match/last_run.json`).

Auto-apply (precision > recall): exact/canonical/`exact_multipart_group`/
`multipart_group` (без risk) — да; `fuzzy_*` — при score ≥ MIN_SCORE без
low_margin/text_layer; `llm_semantic` — при confidence ≥ LLM_MIN; text_layer /
low_margin / duplicate-без-сильных-признаков → на ручную.

| Переменная | Default | Назначение |
|---|---|---|
| `STAGE_COMPARISON_STAMP_AUTO_APPLY_MIN_SCORE` | `0.80` | порог auto-apply fuzzy |
| `STAGE_COMPARISON_STAMP_AUTO_APPLY_LLM_MIN_CONFIDENCE` | `0.85` | порог auto-apply LLM |
| `STAGE_COMPARISON_STAMP_AUTO_APPLY_TEXT_LAYER` | `false` | применять text_layer автоматически |
| `STAGE_COMPARISON_STAMP_AUTO_OVERWRITE_EXISTING` | `false` | дефолт перезаписи ручного alignment |

Endpoints (session-level — «проект» в stage_comparison = сессия):
`POST /sessions/{sid}/page-alignment/auto-match` (старт),
`GET …/auto-match/{job_id}` (прогресс), `POST …/auto-match/{job_id}/cancel`,
`GET …/auto-match-last` (последний прогон для reload).

Раздел «2. Связь блоков» остаётся ручным: кнопка **«🏷 Сопоставить листы»** +
чекбокс «🧠 ИИ-доматчинг» → suggestions → подтверждение → PUT. Ничего не
применяется автоматически.

## Безопасность

- always-on, но это отдельная кнопка — без клика поведение не меняется;
- матчер чистый, fail-soft: нет MD → `suggested_items=[]` + warning, пара не падает;
- детерминированный путь офлайн (без Qwen/сети/`crop_url`); LLM-слой включается
  только при `use_llm=true` + доступном Claude Code CLI и тоже fail-soft;
- LLM работает по именам листов (не по содержимому чертежей), ничего не
  применяет — только предлагает пары на подтверждение;
- ничего не применяется без явного «Применить» (PUT page-alignment).

## Тесты

[tests/test_stage_comparison_stamp_matching.py](../tests/test_stage_comparison_stamp_matching.py)
— нормализация, forward-fill, text_layer фолбэк, distinctive-имя через большой
сдвиг (золотой кейс), in-order дубликаты, margin-подавление неоднозначных,
валидность items для `alignment.validate`, обвязка `store.suggest_alignment_by_stamp`.

[tests/test_stage_comparison_stamp_llm_match.py](../tests/test_stage_comparison_stamp_llm_match.py)
— LLM-слой: build/parse промпта, дедуп page и фильтр confidence, мок-provider
(done/error/empty side), инъекция пары в `match_sheet_indexes` с `llm_semantic`,
запрет перебить детерминированный матч, fail-soft на исключении fn, обвязка
`store.suggest_alignment_by_stamp(use_llm=True)` (provider доступен / недоступен).

[tests/test_stage_comparison_stamp_matching_v2.py](../tests/test_stage_comparison_stamp_matching_v2.py)
— усиление: canonicalize + анти-слияние видов схем, извлечение признаков
(kind/system/equipment/floor/building, пропуск напряжения), hard-gates
(ВРУ-1≠ВРУ-2, ГРЩ≠ВРУ, этаж/корпус/секция, план↔спецификация), `exact_canonical_name`
без LLM, mutual-best (слабый левый не уводит правый), text_layer risk-флаг и
строгий порог, LLM-adjudicator (только из кандидатов, hard-gate блокирует
форсированную пару, membership-фильтр, fail-soft).

## Контролируемая проверка (ИОС1.1, реальная пара)

Session `ba413a93c5754f6c` / pair `pf06effb7` (60 vs 48 страниц):
matched=19, confidence 0.995, без скремблинга. Золотой кейс —
«Однолинейная расчетная схема ВРУ.ИТП» найдена **стр.51 (стар) ↔ стр.32 (нов)**
(сдвиг 19 страниц), плюс 17 точных совпадений текстовой части. Неоднозначные
графические листы «Часть 1. …» оставлены непарными для ручного матча.

## Связанные файлы

- [stamp_matching.py](../backend/app/services/stage_comparison/stamp_matching.py) — детерминированный матчер + опц. `llm_match_fn`
- [stamp_llm_match.py](../backend/app/services/stage_comparison/stamp_llm_match.py) — Haiku-доматчинг остатка
- [store.py](../backend/app/services/stage_comparison/store.py) — `suggest_alignment_by_stamp(use_llm=…)`, `_page_text_index_from_result_json`
- [text_llm_provider.py](../backend/app/services/stage_comparison/text_llm_provider.py) — `ClaudeCodeProvider` (`claude -p`)
- [alignment.py](../backend/app/services/stage_comparison/alignment.py) — карта слотов (`page_alignment`)
- [evidence_first_fallback.py](../backend/app/services/stage_comparison/evidence_first_fallback.py) — `build_fact_index` (переиспользуется)
