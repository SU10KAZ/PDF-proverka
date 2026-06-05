# Stage Comparison — сопоставление листов по штампу (page-alignment)

**Дата:** 2026-06-05 (LLM-доматчинг — 2026-06-06)
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
