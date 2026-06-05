# Stage Comparison — сопоставление листов по штампу (page-alignment)

**Дата:** 2026-06-05
**Статус:** always-on (новый необязательный инструмент в «Связь блоков», ничего не ломает)
**Модули:**
- [backend/app/services/stage_comparison/stamp_matching.py](../backend/app/services/stage_comparison/stamp_matching.py) — чистый матчер
- [backend/app/services/stage_comparison/store.py](../backend/app/services/stage_comparison/store.py) — `suggest_alignment_by_stamp`
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

## Безопасность

- always-on, но это отдельная кнопка — без клика поведение не меняется;
- матчер чистый, fail-soft: нет MD → `suggested_items=[]` + warning, пара не падает;
- офлайн: ни Qwen, ни сети, ни `crop_url` — только чтение MD/result.json с диска;
- ничего не применяется без явного «Применить» (PUT page-alignment).

## Тесты

[tests/test_stage_comparison_stamp_matching.py](../tests/test_stage_comparison_stamp_matching.py)
— нормализация, forward-fill, text_layer фолбэк, distinctive-имя через большой
сдвиг (золотой кейс), in-order дубликаты, margin-подавление неоднозначных,
валидность items для `alignment.validate`, обвязка `store.suggest_alignment_by_stamp`.

## Контролируемая проверка (ИОС1.1, реальная пара)

Session `ba413a93c5754f6c` / pair `pf06effb7` (60 vs 48 страниц):
matched=19, confidence 0.995, без скремблинга. Золотой кейс —
«Однолинейная расчетная схема ВРУ.ИТП» найдена **стр.51 (стар) ↔ стр.32 (нов)**
(сдвиг 19 страниц), плюс 17 точных совпадений текстовой части. Неоднозначные
графические листы «Часть 1. …» оставлены непарными для ручного матча.

## Связанные файлы

- [stamp_matching.py](../backend/app/services/stage_comparison/stamp_matching.py)
- [store.py](../backend/app/services/stage_comparison/store.py) — `suggest_alignment_by_stamp`, `_page_text_index_from_result_json`
- [alignment.py](../backend/app/services/stage_comparison/alignment.py) — карта слотов (`page_alignment`)
- [evidence_first_fallback.py](../backend/app/services/stage_comparison/evidence_first_fallback.py) — `build_fact_index` (переиспользуется)
