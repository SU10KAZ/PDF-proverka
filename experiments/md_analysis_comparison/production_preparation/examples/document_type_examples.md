# Document Type Detection Examples

**Дата:** 2026-05-20

6 примеров MD-snippet'ов → detected document_type → ожидаемое поведение
Phase 1. Используется при unit-тестировании `document_type_detector.py`.

Reference: [a1_v2_final_recommendation.md "What is proven"](../../algorithm_research/reports/a1_v2_final_recommendation.md).

---

## 1. `full_rd` — типичный полный РД

**MD snippet (первые ~20 строк):**

```markdown
# Раздел ЭОМ — Электроснабжение
## Пояснительная записка

1. Исходные данные. На основании задания заказчика и архитектурных решений
   разработан раздел ЭОМ для объекта "Жилой дом, 17 этажей".

2. Состав раздела. В состав РД входят:
   - Пояснительная записка.
   - Расчёт электрических нагрузок.
   - Однолинейная схема ВРУ-1.
   - Планы прокладки кабельных трасс по этажам.
   - Спецификация оборудования.
   - Кабельный журнал.

3. Категория надёжности электроснабжения. I категория согласно ПУЭ-7.
```

**Detected:** `full_rd` (confidence ~0.95).

**Сигналы:**
- "В состав РД входят" — типичная full_rd фраза.
- Перечисление всех обязательных разделов.
- Структура "Пояснительная записка / Раздел N / ...".

**Phase 1 поведение:**
- Lens **не запускается** (default `STAGE01_COMPLETENESS_BY_DOC_TYPE` не
  включает full_rd).
- Pipeline сводится к current_method only с новым prompt'ом.

---

## 2. `audit_comparison` — сравнение разделов

**MD snippet:**

```markdown
# Аудит электрических нагрузок ВРУ-1 (ЭОМ vs ОВ)

| № | Источник | Расход / Мощность | По ЭОМ | По ОВ |
|---|---|---|---|---|
| 1 | Приточная установка П1 | 4,0 кВт | 4,0 | 4,0 |
| 2 | Вытяжная установка В1 | 4,5 кВт | 4,5 | 4,5 |
| 3 | Тепловая завеса ТЗ-1 | 4,0 кВт | — | 4,0 |
| 4 | Тепловая завеса ТЗ-2 | 4,0 кВт | — | 4,0 |
| ... |
| ИТОГО ОВ нагрузка | | 12,0 кВт | 18,5 кВт |

## Выводы
- В ЭОМ не учтены тепловые завесы.
- Несоответствие итоговой суммы.
```

**Detected:** `audit_comparison` (confidence ~0.95).

**Сигналы:**
- Таблица с колонками "По X / По Y".
- Frame: "vs", "аудит сравнения", "ЭОМ vs ОВ".
- Короткий объём (< 100 строк), фокус на сопоставлении.

**Phase 1 поведение:**
- Lens **запускается** (audit_comparison в default route).
- `is_beyond_gt_useful` findings приветствуются.
- KILL-LIST блокирует "отсутствует пояснительная записка" — wrong scope.

---

## 3. `tz_vs_rd` — сравнение ТЗ vs РД

**MD snippet:**

```markdown
# Соответствие РД техническому заданию

| ТЗ | Требование | Реализация в РД | Соответствие |
|---|---|---|---|
| п.4.2.1 | Двухтрубная система отопления | Однотрубная | НЕТ |
| п.4.2.3 | Поквартирный учёт тепла | Не предусмотрен | НЕТ |
| п.5.1.1 | Резерв насосов 2N+1 | 1+1 | НЕТ |
| п.5.2.1 | Производитель: Wilo/Grundfos | IMP Pumps NMT Smart | Подтверждение эквивалентности отсутствует |
```

**Detected:** `tz_vs_rd` (confidence ~0.9).

**Сигналы:**
- "Соответствие РД техническому заданию" frame.
- Таблица с колонками "ТЗ / Реализация / Соответствие".
- Явные ссылки на пункты ТЗ (п.4.2.1, п.4.2.3...).

**Phase 1 поведение:**
- Lens **НЕ запускается** в default config (`tz_vs_rd` исключён до remediation).
- Можно опционально включить через
  `STAGE01_COMPLETENESS_BY_DOC_TYPE="audit_comparison,tz_vs_rd"`.
- Если запущена — findings только по позициям, упомянутым в ТЗ.

---

## 4. `specification_only` — спецификация

**MD snippet:**

```markdown
# Спецификация заполнений оконных и дверных проёмов

| Поз. | Наименование | Размер, мм | Кол-во | Заполнение |
|---|---|---|---|---|
| Б1 | Балкон тип А | 1200×2400 | 18 | СПД 4М1-12Ar-4М1 |
| Б2 | Лоджия тип Б | 1500×2400 | 56 | Однокамерный (без формулы) |
| Б3 | Холодное остекление | 900×2200 | 12 | Одинарное |
| Б4 | Балкон угловой | 2400×2400 | 4 | Не указано |
| Б5 | Двухкамерный пакет | 1200×2400 | 8 | 4-16-4 (формула однокамерного!) |
```

**Detected:** `specification_only` (confidence ~0.85).

**Сигналы:**
- Заголовок "Спецификация" / "Ведомость".
- > 80% содержимого — таблицы с параметрами.
- Нет блоков "Пояснительная записка", "Расчёт".
- Короткий объём focused на одном артефакте.

**Phase 1 поведение:**
- Lens **НЕ запускается** в default (до accumulation 3+ кейсов).
- Если запущена — KILL-LIST блокирует "отсутствует кабельный журнал", "нет
  однолинейной схемы" — это не предмет спецификации.

---

## 5. Manual override через project_info

**case.json:**
```json
{
  "project_id": "EOM/...",
  "section": "EOM",
  "document_type": "audit_comparison"   // EXPLICIT
}
```

**Detected:** `audit_comparison` (confidence = 1.0, source = "manual").

Detector skip эвристику и доверяет engineer'у.

**Когда полезно:**
- Edge cases где эвристика ошибается.
- Engineer заранее знает scope документа.
- A/B testing разных routes для same project_id.

---

## 6. Ambiguous case → fallback to full_rd

**MD snippet:**

```markdown
# Раздел ОВ

Информация по системе вентиляции жилого дома, 17 этажей.

Кратко: основные параметры приточно-вытяжной вентиляции, расчёт нагрузок,
оборудование.

(далее ~50 строк свободного текста без явной структуры).
```

**Detected:** `full_rd` (confidence ~0.55, ниже порога
`STAGE01_DOCUMENT_TYPE_CONFIDENCE_MIN=0.6` → fallback).

**Сигналы:**
- Нет явных маркеров audit_comparison / tz_vs_rd / specification.
- Свободный текст преобладает.
- Длина средняя — может быть incomplete РД или summary.

**Phase 1 поведение:**
- `full_rd` route → lens **не запускается** (default).
- Engineer может через project_info override установить более точный тип.

---

## Detector эвристика (псевдокод)

```python
def detect_document_type(md_path, project_info):
    # 1. Manual override
    if project_info.get("document_type"):
        return project_info["document_type"], 1.0

    md = read_text(md_path)

    # 2. Эвристики (порядок важен — более specific first)
    if _is_tz_vs_rd(md):     # ищет "ТЗ" + табл. сопоставлений + пункты ТЗ
        return "tz_vs_rd", _confidence_tz_vs_rd(md)

    if _is_audit_comparison(md):  # ищет "vs", "сравнение", табл. с двумя источниками
        return "audit_comparison", _confidence_audit(md)

    if _is_specification_only(md):  # > 80% таблиц, нет ПЗ блока
        return "specification_only", _confidence_spec(md)

    if _is_full_rd(md):       # явные ссылки на состав РД
        return "full_rd", _confidence_full_rd(md)

    # 3. Fallback
    return "full_rd", 0.5  # ниже порога → consumer должен сам решить
```

---

## Testing matrix

| Test ID | Описание | Expected doc_type | Expected confidence |
|---|---|---|---|
| TT-1 | Full РД snippet | full_rd | ≥ 0.85 |
| TT-2 | Audit comparison table | audit_comparison | ≥ 0.85 |
| TT-3 | TZ vs RD table | tz_vs_rd | ≥ 0.85 |
| TT-4 | Pure spec table | specification_only | ≥ 0.80 |
| TT-5 | Manual override = audit_comparison | audit_comparison | 1.0 |
| TT-6 | Ambiguous (no markers) | full_rd (fallback) | < 0.60 |

См. `tests/findings/test_document_type_detector.py` (Phase 1).
