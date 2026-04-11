# Таксономия AI-аудита ПД/РД v4 (финальная)

**Источники:**
- Структура классов: нормативная таксономия от консультанта (11 классов сверху вниз от ПД/РД)
- Правила фильтрации: эксперт-аудитор (границы scope, контекстные правила)
- Маппинг KEEPs: эталон проекта `133_23-ГК-ГРЩ` (5 definite KEEPs)
- Дополнительные классы: опыт из 33 Opus findings

**Область применения:** автоматический AI-аудит с приоритетом на recall
**Тестовый раздел:** ГРЩ (Главный Распределительный Щит) жилого МКД

---

## Scope для раздела ГРЩ

### В scope
- Кабельные линии между ГРЩ и ВРУ (сечения, марки, количество жил N/PE)
- Автоматы вводных и отходящих линий ГРЩ (марки, номиналы, расцепители)
- Спецификации оборудования ГРЩ, ЩСН, ЩУ
- Трансформаторы тока (класс точности, коэффициенты)
- Примечания к листам ГРЩ (требования к оборудованию, монтажу)
- Категория надёжности электроснабжения (из общих данных)
- Компенсация реактивной мощности (УКРМ)
- Расчётные таблицы нагрузок (арифметика)
- Однолинейная схема ГРЩ
- Узлы раскладки кабелей (план и разрезы)
- Планы расположения оборудования ГРЩ

### Вне scope (автоматически исключается)
- Заземление и молниезащита → отдельный раздел ЭГ
- УЗДП квартирных щитов → раздел ЭМ
- Селективность УЗО в группах → раздел ЭМ
- Соответствие ТП-ГРЩ → внешний раздел электроснабжения
- Рабочая документация внутреннего электроснабжения квартир

---

## 11 классов ошибок (нормативная структура)

### Класс 1 — Комплектность и обязательность
**Priority: P4** (низкий для первой версии)

**Суть:** отсутствие обязательного элемента документации.

**Подтипы:**
- `missing_referenced_sheet` — есть ссылка "см. лист N", листа нет
- `missing_specification_item` — оборудование на схеме, нет в спецификации
- `missing_calculation_input` — отсутствуют исходные данные для заявленного расчёта
- `missing_note_target` — примечание "см. узел X", узла нет

**Детектирование:** Python анализ `references` между листами/таблицами.

---

### Класс 2 — Идентичность и адресация сущностей
**Priority: P1** (критический)

**Суть:** требование уникальности или однозначности идентификации сущности нарушено.

**Подтипы:**
- `duplicate_marker` — один маркер (М-1.5, QF1.1) использован для разных объектов
- `swapped_descriptions` — позиция A описана как позиция B и наоборот (**#2**)
- `ambiguous_reference` — ссылка может относиться к нескольким объектам
- `same_designation_different_objects` — одно обозначение для разного оборудования

**Покрывает KEEPs:**
- `#2` ЩУ-2/Т и ЩУ-12/Т перепутаны в спецификации → `swapped_descriptions`
- `#5` Дубль маркера М-1.5 на схеме РП1 → `duplicate_marker`

**Детектирование:**
```python
# duplicate_marker
for marker, mentions in group_by_marker_on_same_sheet(symbol_table):
    destinations = {m.destination for m in mentions}
    if len(destinations) > 1:
        yield candidate(
            class_=2, subtype='duplicate_marker',
            entity=marker, destinations=destinations
        )

# swapped_descriptions — требует semantic check через LLM
# signal: description содержит упоминание количества (12 счётчиков)
# которое не соответствует идентификатору (ЩУ-2/Т → 2 счётчика)
```

---

### Класс 3 — Межлистовая консистентность одной сущности
**Priority: P1** (критический)

**Суть:** один и тот же объект имеет разные атрибуты в разных представлениях (схема ↔ спецификация ↔ узел ↔ план ↔ пояснительная записка).

**Подтипы:**
- `name_position_mismatch` — позиция/имя отличается
- `parameter_mismatch` — параметр отличается
- `destination_mismatch` — источник/приёмник отличается
- `cable_composition_mismatch` — состав кабеля (марка, жилы, сечения) отличается

**Покрывает KEEPs:**
- `#7` Нейтраль 150 vs 120 мм² → `cable_composition_mismatch (neutral_section)`
- `#18` ВРУ-1 1 PE vs 2 PE → `cable_composition_mismatch (pe_count)`

**Детектирование:**
```python
# cable_composition_mismatch
for line_id, cable in canonical_entities['cables'].items():
    for field in ['mark', 'phase_section', 'pe_count',
                  'pe_section', 'neutral_section']:
        values = cable.get_attribute_values(field)
        if len(set(v.value for v in values)) > 1:
            yield candidate(
                class_=3, subtype='cable_composition_mismatch',
                entity=line_id, field=field, values=values
            )
```

**Критическое правило:** сопоставление кабелей **только по точному line_id**
(`M-1.1 == M-1.1`). Никакого fuzzy matching по числам — это источник FP
(например #20 в эталоне: модель сопоставила М-1.5 с другим кабелем по числу 240,
получила ложное расхождение).

---

### Класс 4 — Совместимость "примечание / требование / выбранное решение"
**Priority: P1** (критический)

**Суть:** примечание декларирует требование, фактическое оборудование/решение ему не соответствует. Семантическая совместимость, не идентичность.

**Подтипы:**
- `note_vs_equipment_type` — тип оборудования не соответствует требованию (**#55**)
- `note_vs_equipment_class` — класс точности/исполнения не соответствует
- `general_vs_specific_conflict` — общий принцип не совпадает с частным решением
- `table_constraint_vs_choice` — таблица допущений противоречит выбору

**Покрывает KEEPs:**
- `#55` ВА-335А (термомагнитный) vs "электронные расцепители" в note → `note_vs_equipment_type`

**Детектирование:** требует LLM-вспомогательного парсинга правил из текста примечаний.

```python
# note_vs_equipment_type
for note in canonical_entities['notes']:
    rule = llm_parse_note_to_rule(note.text)
    # rule = {requirement: "electronic_trip", scope: "outgoing_breakers_grsch"}
    if rule is None or rule.requirement is None:
        continue
    scope_entities = resolve_scope(rule.scope, canonical_entities)
    for entity in scope_entities:
        if not entity_matches_rule(entity, rule):
            yield candidate(
                class_=4, subtype='note_vs_equipment_type',
                rule_source=note, violating_entity=entity,
                requirement=rule.requirement
            )
```

---

### Класс 5 — Топологическая и функциональная логика
**Priority: P2** (высокий, после MVP)

**Суть:** ошибки в структуре и связях системы.

**Подтипы:**
- `invalid_route` — невозможный/нелогичный маршрут
- `broken_connectivity` — нарушенная связность
- `wrong_source_reserve` — неверный источник/резерв/АВР
- `missing_required_link` — отсутствие обязательного звена
- `protection_branch_mismatch` — несогласованность защиты и питаемой ветви

**Детектирование:** граф проекта + правила связности.

**В эталоне:** 0 KEEP, но 2 unknown (#50 источник ЩСН, #44 ссылка на внешний проект).

---

### Класс 6 — Количественная и параметрическая адекватность
**Priority: P1** (критический для параметров, без расчётов)

**Суть:** сущность идентифицирована правильно, но параметры противоречат друг другу или нарушают нормативный минимум.

**Подтипы:**
- `section_vs_current_mismatch` — сечение не соответствует току
- `pe_minimum_ratio_violation` — PE < S/2 для S>35мм² (ПУЭ табл.1.7.5)
- `transformer_overload_marginal` — загрузка на пределе
- `breaker_ics_insufficient` — отключающая способность меньше Iкз
- `demand_factor_suspicious` — Kс нереалистично низкий для мощной нагрузки

**Детектирование:** Python rules поверх извлечённых параметров.

**Разница с классом 7:** здесь сравнение без сложного расчёта (PE = 185/2 = 92.5, фактически 185 — ок). Если требуется формула с несколькими входами — это класс 7.

**В эталоне:** несколько unknown (#56, #57, #67, #68).

---

### Класс 7 — Расчётная достаточность и инженерные проверки
**Priority: P2** (после MVP)

**Суть:** численные проверки по формулам, требующие нескольких входных значений.

**Подтипы:**
- `voltage_drop_exceeded` — ΔU > 5% от ТП до потребителя
- `short_circuit_trip_time` — t_срабатывания > 0.4с при Iкз(1)
- `selectivity_violation` — нарушена селективность ступеней
- `thermal_stability_violation` — сечение не выдерживает термическую стойкость
- `ct_saturation` — насыщение ТТ при рабочем токе

**Детектирование:** **ДЕТЕРМИНИРОВАННЫЙ Python**, LLM только извлекает числа.

```python
def check_voltage_drop(path):
    total_delta_u = 0
    for segment in path.segments:
        I = segment.current_a
        L = segment.length_m
        s = segment.section_mm2
        material = segment.material  # Cu/Al
        total_delta_u += compute_delta_u(I, L, s, material)
    if total_delta_u > 5.0:
        yield candidate(
            class_=7, subtype='voltage_drop_exceeded',
            total=total_delta_u, limit=5.0, path=path
        )
```

**В эталоне:** большой пул unknown (#35 потери 7.5%, #41 cosφ, #40 Ics).

---

### Класс 8 — Междисциплинарная координация
**Priority: P3** (после production)

**Суть:** ошибки на стыке разделов.

**Подтипы:**
- `eom_ar_kr_conflict` — ЭОМ ↔ АР/КР
- `eom_ov_vk_conflict` — ЭОМ ↔ ОВ/ВК
- `space_routing_conflict` — архитектурные габариты ↔ инженерные трассы
- `notes_graphics_mismatch` — пояснительная записка ↔ графика

**Для ГРЩ:** не основной класс, но важный для проверки что питание ЩНО (#44) соответствует смежному проекту.

---

### Класс 9 — Версионность, ревизии, актуальность
**Priority: P3**

**Суть:** проект "разъехался" по версиям.

**Подтипы:**
- `outdated_revision_reference` — ссылка на старую редакцию
- `sheet_updated_table_not` — лист обновлён, зависимая таблица нет
- `specification_wrong_revision` — спецификация от другой ревизии

**Не путать с устаревшими ГОСТ/СП!** Это другой класс — см. Класс 11.

---

### Класс 10 — Реализуемость и эксплуатационная пригодность
**Priority: P3**

**Суть:** проект формально непротиворечив, но плохой к исполнению.

**Подтипы:**
- `no_service_access` — нет места для обслуживания
- `ambiguous_mounting` — "монтаж определить по месту" для критичных узлов
- `missing_operational_logic` — логика эксплуатации не раскрыта

**В эталоне:** #22, #24, #54.

---

### Класс 11 — Оформление, читаемость, нормативные ссылки
**Priority: P4** (низший, много шума)

**Суть:** самый шумный класс. Максимум severity = РЕКОМЕНДАТЕЛЬНОЕ.

**Подтипы:**
- `outdated_norm_reference` — устаревший ГОСТ/СП (не материальная ошибка)
- `typo_in_identifier` — опечатка в шифре, адресе, позиции
- `ocr_artifact` — латиница/кириллица, потеря цифр (**отключаем как finding**)
- `copypaste_from_other_section` — текст скопирован не из своего раздела
- `illegible_drawing` — нечитаемые надписи

**Правило severity:** этот класс **никогда** не КРИТИЧЕСКОЕ. Максимум РЕКОМЕНДАТЕЛЬНОЕ.

**В эталоне:** 11+ записей, все REMOVE.

---

## Priority разбивка для AI-аудита

| Priority | Классы | Rationale |
|---|---|---|
| **P1** | 2, 3, 4, 6 | Покрывают все 5 definite KEEPs, максимум value/effort |
| **P2** | 5, 7 | Расширение после MVP, инженерная глубина |
| **P3** | 8, 9, 10 | После production, для enterprise-режима |
| **P4** | 1, 11 | Низкий recall, высокий шум. В ручном режиме или отключены |

### P1 покрытие KEEPs

| KEEP | Класс | Подтип | Генератор |
|---|---|---|---|
| #2 ЩУ-2/Т | 2 | swapped_descriptions | `gen_identity_swap` |
| #5 дубль М-1.5 | 2 | duplicate_marker | `gen_duplicate_marker` |
| #7 нейтраль 150/120 | 3 | cable_composition_mismatch (neutral_section) | `gen_cable_mismatch` |
| #18 ВРУ-1 1/2 PE | 3 | cable_composition_mismatch (pe_count) | `gen_cable_mismatch` |
| #55 ВА-335А | 4 | note_vs_equipment_type | `gen_note_equipment_conflict` |

**Итого:** 4 генератора покрывают 100% ground truth.

---

## Правила фильтрации (обязательные в judge stage)

Эти правила применяются **ко всем кандидатам** независимо от класса. Они
основаны на экспертном знании и эмпирическом анализе эталона.

### Rule F1 — Scope boundary check
Перед созданием finding:
```python
if candidate.entity.section not in SCOPE_OF_CURRENT_DISCIPLINE:
    candidate.verdict = "out_of_scope"
    return
```
Для ГРЩ исключаются сущности раздела ЭГ, ЭМ (квартирные щиты), молниезащита.

### Rule F2 — OCR artifact detection
Если расхождение — это одно из:
- Латиница vs кириллица в одинаковой марке (НФ vs HF)
- Одна цифра в большом числе (1027 vs 1028, 648 vs 448)
- Частичная потеря символа в шифре (83 vs 133)

→ `likely_ocr_artifact = true`, **не поднимать** в финальные findings.

### Rule F3 — Cable identification strictness
Для класса 3 (cable_composition_mismatch): сопоставлять кабели **только по точному line_id**.
Никакого fuzzy matching по числам сечения — это источник ложных срабатываний.

```python
def find_same_cable(line_id_a, line_id_b):
    return line_id_a.strip().upper() == line_id_b.strip().upper()
    # Не: fuzzy_match(a.section, b.section) > 0.8
```

### Rule F4 — Outdated norms severity
Устаревший ГОСТ/СП → **максимум severity = РЕКОМЕНДАТЕЛЬНОЕ**.
Обязательный комментарий: "Замена без материальной нагрузки на проект".

Никогда не `КРИТИЧЕСКОЕ`, даже если норма в обязательном перечне.

### Rule F5 — Phase imbalance context
Не генерировать finding "перекос фаз" если:
- Общая нагрузка секции < 30 кВт
- Есть один однофазный потребитель мощнее остальных в 5+ раз
- Все остальные потребители маломощные (< 2 кВт)

**Основание:** эксперт подтвердил что это **физическое следствие** выбранной
схемы потребителей, а не ошибка проекта.

### Rule F6 — Different entities check
Для классов 2 и 3: перед объявлением mismatch убедиться что сравниваются
**одни и те же сущности**, а не просто совпавшие по какому-то признаку.

Если evidence A говорит про М-1.4, а evidence B про М-1.5 — это разные кабели,
не конфликт.

---

## Архитектурные слои (от консультанта, v4)

```
PDF
 ↓
[Layer A] Raw evidence       — страницы, блоки, OCR, bbox, images
 ↓
[Layer B] Typed extraction    — LLM: только факты, НЕ findings
 ↓
[Layer C] Canonical memory    — граф проекта, normalized entities
 ↓
[Layer D] Candidate generators — Python: детерминированные правила
 ↓
[Layer E] Candidate bundles   — маленькие объекты для проверки
 ↓
[Layer F] Judge stage         — LLM на один кандидат + фильтры
 ↓
[Layer G] Deterministic math  — Python расчёты вне LLM
 ↓
[Layer H] Selective escalation — Opus/Sonnet только на спорные bundles
 ↓
Final findings
```

### Разделение ответственности

| Layer | Что делает | Что НЕ делает | Модель |
|---|---|---|---|
| B | Извлекает факты с блока | Не ищет ошибки, не merge | Gemini/GPT (extractor) |
| C | Строит граф сущностей | — | Python |
| D | Генерирует кандидатов | Не решает истина/ложь | Python |
| F | Verifies один candidate | Не ищет новые ошибки | Gemini/GPT (judge) |
| G | Считает формулы | Не интерпретирует | Python |
| H | Verifies uncertain/hard | Только escalation | Sonnet 4.6 |

**Ключевое отличие от текущей архитектуры:** дешёвая модель больше не является
«глобальным аудитором». Она — **extractor** (максимум recall на фактах) и
**judge** (одна бинарная задача).

---

## Новый формат эталона

**Текущий формат:** текстовое описание + verdict.
**Проблема:** при перефразировании finding — fuzzy score падает ниже threshold, ложный FN.

**Новый формат:**
```json
{
  "etalon_id": 2,
  "class": 2,
  "subtype": "swapped_descriptions",
  "entity_key": {
    "type": "panel_pair",
    "ids": ["ЩУ-2/Т", "ЩУ-12/Т"]
  },
  "field_under_check": "description_vs_id",
  "expected_conflict": "ЩУ-2/Т описан как 12-счётчиковый, ЩУ-12/Т как 2-счётчиковый",
  "evidence_pages": [15],
  "severity": "РЕКОМЕНДАТЕЛЬНОЕ",
  "status": "keep",
  "notes_for_ai": "Классический mixup; description contains count mismatch"
}
```

**Преимущество:** матчинг по `(class, subtype, entity_key, field)` — устойчив к
любой формулировке finding. Оценка = «поймал ли генератор этот класс на этой
сущности с этим полем».

---

## План реализации MVP (из 4 генераторов)

### Шаг 1 — Extractor промт (1 час)
Новый промт `block_extraction_task.md` для **Gemini**. Задача: заполнить JSON schema.
Не искать ошибки. Не думать про cross-block.

Вход: PNG блока + page context.
Выход: `entities.{cables, breakers, panels, notes}` с evidence и uncertainty.

### Шаг 2 — Canonical memory builder (1 час)
`build_canonical_memory.py` — Python. Читает все `block_extraction_*.json` +
`01_text_analysis.json`. Строит:
- `canonical_entities[cables]` — dict by `line_id`
- `canonical_entities[panels]` — dict by normalized panel id
- `canonical_entities[breakers]` — dict by designation
- `canonical_entities[notes]` — list

Каждая canonical entity имеет `all_mentions`, `resolved_attributes`,
`conflicting_attributes`.

### Шаг 3 — 4 candidate generators (1 час)
`candidate_generators.py`:
```python
def gen_duplicate_marker(memory) → [candidates]    # покрывает #5
def gen_identity_swap(memory)     → [candidates]    # покрывает #2
def gen_cable_mismatch(memory)    → [candidates]    # покрывает #7, #18
def gen_note_equipment_conflict(memory, llm) → [candidates]  # покрывает #55
```

### Шаг 4 — Judge stage (30 минут)
Один LLM вызов на candidate. Переиспользовать инфраструктуру findings_critic.
Prompt — минимальный, по candidate bundle.

### Шаг 5 — Новый эталон и evaluator (1 час)
- Пересобрать `etalon.xlsx` в новый формат (74 записи → structured)
- Обновить `evaluate_against_etalon.py` для матчинга по `(class, subtype, entity_key, field)`

### Шаг 6 — Прогон и оценка (30 минут)
Новый pipeline на ГРЩ. Расчёт recall/precision по каждому классу отдельно.

### Ожидаемый результат
- **Recall класс 2:** 100% (#2, #5) — простая Python проверка
- **Recall класс 3:** 100% (#7, #18) — если extractor правильно заполнит `pe_count` и `neutral_section`
- **Recall класс 4:** 100% (#55) — если LLM note_parser правильно извлечёт требование
- **Итого recall MVP:** 5/5 = **100%** на ГРЩ

---

## Открытые вопросы

### Q1 — typed_facts schema
Ждём от консультанта точную схему для Layer B. Пока могу делать MVP-версию
(минимальный schema только под 4 генератора).

### Q2 — candidate bundle schema
То же — ждём от консультанта или делаем MVP.

### Q3 — note parsing для класса 4
Самый сложный подкласс. Нужен отдельный LLM helper который из текста примечания
делает структурированное правило `{requirement, scope}`. Это тонкое место,
может потребовать few-shot примеры.

### Q4 — Новый эталон
74 записи нужно пересобрать. Это твоя задача (эксперт) — я не могу правильно
определить `entity_key` для unknown записей без просмотра проекта.

---

## Что дальше

**Две параллельные ветки:**

1. **Ты отправляешь 33 findings консультанту** → получаешь typed_facts schema,
   candidate bundle schema, judge output schema, pseudocode генераторов.
   Время: ~30 минут на ожидание ответа.

2. **Я начинаю MVP прототип** по Шагам 1-3 на этой v4 таксономии с
   минимальной собственной schema. Когда консультант пришлёт свою — сверим,
   возьмём лучшее. Время: ~3-4 часа.

**Альтернатива — последовательно:** подождать консультанта, потом делать прототип
с его schema. Надёжнее, но медленнее.

Рекомендую **параллельно**. Schema консультанта будет лучше моей для production,
но моя работающая за 4 часа — лучше его идеальной за 2 дня.
